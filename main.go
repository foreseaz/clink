package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/Shopify/sarama"

	"github.com/auxten/clink/api"
	"github.com/auxten/clink/core"
	"github.com/auxten/clink/kafka"
	"github.com/auxten/clink/ngnx"
)

// Sarama configuration options
var (
	brokers    string
	version    string
	group      string
	topics     string
	assignor   string
	schemaFile string
	dataFile   string
	apiAddr    string
	apiPort    int
	logger     string
	oldest     = true
	verbose    = false
)

func init() {
	flag.StringVar(&apiAddr, "addr", "0.0.0.0", "HTTP API endpoint address")
	flag.StringVar(&logger, "logger", "stderr", "Logger to use, stdout/stderr/file")
	flag.IntVar(&apiPort, "port", 8081, "HTTP API endpoint port")
	flag.StringVar(&schemaFile, "schema", "", "Schema config file")
	flag.StringVar(&dataFile, "data", "", "Data file json message per line")
	flag.StringVar(&brokers, "brokers", "", "Kafka bootstrap brokers to connect to, as a comma separated list")
	flag.StringVar(&group, "group", "", "Kafka consumer group definition")
	flag.StringVar(&version, "version", "2.1.1", "Kafka cluster version")
	flag.StringVar(&topics, "topics", "", "Kafka topics to be consumed, as a comma separated list")
	flag.StringVar(&assignor, "assignor", "range", "Consumer group partition assignment strategy (range, roundrobin, sticky)")
	flag.BoolVar(&oldest, "oldest", true, "Kafka consumer consume initial offset from oldest")
	flag.BoolVar(&verbose, "verbose", false, "Sarama logging")
	flag.Parse()

	if len(schemaFile) == 0 {
		log.Fatal("need to specify schema of clink job")
	}

	if len(dataFile) == 0 {
		/*
			Kafka data source
		*/
		if len(brokers) == 0 {
			log.Fatal("no Kafka bootstrap brokers defined, please set the -brokers flag")
		}

		if len(topics) == 0 {
			log.Fatal("no topics given to be consumed, please set the -topics flag")
		}

		if len(group) == 0 {
			log.Fatal("no Kafka consumer group defined, please set the -group flag")
		}
	}
}

func main() {
	log.Println("Starting clink job")

	if verbose {
		log.SetLevel(log.DebugLevel)
	}

	var (
		schm *core.Schema
		err  error
	)

	if schm, err = core.LoadConf(schemaFile); err != nil {
		log.WithError(err).Errorf("load schema conf %s", schemaFile)
		return
	}

	eng := ngnx.NewEngine(schm.Engine, schemaFile, schm)
	if err = eng.InitTables(); err != nil {
		log.Errorf("init table failed: %v", err)
		return
	}

	serv := &api.Server{
		Port:    apiPort,
		Address: apiAddr,
		Log:     logger,
		Engine:  eng,
	}
	// continue here!
	httpServ := api.StartServer(serv)

	if log.GetLevel() > log.WarnLevel {
		if schemaStr, err := eng.ShowSchema(); err != nil {
			log.WithError(err).Error("getting schema")
			return
		} else {
			log.Debugln(schemaStr)
		}
		if indexStr, err := eng.ShowIndex(); err != nil {
			log.WithError(err).Error("getting index")
			return
		} else {
			log.Debugln(indexStr)
		}
	}

	if len(dataFile) > 0 {
		var (
			f             *os.File
			table         string
			counter       int64
			generalResult [][]interface{}
		)
		if f, err = os.Open(dataFile); err != nil {
			log.WithError(err).Errorf("open %s", dataFile)
		}
		if len(schm.Tables) == 1 {
			table = schm.Tables[0].Name
		} else {
			log.Fatal("multi table data file mode tobe implemented")
		}
		sc := bufio.NewScanner(f)
		start := time.Now()
		for sc.Scan() {
			if err = eng.Exec(table, sc.Text()); err != nil {
				log.WithError(err).Errorf("processing %s", sc.Text())
				return
			}
			counter++
			if log.GetLevel() > log.WarnLevel && counter%100000 == 0 {
				log.Debugf("Processing %d lines data", counter)
			}
		}
		duration := time.Since(start)
		perMsgNano := duration.Nanoseconds() / counter
		log.Printf("%d messages in %s, %s per message.", counter, duration, time.Duration(perMsgNano))

		if len(schm.Query) != 0 {
			if generalResult, err = eng.Query(schm.Query); err != nil {
				log.WithError(err).Errorf("marshal rows to json")
				return
			}
			j, _ := json.Marshal(generalResult)
			fmt.Print(string(j))
		}

		api.ServerKeeper(httpServ)
	} else {
		/*
			Kafka data source
		*/
		version, err := sarama.ParseKafkaVersion(version)
		if err != nil {
			log.Fatalf("Error parsing Kafka version: %v", err)
		}

		/**
		 * Construct a new Sarama configuration.
		 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
		 */
		config := sarama.NewConfig()
		config.Version = version

		switch assignor {
		case "sticky":
			config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
		case "roundrobin":
			config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
		case "range":
			config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
		default:
			log.Fatalf("Unrecognized consumer group partition assignor: %s", assignor)
		}

		if oldest {
			config.Consumer.Offsets.Initial = sarama.OffsetOldest
		}

		/**
		 * Setup a new Sarama consumer group
		 */
		consumer := kafka.Consumer{
			Ready: make(chan bool),
		}

		ctx, cancel := context.WithCancel(context.Background())
		client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
		if err != nil {
			log.Fatalf("Error creating consumer group client: %v", err)
		}

		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				// `Consume` should be called inside an infinite loop, when a
				// server-side rebalance happens, the consumer session will need to be
				// recreated to get the new claims
				if err := client.Consume(ctx, strings.Split(topics, ","), &consumer); err != nil {
					log.Fatalf("Error from consumer: %v", err)
				}
				// check if context was cancelled, signaling that the consumer should stop
				if ctx.Err() != nil {
					return
				}
				consumer.Ready = make(chan bool)
			}
		}()

		<-consumer.Ready // Await till the consumer has been set up
		log.Println("Sarama consumer up and running!...")

		sigterm := make(chan os.Signal, 1)
		signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
		select {
		case <-ctx.Done():
			log.Println("terminating: context cancelled")
		case <-sigterm:
			log.Println("terminating: via signal")
		}
		cancel()
		wg.Wait()
		if err = client.Close(); err != nil {
			log.Fatalf("Error closing client: %v", err)
		}
	}
}
