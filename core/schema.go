package core

import (
	"io/ioutil"
	"time"

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

type Schema struct {
	Name     string
	Engine   string
	Query    string
	Tables   []Table
	TableMap map[string]*Table `yaml:"-"` // tableName:table
}

type DataSource interface{}

type KafkaSrc struct {
	DataSource `yaml:"-"`
	// EndPointConfig in json format
	EndPointConfig string
	// OptTypePath is the json path of Operation type.
	// eg. INSERT/UPDATE
	OptTypePath string
	// Topic is the kafka topic name, one topic per table
	Topic string
}

type MySQLSrc struct {
	DataSource `yaml:"-"`
	// Dsn is the connection string of DB
	// eg. `username:password@protocol(address)/dbname?param=value`
	Dsn string
	// Table name or a Select query return a table
	Table  string
	Select string
	// TTL is the cache time of the table
	TTL time.Duration
}

// Table is the description of table in ngnx.
type Table struct {
	Name string
	// Type is the type of table, eg. kafka/mysql
	Type string
	// DataSource is the data source description
	DataSource DataSource
	// Pk is the primary key name.
	Pk string
	// Cols are the columns in the Table
	Cols []Col
	// Index is the column names array to build an index
	Index []string
}

type Col struct {
	// Name is the column name
	Name string
	// Type is the column schema type
	Type string
	// Extra column description, eg. UNIQUE/DEFAULT
	Extra string
	// InsertPath is the json path of INSERT column data
	InsertPath string
	// UpdatePath is the json path of UPDATE column data
	UpdatePath string
}

func LoadConf(configPath string) (schema *Schema, err error) {
	var configBytes []byte
	if configBytes, err = ioutil.ReadFile(configPath); err != nil {
		log.WithError(err).Error("read config file failed")
	}
	schema = &Schema{}
	if err = yaml.Unmarshal(configBytes, schema); err != nil {
		log.WithError(err).Error("unmarshal config file failed")
		return nil, err
	}
	schema.TableMap = make(map[string]*Table)
	for _, t := range schema.Tables {
		schema.TableMap[t.Name] = &t
	}
	return
}
