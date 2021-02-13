package api

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"

	log "github.com/sirupsen/logrus"

	"github.com/auxten/clink/core"
)

// Server specific settings.
type Server struct {
	Port    int         `yaml:"port"` // reserve -1 for random port
	Address string      `yaml:"address"`
	Log     string      `yaml:"log"`
	Engine  core.Engine `yaml:"-"`
}

func StartServer(server *Server) {
	setupLog(server.Log)

	srv := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", server.Address, server.Port),
		Handler: newRouter(server.Engine),
	}

	// Initializing the server in a goroutine so that
	// it won't block the graceful shutdown handling below
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server with
	// a timeout of 5 seconds.
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	// The context is used to inform the server it has 5 seconds to finish
	// the request it is currently handling
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}

	log.Println("Server exiting")
}

func setupLog(logMethod string) {
	switch logMethod {
	case "stdout":
		log.SetOutput(os.Stdout)
	case "stderr":
		log.SetOutput(os.Stderr)
	case "":
		log.SetOutput(ioutil.Discard)
	default:
		log.SetOutput(&lumberjack.Logger{
			Filename:   logMethod,
			MaxSize:    500,
			MaxAge:     28,
			MaxBackups: 3,
			Compress:   true,
		})
	}
}
