package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/valyala/fasthttp"

	"github.com/capella-pw/queue/http"

	log "github.com/sirupsen/logrus"
)

func readSettings() {
	var settingsFile string
	flag.StringVar(&settingsFile, "file", "settings.json", "sets settings file path, if empty \"settings.json\" used")
	flag.Parse()
}

func main() {

	readSettings()

	api := &fasthttp.Server{
		Handler: http.FastHTTPHandler,
	}

	serverErrors := make(chan error, 1)
	go func() {
		log.Infof("Listen and serve :8080")
		serverErrors <- api.ListenAndServe(":8080")
	}()

	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErrors:
		log.Fatalf("Can`t start server; %v", err)

	case <-osSignals:
		log.Infof("Start shutdown...")
		go func() {
			if err := api.Shutdown(); err != nil {
				log.Infof("Graceful shutdown did not complete in 5s : %v", err)
			}
		}()
	}

	log.Infof("Complete shutdown")
}
