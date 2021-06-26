package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/capella-pw/queue/compress"
	"github.com/myfantasy/mft"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"

	_ "github.com/lib/pq"
)

var fDebug = flag.String("log_level", "info",
	`Levels: fatal, error, warn [warning], info, debug, trace`)

var fConnFile = flag.String("cf", "rules.json",
	`Rules file, requare for get connections and rules`)

var fListenAddress = flag.String("l", ":9676",
	"Listen address and port for example :8080 localhost:8989 etc")

var fTlsKey = flag.String("tls_key", "",
	"tls key; example `app/key.pem`")

var fTlsCert = flag.String("tls_cert", "",
	"tls certificate; example `app/cert.pem`")

var RunnedSettings *Settings

func main() {
	flag.Parse()

	llevel, er0 := log.ParseLevel(*fDebug)
	if er0 != nil {
		log.Fatal(er0)
	}
	log.SetLevel(llevel)

	body, er0 := ioutil.ReadFile(*fConnFile)
	if er0 != nil {
		log.Fatalf("Read file %v fail: %v\n", *fConnFile, er0)
	}

	compressor := compress.GeneratorCreate(7)
	settings, err := SettingsCreateFromJson(body, compressor)
	if err != nil {
		log.Fatalf("Parse file %v fail: %v\n", *fConnFile, err)
	}

	RunnedSettings = settings
	for _, t := range RunnedSettings.Transfers {
		go t.LoadMessagesDO(RunnedSettings)
	}
	for _, a := range RunnedSettings.Actions {
		go a.CallDO(RunnedSettings)
	}

	api := &fasthttp.Server{
		Handler: fastHTTPHandler,
	}

	serverErrors := make(chan error, 1)
	go func() {
		if *fTlsKey != "" {
			log.Infof("Listen and serve TLS %v", *fListenAddress)
			serverErrors <- api.ListenAndServeTLS(*fListenAddress,
				*fTlsCert, *fTlsKey)
		} else {
			log.Infof("Listen and serve %v", *fListenAddress)
			serverErrors <- api.ListenAndServe(*fListenAddress)
		}
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

func fastHTTPHandler(ctx *fasthttp.RequestCtx) {
	path := string(ctx.Request.URI().Path())
	log.Tracef("http call path %v", path)
	if path == "/ping" {
		ping(ctx)
		return
	}
	notFound(ctx)
}

func unknownInternalError(ctx *fasthttp.RequestCtx) {
	ctx.Response.SetStatusCode(500)
	log.Tracef("unknownInternalError")
}

func notFound(ctx *fasthttp.RequestCtx) {
	ctx.Response.SetStatusCode(404)
	log.Tracef("notFound")
}

type ErrorResult struct {
	Type        string `json:"type"`
	Name        string `json:"name"`
	Description string `json:"descr"`

	LastStart     time.Time  `json:"last_start"`
	LastComplete  time.Time  `json:"last_complete"`
	LastFail      time.Time  `json:"last_fail"`
	LastFailError *mft.Error `json:"last_fail_error"`
	LastFailOp    string     `json:"last_fail_op"`

	OK bool `json:"is_ok"`
}

func ping(ctx *fasthttp.RequestCtx) {
	res := []ErrorResult{}
	allOk := true
	for _, t := range RunnedSettings.Transfers {
		res = append(res, ErrorResult{
			Type:        "TR",
			Name:        t.Name,
			Description: t.Description,

			LastStart:     t.LastStart,
			LastComplete:  t.LastComplete,
			LastFail:      t.LastFail,
			LastFailError: t.LastFailError,
			LastFailOp:    t.LastFailOp,

			OK: !t.LastFail.After(t.LastComplete),
		})

		if t.LastFail.After(t.LastComplete) {
			allOk = false
		}
	}
	for _, a := range RunnedSettings.Actions {
		res = append(res, ErrorResult{
			Type:        "A",
			Name:        a.Name,
			Description: a.Description,

			LastStart:     a.LastStart,
			LastComplete:  a.LastComplete,
			LastFail:      a.LastFail,
			LastFailError: a.LastFailError,
			LastFailOp:    a.LastFailOp,

			OK: a.LastFail.After(a.LastComplete),
		})
		if a.LastFail.After(a.LastComplete) {
			allOk = false
		}
	}

	sort.Slice(res, func(i, j int) bool {
		return (!res[i].OK && res[j].OK) ||
			res[i].LastFail.After(res[j].LastFail) ||
			res[i].LastFail.Equal(res[j].LastFail) && res[i].LastComplete.Before(res[j].LastComplete)
	})

	b, err := json.Marshal(res)

	if err != nil {
		log.Errorf("ping result marshal error: %v\n", err)
		ctx.Response.SetStatusCode(500)
		return
	}

	ctx.Response.SetBody(b)
	if allOk {
		ctx.Response.SetStatusCode(200)
		log.Tracef("ping complete")
		return
	}
	ctx.Response.SetStatusCode(500)
	log.Tracef("ping complete has errors")
}
