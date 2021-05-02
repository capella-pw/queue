package main

import (
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/myfantasy/mft"
	"github.com/valyala/fasthttp"

	"github.com/capella-pw/queue/cluster"
	"github.com/capella-pw/queue/cluster/cap"
	"github.com/capella-pw/queue/cluster/http_service"
	"github.com/capella-pw/queue/compress"
	"github.com/capella-pw/queue/storage"

	log "github.com/sirupsen/logrus"
)

var fDebug = flag.String("log_level", "info",
	`Levels: fatal, error, warn [warning], info, debug, trace`)

var fConfigFile = flag.String("cfg", "stor.config.json",
	"Sets storage config file path")

var fClusterMountName = flag.String("cmn", "default",
	"Cluster storage mount name")

var fClusterRelativePath = flag.String("crp", "",
	"Cluster storage replative path")

var fLoadTimeout = flag.Duration("ct", time.Second*5,
	"Cluster load and save timeout")

var fCompressDefaultLevel = flag.Int("cl", 7,
	"Compress default level (ZIP)")

var fListenAddress = flag.String("l", ":8676",
	"Listen address and port for example :8080 localhost:8989 etc")

var storageGenerator *storage.Generator
var compressor *compress.Generator

var clusterFastHTTPHandler func(ctx *fasthttp.RequestCtx)

func createCompressGenerator() {
	compressor = compress.GeneratorCreate(*fCompressDefaultLevel)
}

func createStorageGenerator() error {
	path := filepath.FromSlash(*fConfigFile)

	llevel, err := log.ParseLevel(*fDebug)
	if err != nil {
		log.Fatal(err)
	}
	log.SetLevel(llevel)

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	var generatorInfo storage.GeneratorInfo

	err = json.Unmarshal(data, &generatorInfo)
	if err != nil {
		return err
	}

	storageGenerator = storage.CreateGenerator(generatorInfo, compressor)

	return nil
}

func main() {

	flag.Parse()

	createCompressGenerator()

	if err := createStorageGenerator(); err != nil {
		log.Fatal(err)
	}

	data, err := cluster.LoadClusterData(*fLoadTimeout,
		storageGenerator,
		*fClusterMountName, *fClusterRelativePath)
	if err != nil {
		log.Fatal(err)
	}

	onChangeFunc, err := cluster.OnChangeFuncGenerate(*fLoadTimeout,
		storageGenerator,
		*fClusterMountName, *fClusterRelativePath)
	if err != nil {
		log.Fatal(err)
	}

	cl := cluster.SimpleClusterCreate(storageGenerator,
		func(err *mft.Error) bool {
			log.Errorln(err)
			return true
		},
		onChangeFunc,
		nil, // checkPermissionFunc
		cluster.QueueGeneratorCreate(),
		cluster.ExternalClusterGeneratorCreate(),
		cluster.HandlerGeneratorCreate(),
		compressor,
	)

	err = cl.LoadFullStructRaw(data)
	if err != nil {
		log.Fatal(err)
	}

	var c cluster.Cluster
	c = cl

	clusterFastHTTPHandler = http_service.FastHTTPHandler(
		cluster.ClusterServiceJsonCreate(cluster.CheckAuthFuncEmpty, c, compressor),
		func() (ctx context.Context, doOnCompete func()) {
			return context.WithTimeout(context.Background(), time.Second*5)
		},
	)

	// start API

	api := &fasthttp.Server{
		Handler: fastHTTPHandler,
	}

	serverErrors := make(chan error, 1)
	go func() {
		log.Infof("Listen and serve %v", *fListenAddress)
		serverErrors <- api.ListenAndServe(*fListenAddress)
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
	if path == cap.ClusterMethodPath {
		if clusterFastHTTPHandler != nil {
			clusterFastHTTPHandler(ctx)
			return
		}
		unknownInternalError(ctx)
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
