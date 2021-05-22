package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/capella-pw/queue/cluster"
	"github.com/capella-pw/queue/cluster/cap"
	"github.com/capella-pw/queue/compress"
)

func main() {

	compressor := compress.GeneratorCreate(7)

	cc := cap.CreateClusterConnection(compressor,
		cap.CreateConnection("http://localhost:8676", true, time.Second*5, 5, 5),
		"", "", nil, compress.Zip, compress.Zip, false)

	cc.Init()

	cl := cc.Cluster()

	// Create handler
	err := cl.AddHandler(nil, cluster.HandlerDescription{
		Name:     "test_queue_unload",
		UserName: "",
		Type:     cluster.BlockUnloadHandlerType,
		QueueNames: []string{
			"test_queue",
		},
		Params: cluster.BlockUnloadHandlerParams{
			Interval: time.Second * 1,  // interval between call
			Wait:     time.Second * 50, // wait save timeout

			StorageMemoryTime:   10 * time.Minute,
			StorageLastLoadTime: 10 * time.Second,
		}.ToJson(),
	})

	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	// Start handler
	h, exists, err := cl.GetHandler(nil, "test_queue_unload")
	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	if !exists {
		log.Fatalln("Handler `test_queue_unload` does not exists")
		os.Exit(1)
		return
	}

	err = h.Start(context.Background())

	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	fmt.Println("OK")

}
