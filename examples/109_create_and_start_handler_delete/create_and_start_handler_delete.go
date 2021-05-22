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
		Name:     "test_queue_delete",
		UserName: "",
		Type:     cluster.BlockDeleteHandlerType,
		QueueNames: []string{
			"test_queue",
		},
		Params: cluster.BlockDeleteHandlerParams{
			Interval:   time.Second * 30,  // interval between call
			WaitMark:   time.Second * 300, // wait mark for delete timeout
			WaitDelete: time.Second * 300, // wait delete timeout

			StorageTime: time.Hour * 24 * 32, // delete message after 32 day
			LimitDelete: 1000,                // limit deleted block (one iteration)
		}.ToJson(),
	})

	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	// Start handler
	h, exists, err := cl.GetHandler(nil, "test_queue_delete")
	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	if !exists {
		log.Fatalln("Handler `test_queue_delete` does not exists")
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
