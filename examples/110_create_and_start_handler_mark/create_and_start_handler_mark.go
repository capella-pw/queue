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

	ctx := context.Background()

	// Create handler
	err := cl.AddHandler(ctx, nil, cluster.HandlerDescription{
		Name:     "test_queue_mark",
		UserName: "",
		Type:     cluster.BlockMarkHandlerType,
		QueueNames: []string{
			"test_queue",
		},
		Params: cluster.BlockMarkHandlerParams{
			Interval:          time.Second * 30,  // interval between call
			WaitMark:          time.Second * 300, // wait mark for update (move) timeout
			WaitUpdate:        time.Second * 300, // wait update (move) timeout
			LimitUpdateBlocks: 1000,              // limit update (move) block (one iteration)

			/*
				// ./examples/100_create_queue:
				MarkerBlockDataStorageMountName: map[string]string{
					"":  "fast",
					"a": "compress1",
					"b": "compress",
					"c": "compress9",
				},
			*/
			Conditions: []cluster.MarkCondition{
				{
					Mark:     "a",
					FromTime: time.Minute * 10,
					ToTime:   time.Hour * 2,
				},
				{
					Mark:     "b",
					FromTime: time.Hour * 2,
					ToTime:   time.Hour * 24 * 3,
				},
				{
					Mark:     "c",
					FromTime: time.Hour * 24 * 3,
					ToTime:   time.Hour * 24 * 32,
				},
			},
		}.ToJson(),
	})

	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	// Start handler
	h, exists, err := cl.GetHandler(ctx, nil, "test_queue_mark")
	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	if !exists {
		log.Fatalln("Handler `test_queue_mark` does not exists")
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
