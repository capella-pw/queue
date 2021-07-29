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

	err := cl.AddHandler(ctx, nil, cluster.HandlerDescription{
		Name:       "test_queue_regularly_save",
		UserName:   "",
		Type:       cluster.RegularlySaveHandlerType,
		QueueNames: []string{"test_queue"},
		Params: cluster.RegularlySaveHandlerParams{
			Interval: time.Millisecond * 300, // interval between call
			Wait:     time.Second * 5,        // wait save timeout
		}.ToJson(),
	})

	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}
	fmt.Println("OK")

}
