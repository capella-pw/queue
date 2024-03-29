package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/capella-pw/queue/cluster"
	"github.com/capella-pw/queue/cluster/cap"
	"github.com/capella-pw/queue/cn"
	"github.com/capella-pw/queue/compress"
	"github.com/myfantasy/segment"
)

func main() {

	compressor := compress.GeneratorCreate(7)

	cc := cap.CreateClusterConnection(compressor,
		cap.CreateConnection("http://localhost:8676", true, time.Second*5, 5, 5),
		"", "", nil, compress.Zip, compress.Zip, false)

	cc.Init()

	cl := cc.Cluster()

	ctx := context.Background()

	err := cl.AddQueue(ctx, nil, cluster.QueueDescription{
		Name: "test_queue",
		Type: cluster.SimpleQueueType,
		Params: cluster.SimpleQueueParams{
			CntLimit:                   10000,
			TimeLimit:                  time.Second * 10,
			LenLimit:                   1e7,
			MetaStorageMountName:       "meta", // ../../config/stor.config.json
			SubscriberStorageMountName: "meta", // ../../config/stor.config.json
			MarkerBlockDataStorageMountName: map[string]string{
				"":  "fast",
				"a": "compress1",
				"b": "compress",
				"c": "compress9",
			},
			Segments:        segment.MakeSegments().AddSegment(segment.Segment{From: -100000000, To: 100000000}),
			DefaultSaveMode: cn.SaveMarkSaveMode,
		}.ToJson(),
	})

	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}
	fmt.Println("OK")

}
