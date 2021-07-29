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
		Name: "test_queue_2",
		Type: cluster.SimpleQueueType,
		Params: cluster.SimpleQueueParams{
			CntLimit:                   30000,
			TimeLimit:                  time.Second * 50,
			LenLimit:                   1e7,
			MetaStorageMountName:       "meta", // ../../config/stor.config.json
			SubscriberStorageMountName: "meta", // ../../config/stor.config.json
			MarkerBlockDataStorageMountName: map[string]string{
				"":  "fast",
				"a": "compress1",
				"b": "compress",
				"c": "compress9",
			},
		}.ToJson(),
	})

	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	// Direct copy
	err = cl.AddHandler(ctx, nil, cluster.HandlerDescription{
		Name:     "test_queue_to_test_queue_2_copy_unique",
		UserName: "",
		Type:     cluster.CopyUniqueHandlerType,
		QueueNames: []string{
			"test_queue",   // from
			"test_queue_2", // to
		},
		Params: cluster.CopyUniqueHandlerParams{
			Interval: time.Millisecond * 30, // interval between call
			Wait:     time.Second * 5,       // wait save timeout

			SaveModeSrc:    cn.SaveMarkSaveMode,
			SaveModeDst:    cn.SaveMarkSaveMode,
			SubscriberName: "test_subscr",
			CntLimit:       100,
			DoSaveDst:      true,

			Segments: nil,
		}.ToJson(),
	})

	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	// Back copy
	err = cl.AddHandler(ctx, nil, cluster.HandlerDescription{
		Name:     "test_queue_2_to_test_queue_copy_unique",
		UserName: "",
		Type:     cluster.CopyUniqueHandlerType,
		QueueNames: []string{
			"test_queue_2", // from
			"test_queue",   // to
		},
		Params: cluster.CopyUniqueHandlerParams{
			Interval: time.Millisecond * 30, // interval between call
			Wait:     time.Second * 5,       // wait save timeout

			SaveModeSrc:    cn.SaveMarkSaveMode,
			SaveModeDst:    cn.SaveMarkSaveMode,
			SubscriberName: "test_subscr2",
			CntLimit:       100,
			DoSaveDst:      true,

			Segments: nil,
		}.ToJson(),
	})

	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	fmt.Println("OK")

}
