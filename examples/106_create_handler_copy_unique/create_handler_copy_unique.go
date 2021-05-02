package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/capella-pw/queue/cluster"
	"github.com/capella-pw/queue/cluster/cap"
	"github.com/capella-pw/queue/compress"
	"github.com/capella-pw/queue/queue"
)

func main() {

	compressor := compress.GeneratorCreate(7)

	cc := cap.CreateClusterConnection(compressor,
		cap.CreateConnection("http://localhost:8676", true, time.Second*5, 5, 5),
		"", nil, compress.Zip, compress.Zip)

	cc.Init()

	cl := cc.Cluster()

	err := cl.AddQueue(nil, cluster.QueueDescription{
		Name: "test_queue_2",
		Type: cluster.SimpleQueueType,
		Params: cluster.SimpleQueueParams{
			CntLimit:                   100,
			TimeLimit:                  time.Second * 10,
			LenLimit:                   1e7,
			MetaStorageMountName:       "default", // ../../config/stor.config.json
			SubscriberStorageMountName: "default", // ../../config/stor.config.json
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

	err = cl.AddHandler(nil, cluster.HandlerDescription{
		Name:     "test_queue_to_test_queue_2_copy_unique",
		UserName: "",
		Type:     cluster.CopyUniqueHandlerType,
		QueueNames: []string{
			"test_queue",   // from
			"test_queue_2", // to
		},
		Params: cluster.CopyUniqueHandlerParams{
			Interval: time.Millisecond * 300, // interval between call
			Wait:     time.Second * 5,        // wait save timeout

			SaveModeSrc:    queue.SaveMarkSaveMode,
			SaveModeDst:    queue.SaveMarkSaveMode,
			SubscriberName: "test_subscr",
			CntLimit:       100,
			DoSaveDst:      true,
		}.ToJson(),
	})

	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	fmt.Println("OK")

}
