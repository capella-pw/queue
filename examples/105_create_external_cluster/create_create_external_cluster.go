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

	err := cl.AddExternalCluster(ctx, nil, cluster.ExternalClusterDescription{
		Name:   "ec_test_self",
		Type:   cap.HttpExternalClusterType,
		Params: cc.ToJson(),
	})

	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	q, exists, err := cl.GetQueue(ctx, nil, "ec_test_self/test_queue")
	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	if !exists {
		log.Fatalln("Queue `test_queue` does not exists")
		os.Exit(1)
		return
	}

	message := "Hello WORLD"

	msgs, err := q.Get(context.Background(), 0, 1)

	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}
	if len(msgs) != 1 {
		log.Fatalf("Queue `test_queue` does not have msgs %v != 1 \n", len(msgs))
		os.Exit(1)
		return
	}
	if string(msgs[0].Message) != message {
		log.Fatalf("Queue `test_queue` queue message: `%v` != `%v` \n",
			string(msgs[0].Message), message)
		os.Exit(1)
		return
	}

	fmt.Println("OK")

}
