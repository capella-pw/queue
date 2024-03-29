package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

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

	q, exists, err := cl.GetQueue(ctx, nil, "test_queue")
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

	id, err := q.Add(context.Background(), nil, []byte(message),
		5, time.Now().Unix(), "s1", 1, cn.SaveImmediatelySaveMode)

	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}
	fmt.Println(id)
}
