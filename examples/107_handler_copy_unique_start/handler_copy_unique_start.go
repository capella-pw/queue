package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/capella-pw/queue/cluster/cap"
	"github.com/capella-pw/queue/compress"
	"github.com/capella-pw/queue/queue"
)

func main() {

	compressor := compress.GeneratorCreate(7)

	cc := cap.CreateClusterConnection(compressor,
		cap.CreateConnection("http://localhost:8676", true, time.Second*5, 5, 5),
		"", "", nil, compress.Zip, compress.Zip, false)

	cc.Init()

	cl := cc.Cluster()

	// Direct copy
	h, exists, err := cl.GetHandler(nil, "test_queue_to_test_queue_2_copy_unique")
	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	if !exists {
		log.Fatalln("Handler `test_queue_to_test_queue_2_copy_unique` does not exists")
		os.Exit(1)
		return
	}

	err = h.Start(context.Background())

	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	// Back copy
	h, exists, err = cl.GetHandler(nil, "test_queue_2_to_test_queue_copy_unique")
	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	if !exists {
		log.Fatalln("Handler `test_queue_2_to_test_queue_copy_unique` does not exists")
		os.Exit(1)
		return
	}

	err = h.Start(context.Background())

	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	// Send message to queue 2
	q, exists, err := cl.GetQueue(nil, "test_queue_2")
	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	if !exists {
		log.Fatalln("Queue `test_queue_2` does not exists")
		os.Exit(1)
		return
	}

	message := "Hello WORLD 2"

	_, err = q.Add(context.Background(), []byte(message), 5, time.Now().Unix(), "s2", 3, queue.SaveImmediatelySaveMode)

	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	fmt.Println("OK")

}
