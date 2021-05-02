package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/capella-pw/queue/cluster/cap"
	"github.com/capella-pw/queue/compress"
)

func main() {

	compressor := compress.GeneratorCreate(7)

	cc := cap.CreateClusterConnection(compressor,
		cap.CreateConnection("http://localhost:8676", true, time.Second*5, 5, 5),
		"", nil, compress.Zip, compress.Zip)

	cc.Init()

	cl := cc.Cluster()

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
	fmt.Println("OK")

}
