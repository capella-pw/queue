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
		"", nil, compress.Zip, compress.Zip)

	cc.Init()

	cl := cc.Cluster()

	fmt.Println("wait for 5 sec ...")

	time.Sleep(10 * time.Second)

	callCheck(cl, "test_queue_unload")
	callCheck(cl, "test_queue_delete")
	callCheck(cl, "test_queue_mark")

	fmt.Println("OK")

}

func callCheck(cl cluster.Cluster, handlerName string) {
	fmt.Printf("%v: check start\n", handlerName)

	h, exists, err := cl.GetHandler(nil, handlerName)
	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	if !exists {
		log.Fatalln("Handler `" + handlerName + "` does not exists")
		os.Exit(1)
		return
	}

	err = h.LastError(context.Background())

	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	lc, err := h.LastComplete(context.Background())

	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	fmt.Printf("%v: %v\n", handlerName, lc)
}
