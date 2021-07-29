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
		cap.CreateConnection("https://localhost:8676", true, time.Second*5, 5, 5),
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

	msgs, err := q.Get(context.Background(), 0, 100)

	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}
	if string(msgs[0].Message) != message {
		log.Fatalf("Queue `test_queue` queue message: `%v` != `%v; msg ext id : %v` \n",
			string(msgs[0].Message), message, msgs[0].ExternalID)
		os.Exit(1)
		return
	}

	for _, m := range msgs {
		fmt.Printf("id: %v ext_id: %v\n", m.ID, m.ExternalID)
	}

	fmt.Println("OK")
}
