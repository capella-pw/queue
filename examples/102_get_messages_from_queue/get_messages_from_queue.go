package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/capella-pw/queue/cluster/cap"
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

	q, exists, err := cl.GetQueue(nil, "test_queue")
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
	if msgs[0].Segment != 1 {
		log.Fatalf("Queue `test_queue` queue message segment: `%v` != `%v` \n",
			msgs[0].Segment, 1)
		os.Exit(1)
		return
	}

	checkLastId := msgs[0].ID

	msgs, lastId, err := q.GetSegment(context.Background(), 0, 1, segment.MakeSegments())
	if err != nil {
		log.Fatalf("GetSegment err: %v\n", err)
		os.Exit(1)
		return
	}
	if len(msgs) > 0 {
		log.Fatalf("Queue `test_queue` have msgs (GetSegment with empty segments) %v != 0 \n", len(msgs))
		os.Exit(1)
		return
	}

	if checkLastId != lastId {
		log.Fatalf("Queue `test_queue` last id != last check id %v != %v \n", lastId, checkLastId)
		os.Exit(1)
		return
	}

	fmt.Println("OK")
}
