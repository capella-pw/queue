package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/capella-pw/queue/cluster/cap"
	"github.com/capella-pw/queue/compress"
	"github.com/capella-pw/queue/queue"

	log "github.com/sirupsen/logrus"
)

var mx sync.Mutex
var sendCnt int
var sendGo int
var errCnt int
var doFor = true

func main() {

	log.SetLevel(log.TraceLevel)

	compressor := compress.GeneratorCreate(7)

	cc := cap.CreateClusterConnection(compressor,
		cap.CreateConnection("http://localhost:8676", true, time.Second*5, 5000, 5),
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
	fmt.Println("!!!!!!!!!!!!")
	go printCnt()

	var srvId int64
	srvId = 100
	for doFor {
		time.Sleep(time.Millisecond * 5)
		go sendMsg(q, srvId)
		srvId++
	}
}

func printCnt() {
	start := time.Now()
	for {
		time.Sleep(time.Second * 2)
		fmt.Printf("Cnt: %v (%v|%v) \twork: %v\n", sendCnt, sendGo-sendCnt, errCnt, time.Since(start))
	}
}

func sendMsg(q queue.Queue, srvId int64) {
	mx.Lock()
	sendGo++
	mx.Unlock()

	message := "Hello WORLD Iteration"

	_, err := q.Add(context.Background(), []byte(message), srvId, time.Now().Unix(), "s1", srvId%13, queue.SaveWaitSaveMode)

	if err != nil {
		fmt.Printf("Cnt: %v (%v|%v)\n%v\n", sendCnt, sendGo-sendCnt, errCnt, srvId)
		log.Print(err)

		doFor = false

		mx.Lock()
		errCnt++
		mx.Unlock()
		return
	}

	mx.Lock()
	sendCnt++
	mx.Unlock()
}
