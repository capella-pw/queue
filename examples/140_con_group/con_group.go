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
	"github.com/capella-pw/queue/security/authentication/basic"
	"github.com/myfantasy/mft"
)

func main() {

	compressor := compress.GeneratorCreate(7)

	cgCr := cap.ConGroupGenerate().AddConnection(
		"fail1", cap.CreateClusterConnection(nil,
			cap.CreateConnection("http://localhost:18676", true, time.Second*5, 5, 5),
			"", "", nil, compress.Zip, compress.Zip, false),
	).AddConnection(
		"fail2", cap.CreateClusterConnection(nil,
			cap.CreateConnection("http://localhost:28676", true, time.Second*5, 5, 5),
			"", "", nil, compress.Zip, compress.Zip, false),
	).AddConnection(
		"ok1", cap.CreateClusterConnection(nil,
			cap.CreateConnection("http://localhost:8676", true, time.Second*5, 5, 5),
			basic.AuthType, "admin", basic.PasswordMarshal("Pa$$w0rd"),
			compress.Zip, compress.Zip, false),
	).AddConnection(
		"ok2", cap.CreateClusterConnection(nil,
			cap.CreateConnection("http://localhost:8676", true, time.Second*5, 5, 5),
			"", "", nil, compress.Zip, compress.Zip, false),
	)

	cgCr.PriorityGroups["test"] = &cap.PriorityGroup{
		MinSuccess: 2,
		Steps: []cap.PriorityGroupStep{
			{
				StepCallCount:     1,
				IgnoreHealthCheck: true,
				ConNames:          []string{"fail1", "fail2", "ok1"},
			},
			{
				StepCallCount:     3,
				IgnoreHealthCheck: true,
				ConNames:          []string{"fail1", "fail2", "ok1"},
			},
			{
				StepCallCount:     2,
				IgnoreHealthCheck: true,
				ConNames:          []string{"fail1", "ok2", "ok1"},
			},
			{
				StepCallCount:     2,
				IgnoreHealthCheck: true,
				ConNames:          []string{"ok1", "ok1"},
			},
		},
	}

	cgRM := cgCr.ToJson()

	cg, err := cap.ConGroupFromJson(cgRM, compressor)

	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	message := "Hello WORLD"
	f := cap.QueueAddUnique("test_queue",
		[]byte(message), 8, time.Now().Unix(), "s1", 1, cn.SaveImmediatelySaveMode,
		func(id int64) {
			fmt.Println(id)
		})

	err = cg.FuncDO(context.Background(), "test", f, errFunc)
	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	fmt.Println(string(cgRM))

	fmt.Println("OK")

}

func errFunc(err *mft.Error) {
	log.Printf("Error: %v", err)
}
