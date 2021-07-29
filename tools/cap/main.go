package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/capella-pw/queue/cluster"
	"github.com/capella-pw/queue/cluster/cap"
	"github.com/capella-pw/queue/compress"
	"github.com/capella-pw/queue/queue"
	"github.com/myfantasy/mft"
	log "github.com/sirupsen/logrus"
)

var fDebug = flag.String("log_level", "info",
	`Levels: fatal, error, warn [warning], info, debug, trace`)

var fTimeout = flag.Duration("timeout", time.Second*5,
	`Execute command timeout`)

var fConnFile = flag.String("cf", "connection.json",
	`Connections file, requare for get connections`)

var fConnectionName = flag.String("cn", "admin",
	`Connection name`)

var fCmd = flag.String("cmd", "",
	`Command

	ping - ping cluster

	cluster_name_set - sets cluster name (requare "name")
	cluster_name_get - gets cluster name

	next_id - gets next id from cluster
	next_ids - gets next id from cluster (requare "qty")

	q_add - creates new queue (requare "p" or "pf")
		example: ./cap -cmd q_add -pf new_queue.json
		example: ./cap -cmd q_add -pf new_queue2.json
	q_drop - drops queue (requare "name")
	q_descr - gets queue description (requare "name")
	q_list - gets queues list
	q_get - gets messages from queue (requare "name", "qty" and "id")
		example: ./cap -cmd q_get -name example_queue -qty 10 -id 0
	q_au - queue add unique messages (requare "name", "save_mode", "p" or "pf")
		example: ./cap -cmd q_au -name example_queue -pf new_messages.json -save_mode 2
		example: ./cap -cmd q_au -name example_queue2 -pf new_messages2.json -save_mode 2
	
	exc_add - creates external cluster
		example: ./cap -cmd exc_add -pf new_external_cluster.json
	exc_drop - drops external cluster (requare "name")
	exc_descr - gets external cluster description (requare "name")
	exc_list - gets external clusters list

	h_add - creates handler
		example: ./cap -cmd h_add -pf new_copy_handler.json
		example: ./cap -cmd h_add -pf new_copy_handler2.json
		example: ./cap -cmd h_add -pf new_delete_handler.json
		example: ./cap -cmd h_add -pf new_mark_handler.json
		example: ./cap -cmd h_add -pf new_regularly_save_handler.json
		example: ./cap -cmd h_add -pf new_regularly_save_handler2.json
		example: ./cap -cmd h_add -pf new_unload_handler.json

	h_drop - drops handler (requare "name")
	h_descr - gets handler description (requare "name")
	h_list - gets handler list
	h_start - starts handler (requare "name")
	h_stop - stops handler (requare "name")
	h_last_error - show handler's last error (requare "name")
	h_last_complete - show handler's last complete time (requare "name")
	h_is_started - show handler is started (requare "name")
	`)

var fParamsFileName = flag.String("pf", "",
	`Path to file with params in JSON format (if "-p" set then used "-p")`)

var fParams = flag.String("p", "",
	`Params in JSON format (if "-pf" set then "-pf" will be ignored)`)

var fName = flag.String("name", "",
	`Object name`)

var fQty = flag.Int("qty", 0,
	`Quantity in request`)

var fID = flag.Int64("id", 0,
	`id of message`)

var fSaveMode = flag.Int("save_mode", 2,
	`Save mode
		0 - NotSave (not mark queue as need changed)
		1 - SaveImmediately (save message immediatly after add element)
		2 - SaveMark (not wait save message after add element (saving do on schedule)) THE BEST WAY
		3 - SaveWait (wait save message after add element (saving do on schedule)) THE BEST WAY
	`)

func GetParams(v interface{}) {
	var body []byte
	var er0 error
	if *fParams != "" {
		body = []byte(*fParams)
	} else if *fParamsFileName != "" {
		body, er0 = ioutil.ReadFile(*fParamsFileName)
		if er0 != nil {
			log.Fatalf("Read file %v fail: %v\n", *fParamsFileName, er0)
		}
	} else {
		log.Fatal(`Params "-p" and "-pf" are not set`)
	}

	er0 = json.Unmarshal(body, v)
	if er0 != nil {
		log.Fatalf("Fail unmarshal params: %v\n", er0)
	}
}

func main() {
	flag.Parse()

	llevel, er0 := log.ParseLevel(*fDebug)
	if er0 != nil {
		log.Fatal(er0)
	}
	log.SetLevel(llevel)

	body, er0 := ioutil.ReadFile(*fConnFile)
	if er0 != nil {
		log.Fatalf("Read file %v fail: %v\n", *fConnFile, er0)
	}

	compressor := compress.GeneratorCreate(7)
	cg, err := cap.ConGroupFromJson(body, compressor)
	if err != nil {
		log.Fatalf("Parce file %v fail: %v\n", *fConnFile, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *fTimeout)
	defer cancel()

	if *fCmd == "ping" {
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				err = c.Ping(ctx, nil)
				return err
			})
		if err != nil {
			fmt.Printf("Ping %v error: %v\n", *fConnectionName, err)
			os.Exit(1)
		}
		fmt.Println("OK")
		os.Exit(0)
	} else if *fCmd == "cluster_name_set" {
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				err = c.SetName(ctx, nil, *fName)
				return err
			})
		if err != nil {
			fmt.Printf("Set cluster name `%v` to %v error: %v\n", *fName, *fConnectionName, err)
			os.Exit(1)
		}
		fmt.Println("OK")
		os.Exit(0)
	} else if *fCmd == "cluster_name_get" {
		var name string
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				name, err = c.GetName(ctx, nil)
				return err
			})
		if err != nil {
			fmt.Printf("Get cluster name from %v error: %v\n", *fConnectionName, err)
			os.Exit(1)
		}
		fmt.Println(name)
		os.Exit(0)
	} else if *fCmd == "next_id" {
		var id int64
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				id, err = c.GetNextId(ctx, nil)
				return err
			})
		if err != nil {
			fmt.Printf("Get next id from %v error: %v\n", *fConnectionName, err)
			os.Exit(1)
		}
		fmt.Println(id)
		os.Exit(0)
	} else if *fCmd == "next_ids" {
		var ids []int64
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				ids, err = c.GetNextIds(ctx, nil, *fQty)
				return err
			})
		if err != nil {
			fmt.Printf("Get next ids from %v error: %v\n", *fConnectionName, err)
			os.Exit(1)
		}
		bdy, _ := json.MarshalIndent(ids, "", "  ")
		fmt.Println(string(bdy))
		os.Exit(0)
	} else if *fCmd == "q_add" {
		var qd cluster.QueueDescription
		GetParams(&qd)
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				err = c.AddQueue(ctx, nil, qd)
				return err
			})
		if err != nil {
			fmt.Printf("Add Queue %v to %v error: %v\n", qd.Name, *fConnectionName, err)
			os.Exit(1)
		}
		fmt.Println("OK")
		os.Exit(0)
	} else if *fCmd == "q_drop" {
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				err = c.DropQueue(ctx, nil, *fName)
				return err
			})
		if err != nil {
			fmt.Printf("Drop Queue %v from %v error: %v\n", *fName, *fConnectionName, err)
			os.Exit(1)
		}
		fmt.Println("OK")
		os.Exit(0)
	} else if *fCmd == "q_descr" {
		var queueDescription cluster.QueueDescription
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				queueDescription, err = c.GetQueueDescription(ctx, nil, *fName)
				return err
			})
		if err != nil {
			fmt.Printf("Get Queue Description %v from %v error: %v\n", *fName, *fConnectionName, err)
			os.Exit(1)
		}
		bt, er0 := json.MarshalIndent(queueDescription, "", "  ")
		if er0 != nil {
			log.Fatalf("Marshal Queue Description from %v fail: %v\n", *fConnectionName, er0)
		}
		fmt.Println(string(bt))
		os.Exit(0)
	} else if *fCmd == "q_list" {
		var names []string
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				names, err = c.GetQueuesList(ctx, nil)
				return err
			})
		if err != nil {
			fmt.Printf("Get Queues List %v from %v error: %v\n", *fName, *fConnectionName, err)
			os.Exit(1)
		}
		bt, er0 := json.MarshalIndent(names, "", "  ")
		if er0 != nil {
			log.Fatalf("Marshal Queues List from %v fail: %v\n", *fConnectionName, er0)
		}
		fmt.Println(string(bt))
		os.Exit(0)
	} else if *fCmd == "q_get" {
		var q queue.Queue
		var exists bool
		var messages []*queue.MessageWithMeta
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				q, exists, err = c.GetQueue(ctx, nil, *fName)

				if err != nil {
					return err
				}
				if !exists {
					return err
				}

				messages, err = q.Get(ctx, *fID, *fQty)
				return err
			})
		if err != nil {
			fmt.Printf("Get Queue messages `%v` from `%v` error: %v\n", *fName, *fConnectionName, err)
			os.Exit(1)
		}
		if !exists {
			fmt.Printf("Get Queue messages `%v` from `%v` error: queue does not exists\n", *fName, *fConnectionName)
			os.Exit(1)
		}
		bt, er0 := json.MarshalIndent(messages, "", "  ")
		if er0 != nil {
			log.Fatalf("Marshal Queue messages from `%v` fail: %v\n", *fConnectionName, er0)
		}
		fmt.Println(string(bt))
		os.Exit(0)
	} else if *fCmd == "q_au" {
		var q queue.Queue
		var exists bool
		var messages []queue.Message
		var ids []int64
		GetParams(&messages)
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				q, exists, err = c.GetQueue(ctx, nil, *fName)

				if err != nil {
					return err
				}
				if !exists {
					return err
				}

				ids, err = q.AddUniqueList(ctx, messages, *fSaveMode)

				return err
			})
		if err != nil {
			fmt.Printf("Queue Add Unique `%v` to `%v` error: %v\n", *fName, *fConnectionName, err)
			os.Exit(1)
		}
		if !exists {
			fmt.Printf("Queue Add Unique `%v` from `%v` error: queue does not exists\n", *fName, *fConnectionName)
			os.Exit(1)
		}
		bt, er0 := json.MarshalIndent(ids, "", "  ")
		if er0 != nil {
			log.Fatalf("Marshal Queue message IDs from `%v` fail: %v\n", *fConnectionName, er0)
		}
		fmt.Println(string(bt))
		os.Exit(0)
	} else if *fCmd == "exc_add" {
		var ecd cluster.ExternalClusterDescription
		GetParams(&ecd)
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				err = c.AddExternalCluster(ctx, nil, ecd)
				return err
			})
		if err != nil {
			fmt.Printf("Add External Cluster `%v` to `%v` error: %v\n", ecd.Name, *fConnectionName, err)
			os.Exit(1)
		}
		fmt.Println("OK")
		os.Exit(0)
	} else if *fCmd == "exc_drop" {
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				err = c.DropExternalCluster(ctx, nil, *fName)
				return err
			})
		if err != nil {
			fmt.Printf("Drop External Cluster %v from %v error: %v\n", *fName, *fConnectionName, err)
			os.Exit(1)
		}
		fmt.Println("OK")
		os.Exit(0)
	} else if *fCmd == "exc_descr" {
		var clusterParams cluster.ExternalClusterDescription
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				clusterParams, err = c.GetExternalClusterDescription(ctx, nil, *fName)
				return err
			})
		if err != nil {
			fmt.Printf("Get External Cluster Description %v from %v error: %v\n", *fName, *fConnectionName, err)
			os.Exit(1)
		}
		bt, er0 := json.MarshalIndent(clusterParams, "", "  ")
		if er0 != nil {
			log.Fatalf("Marshal External Cluster Description from %v fail: %v\n", *fConnectionName, er0)
		}
		fmt.Println(string(bt))
		os.Exit(0)
	} else if *fCmd == "exc_list" {
		var names []string
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				names, err = c.GetExternalClustersList(ctx, nil)
				return err
			})
		if err != nil {
			fmt.Printf("Get External Clusters List %v from %v error: %v\n", *fName, *fConnectionName, err)
			os.Exit(1)
		}
		bt, er0 := json.MarshalIndent(names, "", "  ")
		if er0 != nil {
			log.Fatalf("Marshal External Clusters List from %v fail: %v\n", *fConnectionName, er0)
		}
		fmt.Println(string(bt))
		os.Exit(0)
	} else if *fCmd == "h_add" {
		var handlerParams cluster.HandlerDescription
		GetParams(&handlerParams)
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				err = c.AddHandler(ctx, nil, handlerParams)
				return err
			})
		if err != nil {
			fmt.Printf("Add Handler `%v` to `%v` error: %v\n", handlerParams.Name, *fConnectionName, err)
			os.Exit(1)
		}
		fmt.Println("OK")
		os.Exit(0)
	} else if *fCmd == "h_drop" {
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				err = c.DropHandler(ctx, nil, *fName)
				return err
			})
		if err != nil {
			fmt.Printf("Drop Handler %v from %v error: %v\n", *fName, *fConnectionName, err)
			os.Exit(1)
		}
		fmt.Println("OK")
		os.Exit(0)
	} else if *fCmd == "h_descr" {
		var handlerParams cluster.HandlerDescription
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				handlerParams, err = c.GetHandlerDescription(ctx, nil, *fName)
				return err
			})
		if err != nil {
			fmt.Printf("Get Handler Description %v from %v error: %v\n", *fName, *fConnectionName, err)
			os.Exit(1)
		}
		bt, er0 := json.MarshalIndent(handlerParams, "", "  ")
		if er0 != nil {
			log.Fatalf("Marshal Handler Description from %v fail: %v\n", *fConnectionName, er0)
		}
		fmt.Println(string(bt))
		os.Exit(0)
	} else if *fCmd == "h_list" {
		var names []string
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				names, err = c.GetHandlersList(ctx, nil)
				return err
			})
		if err != nil {
			fmt.Printf("Get Handlers List from %v error: %v\n", *fConnectionName, err)
			os.Exit(1)
		}
		bt, er0 := json.MarshalIndent(names, "", "  ")
		if er0 != nil {
			log.Fatalf("Marshal Handlers List from %v fail: %v\n", *fConnectionName, er0)
		}
		fmt.Println(string(bt))
		os.Exit(0)
	} else if *fCmd == "h_start" {
		var handler cluster.Handler
		var exists bool
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				handler, exists, err = c.GetHandler(ctx, nil, *fName)

				if err != nil {
					return err
				}
				if !exists {
					return err
				}

				err = handler.Start(ctx)

				return err
			})
		if err != nil {
			fmt.Printf("Start Handler %v from %v error: %v\n", *fName, *fConnectionName, err)
			os.Exit(1)
		}
		if !exists {
			fmt.Printf("Start Handler %v from %v error: handler does not exists\n", *fName, *fConnectionName)
			os.Exit(1)
		}
		fmt.Println("OK")
		os.Exit(0)
	} else if *fCmd == "h_stop" {
		var handler cluster.Handler
		var exists bool
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				handler, exists, err = c.GetHandler(ctx, nil, *fName)

				if err != nil {
					return err
				}
				if !exists {
					return err
				}

				err = handler.Stop(ctx)

				return err
			})
		if err != nil {
			fmt.Printf("Stop Handler %v from %v error: %v\n", *fName, *fConnectionName, err)
			os.Exit(1)
		}
		if !exists {
			fmt.Printf("Stop Handler %v from %v error: handler does not exists\n", *fName, *fConnectionName)
			os.Exit(1)
		}
		fmt.Println("OK")
		os.Exit(0)
	} else if *fCmd == "h_last_error" {
		var handler cluster.Handler
		var exists bool
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				handler, exists, err = c.GetHandler(ctx, nil, *fName)

				if err != nil {
					return err
				}
				if !exists {
					return err
				}

				err = handler.LastError(ctx)

				return err
			})
		if err != nil {
			fmt.Printf("Last Handler %v ERROR from %v error: %v\n", *fName, *fConnectionName, err)
			os.Exit(1)
		}
		if !exists {
			fmt.Printf("Last Handler %v from %v error FAIL GET: handler does not exists\n", *fName, *fConnectionName)
			os.Exit(1)
		}
		fmt.Println("No errors")
		os.Exit(0)
	} else if *fCmd == "h_last_complete" {
		var handler cluster.Handler
		var exists bool
		var lastComplete time.Time
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				handler, exists, err = c.GetHandler(ctx, nil, *fName)

				if err != nil {
					return err
				}
				if !exists {
					return err
				}

				lastComplete, err = handler.LastComplete(ctx)

				return err
			})
		if err != nil {
			fmt.Printf("Last Complete Handler %v from %v error: %v\n", *fName, *fConnectionName, err)
			os.Exit(1)
		}
		if !exists {
			fmt.Printf("Last Complete Handler %v from %v error: handler does not exists\n", *fName, *fConnectionName)
			os.Exit(1)
		}
		fmt.Println(lastComplete)
		os.Exit(0)
	} else if *fCmd == "h_is_started" {
		var handler cluster.Handler
		var exists bool
		var isStarted bool
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				handler, exists, err = c.GetHandler(ctx, nil, *fName)

				if err != nil {
					return err
				}
				if !exists {
					return err
				}

				isStarted, err = handler.IsStarted(ctx)

				return err
			})
		if err != nil {
			fmt.Printf("IsStarted Handler %v from %v error: %v\n", *fName, *fConnectionName, err)
			os.Exit(1)
		}
		if !exists {
			fmt.Printf("IsStarted Handler %v from %v error: handler does not exists\n", *fName, *fConnectionName)
			os.Exit(1)
		}
		fmt.Println(isStarted)
		os.Exit(0)
	} else {
		fmt.Println("No comand")
	}
	//cg.
}
