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
	cluster_name_get - gets cluster name (requare "qty")

	next_id - gets next id from cluster
	next_ids - gets next id from cluster

	q_add - creates new queue (requare "p" or "pf")
		example: ./cap -cmd q_add -pf new_queue.json
	q_drop - drops queue (requare "name")
	q_descr - gets queue description (requare "name")
	q_list - gets queues list

	exc_add - creates external cluster
		example: ./cap -cmd exc_add -pf new_external_cluster.json
	exc_drop - drops external cluster (requare "name")
	exc_descr - gets external cluster description (requare "name")
	exc_list - gets external clusters list
	`)

var fParamsFileName = flag.String("pf", "",
	`Path to file with params in JSON format (if "-p" set then used "-p")`)

var fParams = flag.String("p", "",
	`Params in JSON format (if "-pf" set then "-pf" will be ignored)`)

var fName = flag.String("name", "",
	`Object name`)

var fQty = flag.Int("qty", 0,
	`Quantity in request`)

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
				err = c.Ping(nil)
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
				err = c.SetName(nil, *fName)
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
				name, err = c.GetName(nil)
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
				id, err = c.GetNextId(nil)
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
				ids, err = c.GetNextIds(nil, *fQty)
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
				err = c.AddQueue(nil, qd)
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
				err = c.DropQueue(nil, *fName)
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
				queueDescription, err = c.GetQueueDescription(nil, *fName)
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
				names, err = c.GetQueuesList(nil)
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
	} else if *fCmd == "exc_add" {
		var ecd cluster.ExternalClusterDescription
		GetParams(&ecd)
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				err = c.AddExternalCluster(nil, ecd)
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
				err = c.DropExternalCluster(nil, *fName)
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
				clusterParams, err = c.GetExternalClusterDescription(nil, *fName)
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
				names, err = c.GetExternalClustersList(nil)
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
	} else {
		fmt.Println("No comand")
	}
	//cg.
}
