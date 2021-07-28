package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/capella-pw/queue/cluster"
	"github.com/capella-pw/queue/cluster/cap"
	"github.com/capella-pw/queue/cn"
	"github.com/capella-pw/queue/compress"
	"github.com/capella-pw/queue/security/authentication/basic"
	"github.com/capella-pw/queue/security/authorization"
)

func main() {

	compressor := compress.GeneratorCreate(7)

	cc := cap.CreateClusterConnection(compressor,
		cap.CreateConnection("http://localhost:8676", true, time.Second*5, 5, 5),
		basic.AuthType, "admin", basic.PasswordMarshal("Pa$$w0rd"),
		compress.Zip, compress.Zip, false)

	cc.Init()

	cl := cc.Cluster()

	// You may replace cc to cluster.ClusterUserName("") or nil
	err := basic.AddUseCluster(cl, cc, basic.UserSend{
		Name: "tech_user",
		Pwd:  "Pa$$w0rd_tech",
	})

	if err != nil {
		log.Fatalf("Fail to create `tech_user` user %v\n", err)
		os.Exit(1)
		return
	}

	err = authorization.AddUserUseCluster(cl, cc, "tech_user", false)
	if err != nil {
		log.Fatalf("Fail to create `tech_user` user as no admin %v\n", err)
		os.Exit(1)
		return
	}

	err = authorization.UserRuleSetUseCluster(cl, cc, "tech_user",
		cn.ClusterSelfObjectType, cn.GetQueueAction, "*", true)
	if err != nil {
		log.Fatalf("Fail to set `tech_user` object queue allow %v\n", err)
		os.Exit(1)
		return
	}

	err = basic.EnableUseCluster(cl, cc, "tech_user")

	if err != nil {
		log.Fatalf("Fail to enable `tech_user` user %v\n", err)
		os.Exit(1)
		return
	}
	// create ext cluster

	ccTech := cap.CreateClusterConnection(compressor,
		cap.CreateConnection("http://localhost:8676", true, time.Second*5, 5, 5),
		basic.AuthType, "tech_user", basic.PasswordMarshal("Pa$$w0rd_tech"),
		compress.Zip, compress.Zip,
		true) // !!!!!!!!!!!!!!! IMPORTANT TRUE !!!!!!!!!!!!!!!!!!!!!

	err = cl.AddExternalCluster(nil, cluster.ExternalClusterDescription{
		Name:   "ec_test_self_tech",
		Type:   cap.HttpExternalClusterType,
		Params: ccTech.ToJson(),
	})

	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
		return
	}

	q, exists, err := cl.GetQueue(nil, "ec_test_self_tech/test_queue")
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
	if len(msgs) < 1 {
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

	users, err := basic.GetUseCluster(cl, nil)
	if err != nil {
		log.Fatalf("Fail to get user bye new User `admin` %v\n", err)
		os.Exit(1)
		return
	}

	b, er0 := json.MarshalIndent(users, "", "  ")
	if er0 != nil {
		log.Fatalf("Fail to marshal User %v\n", er0)
		os.Exit(1)
		return
	}

	fmt.Println(string(b))

	fmt.Println("OK")

}
