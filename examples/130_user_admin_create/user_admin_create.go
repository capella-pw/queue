package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/capella-pw/queue/cluster/cap"
	"github.com/capella-pw/queue/compress"
	"github.com/capella-pw/queue/security/authentication/basic"
	"github.com/capella-pw/queue/security/authorization"
)

func main() {

	compressor := compress.GeneratorCreate(7)

	cc := cap.CreateClusterConnection(compressor,
		cap.CreateConnection("http://localhost:8676", true, time.Second*5, 5, 5),
		"", "", nil, compress.Zip, compress.Zip, false)

	cc.Init()

	cl := cc.Cluster()

	// You may replace cc to cluster.ClusterUserName("") or nil
	err := basic.AddUseCluster(cl, cc, basic.UserSend{
		Name: "admin",
		Pwd:  "Pa$$w0rd",
	})

	if err != nil {
		log.Fatalf("Fail to create user %v\n", err)
		os.Exit(1)
		return
	}

	err = authorization.AddUserUseCluster(cl, cc, "admin", true)
	if err != nil {
		log.Fatalf("Fail to set `admin` user as admin %v\n", err)
		os.Exit(1)
		return
	}

	err = basic.EnableUseCluster(cl, cc, "admin")
	if err != nil {
		log.Fatalf("Fail to enable user %v\n", err)
		os.Exit(1)
		return
	}

	ccAdm := cap.CreateClusterConnection(compressor,
		cap.CreateConnection("http://localhost:8676", true, time.Second*5, 5, 5),
		basic.AuthType, "admin", basic.PasswordMarshal("Pa$$w0rd"), compress.Zip, compress.Zip,
		false)

	ccAdm.Init()

	clAdm := ccAdm.Cluster()

	err = basic.DisableUseCluster(clAdm, ccAdm, "")
	if err != nil {
		log.Fatalf("Fail to disable empty name user (``) %v\n", err)
		os.Exit(1)
		return
	}

	users, err := basic.GetUseCluster(clAdm, nil)
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

	err = basic.AddUseCluster(cl, cc, basic.UserSend{
		Name: "admin2",
		Pwd:  "Pa$$w0rd",
	})

	if err == nil {
		log.Fatalf("Fail to create user `admin2` error should be with code 10300901\n")
		os.Exit(1)
		return
	} else if err.Code != 10300901 {
		log.Fatalf("Fail to create user `admin2` %v\n", err)
		os.Exit(1)
		return
	}

	err = basic.EnableUseCluster(clAdm, ccAdm, "")
	if err != nil {
		log.Fatalf("Fail to enable empty name user (``) %v\n", err)
		os.Exit(1)
		return
	}

	fmt.Println("OK")

}
