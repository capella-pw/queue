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
	"github.com/capella-pw/queue/security/authentication/basic"
	"github.com/capella-pw/queue/security/authorization"
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

var fIsAdmin = flag.Bool("is_admin", false,
	`Sets user as "ADMIN"`)

var fCmd = flag.String("cmd", "",
	`Command

	ping - ping cluster

	create_user - creates basic user (requare "un" and password)
	update_user - update basic user (requare "un" and password)
	enable_user - enables basic user (requare "un)
	disable_user - disables basic user (requare "un)
	drop_user - drops basic user (requare "un)
	get_users - gets all users
	
	add_sec_user - adds user to authorization (requare "un" and "is_admin")
	set_is_admin_sec_user - sets for authorization user is_admin flag (requare "un" and "is_admin")
	drop_sec_user - drops for authorization user (requare "un")
	set_rule - sets rule for authorization user (requare "un", "a", "ot", "act" and "on")
	drop_rule - drop rule for authorization user (requare "un", "ot", "act" and "on")
	get_sec_users - gets all authorization users
	`)

var fUserName = flag.String("un", "",
	`User name`)
var fUserPwd = flag.String("pwd", "",
	`User Password
	works only when "-pwd_flag true"`)
var fUserPwdFlag = flag.Bool("pwd_flag", false,
	`Enable User Password from flags`)

var fAllowed = flag.Bool("a", true,
	`Permission is alloweds`)
var fObjectType = flag.String("ot", "*",
	`Object type`)
var fAction = flag.String("act", "*",
	`Action on object type`)
var fObjectName = flag.String("on", "*",
	`Object name`)

func getPwd() string {

	if *fUserPwdFlag {
		return *fUserPwd
	}

	var pwd string

	fmt.Print("Enter your pwd: ")
	fmt.Scanf("%s", &pwd)

	return pwd
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
	} else if *fCmd == "create_user" {
		pwd := getPwd()
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				err = basic.AddUseCluster(c, nil, basic.UserSend{
					Name: *fUserName,
					Pwd:  pwd,
				})
				return err
			})
		if err != nil {
			fmt.Printf("Create user %v on %v error: %v\n", *fUserName, *fConnectionName, err)
			os.Exit(1)
		}
		fmt.Println("OK")
		os.Exit(0)
	} else if *fCmd == "update_user" {
		pwd := getPwd()
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				err = basic.UpdateUseCluster(c, nil, basic.UserSend{
					Name: *fUserName,
					Pwd:  pwd,
				})
				return err
			})
		if err != nil {
			fmt.Printf("Updae user %v on %v error: %v\n", *fUserName, *fConnectionName, err)
			os.Exit(1)
		}
		fmt.Println("OK")
		os.Exit(0)
	} else if *fCmd == "enable_user" {
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				err = basic.EnableUseCluster(c, nil, *fUserName)
				return err
			})
		if err != nil {
			fmt.Printf("Enable user %v on %v error: %v\n", *fUserName, *fConnectionName, err)
			os.Exit(1)
		}
		fmt.Println("OK")
		os.Exit(0)
	} else if *fCmd == "disable_user" {
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				err = basic.DisableUseCluster(c, nil, *fUserName)
				return err
			})
		if err != nil {
			fmt.Printf("Disable user %v on %v error: %v\n", *fUserName, *fConnectionName, err)
			os.Exit(1)
		}
		fmt.Println("OK")
		os.Exit(0)
	} else if *fCmd == "drop_user" {
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				err = basic.DropUseCluster(c, nil, *fUserName)
				return err
			})
		if err != nil {
			fmt.Printf("Drop user %v on %v error: %v\n", *fUserName, *fConnectionName, err)
			os.Exit(1)
		}
		fmt.Println("OK")
		os.Exit(0)
	} else if *fCmd == "get_users" {
		var users []basic.UserCut
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				users, err = basic.GetUseCluster(c, nil)
				return err
			})
		if err != nil {
			fmt.Printf("Drop user %v on %v error: %v\n", *fUserName, *fConnectionName, err)
			os.Exit(1)
		}
		bt, er0 := json.MarshalIndent(users, "", "  ")
		if er0 != nil {
			log.Fatalf("Marhak users from %v fail: %v\n", *fConnectionName, er0)
		}
		fmt.Println(string(bt))
		os.Exit(0)
	} else if *fCmd == "add_sec_user" {
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				err = authorization.AddUserUseCluster(c, nil, *fUserName, *fIsAdmin)
				return err
			})
		if err != nil {
			fmt.Printf("Add authorization user %v on %v error: %v\n", *fUserName, *fConnectionName, err)
			os.Exit(1)
		}
		fmt.Println("OK")
		os.Exit(0)
	} else if *fCmd == "set_is_admin_sec_user" {
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				err = authorization.SetUserAdminUseCluster(c, nil, *fUserName, *fIsAdmin)
				return err
			})
		if err != nil {
			fmt.Printf("Set for authorization user %v on %v flag is_idmin: %v error: %v\n",
				*fUserName, *fConnectionName, *fIsAdmin, err)
			os.Exit(1)
		}
		fmt.Println("OK")
		os.Exit(0)
	} else if *fCmd == "drop_sec_user" {
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				err = authorization.DropUserUseCluster(c, nil, *fUserName)
				return err
			})
		if err != nil {
			fmt.Printf("Drop authorization user %v on %v error: %v\n",
				*fUserName, *fConnectionName, err)
			os.Exit(1)
		}
		fmt.Println("OK")
		os.Exit(0)
	} else if *fCmd == "set_rule" {
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				err = authorization.UserRuleSetUseCluster(c, nil, *fUserName,
					*fObjectType, *fAction, *fObjectName, *fAllowed)
				return err
			})
		if err != nil {
			fmt.Printf("Set rule for authorization user %v on %v error: %v\n",
				*fUserName, *fConnectionName, err)
			os.Exit(1)
		}
		fmt.Println("OK")
		os.Exit(0)
	} else if *fCmd == "drop_rule" {
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				err = authorization.UserRuleDropUseCluster(c, nil, *fUserName,
					*fObjectType, *fAction, *fObjectName)
				return err
			})
		if err != nil {
			fmt.Printf("Drop rule for authorization user %v on %v error: %v\n",
				*fUserName, *fConnectionName, err)
			os.Exit(1)
		}
		fmt.Println("OK")
		os.Exit(0)
	} else if *fCmd == "get_sec_users" {
		var sOut *authorization.SecurityATRZ
		err = cg.FuncDOName(ctx, *fConnectionName,
			func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
				sOut, err = authorization.GetUseCluster(c, nil)
				return err
			})
		if err != nil {
			fmt.Printf("Drop rule for authorization user %v on %v error: %v\n",
				*fUserName, *fConnectionName, err)
			os.Exit(1)
		}
		bt, er0 := json.MarshalIndent(sOut, "", "  ")
		if er0 != nil {
			log.Fatalf("Marshal authorization users from %v fail: %v\n", *fConnectionName, er0)
		}
		fmt.Println(string(bt))
		os.Exit(0)
	} else {
		fmt.Println("No comand")
	}
	//cg.
}
