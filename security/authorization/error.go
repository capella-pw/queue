package authorization

import (
	"fmt"

	"github.com/capella-pw/queue/cluster"
	"github.com/myfantasy/mft"
)

// Errors codes and description
var Errors map[int]string = map[int]string{
	10310000: "Security.OnChange: fail",

	10310100: "SecurityATRZ.AddUser: user %v already exists",
	10300101: "SecurityATRZ.AddUser: Permission check fail",
	10300102: "SecurityATRZ.AddUser: Permission denied",

	10310200: "SecurityATRZ.SetUserAdmin: user %v does not exists",
	10300201: "SecurityATRZ.SetUserAdmin: Permission check fail",
	10300202: "SecurityATRZ.SetUserAdmin: Permission denied",

	10310300: "SecurityATRZ.DropUser: user %v does not exists",
	10300301: "SecurityATRZ.DropUser: Permission check fail",
	10300302: "SecurityATRZ.DropUser: Permission denied",

	10310400: "SecurityATRZ.UserRuleSet: user %v does not exists",
	10300401: "SecurityATRZ.UserRuleSet: Permission check fail",
	10300402: "SecurityATRZ.UserRuleSet: Permission denied",

	10310500: "SecurityATRZ.UserRuleDrop: user %v does not exists",
	10300501: "SecurityATRZ.UserRuleDrop: Permission check fail",
	10300502: "SecurityATRZ.UserRuleDrop: Permission denied",

	10310600: "StorageOnChangeFuncGenerator: fail generate json",
	10310601: "StorageOnChangeFuncGenerator: fail save data",

	10310700: "SecurityATRZ.StorageLoad: fail load data",
	10310701: "SecurityATRZ.StorageLoad: fail unmarshal data",

	10310800: "SecurityATRZ.Get: fail marshal",
	10310801: "SecurityATRZ.Get: fail unmarshal",
	10310802: "SecurityATRZ.Get: Permission check fail",
	10310803: "SecurityATRZ.Get: Permission denied",
}

// GenerateError -
func GenerateError(key int, a ...interface{}) *mft.Error {
	if text, ok := Errors[key]; ok {
		return mft.ErrorCS(key, fmt.Sprintf(text, a...))
	}
	panic(fmt.Sprintf("authorization.GenerateError, error not found code:%v", key))
}

// GenerateError -
func GenerateErrorForClusterUser(user cluster.ClusterUser, key int, a ...interface{}) *mft.Error {
	userName := "???"
	if user != nil {
		userName = "\"" + user.GetName() + "\""
	}
	if text, ok := Errors[key]; ok {
		return mft.ErrorCS(key, "[user:"+userName+"] "+fmt.Sprintf(text, a...))
	}
	panic(fmt.Sprintf("authorization.GenerateErrorForClusterUser, error not found code:%v", key))
}

// GenerateErrorE -
func GenerateErrorE(key int, err error, a ...interface{}) *mft.Error {
	if text, ok := Errors[key]; ok {
		return mft.ErrorCSE(key, fmt.Sprintf(text, a...), err)
	}
	panic(fmt.Sprintf("authorization.GenerateErrorE, error not found code:%v error:%v", key, err))
}

// GenerateError -
func GenerateErrorForClusterUserE(user cluster.ClusterUser, key int, err error, a ...interface{}) *mft.Error {
	userName := "???"
	if user != nil {
		userName = "\"" + user.GetName() + "\""
	}
	if text, ok := Errors[key]; ok {
		return mft.ErrorCSE(key, "[user:"+userName+"] "+fmt.Sprintf(text, a...), err)
	}
	panic(fmt.Sprintf("authorization.GenerateErrorForClusterUserE, error not found code:%v", key))
}
