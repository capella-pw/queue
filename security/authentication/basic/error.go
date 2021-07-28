package basic

import (
	"fmt"

	"github.com/capella-pw/queue/cn"
	"github.com/myfantasy/mft"
)

// Errors codes and description
var Errors map[int]string = map[int]string{
	10300000: "Security.OnChange: fail",

	10300100: "Security.Add: User `%v` is already exists",
	10300101: "Security.Add: Permission denied",
	10300102: "Security.Add: Permission check fail",

	10300200: "Security.Update: User `%v` does not exists",
	10300201: "Security.Update: Permission denied",
	10300202: "Security.Update: Permission check fail",

	10300300: "Security.Enable: User `%v` does not exists",
	10300301: "Security.Enable: Permission denied",
	10300302: "Security.Enable: Permission check fail",

	10300400: "Security.Disable: User `%v` does not exists",
	10300401: "Security.Disable: Permission denied",
	10300402: "Security.Disable: Permission check fail",

	10300500: "Security.Drop: User `%v` does not exists",
	10300501: "Security.Drop: Permission denied",
	10300502: "Security.Drop: Permission check fail",

	10300600: "StorageOnChangeFuncGenerator: fail generate json",
	10300601: "StorageOnChangeFuncGenerator: fail save data",
	10300700: "SecurityATRZ.StorageLoad: fail load data",
	10300701: "SecurityATRZ.StorageLoad: fail unmarshal data",

	10300800: "Security.Get: Permission denied",
	10300801: "Security.Get: Permission check fail",

	10300900: "Security.CheckAuthFunc: Unkown user `%v`",
	10300901: "Security.CheckAuthFunc: User is disabled `%v`",
	10300902: "Security.CheckAuthFunc: Auth user fail Unmarshal",
	10300903: "Security.CheckAuthFunc: Pwd check fail for user `%v`",
	10300904: "Security.CheckAuthFunc: Permission Impresonate fail",
	10300905: "Security.CheckAuthFunc: Permission Impresonate denied: request: `%v`, Service: `%v`",
	10300906: "Security.CheckAuthFunc: Request is NIL",
	10300907: "Security.CheckAuthFunc: Incorrect AuthType",

	10301000: "PasswordMarshal: Fail marshal PWD",
}

// GenerateError -
func GenerateError(key int, a ...interface{}) *mft.Error {
	if text, ok := Errors[key]; ok {
		return mft.ErrorCS(key, fmt.Sprintf(text, a...))
	}
	panic(fmt.Sprintf("basic.GenerateError, error not found code:%v", key))
}

// GenerateError -
func GenerateErrorForClusterUser(user cn.CapUser, key int, a ...interface{}) *mft.Error {
	userName := "???"
	if user != nil {
		userName = "\"" + user.GetName() + "\""
	}
	if text, ok := Errors[key]; ok {
		return mft.ErrorCS(key, "[user:"+userName+"] "+fmt.Sprintf(text, a...))
	}
	panic(fmt.Sprintf("basic.GenerateErrorForClusterUser, error not found code:%v", key))
}

// GenerateErrorE -
func GenerateErrorE(key int, err error, a ...interface{}) *mft.Error {
	if text, ok := Errors[key]; ok {
		return mft.ErrorCSE(key, fmt.Sprintf(text, a...), err)
	}
	panic(fmt.Sprintf("basic.GenerateErrorE, error not found code:%v error:%v", key, err))
}

// GenerateError -
func GenerateErrorForClusterUserE(user cn.CapUser, key int, err error, a ...interface{}) *mft.Error {
	userName := "???"
	if user != nil {
		userName = "\"" + user.GetName() + "\""
	}
	if text, ok := Errors[key]; ok {
		return mft.ErrorCSE(key, "[user:"+userName+"] "+fmt.Sprintf(text, a...), err)
	}
	panic(fmt.Sprintf("basic.GenerateErrorForClusterUserE, error not found code:%v", key))
}
