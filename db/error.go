package db

import (
	"fmt"

	"github.com/myfantasy/mft"
)

// Errors codes and description
var Errors map[int]string = map[int]string{
	10400000: "IRecord.ToJson: fail marshal",
	10400001: "SRecord.ToJson: fail marshal",
	10400002: "IItem.ToJson: fail marshal",
	10400003: "SItem.ToJson: fail marshal",

	10401000: "SimpleIndex.IAdd: read lock fail wait",
	10401001: "SimpleIndex.IAdd: Permission denied",

	10401050: "SimpleIndex.SAdd: read lock fail wait",
	10401051: "SimpleIndex.SAdd: Permission denied",
}

// GenerateError -
func GenerateError(key int, a ...interface{}) *mft.Error {
	if text, ok := Errors[key]; ok {
		return mft.ErrorCS(key, fmt.Sprintf(text, a...))
	}
	panic(fmt.Sprintf("db.GenerateError, error not found code:%v", key))
}

// GenerateErrorE -
func GenerateErrorE(key int, err error, a ...interface{}) *mft.Error {
	if text, ok := Errors[key]; ok {
		return mft.ErrorCSE(key, fmt.Sprintf(text, a...), err)
	}
	panic(fmt.Sprintf("db.GenerateErrorE, error not found code:%v error:%v", key, err))
}

// GenerateError -
func GenerateErrorForDBUser(user DBUser, key int, a ...interface{}) *mft.Error {
	userName := "???"
	if user != nil {
		userName = "\"" + user.GetName() + "\""
	}
	if text, ok := Errors[key]; ok {
		return mft.ErrorCS(key, "[user:"+userName+"] "+fmt.Sprintf(text, a...))
	}
	panic(fmt.Sprintf("db.GenerateErrorForDBUser, error not found code:%v", key))
}

// GenerateError -
func GenerateErrorForDBUserE(user DBUser, key int, err error, a ...interface{}) *mft.Error {
	userName := "???"
	if user != nil {
		userName = "\"" + user.GetName() + "\""
	}
	if text, ok := Errors[key]; ok {
		return mft.ErrorCSE(key, "[user:"+userName+"] "+fmt.Sprintf(text, a...), err)
	}
	panic(fmt.Sprintf("db.GenerateErrorForDBUserE, error not found code:%v", key))
}
