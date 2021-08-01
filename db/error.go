package db

import (
	"fmt"

	"github.com/capella-pw/queue/cn"
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

	10401002: "SimpleIndex.IAdd: Check records segments fail i: %v, s:%v",
	10401052: "SimpleIndex.SAdd: Check records segments fail i: %v, s:%v",
	10401003: "SimpleIndex.IAdd: No valid data",
	10401053: "SimpleIndex.SAdd: No valid data",
	10401004: "SimpleIndex.IAdd: Promote for add segment",
	10401054: "SimpleIndex.SAdd: Promote for add segment",

	10401100: "ISimpleIndexBlock.Add: Lock fail wait",
	10401150: "SSimpleIndexBlock.Add: Lock fail wait",
	10401101: "ISimpleIndexBlockSegment.Add: Impossible situation index of start more then end block",
	10401151: "SSimpleIndexBlockSegment.Add: Impossible situation index of start more then end block",
	10401102: "ISimpleIndexBlockSegment.Add: RLock fail wait",
	10401152: "SSimpleIndexBlockSegment.Add: RLock fail wait",
	10401103: "ISimpleIndexBlockSegment.Add: Promote for add first element fail wait",
	10401153: "SSimpleIndexBlockSegment.Add: Promote for add first element fail wait",
	10401104: "ISimpleIndexBlockSegment.Add: Promote for split by count fail wait",
	10401154: "SSimpleIndexBlockSegment.Add: Promote for split by count fail wait",
	10401105: "ISimpleIndexBlockSegment.Add: Impossible situation index of start more then end block (locks)",
	10401155: "SSimpleIndexBlockSegment.Add: Impossible situation index of start more then end block (locks)",
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
func GenerateErrorForCapUser(user cn.CapUser, key int, a ...interface{}) *mft.Error {
	userName := "???"
	if user != nil {
		userName = "\"" + user.GetName() + "\""
	}
	if text, ok := Errors[key]; ok {
		return mft.ErrorCS(key, "[user:"+userName+"] "+fmt.Sprintf(text, a...))
	}
	panic(fmt.Sprintf("db.GenerateErrorForcn.CapUser, error not found code:%v", key))
}

// GenerateError -
func GenerateErrorForCapUserE(user cn.CapUser, key int, err error, a ...interface{}) *mft.Error {
	userName := "???"
	if user != nil {
		userName = "\"" + user.GetName() + "\""
	}
	if text, ok := Errors[key]; ok {
		return mft.ErrorCSE(key, "[user:"+userName+"] "+fmt.Sprintf(text, a...), err)
	}
	panic(fmt.Sprintf("db.GenerateErrorForcn.CapUserE, error not found code:%v", key))
}
