package storage

import (
	"fmt"

	"github.com/myfantasy/mft"
)

// Errors codes and description
var Errors map[int]string = map[int]string{
	10000000: "MapSorage: not found name: %v",
	10000001: "MapSorage: name exists: %v",
	10000002: "File: error: %v",
	10000003: "Mkdir error: path: %v",

	10001000: "Cluster.Create: Lock mutex fail wait",
	10001001: "Cluster.Create: storage type %v is not exists",
	10001002: "Cluster.Create: mount %v is not exists",
}

// GenerateError -
func GenerateError(key int, a ...interface{}) *mft.Error {
	if text, ok := Errors[key]; ok {
		return mft.ErrorCS(key, fmt.Sprintf(text, a...))
	}
	panic(fmt.Sprintf("storage.GenerateError, error not found code:%v", key))
}

// GenerateErrorE -
func GenerateErrorE(key int, err error, a ...interface{}) *mft.Error {
	if text, ok := Errors[key]; ok {
		return mft.ErrorCSE(key, fmt.Sprintf(text, a...), err)
	}
	panic(fmt.Sprintf("storage.GenerateErrorE, error not found code:%v error:%v", key, err))
}
