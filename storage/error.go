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
}

// GenerateError -
func GenerateError(key int, a ...interface{}) *mft.Error {
	if text, ok := Errors[key]; ok {
		return mft.ErrorCS(key, fmt.Sprintf(text, a...))
	}
	panic(fmt.Sprintf("queue.GenerateError, error not found code:%v", key))
}
