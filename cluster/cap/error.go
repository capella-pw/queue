package cap

import (
	"fmt"

	"github.com/myfantasy/mft"
)

// Errors codes and description
var Errors map[int]string = map[int]string{
	10190000: "ConnectionFromJson: Unmarshal fail",

	10190001: "Connection.DoRawQuery: request create error server: %v",
	10190002: "Connection.DoRawQuery: send request error server: %v",
	10190003: "Connection.DoRawQuery: send request read body fail server: %v",

	10190100: "ClusterConnection.CallFunc: Marshal request fail",
	10190101: "ClusterConnection.CallFunc: Compress request fail",
	10190102: "ClusterConnection.CallFunc: Send request fail",
	10190103: "ClusterConnection.CallFunc: Responce code is not 200 responce code is: %v body: %v",
	10190104: "ClusterConnection.CallFunc: Restore responce fail",
	10190106: "ClusterConnection.CallFunc: Unmarshal responce fail",

	10190200: "ClusterConnection.ToJson: marshal error",

	10190300: "HttpExternalClusterNewGenerator: unmarshal error ec.name: %v",
	10190301: "HttpExternalClusterNewGenerator: connection should be set (ClusterConnection.Connection) ec.name: %v",
	10190302: "HttpExternalClusterNewGenerator: Connection.QueryWait should be > 0 ec.name: %v value: %v",
	10190303: "HttpExternalClusterNewGenerator: Connection.Server should be set ec.name: %v",
	10190304: "HttpExternalClusterNewGenerator: encrypt AuthentificationInfo fail ec.name: %v",
	10190305: "HttpExternalClusterNewGenerator: DecryptAlg ia not correct ec.name: %v. Actual %v != Expired %v",
	10190306: "HttpExternalClusterNewGenerator: params marshal fail ec.name: %v",

	10190400: "HttpExternalClusterLoadGenerator: unmarshal error ec.name: %v",
	10190401: "HttpExternalClusterLoadGenerator: connection should be set (ClusterConnection.Connection) ec.name: %v",
	10190402: "HttpExternalClusterLoadGenerator: decrypt AuthentificationInfo fail ec.name: %v",

	10191000: "ConGroup.ToJson: marshal error",

	10191010: "ConGroupFromJson: unmarshal error",

	10191100: "ConGroup.FuncDO: send fail %v of %v",
	10191101: "ConGroup.FuncDO: Acquire semaphore send fail %v of %v",
	10191102: "ConGroup.FuncDO: Execute on cluser %v failed",
	10191103: "ConGroup.FuncDO: Priority Group with name %v does not exists",
	10191104: "ConGroup.FuncDO: Cluster with name %v does not exists; done %v of %v",

	10191130: "ConGroup.FuncDOName: Cluster with name %v does not exists",

	10191200: "QueueAddUnique: Queue `%v` does not exists",
	10191201: "QueueAddUnique: Queue `%v` get error",
	10191202: "QueueAddUnique: Queue `%v` AddUnique error",

	10191210: "QueueAddUniqueList: Queue `%v` does not exists",
	10191211: "QueueAddUniqueList: Queue `%v` get error",
	10191212: "QueueAddUniqueList: Queue `%v` AddUnique error",
}

// GenerateError -
func GenerateError(key int, a ...interface{}) *mft.Error {
	if text, ok := Errors[key]; ok {
		return mft.ErrorCS(key, fmt.Sprintf(text, a...))
	}
	panic(fmt.Sprintf("cap.GenerateError, error not found code:%v", key))
}

// GenerateErrorE -
func GenerateErrorE(key int, err error, a ...interface{}) *mft.Error {
	if text, ok := Errors[key]; ok {
		return mft.ErrorCSE(key, fmt.Sprintf(text, a...), err)
	}
	panic(fmt.Sprintf("cap.GenerateErrorE, error not found code:%v error:%v", key, err))
}
