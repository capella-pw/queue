package cluster

import (
	"fmt"

	"github.com/myfantasy/mft"
)

// Errors codes and description
var Errors map[int]string = map[int]string{
	10100000: "SimpleCluster.GetName: Permission denied",

	10101000: "SimpleCluster.SetName: Permission denied",

	10101100: "SimpleCluster.SetValueInternal: save error",

	10101200: "ExternalAbstractCluster.SetValueInternal: not allowed",
	10101201: "ExternalAbstractCluster.OnChange: not allowed",

	10101300: "SimpleCluster.OnChange: fail call save func",

	10102000: "SimpleCluster.GetFullStruct: Permission denied",
	10102001: "SimpleCluster.GetFullStruct: marshal fail",

	10103000: "SimpleCluster.LoadFullStruct: Permission denied",
	10103001: "SimpleCluster.LoadFullStruct: unmarshal fail",
	10103002: "SimpleCluster.LoadFullStruct: load queue `%v` fail. Type `%v` is not exists.",
	10103003: "SimpleCluster.LoadFullStruct: load queue `%v` fail. Type `%v`.",
	10103004: "SimpleCluster.LoadFullStruct: load external cluster `%v` fail. Type `%v` is not exists.",
	10103005: "SimpleCluster.LoadFullStruct: load external cluster `%v` fail. Type `%v`.",
	10103006: "SimpleCluster.LoadFullStruct: load handler `%v` fail. Type `%v` is not exists.",
	10103007: "SimpleCluster.LoadFullStruct: load handler `%v` fail. Type `%v`.",
	10103008: "SimpleCluster.LoadFullStruct: load handler `%v` RUN fail. Type `%v`.",

	10104000: "SimpleCluster.AddQueue: Permission denied",
	10104001: "SimpleCluster.AddQueue: not exists queue type: %v",
	10104002: "SimpleCluster.AddQueue: queue with name %v already exists",

	10105000: "SimppleQueueNewGenerator: queue name is not set",
	10105001: "SimppleQueueNewGenerator: fail to unmarshal params",
	10105002: "SimppleQueueNewGenerator: meta storage create error queue:%v",
	10105003: "SimppleQueueNewGenerator: subscriber storage create error queue:%v",
	10105004: "SimppleQueueNewGenerator: block storage create error queue:%v block:%v",
	10105005: "SimppleQueueNewGenerator: queue first save error queue:%v",

	10105100: "SimpleQueueParams.ToJson: marshal error",

	10106000: "SimppleQueueLoadGenerator: CreateOnLoad queue %v can't be load",
	10106001: "SimppleQueueLoadGenerator: fail to unmarshal params",
	10106002: "SimppleQueueLoadGenerator: meta storage create error queue:%v",
	10106003: "SimppleQueueLoadGenerator: subscriber storage create error queue:%v",
	10106004: "SimppleQueueLoadGenerator: block storage create error queue:%v block:%v",
	10106005: "SimppleQueueLoadGenerator: queue load error queue:%v",

	10107000: "ResponceBody.MustMarshal: Fail marshal",
	10107001: "ResponceBodySoftUnmarshal: Fail unmarshal",
	10107002: "ResponceBody.UnmarshalInnerObject: Fail unmarshal",
	10107003: "RequestBody.UnmarshalInnerObject: Fail unmarshal",
	10107004: "MarshalRequestMust: Fail marshal",
	10107005: "MarshalResponceMust: Fail marshal",

	10107100: "CallFuncInCluster: Unknown operation %v",
	10107101: "UnmarshalInnerObjectAndFindQueue: Queue is not exists %v",
	10107102: "CallFuncInCluster: Cluster is not exists %v",
	10107103: "UnmarshalInnerObjectAndFindHandler: Handler is not exists %v",

	10108000: "SimpleCluster.DropQueue: Permission denied",

	10109000: "SimpleCluster.GetQueueDescription: Permission denied",
	10109001: "SimpleCluster.GetQueueDescription: Queue is not exists",

	10110000: "SimpleCluster.GetQueuesList: Permission denied",

	10111000: "SimpleCluster.GetQueue: Permission denied",
	10111001: "SimpleCluster.GetQueue: Get subcluster `%v` fail (queue: `%v`)",
	10111002: "SimpleCluster.GetQueue: Subcluster `%v` does not exist (queue: `%v`)",
	10111003: "SimpleCluster.GetQueue: Subcluster `%v` fail get GetQueue `%v`",

	10112000: "SimpleCluster.AddExternalCluster: Permission denied",
	10112001: "SimpleCluster.AddExternalCluster: not exists external cluster type: %v",

	10113000: "SimpleCluster.DropExternalCluster: Permission denied",

	10114000: "SimpleCluster.GetExternalClusterDescription: Permission denied",
	10114001: "SimpleCluster.GetExternalClusterDescription: External cluster is not exists",

	10115000: "SimpleCluster.GetExternalClustersList: Permission denied",

	10116000: "SimpleCluster.GetExternalCluster: Permission denied",
	10116001: "SimpleCluster.GetExternalCluster: Get subcluster `%v` fail (next: `%v`)",
	10116002: "SimpleCluster.GetExternalCluster: Subcluster `%v` does not exist (next: `%v`)",
	10116003: "SimpleCluster.GetExternalCluster: Subcluster `%v` fail get GetExternalCluster `%v`",

	10117000: "SimpleCluster.AddHandler: Permission denied",
	10117001: "SimpleCluster.AddHandler: not exists hanler type: %v",
	10117002: "SimpleCluster.AddHandler: handler with name %v already exists",

	10117100: "SimpleCluster.DropHandler: Permission denied",
	10117101: "SimpleCluster.DropHandler: Stop fail",

	10117200: "SimpleCluster.GetHandlerDescription: Permission denied",
	10117201: "SimpleCluster.GetHandlerDescription: Handler is not exists %v",

	10117300: "SimpleCluster.GetHandlersList: Permission denied",

	10117400: "RegularlySaveHandler.Start.go: Queue `%v` get error",
	10117401: "RegularlySaveHandler.Start.go: Queue `%v` does not exists",
	10117402: "RegularlySaveHandler.Start.go: Queue `%v` save fail",
	10117403: "RegularlySaveHandler.Start: Save cluster fail on %v",
	10117404: "RegularlySaveHandler.Stop: Save cluster fail on %v",

	10117500: "RegularlySaveNewGenerator: len(QueueNames): %v != 1",
	10117501: "RegularlySaveNewGenerator: unmarhal params error",
	10117502: "RegularlySaveNewGenerator: Interval: %v should be >0",
	10117503: "RegularlySaveNewGenerator: Wait: %v should be >0",

	10117600: "RegularlySaveNewGenerator: len(QueueNames): %v != 1",
	10117601: "RegularlySaveNewGenerator: unmarhal params error",

	10117700: "RegularlySaveHandlerParams.ToJson: marshal error",

	10117800: "SimpleCluster.GetHandler: Permission denied",

	10117900: "CopyUniqueNewGenerator: len(QueueNames): %v != 2",
	10117901: "CopyUniqueNewGenerator: unmarhal params error",
	10117902: "CopyUniqueNewGenerator: Interval: %v should be >0",
	10117903: "CopyUniqueNewGenerator: Wait: %v should be >0",

	10118000: "CopyUniqueHandler.Start: SRC Queue `%v` get error",
	10118001: "CopyUniqueHandler.Start: SRC Queue `%v` does not exists",
	10118003: "CopyUniqueHandler.Start: DST Queue `%v` get error",
	10118004: "CopyUniqueHandler.Start: DST Queue `%v` does not exists",
	10118005: "CopyUniqueHandler.Start.go: Copy error SrcQueue `%v` DstQueue `%v` Name `%v`",
	10118006: "CopyUniqueHandler.Start: Save cluster fail on %v",
	10118007: "CopyUniqueHandler.Stop: Save cluster fail on %v",

	10118100: "CopyUniqueLoadGenerator: len(QueueNames): %v != 2",
	10118101: "CopyUniqueLoadGenerator: unmarhal params error",

	10118200: "BlockDeleteHandler.Start.go: Queue `%v` get error",
	10118201: "BlockDeleteHandler.Start.go: Queue `%v` does not exists",
	10118202: "BlockDeleteHandler.Start.go: Queue `%v` queue is not queue.SimpleQueue",
	10118203: "BlockDeleteHandler.Start.go: Queue `%v` SetDelete fail",
	10118204: "BlockDeleteHandler.Start.go: Queue `%v` DeleteBlocks fail",
	10118205: "BlockDeleteHandler.Start: Save cluster fail on %v",
	10118206: "BlockDeleteHandler.Stop: Save cluster fail on %v",

	10118220: "BlockDeleteHandler: len(QueueNames): %v != 1",
	10118221: "BlockDeleteHandler: unmarhal params error",
	10118222: "BlockDeleteHandler: Interval: %v should be >0",
	10118223: "BlockDeleteHandler: WaitMark: %v should be >0",
	10118224: "BlockDeleteHandler: WaitDelete: %v should be >0",
	10118225: "BlockDeleteHandler: LimitDelete: %v should be >0",
	10118226: "BlockDeleteHandler: StorageTime: %v should be >0",

	10118240: "BlockDeleteHandler: len(QueueNames): %v != 1",
	10118241: "BlockDeleteHandler: unmarhal params error",

	10118260: "BlockDeleteHandler.ToJson: marshal error",

	10118300: "BlockUnloadHandler.Start.go: Queue `%v` get error",
	10118301: "BlockUnloadHandler.Start.go: Queue `%v` does not exists",
	10118302: "BlockUnloadHandler.Start.go: Queue `%v` queue is not queue.SimpleQueue",
	10118303: "BlockUnloadHandler.Start.go: Queue `%v` SetUnload fail",
	10118304: "BlockUnloadHandler.Start: Save cluster fail on %v",
	10118305: "BlockUnloadHandler.Stop: Save cluster fail on %v",

	10118320: "BlockUnloadHandler: len(QueueNames): %v != 1",
	10118321: "BlockUnloadHandler: unmarhal params error",
	10118322: "BlockUnloadHandler: Interval: %v should be >0",
	10118323: "BlockUnloadHandler: Wait: %v should be >0",
	10118324: "BlockUnloadHandler: StorageMemoryTime: %v should be >0",
	10118325: "BlockUnloadHandler: StorageLastLoadTime: %v should be >0",

	10118340: "BlockUnloadHandler: len(QueueNames): %v != 1",
	10118341: "BlockUnloadHandler: unmarhal params error",

	10118360: "BlockUnloadHandler.ToJson: marshal error",

	10118400: "BlockMarkHandler.Start.go: Queue `%v` get error",
	10118401: "BlockMarkHandler.Start.go: Queue `%v` does not exists",
	10118402: "BlockMarkHandler.Start.go: Queue `%v` queue is not queue.SimpleQueue",
	10118403: "BlockMarkHandler.Start.go: Queue `%v` SetMarks fail",
	10118404: "BlockMarkHandler.Start.go: Queue `%v` UpdateMarks fail",
	10118405: "BlockMarkHandler.Start: Save cluster fail on %v",
	10118406: "BlockMarkHandler.Stop: Save cluster fail on %v",

	10118420: "BlockMarkHandler: len(QueueNames): %v != 1",
	10118421: "BlockMarkHandler: unmarhal params error",
	10118422: "BlockMarkHandler: Interval: %v should be >0",
	10118423: "BlockMarkHandler: WaitMark: %v should be >0",
	10118424: "BlockMarkHandler: WaitUpdate: %v should be >0",
	10118425: "BlockMarkHandler: LimitUpdateBlocks: %v should be >0",
	10118426: "BlockMarkHandler: len(rshp.Conditions): 0 should be >0",
	10118427: "BlockMarkHandler: [%v] Mark: %v: Condition.FromTime: %v should be >=0",
	10118428: "BlockMarkHandler: [%v] Mark: %v: should be Condition.FromTime (%v) < Condition.ToTime (%v)",

	10118440: "BlockMarkHandler: len(QueueNames): %v != 1",
	10118441: "BlockMarkHandler: unmarhal params error",

	10118460: "BlockMarkHandler.ToJson: marshal error",

	// ----
	10120000: "ClusterService.Call: Current server time less then client time. Server:%v client:%v",
	10120001: "ClusterService.Call: Current server time more then client time + duration. server:%v client:%v duration:%v responce_duration:%v",

	10120100: "ClusterServiceJsonCreate.Marshal: marshal to json fail",
	10120101: "ClusterServiceJsonCreate.Marshal: compress fail",
	10120102: "ClusterServiceJsonCreate.Unmarshal: restore fail alg: %v alg_set: %v",
	10120103: "ClusterServiceJsonCreate.Unmarshal: unmarshal fail. ct: %v, au: %v",
}

// GenerateError -
func GenerateError(key int, a ...interface{}) *mft.Error {
	if text, ok := Errors[key]; ok {
		return mft.ErrorCS(key, fmt.Sprintf(text, a...))
	}
	panic(fmt.Sprintf("queue.GenerateError, error not found code:%v", key))
}

// GenerateError -
func GenerateErrorForClusterUser(user ClusterUser, key int, a ...interface{}) *mft.Error {
	userName := "???"
	if user != nil {
		userName = "\"" + user.GetName() + "\""
	}
	if text, ok := Errors[key]; ok {
		return mft.ErrorCS(key, "[user:"+userName+"] "+fmt.Sprintf(text, a...))
	}
	panic(fmt.Sprintf("queue.GenerateErrorForClusterUser, error not found code:%v", key))
}

// GenerateErrorE -
func GenerateErrorE(key int, err error, a ...interface{}) *mft.Error {
	if text, ok := Errors[key]; ok {
		return mft.ErrorCSE(key, fmt.Sprintf(text, a...), err)
	}
	panic(fmt.Sprintf("queue.GenerateErrorE, error not found code:%v error:%v", key, err))
}

// GenerateError -
func GenerateErrorForClusterUserE(user ClusterUser, key int, err error, a ...interface{}) *mft.Error {
	userName := "???"
	if user != nil {
		userName = "\"" + user.GetName() + "\""
	}
	if text, ok := Errors[key]; ok {
		return mft.ErrorCSE(key, "[user:"+userName+"] "+fmt.Sprintf(text, a...), err)
	}
	panic(fmt.Sprintf("queue.GenerateErrorForClusterUserE, error not found code:%v", key))
}
