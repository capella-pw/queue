package cluster

import (
	"context"
	"encoding/json"
	"time"

	"github.com/capella-pw/queue/queue"
	"github.com/myfantasy/mft"
)

// RequestBody request
type RequestBody struct {
	Name       string          `json:"name"`
	User       string          `json:"user"`
	Action     string          `json:"action"`
	Body       json.RawMessage `json:"body,omitempty"`
	ObjectName string          `json:"object_name,omitempty"`
}

// ResponceBody responce
type ResponceBody struct {
	Body json.RawMessage `json:"body"`
	Err  *mft.Error      `json:"error"`
}

func (rb *RequestBody) GetName() string {
	return rb.User
}

// Operation names
const (
	OpGetName = "get_name"
	OpSetName = "set_name"

	OpAddQueue            = "add_q"
	OpDropQueue           = "drop_q"
	OpGetQueueDescription = "gd_q"
	OpGetQueuesList       = "list_q"

	OpAddExternalCluster            = "add_ec"
	OpDropExternalCluster           = "drop_ec"
	OpGetExternalClusterDescription = "gd_ec"
	OpGetExternalClustersList       = "list_ec"

	OpNestedCall = "nested_call"

	OpAddHandler            = "add_h"
	OpDropHandler           = "drop_h"
	OpGetHandlerDescription = "gd_h"
	OpGetHandlersList       = "list_h"

	OpCheckPermission = "check_perms"

	OpGetFullStruct  = "full_struct_get"
	OpLoadFullStruct = "full_struct_set"

	OpQueueAdd           = "q_add"
	OpQueueAddList       = "q_add_list"
	OpQueueGet           = "q_get"
	OpQueueSaveAll       = "q_save_all"
	OpQueueAddUnique     = "q_add_unique"
	OpQueueAddUniqueList = "q_add_unique_list"

	OpQueueSubscriberSetLastRead         = "q_subs_set_last"
	OpQueueSubscriberGetLastRead         = "q_subs_get_last"
	OpQueueSubscriberAddReplicaMember    = "q_subs_add_r_m"
	OpQueueSubscriberRemoveReplicaMember = "q_subs_rm_r_m"
	OpQueueSubscriberGetReplicaCount     = "q_subs_get_r_m_cnt"

	OpHandlerStart        = "h_start"
	OpHandlerStop         = "h_stop"
	OpHandlerLastComplete = "h_last_complete"
	OpHandlerLastError    = "h_last_error"
)

func (responce *ResponceBody) UnmarshalInnerObject(v interface{}) (err *mft.Error) {
	if responce.Err != nil {
		return responce.Err
	}
	if len(responce.Body) == 0 {
		return nil
	}
	er0 := json.Unmarshal(responce.Body, v)
	if er0 != nil {
		return GenerateErrorE(10107002, er0)
	}

	return nil
}

func (request *RequestBody) UnmarshalInnerObject(v interface{}) (err *mft.Error) {
	if len(request.Body) == 0 || len(request.Body) == 4 && string(request.Body) == "null" {
		return nil
	}
	er0 := json.Unmarshal(request.Body, v)
	if er0 != nil {
		return GenerateErrorE(10107003, er0)
	}

	return nil
}

func MarshalRequestMust(user ClusterUser, action string, v interface{}) *RequestBody {
	request := &RequestBody{
		User:   GetUserName(user),
		Action: action,
	}
	if v != nil {
		b, er0 := json.MarshalIndent(v, "", "  ")
		if er0 != nil {
			panic(GenerateErrorE(10107004, er0))
		}

		request.Body = b
	}

	return request
}

func MarshalResponceMust(v interface{}, err *mft.Error) *ResponceBody {
	responce := &ResponceBody{
		Err: err,
	}
	if v != nil {
		b, er0 := json.MarshalIndent(v, "", "  ")
		if er0 != nil {
			panic(GenerateErrorE(10107005, er0))
		}

		responce.Body = b
	}

	return responce
}

func UnmarshalInnerObjectAndFindQueue(cluster Cluster, request *RequestBody, v interface{}) (queue queue.Queue, responce *ResponceBody, ok bool) {
	err := request.UnmarshalInnerObject(v)
	if err != nil {
		responce = MarshalResponceMust(nil, err)
		return nil, responce, false
	}

	queue, exists, err := cluster.GetQueue(request, request.ObjectName)
	if err != nil {
		responce = MarshalResponceMust(nil, err)
		return nil, responce, false
	}
	if !exists {
		responce = MarshalResponceMust(nil, GenerateError(10107101, request.ObjectName))
		return nil, responce, false
	}

	return queue, responce, true
}

func UnmarshalInnerObjectAndFindHandler(cluster Cluster, request *RequestBody, v interface{}) (handler Handler, responce *ResponceBody, ok bool) {
	err := request.UnmarshalInnerObject(v)
	if err != nil {
		responce = MarshalResponceMust(nil, err)
		return nil, responce, false
	}

	handler, exists, err := cluster.GetHandler(request, request.ObjectName)
	if err != nil {
		responce = MarshalResponceMust(nil, err)
		return nil, responce, false
	}
	if !exists {
		responce = MarshalResponceMust(nil, GenerateError(10107103, request.ObjectName))
		return nil, responce, false
	}

	return handler, responce, true
}

func CallFuncInCluster(ctx context.Context, cluster Cluster, request *RequestBody) (responce *ResponceBody) {
	if request.Action == OpGetName {
		name, err := cluster.GetName(request)

		responce = MarshalResponceMust(name, err)
		return responce
	}
	if request.Action == OpSetName {
		var name string

		err := request.UnmarshalInnerObject(&name)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		err = cluster.SetName(request, name)

		responce = MarshalResponceMust(nil, err)
		return responce
	}
	if request.Action == OpAddQueue {
		var queueDescription QueueDescription

		err := request.UnmarshalInnerObject(&queueDescription)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		err = cluster.AddQueue(request, queueDescription)

		responce = MarshalResponceMust(nil, err)
		return responce
	}
	if request.Action == OpDropQueue {
		var name string

		err := request.UnmarshalInnerObject(&name)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		err = cluster.DropQueue(request, name)

		responce = MarshalResponceMust(nil, err)
		return responce
	}
	if request.Action == OpGetQueueDescription {
		var name string

		err := request.UnmarshalInnerObject(&name)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		queueDescription, err := cluster.GetQueueDescription(request, name)

		responce = MarshalResponceMust(queueDescription, err)
		return responce
	}
	if request.Action == OpGetQueuesList {
		names, err := cluster.GetQueuesList(request)

		responce = MarshalResponceMust(names, err)
		return responce
	}

	if request.Action == OpAddExternalCluster {
		var clusterParams ExternalClusterDescription

		err := request.UnmarshalInnerObject(&clusterParams)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		err = cluster.AddExternalCluster(request, clusterParams)

		responce = MarshalResponceMust(nil, err)
		return responce
	}
	if request.Action == OpDropExternalCluster {
		var name string

		err := request.UnmarshalInnerObject(&name)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		err = cluster.DropExternalCluster(request, name)

		responce = MarshalResponceMust(nil, err)
		return responce
	}
	if request.Action == OpGetExternalClusterDescription {
		var name string

		err := request.UnmarshalInnerObject(&name)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		clusterParams, err := cluster.GetExternalClusterDescription(request, name)

		responce = MarshalResponceMust(clusterParams, err)
		return responce
	}
	if request.Action == OpGetExternalClustersList {
		names, err := cluster.GetExternalClustersList(request)

		responce = MarshalResponceMust(names, err)
		return responce
	}

	if request.Action == OpAddHandler {
		var handlerParams HandlerDescription

		err := request.UnmarshalInnerObject(&handlerParams)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		err = cluster.AddHandler(request, handlerParams)

		responce = MarshalResponceMust(nil, err)
		return responce
	}
	if request.Action == OpDropHandler {
		var name string

		err := request.UnmarshalInnerObject(&name)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		err = cluster.DropHandler(request, name)

		responce = MarshalResponceMust(nil, err)
		return responce
	}
	if request.Action == OpGetHandlerDescription {
		var name string

		err := request.UnmarshalInnerObject(&name)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		handlerParams, err := cluster.GetHandlerDescription(request, name)

		responce = MarshalResponceMust(handlerParams, err)
		return responce
	}
	if request.Action == OpGetHandlersList {
		names, err := cluster.GetHandlersList(request)

		responce = MarshalResponceMust(names, err)
		return responce
	}

	if request.Action == OpCheckPermission {
		var params CheckPermissionRequest

		err := request.UnmarshalInnerObject(&params)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		allowed, err := cluster.CheckPermission(request, params.ObjectType, params.Action, params.ObjectName)

		responce = MarshalResponceMust(allowed, err)
		return responce
	}

	if request.Action == OpGetFullStruct {
		data, err := cluster.GetFullStruct(request)

		responce = MarshalResponceMust(data, err)
		return responce
	}
	if request.Action == OpLoadFullStruct {
		var data json.RawMessage

		err := request.UnmarshalInnerObject(&data)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		err = cluster.LoadFullStruct(request, data)

		responce = MarshalResponceMust(nil, err)
		return responce
	}

	// ------------------

	// queue

	if request.Action == OpQueueAdd {
		var qReq QueueAddRequest

		queue, responce, ok := UnmarshalInnerObjectAndFindQueue(cluster, request, &qReq)
		if !ok {
			return responce
		}

		id, err := queue.Add(ctx, qReq.Message.Message, qReq.Message.ExternalID, qReq.Message.ExternalDt, qReq.Message.Source, qReq.SaveMode)

		responce = MarshalResponceMust(id, err)
		return responce
	}
	if request.Action == OpQueueAddList {
		var qReq QueueAddListRequest

		queue, responce, ok := UnmarshalInnerObjectAndFindQueue(cluster, request, &qReq)
		if !ok {
			return responce
		}

		ids, err := queue.AddList(ctx, qReq.Messages, qReq.SaveMode)

		responce = MarshalResponceMust(ids, err)
		return responce
	}

	if request.Action == OpQueueGet {
		var qReq QueueGetRequest

		queue, responce, ok := UnmarshalInnerObjectAndFindQueue(cluster, request, &qReq)
		if !ok {
			return responce
		}

		messages, err := queue.Get(ctx, qReq.IdStart, qReq.CntLimit)

		responce = MarshalResponceMust(messages, err)
		return responce
	}

	if request.Action == OpQueueSaveAll {
		queue, responce, ok := UnmarshalInnerObjectAndFindQueue(cluster, request, nil)
		if !ok {
			return responce
		}

		err := queue.SaveAll(ctx)

		responce = MarshalResponceMust(nil, err)
		return responce
	}

	if request.Action == OpQueueAddUnique {
		var qReq QueueAddRequest

		queue, responce, ok := UnmarshalInnerObjectAndFindQueue(cluster, request, &qReq)
		if !ok {
			return responce
		}

		id, err := queue.AddUnique(ctx, qReq.Message.Message, qReq.Message.ExternalID, qReq.Message.ExternalDt, qReq.Message.Source, qReq.SaveMode)

		responce = MarshalResponceMust(id, err)
		return responce
	}
	if request.Action == OpQueueAddUniqueList {
		var qReq QueueAddListRequest

		queue, responce, ok := UnmarshalInnerObjectAndFindQueue(cluster, request, &qReq)
		if !ok {
			return responce
		}

		ids, err := queue.AddUniqueList(ctx, qReq.Messages, qReq.SaveMode)

		responce = MarshalResponceMust(ids, err)
		return responce
	}

	if request.Action == OpQueueSubscriberSetLastRead {
		var qReq QueueSubscriberSetLastReadRequest

		queue, responce, ok := UnmarshalInnerObjectAndFindQueue(cluster, request, &qReq)
		if !ok {
			return responce
		}

		err := queue.SubscriberSetLastRead(ctx, qReq.Subscriber, qReq.Id, qReq.SaveMode)

		responce = MarshalResponceMust(nil, err)
		return responce
	}
	if request.Action == OpQueueSubscriberGetLastRead {
		var subscriber string

		queue, responce, ok := UnmarshalInnerObjectAndFindQueue(cluster, request, &subscriber)
		if !ok {
			return responce
		}

		id, err := queue.SubscriberGetLastRead(ctx, subscriber)

		responce = MarshalResponceMust(id, err)
		return responce
	}

	if request.Action == OpQueueSubscriberAddReplicaMember {
		var subscriber string

		queue, responce, ok := UnmarshalInnerObjectAndFindQueue(cluster, request, &subscriber)
		if !ok {
			return responce
		}

		err := queue.SubscriberAddReplicaMember(ctx, subscriber)

		responce = MarshalResponceMust(nil, err)
		return responce
	}

	if request.Action == OpQueueSubscriberRemoveReplicaMember {
		var subscriber string

		queue, responce, ok := UnmarshalInnerObjectAndFindQueue(cluster, request, &subscriber)
		if !ok {
			return responce
		}

		err := queue.SubscriberRemoveReplicaMember(ctx, subscriber)

		responce = MarshalResponceMust(nil, err)
		return responce
	}

	if request.Action == OpQueueSubscriberGetReplicaCount {
		var id int64

		queue, responce, ok := UnmarshalInnerObjectAndFindQueue(cluster, request, &id)
		if !ok {
			return responce
		}

		cnt, err := queue.SubscriberGetReplicaCount(ctx, id)

		responce = MarshalResponceMust(cnt, err)
		return responce
	}

	// ----------------------

	// handler
	if request.Action == OpHandlerStart {
		handler, responce, ok := UnmarshalInnerObjectAndFindHandler(cluster, request, nil)
		if !ok {
			return responce
		}

		err := handler.Start(ctx)

		responce = MarshalResponceMust(nil, err)
		return responce
	}
	if request.Action == OpHandlerStop {
		handler, responce, ok := UnmarshalInnerObjectAndFindHandler(cluster, request, nil)
		if !ok {
			return responce
		}

		err := handler.Stop(ctx)

		responce = MarshalResponceMust(nil, err)
		return responce
	}
	if request.Action == OpHandlerLastError {
		handler, responce, ok := UnmarshalInnerObjectAndFindHandler(cluster, request, nil)
		if !ok {
			return responce
		}

		err := handler.LastError(ctx)

		responce = MarshalResponceMust(nil, err)
		return responce
	}
	if request.Action == OpHandlerLastComplete {
		handler, responce, ok := UnmarshalInnerObjectAndFindHandler(cluster, request, nil)
		if !ok {
			return responce
		}

		lastComplete, err := handler.LastComplete(ctx)

		responce = MarshalResponceMust(lastComplete, err)
		return responce
	}

	if request.Action == OpNestedCall {
		var requestNest *RequestBody

		err := request.UnmarshalInnerObject(&requestNest)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		clusterNext, exists, err := cluster.GetExternalCluster(request, request.ObjectName)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}
		if !exists {
			responce = MarshalResponceMust(nil, GenerateError(10107102, request.ObjectName))
			return responce
		}

		responce = CallFuncInCluster(ctx, clusterNext, requestNest)

		return responce
	}

	responce.Err = GenerateError(10107100, request.Action)
	return responce
}

type ExternalAbstractCluster struct {
	CallTimeout time.Duration
	CallFunc    func(ctx context.Context, request *RequestBody) (responce *ResponceBody)
	// case nil then panic
	ThrowErrorFunc func(err *mft.Error) bool
}

func (eac *ExternalAbstractCluster) Call(request *RequestBody) (responce *ResponceBody) {
	ctx := context.Background()
	if eac.CallTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, eac.CallTimeout)
		defer cancel()
	}

	return eac.CallFunc(ctx, request)
}

func GetUserName(user ClusterUser) string {
	if user == nil {
		return ""
	}
	return user.GetName()
}

func (eac *ExternalAbstractCluster) GetName(user ClusterUser) (name string, err *mft.Error) {
	request := MarshalRequestMust(user, OpGetName, nil)
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&name)

	return name, err
}
func (eac *ExternalAbstractCluster) SetName(user ClusterUser, name string) (err *mft.Error) {
	request := MarshalRequestMust(user, OpSetName, name)
	responce := eac.Call(request)
	return responce.Err
}
func (eac *ExternalAbstractCluster) ThrowError(err *mft.Error) bool {
	if eac.ThrowErrorFunc != nil {
		return eac.ThrowErrorFunc(err)
	}
	panic(err)
}
func (eac *ExternalAbstractCluster) AddQueue(user ClusterUser, queueDescription QueueDescription) (err *mft.Error) {
	request := MarshalRequestMust(user, OpAddQueue, queueDescription)
	responce := eac.Call(request)
	return responce.Err
}
func (eac *ExternalAbstractCluster) DropQueue(user ClusterUser, name string) (err *mft.Error) {
	request := MarshalRequestMust(user, OpDropQueue, name)
	responce := eac.Call(request)
	return responce.Err
}
func (eac *ExternalAbstractCluster) GetQueueDescription(user ClusterUser, name string) (queueDescription QueueDescription, err *mft.Error) {
	request := MarshalRequestMust(user, OpGetQueueDescription, name)
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&queueDescription)

	return queueDescription, err
}
func (eac *ExternalAbstractCluster) GetQueuesList(user ClusterUser) (names []string, err *mft.Error) {
	request := MarshalRequestMust(user, OpGetQueuesList, nil)
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&names)

	return names, err
}
func (eac *ExternalAbstractCluster) GetQueue(user ClusterUser, name string) (queue queue.Queue, exists bool, err *mft.Error) {
	// TODO: Make check
	eaq := &ExternalAbstractQueue{
		QueueName: name,
		User:      user,
		CallFunc:  eac.CallFunc,
	}
	return eaq, true, nil
}
func (eac *ExternalAbstractCluster) AddExternalCluster(user ClusterUser, clusterParams ExternalClusterDescription) (err *mft.Error) {
	request := MarshalRequestMust(user, OpAddExternalCluster, clusterParams)
	responce := eac.Call(request)
	return responce.Err
}
func (eac *ExternalAbstractCluster) DropExternalCluster(user ClusterUser, name string) (err *mft.Error) {
	request := MarshalRequestMust(user, OpDropExternalCluster, name)
	responce := eac.Call(request)
	return responce.Err
}
func (eac *ExternalAbstractCluster) GetExternalClusterDescription(user ClusterUser, name string) (clusterParams ExternalClusterDescription, err *mft.Error) {
	request := MarshalRequestMust(user, OpGetExternalClusterDescription, name)
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&clusterParams)

	return clusterParams, err
}
func (eac *ExternalAbstractCluster) GetExternalClustersList(user ClusterUser) (names []string, err *mft.Error) {
	request := MarshalRequestMust(user, OpGetExternalClustersList, nil)
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&names)

	return names, err
}

func (eac *ExternalAbstractCluster) GetExternalCluster(user ClusterUser, name string) (cluster Cluster, exists bool, err *mft.Error) {
	// TODO: Make check
	eacOut := &ExternalAbstractCluster{
		CallTimeout: eac.CallTimeout,
		CallFunc: func(ctx context.Context, requestNest *RequestBody) (responceNest *ResponceBody) {
			request := MarshalRequestMust(user, OpNestedCall, requestNest)
			request.ObjectName = name

			responce := eac.Call(request)

			return responce
		},
		ThrowErrorFunc: eac.ThrowErrorFunc,
	}

	return eacOut, true, nil
}
func (eac *ExternalAbstractCluster) AddHandler(user ClusterUser, handlerParams HandlerDescription) (err *mft.Error) {
	request := MarshalRequestMust(user, OpAddHandler, handlerParams)
	responce := eac.Call(request)
	return responce.Err
}
func (eac *ExternalAbstractCluster) DropHandler(user ClusterUser, name string) (err *mft.Error) {
	request := MarshalRequestMust(user, OpDropHandler, name)
	responce := eac.Call(request)
	return responce.Err
}
func (eac *ExternalAbstractCluster) GetHandlerDescription(user ClusterUser, name string) (handlerParams HandlerDescription, err *mft.Error) {
	request := MarshalRequestMust(user, OpGetHandlerDescription, name)
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&handlerParams)

	return handlerParams, err
}
func (eac *ExternalAbstractCluster) GetHandlersList(user ClusterUser) (names []string, err *mft.Error) {
	request := MarshalRequestMust(user, OpGetHandlersList, nil)
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&names)

	return names, err
}

func (eac *ExternalAbstractCluster) GetHandler(user ClusterUser, name string) (handler Handler, exists bool, err *mft.Error) {
	// TODO: Make check
	eah := &ExternalAbstractHandler{
		HandlerName: name,
		User:        user,
		CallFunc:    eac.CallFunc,
	}
	return eah, true, nil
}

type CheckPermissionRequest struct {
	ObjectType string `json:"object_type"`
	Action     string `json:"action"`
	ObjectName string `json:"object_name"`
}

func (eac *ExternalAbstractCluster) CheckPermission(user ClusterUser, objectType string, action string, objectName string) (allowed bool, err *mft.Error) {
	request := MarshalRequestMust(user, OpCheckPermission, CheckPermissionRequest{
		ObjectType: objectType,
		Action:     action,
		ObjectName: objectName,
	})
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&allowed)

	return allowed, err
}
func (eac *ExternalAbstractCluster) GetFullStruct(user ClusterUser) (data json.RawMessage, err *mft.Error) {
	request := MarshalRequestMust(user, OpGetFullStruct, nil)
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&data)

	return data, err
}
func (eac *ExternalAbstractCluster) LoadFullStruct(user ClusterUser, data json.RawMessage) (err *mft.Error) {
	request := MarshalRequestMust(user, OpLoadFullStruct, data)
	responce := eac.Call(request)
	return responce.Err
}

func (eac *ExternalAbstractCluster) SetValueInternal(string, string) (err *mft.Error) {
	return GenerateError(10101200)
}
func (eac *ExternalAbstractCluster) GetValueInternal(string) (string, bool) {
	return "", false
}

func (eac *ExternalAbstractCluster) OnChange() (err *mft.Error) {
	return GenerateError(10101201)
}

type ExternalAbstractQueue struct {
	QueueName string
	User      ClusterUser
	CallFunc  func(ctx context.Context, request *RequestBody) (responce *ResponceBody)
}

func (eac *ExternalAbstractQueue) MarshalRequestMust(action string, v interface{}) *RequestBody {
	request := MarshalRequestMust(eac.User, action, v)
	request.ObjectName = eac.QueueName

	return request
}

type QueueAddRequest struct {
	Message  queue.Message `json:"msg"`
	SaveMode int           `json:"sm"`
}

func (eac *ExternalAbstractQueue) Add(ctx context.Context, message []byte, externalID int64, externalDt int64, source string, saveMode int) (id int64, err *mft.Error) {
	request := eac.MarshalRequestMust(OpQueueAdd, QueueAddRequest{
		Message: queue.Message{
			ExternalID: externalID,
			ExternalDt: externalDt,
			Source:     source,
			Message:    message,
		},
		SaveMode: saveMode,
	})
	responce := eac.CallFunc(ctx, request)

	err = responce.UnmarshalInnerObject(&id)

	return id, err
}

type QueueAddListRequest struct {
	Messages []queue.Message `json:"msgs"`
	SaveMode int             `json:"sm"`
}

func (eac *ExternalAbstractQueue) AddList(ctx context.Context, messages []queue.Message, saveMode int) (ids []int64, err *mft.Error) {
	request := eac.MarshalRequestMust(OpQueueAddList, QueueAddListRequest{
		Messages: messages,
		SaveMode: saveMode,
	})
	responce := eac.CallFunc(ctx, request)

	err = responce.UnmarshalInnerObject(&ids)

	return ids, err
}

type QueueGetRequest struct {
	IdStart  int64 `json:"msg"`
	CntLimit int   `json:"sm"`
}

func (eac *ExternalAbstractQueue) Get(ctx context.Context, idStart int64, cntLimit int) (messages []*queue.MessageWithMeta, err *mft.Error) {
	request := eac.MarshalRequestMust(OpQueueGet, QueueGetRequest{
		IdStart:  idStart,
		CntLimit: cntLimit,
	})
	responce := eac.CallFunc(ctx, request)

	err = responce.UnmarshalInnerObject(&messages)

	return messages, err
}

func (eac *ExternalAbstractQueue) SaveAll(ctx context.Context) (err *mft.Error) {
	request := eac.MarshalRequestMust(OpQueueSaveAll, nil)
	responce := eac.CallFunc(ctx, request)

	return responce.Err
}

func (eac *ExternalAbstractQueue) AddUnique(ctx context.Context, message []byte, externalID int64, externalDt int64, source string, saveMode int) (id int64, err *mft.Error) {
	request := eac.MarshalRequestMust(OpQueueAddUnique, QueueAddRequest{
		Message: queue.Message{
			ExternalID: externalID,
			ExternalDt: externalDt,
			Source:     source,
			Message:    message,
		},
		SaveMode: saveMode,
	})
	responce := eac.CallFunc(ctx, request)

	err = responce.UnmarshalInnerObject(&id)

	return id, err
}

func (eac *ExternalAbstractQueue) AddUniqueList(ctx context.Context, messages []queue.Message, saveMode int) (ids []int64, err *mft.Error) {
	request := eac.MarshalRequestMust(OpQueueAddUniqueList, QueueAddListRequest{
		Messages: messages,
		SaveMode: saveMode,
	})
	responce := eac.CallFunc(ctx, request)

	err = responce.UnmarshalInnerObject(&ids)

	return ids, err
}

type QueueSubscriberSetLastReadRequest struct {
	Subscriber string `json:"sbscr"`
	Id         int64  `json:"id"`
	SaveMode   int    `json:"sm"`
}

func (eac *ExternalAbstractQueue) SubscriberSetLastRead(ctx context.Context, subscriber string, id int64, saveMode int) (err *mft.Error) {
	request := eac.MarshalRequestMust(OpQueueSubscriberSetLastRead, QueueSubscriberSetLastReadRequest{
		Subscriber: subscriber,
		Id:         id,
		SaveMode:   saveMode,
	})
	responce := eac.CallFunc(ctx, request)

	return responce.Err
}

func (eac *ExternalAbstractQueue) SubscriberGetLastRead(ctx context.Context, subscriber string) (id int64, err *mft.Error) {
	request := eac.MarshalRequestMust(OpQueueSubscriberGetLastRead, subscriber)
	responce := eac.CallFunc(ctx, request)

	err = responce.UnmarshalInnerObject(&id)

	return id, err
}

func (eac *ExternalAbstractQueue) SubscriberAddReplicaMember(ctx context.Context, subscriber string) (err *mft.Error) {
	request := eac.MarshalRequestMust(OpQueueSubscriberAddReplicaMember, subscriber)
	responce := eac.CallFunc(ctx, request)

	err = responce.Err

	return err
}

func (eac *ExternalAbstractQueue) SubscriberRemoveReplicaMember(ctx context.Context, subscriber string) (err *mft.Error) {
	request := eac.MarshalRequestMust(OpQueueSubscriberRemoveReplicaMember, subscriber)
	responce := eac.CallFunc(ctx, request)

	err = responce.Err

	return err
}

func (eac *ExternalAbstractQueue) SubscriberGetReplicaCount(ctx context.Context, id int64) (cnt int, err *mft.Error) {
	request := eac.MarshalRequestMust(OpQueueSubscriberGetReplicaCount, id)
	responce := eac.CallFunc(ctx, request)

	err = responce.UnmarshalInnerObject(&cnt)

	return cnt, err
}

type ExternalAbstractHandler struct {
	HandlerName string
	User        ClusterUser
	CallFunc    func(ctx context.Context, request *RequestBody) (responce *ResponceBody)
}

func (eah *ExternalAbstractHandler) MarshalRequestMust(action string, v interface{}) *RequestBody {
	request := MarshalRequestMust(eah.User, action, v)
	request.ObjectName = eah.HandlerName

	return request
}

func (eah *ExternalAbstractHandler) Start(ctx context.Context) (err *mft.Error) {
	request := eah.MarshalRequestMust(OpHandlerStart, nil)
	responce := eah.CallFunc(ctx, request)

	return responce.Err
}
func (eah *ExternalAbstractHandler) Stop(ctx context.Context) (err *mft.Error) {
	request := eah.MarshalRequestMust(OpHandlerStop, nil)
	responce := eah.CallFunc(ctx, request)

	return responce.Err
}
func (eah *ExternalAbstractHandler) LastComplete(ctx context.Context) (lastComplete time.Time, err *mft.Error) {
	request := eah.MarshalRequestMust(OpHandlerLastComplete, nil)
	responce := eah.CallFunc(ctx, request)

	err = responce.UnmarshalInnerObject(&lastComplete)

	return lastComplete, err
}
func (eah *ExternalAbstractHandler) LastError(ctx context.Context) (err *mft.Error) {
	request := eah.MarshalRequestMust(OpHandlerLastError, nil)
	responce := eah.CallFunc(ctx, request)

	return responce.Err
}
