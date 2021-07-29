package cluster

import (
	"context"
	"encoding/json"
	"time"

	"github.com/capella-pw/queue/cn"
	"github.com/capella-pw/queue/queue"
	"github.com/myfantasy/mft"
	"github.com/myfantasy/segment"
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

func MarshalRequestMust(user cn.CapUser, action string, v interface{}) *RequestBody {
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

func UnmarshalInnerObjectAndFindQueue(ctx context.Context,
	cluster Cluster, request *RequestBody, v interface{}) (queue queue.Queue, responce *ResponceBody, ok bool) {
	err := request.UnmarshalInnerObject(v)
	if err != nil {
		responce = MarshalResponceMust(nil, err)
		return nil, responce, false
	}

	queue, exists, err := cluster.GetQueue(ctx, request, request.ObjectName)
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

func UnmarshalInnerObjectAndFindHandler(ctx context.Context,
	cluster Cluster, request *RequestBody, v interface{}) (handler Handler, responce *ResponceBody, ok bool) {
	err := request.UnmarshalInnerObject(v)
	if err != nil {
		responce = MarshalResponceMust(nil, err)
		return nil, responce, false
	}

	handler, exists, err := cluster.GetHandler(ctx, request, request.ObjectName)
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

type AdditionalCallFuncInClusterFunc func(ctx context.Context,
	cluster Cluster, request *RequestBody) (responce *ResponceBody, ok bool)

func CallFuncInCluster(ctx context.Context, cluster Cluster, request *RequestBody,
	addFunc []AdditionalCallFuncInClusterFunc) (responce *ResponceBody) {
	if request.Action == cn.OpGetName {
		name, err := cluster.GetName(ctx, request)

		responce = MarshalResponceMust(name, err)
		return responce
	}
	if request.Action == cn.OpSetName {
		var name string

		err := request.UnmarshalInnerObject(&name)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		err = cluster.SetName(ctx, request, name)

		responce = MarshalResponceMust(nil, err)
		return responce
	}

	if request.Action == cn.OpPing {
		err := cluster.Ping(ctx, request)

		responce = MarshalResponceMust(nil, err)
		return responce
	}
	if request.Action == cn.OpGetNextId {
		id, err := cluster.GetNextId(ctx, request)

		responce = MarshalResponceMust(id, err)
		return responce
	}
	if request.Action == cn.OpGetNextIds {
		var cnt int

		err := request.UnmarshalInnerObject(&cnt)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		ids, err := cluster.GetNextIds(ctx, request, cnt)

		responce = MarshalResponceMust(ids, err)
		return responce
	}

	if request.Action == cn.OpAddQueue {
		var queueDescription QueueDescription

		err := request.UnmarshalInnerObject(&queueDescription)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		err = cluster.AddQueue(ctx, request, queueDescription)

		responce = MarshalResponceMust(nil, err)
		return responce
	}
	if request.Action == cn.OpDropQueue {
		var name string

		err := request.UnmarshalInnerObject(&name)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		err = cluster.DropQueue(ctx, request, name)

		responce = MarshalResponceMust(nil, err)
		return responce
	}
	if request.Action == cn.OpGetQueueDescription {
		var name string

		err := request.UnmarshalInnerObject(&name)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		queueDescription, err := cluster.GetQueueDescription(ctx, request, name)

		responce = MarshalResponceMust(queueDescription, err)
		return responce
	}
	if request.Action == cn.OpGetQueuesList {
		names, err := cluster.GetQueuesList(ctx, request)

		responce = MarshalResponceMust(names, err)
		return responce
	}

	if request.Action == cn.OpAddExternalCluster {
		var clusterParams ExternalClusterDescription

		err := request.UnmarshalInnerObject(&clusterParams)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		err = cluster.AddExternalCluster(ctx, request, clusterParams)

		responce = MarshalResponceMust(nil, err)
		return responce
	}
	if request.Action == cn.OpDropExternalCluster {
		var name string

		err := request.UnmarshalInnerObject(&name)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		err = cluster.DropExternalCluster(ctx, request, name)

		responce = MarshalResponceMust(nil, err)
		return responce
	}
	if request.Action == cn.OpGetExternalClusterDescription {
		var name string

		err := request.UnmarshalInnerObject(&name)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		clusterParams, err := cluster.GetExternalClusterDescription(ctx, request, name)

		responce = MarshalResponceMust(clusterParams, err)
		return responce
	}
	if request.Action == cn.OpGetExternalClustersList {
		names, err := cluster.GetExternalClustersList(ctx, request)

		responce = MarshalResponceMust(names, err)
		return responce
	}

	if request.Action == cn.OpAddHandler {
		var handlerParams HandlerDescription

		err := request.UnmarshalInnerObject(&handlerParams)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		err = cluster.AddHandler(ctx, request, handlerParams)

		responce = MarshalResponceMust(nil, err)
		return responce
	}
	if request.Action == cn.OpDropHandler {
		var name string

		err := request.UnmarshalInnerObject(&name)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		err = cluster.DropHandler(ctx, request, name)

		responce = MarshalResponceMust(nil, err)
		return responce
	}
	if request.Action == cn.OpGetHandlerDescription {
		var name string

		err := request.UnmarshalInnerObject(&name)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		handlerParams, err := cluster.GetHandlerDescription(ctx, request, name)

		responce = MarshalResponceMust(handlerParams, err)
		return responce
	}
	if request.Action == cn.OpGetHandlersList {
		names, err := cluster.GetHandlersList(ctx, request)

		responce = MarshalResponceMust(names, err)
		return responce
	}

	if request.Action == cn.OpCheckPermission {
		var params CheckPermissionRequest

		err := request.UnmarshalInnerObject(&params)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		allowed, err := cluster.CheckPermission(ctx, request, params.ObjectType, params.Action, params.ObjectName)

		responce = MarshalResponceMust(allowed, err)
		return responce
	}

	if request.Action == cn.OpGetFullStruct {
		data, err := cluster.GetFullStruct(ctx, request)

		responce = MarshalResponceMust(data, err)
		return responce
	}
	if request.Action == cn.OpLoadFullStruct {
		var data json.RawMessage

		err := request.UnmarshalInnerObject(&data)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		err = cluster.LoadFullStruct(ctx, request, data)

		responce = MarshalResponceMust(nil, err)
		return responce
	}

	// ------------------

	// queue

	if request.Action == cn.OpQueueAdd {
		var qReq QueueAddRequest

		queue, responce, ok := UnmarshalInnerObjectAndFindQueue(ctx, cluster, request, &qReq)
		if !ok {
			return responce
		}

		id, err := queue.Add(ctx, qReq.Message.Message, qReq.Message.ExternalID, qReq.Message.ExternalDt, qReq.Message.Source, qReq.Message.Segment, qReq.SaveMode)

		responce = MarshalResponceMust(id, err)
		return responce
	}
	if request.Action == cn.OpQueueAddList {
		var qReq QueueAddListRequest

		queue, responce, ok := UnmarshalInnerObjectAndFindQueue(ctx, cluster, request, &qReq)
		if !ok {
			return responce
		}

		ids, err := queue.AddList(ctx, qReq.Messages, qReq.SaveMode)

		responce = MarshalResponceMust(ids, err)
		return responce
	}

	if request.Action == cn.OpQueueGet {
		var qReq QueueGetRequest

		queue, responce, ok := UnmarshalInnerObjectAndFindQueue(ctx, cluster, request, &qReq)
		if !ok {
			return responce
		}

		messages, err := queue.Get(ctx, qReq.IdStart, qReq.CntLimit)

		responce = MarshalResponceMust(messages, err)
		return responce
	}

	if request.Action == cn.OpQueueGetSegment {
		var qReq QueueGetSegmentRequest

		queue, responce, ok := UnmarshalInnerObjectAndFindQueue(ctx, cluster, request, &qReq)
		if !ok {
			return responce
		}

		messages, lastId, err := queue.GetSegment(ctx, qReq.IdStart, qReq.CntLimit, qReq.Segments)

		responce = MarshalResponceMust(QueueGetSegmentResponce{
			Messages: messages,
			LastId:   lastId,
		}, err)
		return responce
	}

	if request.Action == cn.OpQueueSaveAll {
		queue, responce, ok := UnmarshalInnerObjectAndFindQueue(ctx, cluster, request, nil)
		if !ok {
			return responce
		}

		err := queue.SaveAll(ctx)

		responce = MarshalResponceMust(nil, err)
		return responce
	}

	if request.Action == cn.OpQueueAddUnique {
		var qReq QueueAddRequest

		queue, responce, ok := UnmarshalInnerObjectAndFindQueue(ctx, cluster, request, &qReq)
		if !ok {
			return responce
		}

		id, err := queue.AddUnique(ctx, qReq.Message.Message, qReq.Message.ExternalID, qReq.Message.ExternalDt, qReq.Message.Source, qReq.Message.Segment, qReq.SaveMode)

		responce = MarshalResponceMust(id, err)
		return responce
	}
	if request.Action == cn.OpQueueAddUniqueList {
		var qReq QueueAddListRequest

		queue, responce, ok := UnmarshalInnerObjectAndFindQueue(ctx, cluster, request, &qReq)
		if !ok {
			return responce
		}

		ids, err := queue.AddUniqueList(ctx, qReq.Messages, qReq.SaveMode)

		responce = MarshalResponceMust(ids, err)
		return responce
	}

	if request.Action == cn.OpQueueSubscriberSetLastRead {
		var qReq QueueSubscriberSetLastReadRequest

		queue, responce, ok := UnmarshalInnerObjectAndFindQueue(ctx, cluster, request, &qReq)
		if !ok {
			return responce
		}

		err := queue.SubscriberSetLastRead(ctx, qReq.Subscriber, qReq.Id, qReq.SaveMode)

		responce = MarshalResponceMust(nil, err)
		return responce
	}
	if request.Action == cn.OpQueueSubscriberGetLastRead {
		var subscriber string

		queue, responce, ok := UnmarshalInnerObjectAndFindQueue(ctx, cluster, request, &subscriber)
		if !ok {
			return responce
		}

		id, err := queue.SubscriberGetLastRead(ctx, subscriber)

		responce = MarshalResponceMust(id, err)
		return responce
	}

	if request.Action == cn.OpQueueSubscriberAddReplicaMember {
		var subscriber string

		queue, responce, ok := UnmarshalInnerObjectAndFindQueue(ctx, cluster, request, &subscriber)
		if !ok {
			return responce
		}

		err := queue.SubscriberAddReplicaMember(ctx, subscriber)

		responce = MarshalResponceMust(nil, err)
		return responce
	}

	if request.Action == cn.OpQueueSubscriberRemoveReplicaMember {
		var subscriber string

		queue, responce, ok := UnmarshalInnerObjectAndFindQueue(ctx, cluster, request, &subscriber)
		if !ok {
			return responce
		}

		err := queue.SubscriberRemoveReplicaMember(ctx, subscriber)

		responce = MarshalResponceMust(nil, err)
		return responce
	}

	if request.Action == cn.OpQueueSubscriberGetReplicaCount {
		var id int64

		queue, responce, ok := UnmarshalInnerObjectAndFindQueue(ctx, cluster, request, &id)
		if !ok {
			return responce
		}

		cnt, err := queue.SubscriberGetReplicaCount(ctx, id)

		responce = MarshalResponceMust(cnt, err)
		return responce
	}

	// ----------------------

	// handler
	if request.Action == cn.OpHandlerStart {
		handler, responce, ok := UnmarshalInnerObjectAndFindHandler(ctx, cluster, request, nil)
		if !ok {
			return responce
		}

		err := handler.Start(ctx)

		responce = MarshalResponceMust(nil, err)
		return responce
	}
	if request.Action == cn.OpHandlerStop {
		handler, responce, ok := UnmarshalInnerObjectAndFindHandler(ctx, cluster, request, nil)
		if !ok {
			return responce
		}

		err := handler.Stop(ctx)

		responce = MarshalResponceMust(nil, err)
		return responce
	}
	if request.Action == cn.OpHandlerLastError {
		handler, responce, ok := UnmarshalInnerObjectAndFindHandler(ctx, cluster, request, nil)
		if !ok {
			return responce
		}

		err := handler.LastError(ctx)

		responce = MarshalResponceMust(nil, err)
		return responce
	}
	if request.Action == cn.OpHandlerLastComplete {
		handler, responce, ok := UnmarshalInnerObjectAndFindHandler(ctx, cluster, request, nil)
		if !ok {
			return responce
		}

		lastComplete, err := handler.LastComplete(ctx)

		responce = MarshalResponceMust(lastComplete, err)
		return responce
	}
	if request.Action == cn.OpHandlerIsStarted {
		handler, responce, ok := UnmarshalInnerObjectAndFindHandler(ctx, cluster, request, nil)
		if !ok {
			return responce
		}

		isStarted, err := handler.IsStarted(ctx)

		responce = MarshalResponceMust(isStarted, err)
		return responce
	}

	if request.Action == cn.OpNestedCall {
		var requestNest *RequestBody

		err := request.UnmarshalInnerObject(&requestNest)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}

		clusterNext, exists, err := cluster.GetExternalCluster(ctx, request, request.ObjectName)
		if err != nil {
			responce = MarshalResponceMust(nil, err)
			return responce
		}
		if !exists {
			responce = MarshalResponceMust(nil, GenerateError(10107102, request.ObjectName))
			return responce
		}

		responce = CallFuncInCluster(ctx, clusterNext, requestNest, addFunc)
		return responce
	}

	for _, f := range addFunc {
		responce, ok := f(ctx, cluster, request)
		if ok {
			return responce
		}
	}

	responce = MarshalResponceMust(nil, GenerateError(10107100, request.Action))
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

func GetUserName(user cn.CapUser) string {
	if user == nil {
		return ""
	}
	return user.GetName()
}

func (eac *ExternalAbstractCluster) GetName(ctx context.Context, user cn.CapUser) (name string, err *mft.Error) {
	request := MarshalRequestMust(user, cn.OpGetName, nil)
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&name)

	return name, err
}
func (eac *ExternalAbstractCluster) SetName(ctx context.Context, user cn.CapUser, name string) (err *mft.Error) {
	request := MarshalRequestMust(user, cn.OpSetName, name)
	responce := eac.Call(request)
	return responce.Err
}

func (eac *ExternalAbstractCluster) Ping(ctx context.Context, user cn.CapUser) (err *mft.Error) {
	request := MarshalRequestMust(user, cn.OpPing, nil)
	responce := eac.Call(request)
	return responce.Err
}
func (eac *ExternalAbstractCluster) GetNextId(ctx context.Context, user cn.CapUser) (id int64, err *mft.Error) {
	request := MarshalRequestMust(user, cn.OpGetNextId, nil)
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&id)

	return id, err
}
func (eac *ExternalAbstractCluster) GetNextIds(ctx context.Context, user cn.CapUser, cnt int) (ids []int64, err *mft.Error) {
	request := MarshalRequestMust(user, cn.OpGetNextIds, cnt)
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&ids)

	return ids, err
}

func (eac *ExternalAbstractCluster) ThrowError(err *mft.Error) bool {
	if eac.ThrowErrorFunc != nil {
		return eac.ThrowErrorFunc(err)
	}
	panic(err)
}
func (eac *ExternalAbstractCluster) AddQueue(ctx context.Context, user cn.CapUser, queueDescription QueueDescription) (err *mft.Error) {
	request := MarshalRequestMust(user, cn.OpAddQueue, queueDescription)
	responce := eac.Call(request)
	return responce.Err
}
func (eac *ExternalAbstractCluster) DropQueue(ctx context.Context, user cn.CapUser, name string) (err *mft.Error) {
	request := MarshalRequestMust(user, cn.OpDropQueue, name)
	responce := eac.Call(request)
	return responce.Err
}
func (eac *ExternalAbstractCluster) GetQueueDescription(ctx context.Context, user cn.CapUser, name string) (queueDescription QueueDescription, err *mft.Error) {
	request := MarshalRequestMust(user, cn.OpGetQueueDescription, name)
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&queueDescription)

	return queueDescription, err
}
func (eac *ExternalAbstractCluster) GetQueuesList(ctx context.Context, user cn.CapUser) (names []string, err *mft.Error) {
	request := MarshalRequestMust(user, cn.OpGetQueuesList, nil)
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&names)

	return names, err
}
func (eac *ExternalAbstractCluster) GetQueue(ctx context.Context, user cn.CapUser, name string) (queue queue.Queue, exists bool, err *mft.Error) {
	// TODO: Make check
	eaq := &ExternalAbstractQueue{
		QueueName: name,
		User:      user,
		CallFunc:  eac.CallFunc,
	}
	return eaq, true, nil
}
func (eac *ExternalAbstractCluster) AddExternalCluster(ctx context.Context, user cn.CapUser, clusterParams ExternalClusterDescription) (err *mft.Error) {
	request := MarshalRequestMust(user, cn.OpAddExternalCluster, clusterParams)
	responce := eac.Call(request)
	return responce.Err
}
func (eac *ExternalAbstractCluster) DropExternalCluster(ctx context.Context, user cn.CapUser, name string) (err *mft.Error) {
	request := MarshalRequestMust(user, cn.OpDropExternalCluster, name)
	responce := eac.Call(request)
	return responce.Err
}
func (eac *ExternalAbstractCluster) GetExternalClusterDescription(ctx context.Context, user cn.CapUser, name string) (clusterParams ExternalClusterDescription, err *mft.Error) {
	request := MarshalRequestMust(user, cn.OpGetExternalClusterDescription, name)
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&clusterParams)

	return clusterParams, err
}
func (eac *ExternalAbstractCluster) GetExternalClustersList(ctx context.Context, user cn.CapUser) (names []string, err *mft.Error) {
	request := MarshalRequestMust(user, cn.OpGetExternalClustersList, nil)
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&names)

	return names, err
}

func (eac *ExternalAbstractCluster) GetExternalCluster(ctx context.Context, user cn.CapUser, name string) (cluster Cluster, exists bool, err *mft.Error) {
	// TODO: Make check
	eacOut := &ExternalAbstractCluster{
		CallTimeout: eac.CallTimeout,
		CallFunc: func(ctx context.Context, requestNest *RequestBody) (responceNest *ResponceBody) {
			request := MarshalRequestMust(user, cn.OpNestedCall, requestNest)
			request.ObjectName = name

			responce := eac.Call(request)

			return responce
		},
		ThrowErrorFunc: eac.ThrowErrorFunc,
	}

	return eacOut, true, nil
}
func (eac *ExternalAbstractCluster) AddHandler(ctx context.Context, user cn.CapUser, handlerParams HandlerDescription) (err *mft.Error) {
	request := MarshalRequestMust(user, cn.OpAddHandler, handlerParams)
	responce := eac.Call(request)
	return responce.Err
}
func (eac *ExternalAbstractCluster) DropHandler(ctx context.Context, user cn.CapUser, name string) (err *mft.Error) {
	request := MarshalRequestMust(user, cn.OpDropHandler, name)
	responce := eac.Call(request)
	return responce.Err
}
func (eac *ExternalAbstractCluster) GetHandlerDescription(ctx context.Context, user cn.CapUser, name string) (handlerParams HandlerDescription, err *mft.Error) {
	request := MarshalRequestMust(user, cn.OpGetHandlerDescription, name)
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&handlerParams)

	return handlerParams, err
}
func (eac *ExternalAbstractCluster) GetHandlersList(ctx context.Context, user cn.CapUser) (names []string, err *mft.Error) {
	request := MarshalRequestMust(user, cn.OpGetHandlersList, nil)
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&names)

	return names, err
}

func (eac *ExternalAbstractCluster) GetHandler(ctx context.Context, user cn.CapUser, name string) (handler Handler, exists bool, err *mft.Error) {
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

func (eac *ExternalAbstractCluster) CheckPermission(ctx context.Context, user cn.CapUser, objectType string, action string, objectName string) (allowed bool, err *mft.Error) {
	request := MarshalRequestMust(user, cn.OpCheckPermission, CheckPermissionRequest{
		ObjectType: objectType,
		Action:     action,
		ObjectName: objectName,
	})
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&allowed)

	return allowed, err
}
func (eac *ExternalAbstractCluster) GetFullStruct(ctx context.Context, user cn.CapUser) (data json.RawMessage, err *mft.Error) {
	request := MarshalRequestMust(user, cn.OpGetFullStruct, nil)
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&data)

	return data, err
}
func (eac *ExternalAbstractCluster) LoadFullStruct(ctx context.Context, user cn.CapUser, data json.RawMessage) (err *mft.Error) {
	request := MarshalRequestMust(user, cn.OpLoadFullStruct, data)
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
	User      cn.CapUser
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

func (eac *ExternalAbstractQueue) Add(ctx context.Context, message []byte, externalID int64, externalDt int64, source string, segment int64, saveMode int) (id int64, err *mft.Error) {
	request := eac.MarshalRequestMust(cn.OpQueueAdd, QueueAddRequest{
		Message: queue.Message{
			ExternalID: externalID,
			ExternalDt: externalDt,
			Source:     source,
			Message:    message,
			Segment:    segment,
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
	request := eac.MarshalRequestMust(cn.OpQueueAddList, QueueAddListRequest{
		Messages: messages,
		SaveMode: saveMode,
	})
	responce := eac.CallFunc(ctx, request)

	err = responce.UnmarshalInnerObject(&ids)

	return ids, err
}

type QueueGetRequest struct {
	IdStart  int64 `json:"id_start"`
	CntLimit int   `json:"cnt_limit"`
}

func (eac *ExternalAbstractQueue) Get(ctx context.Context, idStart int64, cntLimit int) (messages []*queue.MessageWithMeta, err *mft.Error) {
	request := eac.MarshalRequestMust(cn.OpQueueGet, QueueGetRequest{
		IdStart:  idStart,
		CntLimit: cntLimit,
	})
	responce := eac.CallFunc(ctx, request)

	err = responce.UnmarshalInnerObject(&messages)

	return messages, err
}

type QueueGetSegmentRequest struct {
	IdStart  int64             `json:"id_start"`
	CntLimit int               `json:"cnt_limit"`
	Segments *segment.Segments `json:"segments"`
}

type QueueGetSegmentResponce struct {
	Messages []*queue.MessageWithMeta `json:"msgs"`
	LastId   int64                    `json:"last_id"`
}

func (eac *ExternalAbstractQueue) GetSegment(ctx context.Context, idStart int64, cntLimit int,
	segments *segment.Segments,
) (messages []*queue.MessageWithMeta, lastId int64, err *mft.Error) {
	var resp QueueGetSegmentResponce

	request := eac.MarshalRequestMust(cn.OpQueueGetSegment, QueueGetSegmentRequest{
		IdStart:  idStart,
		CntLimit: cntLimit,
		Segments: segments,
	})
	responce := eac.CallFunc(ctx, request)

	err = responce.UnmarshalInnerObject(&resp)

	return resp.Messages, resp.LastId, err
}

func (eac *ExternalAbstractQueue) SaveAll(ctx context.Context) (err *mft.Error) {
	request := eac.MarshalRequestMust(cn.OpQueueSaveAll, nil)
	responce := eac.CallFunc(ctx, request)

	return responce.Err
}

func (eac *ExternalAbstractQueue) AddUnique(ctx context.Context, message []byte, externalID int64, externalDt int64, source string, segment int64, saveMode int) (id int64, err *mft.Error) {
	request := eac.MarshalRequestMust(cn.OpQueueAddUnique, QueueAddRequest{
		Message: queue.Message{
			ExternalID: externalID,
			ExternalDt: externalDt,
			Source:     source,
			Message:    message,
			Segment:    segment,
		},
		SaveMode: saveMode,
	})
	responce := eac.CallFunc(ctx, request)

	err = responce.UnmarshalInnerObject(&id)

	return id, err
}

func (eac *ExternalAbstractQueue) AddUniqueList(ctx context.Context, messages []queue.Message, saveMode int) (ids []int64, err *mft.Error) {
	request := eac.MarshalRequestMust(cn.OpQueueAddUniqueList, QueueAddListRequest{
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
	request := eac.MarshalRequestMust(cn.OpQueueSubscriberSetLastRead, QueueSubscriberSetLastReadRequest{
		Subscriber: subscriber,
		Id:         id,
		SaveMode:   saveMode,
	})
	responce := eac.CallFunc(ctx, request)

	return responce.Err
}

func (eac *ExternalAbstractQueue) SubscriberGetLastRead(ctx context.Context, subscriber string) (id int64, err *mft.Error) {
	request := eac.MarshalRequestMust(cn.OpQueueSubscriberGetLastRead, subscriber)
	responce := eac.CallFunc(ctx, request)

	err = responce.UnmarshalInnerObject(&id)

	return id, err
}

func (eac *ExternalAbstractQueue) SubscriberAddReplicaMember(ctx context.Context, subscriber string) (err *mft.Error) {
	request := eac.MarshalRequestMust(cn.OpQueueSubscriberAddReplicaMember, subscriber)
	responce := eac.CallFunc(ctx, request)

	err = responce.Err

	return err
}

func (eac *ExternalAbstractQueue) SubscriberRemoveReplicaMember(ctx context.Context, subscriber string) (err *mft.Error) {
	request := eac.MarshalRequestMust(cn.OpQueueSubscriberRemoveReplicaMember, subscriber)
	responce := eac.CallFunc(ctx, request)

	err = responce.Err

	return err
}

func (eac *ExternalAbstractQueue) SubscriberGetReplicaCount(ctx context.Context, id int64) (cnt int, err *mft.Error) {
	request := eac.MarshalRequestMust(cn.OpQueueSubscriberGetReplicaCount, id)
	responce := eac.CallFunc(ctx, request)

	err = responce.UnmarshalInnerObject(&cnt)

	return cnt, err
}

type ExternalAbstractHandler struct {
	HandlerName string
	User        cn.CapUser
	CallFunc    func(ctx context.Context, request *RequestBody) (responce *ResponceBody)
}

func (eah *ExternalAbstractHandler) MarshalRequestMust(action string, v interface{}) *RequestBody {
	request := MarshalRequestMust(eah.User, action, v)
	request.ObjectName = eah.HandlerName

	return request
}

func (eah *ExternalAbstractHandler) Start(ctx context.Context) (err *mft.Error) {
	request := eah.MarshalRequestMust(cn.OpHandlerStart, nil)
	responce := eah.CallFunc(ctx, request)

	return responce.Err
}
func (eah *ExternalAbstractHandler) Stop(ctx context.Context) (err *mft.Error) {
	request := eah.MarshalRequestMust(cn.OpHandlerStop, nil)
	responce := eah.CallFunc(ctx, request)

	return responce.Err
}
func (eah *ExternalAbstractHandler) LastComplete(ctx context.Context) (lastComplete time.Time, err *mft.Error) {
	request := eah.MarshalRequestMust(cn.OpHandlerLastComplete, nil)
	responce := eah.CallFunc(ctx, request)

	err = responce.UnmarshalInnerObject(&lastComplete)

	return lastComplete, err
}
func (eah *ExternalAbstractHandler) LastError(ctx context.Context) (err *mft.Error) {
	request := eah.MarshalRequestMust(cn.OpHandlerLastError, nil)
	responce := eah.CallFunc(ctx, request)

	return responce.Err
}
func (eah *ExternalAbstractHandler) IsStarted(ctx context.Context) (isStarted bool, err *mft.Error) {
	request := eah.MarshalRequestMust(cn.OpHandlerIsStarted, nil)
	responce := eah.CallFunc(ctx, request)

	err = responce.UnmarshalInnerObject(&isStarted)

	return isStarted, err
}
