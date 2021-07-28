package cluster

import (
	"context"
	"encoding/json"
	"time"

	"github.com/capella-pw/queue/cn"
	"github.com/capella-pw/queue/queue"
	"github.com/myfantasy/mft"
)

// QueueDescription description of queue
type QueueDescription struct {
	Name         string          `json:"name"`
	Type         string          `json:"type"`
	CreateOnLoad bool            `json:"create_on_load"`
	Params       json.RawMessage `json:"params"`
}

// ExternalClusterDescription description of external cluster
type ExternalClusterDescription struct {
	Name   string          `json:"name"`
	Type   string          `json:"type"`
	Params json.RawMessage `json:"params"`
}

// HandlerDescription description of handler
type HandlerDescription struct {
	Name       string          `json:"name"`
	UserName   string          `json:"user_name"`
	Type       string          `json:"type"`
	QueueNames []string        `json:"queue_names"`
	Params     json.RawMessage `json:"params"`
}

// Cluster - cluser for queue
type Cluster interface {
	GetName(user cn.CapUser) (name string, err *mft.Error)
	SetName(user cn.CapUser, name string) (err *mft.Error)

	ThrowError(err *mft.Error) bool

	AddQueue(user cn.CapUser, queueDescription QueueDescription) (err *mft.Error)
	DropQueue(user cn.CapUser, name string) (err *mft.Error)
	GetQueueDescription(user cn.CapUser, name string) (queueDescription QueueDescription, err *mft.Error)
	GetQueuesList(user cn.CapUser) (names []string, err *mft.Error)

	GetQueue(user cn.CapUser, name string) (queue queue.Queue, exists bool, err *mft.Error)

	AddExternalCluster(user cn.CapUser, clusterParams ExternalClusterDescription) (err *mft.Error)
	DropExternalCluster(user cn.CapUser, name string) (err *mft.Error)
	GetExternalClusterDescription(user cn.CapUser, name string) (clusterParams ExternalClusterDescription, err *mft.Error)
	GetExternalClustersList(user cn.CapUser) (names []string, err *mft.Error)

	// GetExternalCluster - gets cluster. Use '/' for separate names.
	GetExternalCluster(user cn.CapUser, name string) (cluster Cluster, exists bool, err *mft.Error)

	AddHandler(user cn.CapUser, handlerParams HandlerDescription) (err *mft.Error)
	DropHandler(user cn.CapUser, name string) (err *mft.Error)
	GetHandlerDescription(user cn.CapUser, name string) (handlerParams HandlerDescription, err *mft.Error)
	GetHandlersList(user cn.CapUser) (names []string, err *mft.Error)
	GetHandler(user cn.CapUser, name string) (handler Handler, exists bool, err *mft.Error)

	CheckPermission(user cn.CapUser, objectType string, action string, objectName string) (allowed bool, err *mft.Error)

	GetFullStruct(user cn.CapUser) (data json.RawMessage, err *mft.Error)
	LoadFullStruct(user cn.CapUser, data json.RawMessage) (err *mft.Error)

	SetValueInternal(string, string) (err *mft.Error)
	GetValueInternal(string) (string, bool)

	OnChange() (err *mft.Error)

	Ping(user cn.CapUser) (err *mft.Error)
	GetNextId(user cn.CapUser) (id int64, err *mft.Error)
	GetNextIds(user cn.CapUser, cnt int) (ids []int64, err *mft.Error)
}

// Handler - handler
type Handler interface {
	Start(ctx context.Context) (err *mft.Error)
	Stop(ctx context.Context) (err *mft.Error)
	LastComplete(ctx context.Context) (lastComplete time.Time, err *mft.Error)
	LastError(ctx context.Context) (err *mft.Error)
	IsStarted(ctx context.Context) (isStarted bool, err *mft.Error)
}
