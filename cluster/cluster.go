package cluster

import (
	"context"
	"encoding/json"
	"time"

	"github.com/capella-pw/queue/queue"
	"github.com/myfantasy/mft"
)

// ClusterUser - user for cluser for queue
type ClusterUser interface {
	GetName() string
}

type ClusterUserName string

func (cun ClusterUserName) GetName() string {
	return string(cun)
}

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

const (
	ClusterSelfObjectType     = "CLUSTER_SELF"
	QueueObjectType           = "QUEUE"
	ExternalClusterObjectType = "EXTERNAL_CLUSTER"
	HandlerObjectType         = "HANDLER"
)

const (
	GetNameAction = "GET_NAME"
	SetNameAction = "SET_NAME"

	PingAction       = "PING"
	GetNextIdAction  = "GET_NEXT_ID"
	GetNextIdsAction = "GET_NEXT_IDS"

	AddQueueAction      = "ADD_QUEUE"
	DropQueueAction     = "DROP_QUEUE"
	GetQueueDescrAction = "GET_QUEUE_DESCR"
	GetQueueAction      = "GET_QUEUE"

	AddExternalClusterAction      = "ADD_EXT_CLUSTER"
	DropExternalClusterAction     = "DROP_EXT_CLUSTER"
	GetExternalClusterDescrAction = "GET_EXT_CLUSTER_DESCR"
	GetExternalClusterAction      = "GET_EXT_CLUSTER"

	AddHandlerAction      = "ADD_HANDLER"
	DropHandlerAction     = "DROP_HANDLER"
	GetHandlerDescrAction = "GET_HANDLER_DESCR"
	GetHandlerAction      = "GET_HANDLER"
)

// Cluster - cluser for queue
type Cluster interface {
	GetName(user ClusterUser) (name string, err *mft.Error)
	SetName(user ClusterUser, name string) (err *mft.Error)

	ThrowError(err *mft.Error) bool

	AddQueue(user ClusterUser, queueDescription QueueDescription) (err *mft.Error)
	DropQueue(user ClusterUser, name string) (err *mft.Error)
	GetQueueDescription(user ClusterUser, name string) (queueDescription QueueDescription, err *mft.Error)
	GetQueuesList(user ClusterUser) (names []string, err *mft.Error)

	GetQueue(user ClusterUser, name string) (queue queue.Queue, exists bool, err *mft.Error)

	AddExternalCluster(user ClusterUser, clusterParams ExternalClusterDescription) (err *mft.Error)
	DropExternalCluster(user ClusterUser, name string) (err *mft.Error)
	GetExternalClusterDescription(user ClusterUser, name string) (clusterParams ExternalClusterDescription, err *mft.Error)
	GetExternalClustersList(user ClusterUser) (names []string, err *mft.Error)

	// GetExternalCluster - gets cluster. Use '/' for separate names.
	GetExternalCluster(user ClusterUser, name string) (cluster Cluster, exists bool, err *mft.Error)

	AddHandler(user ClusterUser, handlerParams HandlerDescription) (err *mft.Error)
	DropHandler(user ClusterUser, name string) (err *mft.Error)
	GetHandlerDescription(user ClusterUser, name string) (handlerParams HandlerDescription, err *mft.Error)
	GetHandlersList(user ClusterUser) (names []string, err *mft.Error)
	GetHandler(user ClusterUser, name string) (handler Handler, exists bool, err *mft.Error)

	CheckPermission(user ClusterUser, objectType string, action string, objectName string) (allowed bool, err *mft.Error)

	GetFullStruct(user ClusterUser) (data json.RawMessage, err *mft.Error)
	LoadFullStruct(user ClusterUser, data json.RawMessage) (err *mft.Error)

	SetValueInternal(string, string) (err *mft.Error)
	GetValueInternal(string) (string, bool)

	OnChange() (err *mft.Error)

	Ping(user ClusterUser) (err *mft.Error)
	GetNextId(user ClusterUser) (id int64, err *mft.Error)
	GetNextIds(user ClusterUser, cnt int) (ids []int64, err *mft.Error)
}

// Handler - handler
type Handler interface {
	Start(ctx context.Context) (err *mft.Error)
	Stop(ctx context.Context) (err *mft.Error)
	LastComplete(ctx context.Context) (lastComplete time.Time, err *mft.Error)
	LastError(ctx context.Context) (err *mft.Error)
	IsStarted(ctx context.Context) (isStarted bool, err *mft.Error)
}
