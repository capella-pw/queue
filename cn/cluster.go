package cn

// Object types
const (
	ClusterSelfObjectType     = "CLUSTER_SELF"
	QueueObjectType           = "QUEUE"
	ExternalClusterObjectType = "EXTERNAL_CLUSTER"
	HandlerObjectType         = "HANDLER"
)

// Actions
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

// Operation names
const (
	OpGetName = "get_name"
	OpSetName = "set_name"

	OpPing       = "ping"
	OpGetNextId  = "get_next_id"
	OpGetNextIds = "get_next_ids"

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
	OpQueueGetSegment    = "q_get_segment"
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
	OpHandlerIsStarted    = "h_is_started"
)
