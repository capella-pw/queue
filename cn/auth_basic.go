package cn

// Object types
const (
	AuthBasicSelfObjectType = "AUTH_BASIC_SELF"
	AuthBasicObjectType     = "AUTH_BASIC"
)

// Actions
const (
	ABAddAction     = "ADD_USER"
	ABUpdateAction  = "UPDATE_USER"
	ABEnableAction  = "ENABLE_USER"
	ABDisableAction = "DISABLE_USER"
	ABGetAction     = "GET_ALL"
	ABDropAction    = "DROP_USER"

	ABImpersonateAction = "IMPERSONATE"
)

// operations
const (
	OpABAddUseCluster     = "SecurityATCB_add_use_cluster"
	OpABUpdateUseCluster  = "SecurityATCB_update_use_cluster"
	OpABEnableUseCluster  = "SecurityATCB_enable_use_cluster"
	OpABDisableUseCluster = "SecurityATCB_disable_use_cluster"
	OpABDropUseCluster    = "SecurityATCB_drop_use_cluster"
	OpABGetUseCluster     = "SecurityATCB_get_use_cluster"
)
