package cn

// Object types
const (
	AuthorizationSelfObjectType = "AUTHORIZATION_SELF"
)

// Actions
const (
	AAddUserAction      = "ADD_USER"
	ASetUserAdminAction = "SET_USER_ADMIN"
	ADropUserAction     = "DROP_USER"
	AUserRuleSetAction  = "USER_RULE_SET"
	AUserRuleDropAction = "USER_RULE_DROP"
	AGetAction          = "GET_USER"
)

// operations
const (
	OpAAddUserUseCluster      = "SecurityATRZ_add_user"
	OpASetUserAdminUseCluster = "SecurityATRZ_set_user_admin"
	OpADropUserUseCluster     = "SecurityATRZ_drop_user"
	OpAUserRuleUseCluster     = "SecurityATRZ_user_role_set"
	OpAUserRuleDropCluster    = "SecurityATRZ_user_role_drop"
	OpAGetUseCluster          = "SecurityATRZ_get"
)
