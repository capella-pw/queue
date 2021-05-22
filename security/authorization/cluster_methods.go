package authorization

import (
	"context"

	"github.com/capella-pw/queue/cluster"
	"github.com/myfantasy/mft"
)

const (
	OpAddUserUseCluster      = "SecurityATRZ_add_user"
	OpSetUserAdminUseCluster = "SecurityATRZ_set_user_admin"
	OpDropUserUseCluster     = "SecurityATRZ_drop_user"
	OpUserRuleUseCluster     = "SecurityATRZ_user_role_set"
	OpUserRuleDropCluster    = "SecurityATRZ_user_role_drop"
	OpGetUseCluster          = "SecurityATRZ_get"
)

func (s *SecurityATRZ) AdditionalCallFuncInClusterFunc(ctx context.Context,
	cl cluster.Cluster, request *cluster.RequestBody) (responce *cluster.ResponceBody, ok bool) {

	if request.Action == OpAddUserUseCluster {
		var rq AddUserRequest

		err := request.UnmarshalInnerObject(&rq)
		if err != nil {
			responce = cluster.MarshalResponceMust(nil, err)
			return responce, true
		}

		err = s.AddUser(request, rq.Name, rq.IsAdmin)

		responce = cluster.MarshalResponceMust(nil, err)
		return responce, true
	}
	if request.Action == OpSetUserAdminUseCluster {
		var rq SetUserAdminRequest

		err := request.UnmarshalInnerObject(&rq)
		if err != nil {
			responce = cluster.MarshalResponceMust(nil, err)
			return responce, true
		}

		err = s.SetUserAdmin(request, rq.Name, rq.IsAdmin)

		responce = cluster.MarshalResponceMust(nil, err)
		return responce, true
	}
	if request.Action == OpDropUserUseCluster {
		var name string

		err := request.UnmarshalInnerObject(&name)
		if err != nil {
			responce = cluster.MarshalResponceMust(nil, err)
			return responce, true
		}

		err = s.DropUser(request, name)

		responce = cluster.MarshalResponceMust(nil, err)
		return responce, true
	}
	if request.Action == OpUserRuleUseCluster {
		var rq UserRuleSetRequest

		err := request.UnmarshalInnerObject(&rq)
		if err != nil {
			responce = cluster.MarshalResponceMust(nil, err)
			return responce, true
		}

		err = s.UserRuleSet(request, rq.Name, rq.ObjectType, rq.Action, rq.ObjectName, rq.Allowed)

		responce = cluster.MarshalResponceMust(nil, err)
		return responce, true
	}
	if request.Action == OpUserRuleDropCluster {
		var rq UserRuleDropRequest

		err := request.UnmarshalInnerObject(&rq)
		if err != nil {
			responce = cluster.MarshalResponceMust(nil, err)
			return responce, true
		}

		err = s.UserRuleDrop(request, rq.Name, rq.ObjectType, rq.Action, rq.ObjectName)

		responce = cluster.MarshalResponceMust(nil, err)
		return responce, true
	}
	if request.Action == OpGetUseCluster {
		names, err := s.Get(request)

		responce = cluster.MarshalResponceMust(names, err)
		return responce, true
	}

	return responce, false
}

type AddUserRequest struct {
	Name    string `json:"name"`
	IsAdmin bool   `json:"is_admin"`
}

func AddUserUseCluster(eac *cluster.ExternalAbstractCluster, user cluster.ClusterUser, name string, isAdmin bool) (err *mft.Error) {
	request := cluster.MarshalRequestMust(user, OpAddUserUseCluster, AddUserRequest{
		Name:    name,
		IsAdmin: isAdmin,
	})
	responce := eac.Call(request)

	return responce.Err
}

type SetUserAdminRequest struct {
	Name    string `json:"name"`
	IsAdmin bool   `json:"is_admin"`
}

func SetUserAdminUseCluster(eac *cluster.ExternalAbstractCluster, user cluster.ClusterUser, name string, isAdmin bool) (err *mft.Error) {
	request := cluster.MarshalRequestMust(user, OpSetUserAdminUseCluster, SetUserAdminRequest{
		Name:    name,
		IsAdmin: isAdmin,
	})
	responce := eac.Call(request)

	return responce.Err
}
func DropUserUseCluster(eac *cluster.ExternalAbstractCluster, user cluster.ClusterUser, name string) (err *mft.Error) {
	request := cluster.MarshalRequestMust(user, OpDropUserUseCluster, name)
	responce := eac.Call(request)

	return responce.Err
}

type UserRuleSetRequest struct {
	Name       string `json:"name"`
	ObjectType string `json:"object_type"`
	Action     string `json:"action"`
	ObjectName string `json:"object_name"`
	Allowed    bool   `json:"allowed"`
}

func UserRuleSetUseCluster(eac *cluster.ExternalAbstractCluster, user cluster.ClusterUser, name string, objectType string, action string, objectName string, allowed bool) (err *mft.Error) {
	request := cluster.MarshalRequestMust(user, OpUserRuleUseCluster, UserRuleSetRequest{
		Name:       name,
		ObjectType: objectType,
		Action:     action,
		ObjectName: objectName,
		Allowed:    allowed,
	})
	responce := eac.Call(request)

	return responce.Err
}

type UserRuleDropRequest struct {
	Name       string `json:"name"`
	ObjectType string `json:"object_type"`
	Action     string `json:"action"`
	ObjectName string `json:"object_name"`
}

func UserRuleDropUseCluster(eac *cluster.ExternalAbstractCluster, user cluster.ClusterUser, name string, objectType string, action string, objectName string) (err *mft.Error) {
	request := cluster.MarshalRequestMust(user, OpUserRuleDropCluster, UserRuleDropRequest{
		Name:       name,
		ObjectType: objectType,
		Action:     action,
		ObjectName: objectName,
	})
	responce := eac.Call(request)

	return responce.Err
}
func GetUseCluster(eac *cluster.ExternalAbstractCluster, user cluster.ClusterUser) (sOut *SecurityATRZ, err *mft.Error) {
	sOut = &SecurityATRZ{
		Users: make(map[string]*User),
	}
	request := cluster.MarshalRequestMust(user, OpGetUseCluster, nil)
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&sOut)

	return sOut, err
}
