package authorization

import (
	"context"

	"github.com/capella-pw/queue/cluster"
	"github.com/capella-pw/queue/cn"
	"github.com/myfantasy/mft"
)

func (s *SecurityATRZ) AdditionalCallFuncInClusterFunc(ctx context.Context,
	cl cluster.Cluster, request *cluster.RequestBody) (responce *cluster.ResponceBody, ok bool) {

	if request.Action == cn.OpAAddUserUseCluster {
		var rq AddUserRequest

		err := request.UnmarshalInnerObject(&rq)
		if err != nil {
			responce = cluster.MarshalResponceMust(nil, err)
			return responce, true
		}

		err = s.AddUser(ctx, request, rq.Name, rq.IsAdmin)

		responce = cluster.MarshalResponceMust(nil, err)
		return responce, true
	}
	if request.Action == cn.OpASetUserAdminUseCluster {
		var rq SetUserAdminRequest

		err := request.UnmarshalInnerObject(&rq)
		if err != nil {
			responce = cluster.MarshalResponceMust(nil, err)
			return responce, true
		}

		err = s.SetUserAdmin(ctx, request, rq.Name, rq.IsAdmin)

		responce = cluster.MarshalResponceMust(nil, err)
		return responce, true
	}
	if request.Action == cn.OpADropUserUseCluster {
		var name string

		err := request.UnmarshalInnerObject(&name)
		if err != nil {
			responce = cluster.MarshalResponceMust(nil, err)
			return responce, true
		}

		err = s.DropUser(ctx, request, name)

		responce = cluster.MarshalResponceMust(nil, err)
		return responce, true
	}
	if request.Action == cn.OpAUserRuleUseCluster {
		var rq UserRuleSetRequest

		err := request.UnmarshalInnerObject(&rq)
		if err != nil {
			responce = cluster.MarshalResponceMust(nil, err)
			return responce, true
		}

		err = s.UserRuleSet(ctx, request, rq.Name, rq.ObjectType, rq.Action, rq.ObjectName, rq.Allowed)

		responce = cluster.MarshalResponceMust(nil, err)
		return responce, true
	}
	if request.Action == cn.OpAUserRuleDropCluster {
		var rq UserRuleDropRequest

		err := request.UnmarshalInnerObject(&rq)
		if err != nil {
			responce = cluster.MarshalResponceMust(nil, err)
			return responce, true
		}

		err = s.UserRuleDrop(ctx, request, rq.Name, rq.ObjectType, rq.Action, rq.ObjectName)

		responce = cluster.MarshalResponceMust(nil, err)
		return responce, true
	}
	if request.Action == cn.OpAGetUseCluster {
		names, err := s.Get(ctx, request)

		responce = cluster.MarshalResponceMust(names, err)
		return responce, true
	}

	return responce, false
}

type AddUserRequest struct {
	Name    string `json:"name"`
	IsAdmin bool   `json:"is_admin"`
}

func AddUserUseCluster(eac *cluster.ExternalAbstractCluster, user cn.CapUser, name string, isAdmin bool) (err *mft.Error) {
	request := cluster.MarshalRequestMust(user, cn.OpAAddUserUseCluster, AddUserRequest{
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

func SetUserAdminUseCluster(eac *cluster.ExternalAbstractCluster, user cn.CapUser, name string, isAdmin bool) (err *mft.Error) {
	request := cluster.MarshalRequestMust(user, cn.OpASetUserAdminUseCluster, SetUserAdminRequest{
		Name:    name,
		IsAdmin: isAdmin,
	})
	responce := eac.Call(request)

	return responce.Err
}
func DropUserUseCluster(eac *cluster.ExternalAbstractCluster, user cn.CapUser, name string) (err *mft.Error) {
	request := cluster.MarshalRequestMust(user, cn.OpADropUserUseCluster, name)
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

func UserRuleSetUseCluster(eac *cluster.ExternalAbstractCluster, user cn.CapUser, name string, objectType string, action string, objectName string, allowed bool) (err *mft.Error) {
	request := cluster.MarshalRequestMust(user, cn.OpAUserRuleUseCluster, UserRuleSetRequest{
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

func UserRuleDropUseCluster(eac *cluster.ExternalAbstractCluster, user cn.CapUser, name string, objectType string, action string, objectName string) (err *mft.Error) {
	request := cluster.MarshalRequestMust(user, cn.OpAUserRuleDropCluster, UserRuleDropRequest{
		Name:       name,
		ObjectType: objectType,
		Action:     action,
		ObjectName: objectName,
	})
	responce := eac.Call(request)

	return responce.Err
}
func GetUseCluster(eac *cluster.ExternalAbstractCluster, user cn.CapUser) (sOut *SecurityATRZ, err *mft.Error) {
	sOut = &SecurityATRZ{
		Users: make(map[string]*User),
	}
	request := cluster.MarshalRequestMust(user, cn.OpAGetUseCluster, nil)
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&sOut)

	return sOut, err
}
