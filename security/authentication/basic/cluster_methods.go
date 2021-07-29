package basic

import (
	"context"

	"github.com/capella-pw/queue/cluster"
	"github.com/capella-pw/queue/cn"
	"github.com/myfantasy/mft"
)

func (s *SecurityATCB) AdditionalCallFuncInClusterFunc(ctx context.Context,
	cl cluster.Cluster, request *cluster.RequestBody) (responce *cluster.ResponceBody, ok bool) {

	if request.Action == cn.OpABAddUseCluster {
		var us UserSend

		err := request.UnmarshalInnerObject(&us)
		if err != nil {
			responce = cluster.MarshalResponceMust(nil, err)
			return responce, true
		}

		err = s.Add(ctx, request, us)

		responce = cluster.MarshalResponceMust(nil, err)
		return responce, true
	}
	if request.Action == cn.OpABUpdateUseCluster {
		var us UserSend

		err := request.UnmarshalInnerObject(&us)
		if err != nil {
			responce = cluster.MarshalResponceMust(nil, err)
			return responce, true
		}

		err = s.Update(ctx, request, us)

		responce = cluster.MarshalResponceMust(nil, err)
		return responce, true
	}
	if request.Action == cn.OpABEnableUseCluster {
		var name string

		err := request.UnmarshalInnerObject(&name)
		if err != nil {
			responce = cluster.MarshalResponceMust(nil, err)
			return responce, true
		}

		err = s.Enable(ctx, request, name)

		responce = cluster.MarshalResponceMust(nil, err)
		return responce, true
	}
	if request.Action == cn.OpABDisableUseCluster {
		var name string

		err := request.UnmarshalInnerObject(&name)
		if err != nil {
			responce = cluster.MarshalResponceMust(nil, err)
			return responce, true
		}

		err = s.Disable(ctx, request, name)

		responce = cluster.MarshalResponceMust(nil, err)
		return responce, true
	}
	if request.Action == cn.OpABDropUseCluster {
		var name string

		err := request.UnmarshalInnerObject(&name)
		if err != nil {
			responce = cluster.MarshalResponceMust(nil, err)
			return responce, true
		}

		err = s.Drop(ctx, request, name)

		responce = cluster.MarshalResponceMust(nil, err)
		return responce, true
	}
	if request.Action == cn.OpABGetUseCluster {
		names, err := s.Get(ctx, request)

		responce = cluster.MarshalResponceMust(names, err)
		return responce, true
	}

	return responce, false
}

func AddUseCluster(eac *cluster.ExternalAbstractCluster, user cn.CapUser, us UserSend) (err *mft.Error) {
	request := cluster.MarshalRequestMust(user, cn.OpABAddUseCluster, us)
	responce := eac.Call(request)

	return responce.Err
}

func UpdateUseCluster(eac *cluster.ExternalAbstractCluster, user cn.CapUser, us UserSend) (err *mft.Error) {
	request := cluster.MarshalRequestMust(user, cn.OpABUpdateUseCluster, us)
	responce := eac.Call(request)

	return responce.Err
}

func EnableUseCluster(eac *cluster.ExternalAbstractCluster, user cn.CapUser, name string) (err *mft.Error) {
	request := cluster.MarshalRequestMust(user, cn.OpABEnableUseCluster, name)
	responce := eac.Call(request)

	return responce.Err
}
func DisableUseCluster(eac *cluster.ExternalAbstractCluster, user cn.CapUser, name string) (err *mft.Error) {
	request := cluster.MarshalRequestMust(user, cn.OpABDisableUseCluster, name)
	responce := eac.Call(request)

	return responce.Err
}
func DropUseCluster(eac *cluster.ExternalAbstractCluster, user cn.CapUser, name string) (err *mft.Error) {
	request := cluster.MarshalRequestMust(user, cn.OpABDropUseCluster, name)
	responce := eac.Call(request)

	return responce.Err
}
func GetUseCluster(eac *cluster.ExternalAbstractCluster, user cn.CapUser) (users []UserCut, err *mft.Error) {
	request := cluster.MarshalRequestMust(user, cn.OpABGetUseCluster, nil)
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&users)

	return users, err
}
