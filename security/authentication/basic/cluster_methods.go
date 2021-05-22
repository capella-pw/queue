package basic

import (
	"context"

	"github.com/capella-pw/queue/cluster"
	"github.com/myfantasy/mft"
)

const (
	OpAddUseCluster     = "SecurityATCB_add_use_cluster"
	OpUpdateUseCluster  = "SecurityATCB_update_use_cluster"
	OpEnableUseCluster  = "SecurityATCB_enable_use_cluster"
	OpDisableUseCluster = "SecurityATCB_disable_use_cluster"
	OpDropUseCluster    = "SecurityATCB_drop_use_cluster"
	OpGetUseCluster     = "SecurityATCB_get_use_cluster"
)

func (s *SecurityATCB) AdditionalCallFuncInClusterFunc(ctx context.Context,
	cl cluster.Cluster, request *cluster.RequestBody) (responce *cluster.ResponceBody, ok bool) {

	if request.Action == OpAddUseCluster {
		var us UserSend

		err := request.UnmarshalInnerObject(&us)
		if err != nil {
			responce = cluster.MarshalResponceMust(nil, err)
			return responce, true
		}

		err = s.Add(request, us)

		responce = cluster.MarshalResponceMust(nil, err)
		return responce, true
	}
	if request.Action == OpUpdateUseCluster {
		var us UserSend

		err := request.UnmarshalInnerObject(&us)
		if err != nil {
			responce = cluster.MarshalResponceMust(nil, err)
			return responce, true
		}

		err = s.Update(request, us)

		responce = cluster.MarshalResponceMust(nil, err)
		return responce, true
	}
	if request.Action == OpEnableUseCluster {
		var name string

		err := request.UnmarshalInnerObject(&name)
		if err != nil {
			responce = cluster.MarshalResponceMust(nil, err)
			return responce, true
		}

		err = s.Enable(request, name)

		responce = cluster.MarshalResponceMust(nil, err)
		return responce, true
	}
	if request.Action == OpDisableUseCluster {
		var name string

		err := request.UnmarshalInnerObject(&name)
		if err != nil {
			responce = cluster.MarshalResponceMust(nil, err)
			return responce, true
		}

		err = s.Disable(request, name)

		responce = cluster.MarshalResponceMust(nil, err)
		return responce, true
	}
	if request.Action == OpDropUseCluster {
		var name string

		err := request.UnmarshalInnerObject(&name)
		if err != nil {
			responce = cluster.MarshalResponceMust(nil, err)
			return responce, true
		}

		err = s.Drop(request, name)

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

func AddUseCluster(eac *cluster.ExternalAbstractCluster, user cluster.ClusterUser, us UserSend) (err *mft.Error) {
	request := cluster.MarshalRequestMust(user, OpAddUseCluster, us)
	responce := eac.Call(request)

	return responce.Err
}

func UpdateUseCluster(eac *cluster.ExternalAbstractCluster, user cluster.ClusterUser, us UserSend) (err *mft.Error) {
	request := cluster.MarshalRequestMust(user, OpUpdateUseCluster, us)
	responce := eac.Call(request)

	return responce.Err
}

func EnableUseCluster(eac *cluster.ExternalAbstractCluster, user cluster.ClusterUser, name string) (err *mft.Error) {
	request := cluster.MarshalRequestMust(user, OpEnableUseCluster, name)
	responce := eac.Call(request)

	return responce.Err
}
func DisableUseCluster(eac *cluster.ExternalAbstractCluster, user cluster.ClusterUser, name string) (err *mft.Error) {
	request := cluster.MarshalRequestMust(user, OpDisableUseCluster, name)
	responce := eac.Call(request)

	return responce.Err
}
func DropUseCluster(eac *cluster.ExternalAbstractCluster, user cluster.ClusterUser, name string) (err *mft.Error) {
	request := cluster.MarshalRequestMust(user, OpDropUseCluster, name)
	responce := eac.Call(request)

	return responce.Err
}
func GetUseCluster(eac *cluster.ExternalAbstractCluster, user cluster.ClusterUser) (users []UserCut, err *mft.Error) {
	request := cluster.MarshalRequestMust(user, OpGetUseCluster, nil)
	responce := eac.Call(request)

	err = responce.UnmarshalInnerObject(&users)

	return users, err
}
