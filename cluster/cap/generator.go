package cap

import (
	"context"
	"encoding/json"

	"github.com/capella-pw/queue/cluster"
	"github.com/capella-pw/queue/compress"
	"github.com/myfantasy/mft"
)

const (
	HttpExternalClusterType = "http_cluster"
)

func HttpExternalClusterNewGenerator(
	ctx context.Context,
	compressor *compress.Generator,
	ecDescription cluster.ExternalClusterDescription,
	idGenerator *mft.G,
) (ecld *cluster.ExternalClusterLoadDescription, err *mft.Error) {
	cc := &ClusterConnection{}

	ecld = &cluster.ExternalClusterLoadDescription{
		Name:   ecDescription.Name,
		Type:   ecDescription.Type,
		Params: ecDescription.Params,
	}

	er0 := json.Unmarshal(ecld.Params, cc)
	if er0 != nil {
		return nil, GenerateErrorE(10190300, er0, ecld.Name)
	}

	if cc.Connection == nil {
		return nil, GenerateError(10190301, ecld.Name)
	}

	if cc.Connection.QueryWait <= 0 {
		return nil, GenerateError(10190302, ecld.Name, cc.Connection.QueryWait)
	}

	if cc.Connection.Server == "" {
		return nil, GenerateError(10190303, ecld.Name)
	}

	return ecld, nil
}

func HttpExternalClusterLoadGenerator(
	ctx context.Context,
	compressor *compress.Generator,
	ecld *cluster.ExternalClusterLoadDescription,
	idGenerator *mft.G,
) (cluster.Cluster, *mft.Error) {
	cc := &ClusterConnection{
		Compressor: compressor,
	}

	er0 := json.Unmarshal(ecld.Params, cc)
	if er0 != nil {
		return nil, GenerateErrorE(10190400, er0, ecld.Name)
	}

	if cc.Connection == nil {
		return nil, GenerateError(10190401, ecld.Name)
	}

	cc.Init()

	return cc.Cluster(), nil
}

func init() {
	cluster.DefaultEcNewGenerator[HttpExternalClusterType] = HttpExternalClusterNewGenerator
	cluster.DefaultEcLoadGenerator[HttpExternalClusterType] = HttpExternalClusterLoadGenerator
}
