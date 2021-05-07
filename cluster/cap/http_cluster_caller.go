package cap

import (
	"context"
	"encoding/json"
	"time"

	"github.com/capella-pw/queue/cluster"
	"github.com/capella-pw/queue/compress"
)

const (
	CompressTypeHeader = "Compress-Type"
	ClusterMethodPath  = "/cluster"
)

type ClusterConnection struct {
	Compressor *compress.Generator `json:"-"`

	Connection *Connection `json:"connection"`

	AuthentificationType string          `json:"auth_type"`
	AuthentificationInfo json.RawMessage `json:"auth_info"`
	PreferContentType    string          `json:"prefer_content_type"`
	SendContentType      string          `json:"send_content_type"`
}

func (cc *ClusterConnection) ToJson() json.RawMessage {
	msg, er0 := json.Marshal(cc)
	if er0 != nil {
		panic(GenerateErrorE(10190200, er0))
	}

	return msg
}

func CreateClusterConnection(compressor *compress.Generator,
	connection *Connection,
	authentificationType string,
	authentificationInfo json.RawMessage,
	preferContentType string,
	sendContentType string) (cc *ClusterConnection) {
	cc = &ClusterConnection{Compressor: compressor,

		Connection: connection,

		AuthentificationType: authentificationType,
		AuthentificationInfo: authentificationInfo,
		PreferContentType:    preferContentType,
		SendContentType:      sendContentType,
	}

	return cc
}

func (cc *ClusterConnection) Init() {
	cc.Connection.Init()
}

func (cc *ClusterConnection) CallFunc() func(ctx context.Context,
	request *cluster.RequestBody) (responce *cluster.ResponceBody) {
	return func(ctx context.Context,
		request *cluster.RequestBody) (responce *cluster.ResponceBody) {

		currentTime := time.Now()
		waitDuration := cc.Connection.QueryWait

		sreq := cluster.ServiceRequest{
			AuthentificationType: cc.AuthentificationType,
			AuthentificationInfo: cc.AuthentificationInfo,

			WaitDuration: waitDuration,
			CurrentTime:  currentTime.UnixNano(),

			PreferContentType: cc.PreferContentType,

			Request: request,
		}

		bodyOut, er0 := json.Marshal(sreq)
		if er0 != nil {
			return &cluster.ResponceBody{Err: GenerateErrorE(10190100, er0)}
		}

		algorithmUsed, resultOut, err :=
			cc.Compressor.Compress(ctx, false, cc.SendContentType, bodyOut, nil)
		if err != nil {
			return &cluster.ResponceBody{Err: GenerateErrorE(10190101, err)}
		}

		bodyIn, headersOut, statusCode, er0 := cc.Connection.DoRawQuery(waitDuration, ClusterMethodPath,
			map[string]string{CompressTypeHeader: algorithmUsed}, resultOut)
		if er0 != nil {
			return &cluster.ResponceBody{Err: GenerateErrorE(10190102, er0)}
		}
		if statusCode != 200 {
			return &cluster.ResponceBody{Err: GenerateError(10190103, statusCode, string(bodyIn))}
		}

		decompressAlg, _ := headersOut[CompressTypeHeader]

		_, resultIn, err := cc.Compressor.Restore(ctx, decompressAlg, bodyIn, nil)
		if err != nil {
			return &cluster.ResponceBody{Err: GenerateErrorE(10190104, err)}
		}

		clusterResponce := cluster.ServiceResponce{}

		er0 = json.Unmarshal(resultIn, &clusterResponce)
		if er0 != nil {
			return &cluster.ResponceBody{Err: GenerateErrorE(10190105, er0)}
		}

		return &clusterResponce.Responce
	}
}

func (cc *ClusterConnection) Cluster() *cluster.ExternalAbstractCluster {
	clusterOut := &cluster.ExternalAbstractCluster{
		CallTimeout: cc.Connection.QueryWait,
		CallFunc:    cc.CallFunc(),
	}
	return clusterOut
}