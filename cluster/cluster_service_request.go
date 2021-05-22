package cluster

import (
	"context"
	"encoding/json"
	"time"

	"github.com/capella-pw/queue/compress"
	"github.com/myfantasy/mft"
)

const (
	CtxStopTimeName = "ctx_stop_time"
)

type ServiceRequest struct {
	AuthentificationType string `json:"auth_type"`
	UserName             string `json:"user_name"`
	AuthentificationInfo []byte `json:"auth_info"`

	WaitDuration time.Duration `json:"wait"`
	CurrentTime  int64         `json:"current_time"`

	PreferContentType string `json:"prefer_content_type"`

	ReplaceNameForce bool `json:"replace_name_force"`

	Request *RequestBody `json:"request"`
}

func (sr *ServiceRequest) GetName() string {
	return sr.UserName
}

type ServiceResponce struct {
	TimeStart  int64 `json:"start"`
	TimeFinish int64 `json:"finish"`

	Responce ResponceBody `json:"responce"`
}

type CheckAuthFunc func(ctx context.Context, serviceRequest *ServiceRequest) (ok bool, failResponce ResponceBody)
type UnmarshalFunc func(ctx context.Context, contentType string, body []byte) (serviceRequest ServiceRequest, ok bool, failResponce ResponceBody)
type MarshalFunc func(ctx context.Context, contentType string, serviceResponce ServiceResponce) (body []byte, outContentType string, htmlCode int)

type ClusterService struct {
	Unmarshal UnmarshalFunc
	Marshal   MarshalFunc

	CheckAuth CheckAuthFunc
	Cluster   Cluster

	// ResponceDuration - Actual duration = ServiceRequest.WaitDuration - ResponceDuration
	ResponceDuration time.Duration
}

func CompressErrorJson(serviceResponceBase ServiceResponce, err *mft.Error) (body []byte) {
	serviceResponce := ServiceResponce{
		TimeStart:  serviceResponceBase.TimeStart,
		TimeFinish: serviceResponceBase.TimeFinish,
	}
	serviceResponce.Responce.Err = err

	body, errOut := json.Marshal(serviceResponce)
	if errOut != nil {
		panic(errOut)
	}

	return body
}

const HtmlCodeOk = 200
const HtmlCodeInternalError = 500

func CheckAuthFuncEmpty(ctx context.Context, serviceRequest *ServiceRequest) (ok bool, failResponce ResponceBody) {
	return true, failResponce
}

func ClusterServiceJsonCreate(checkAuth CheckAuthFunc, cluster Cluster, compressor *compress.Generator) (sc *ClusterService) {
	sc = &ClusterService{
		CheckAuth: checkAuth,
		Cluster:   cluster,

		Marshal: func(ctx context.Context, contentType string, serviceResponce ServiceResponce) (body []byte,
			outContentType string, htmlCode int) {

			respBody, er0 := json.Marshal(serviceResponce)
			if er0 != nil {
				return CompressErrorJson(serviceResponce, GenerateErrorE(10120100, er0)), "", HtmlCodeInternalError
			}

			algorithmUsed, result, err := compressor.Compress(ctx, false, contentType, respBody, nil)
			if err != nil {
				return CompressErrorJson(serviceResponce, GenerateErrorE(10120101, err)), "", HtmlCodeInternalError
			}

			return result, algorithmUsed, HtmlCodeOk
		},

		Unmarshal: func(ctx context.Context, contentType string, body []byte) (serviceRequest ServiceRequest, ok bool, failResponce ResponceBody) {
			algorithmUsed, result, err := compressor.Restore(ctx, contentType, body, nil)
			if err != nil {
				failResponce.Err = GenerateErrorE(10120102, err, algorithmUsed, contentType)
				return serviceRequest, false, failResponce
			}

			er0 := json.Unmarshal(result, &serviceRequest)
			if er0 != nil {
				failResponce.Err = GenerateErrorE(10120103, er0, contentType, algorithmUsed)
				return serviceRequest, false, failResponce
			}

			return serviceRequest, true, failResponce
		},
	}
	return sc
}

func (sc *ClusterService) Call(prepareCtx context.Context,
	contentType string, bodyIn []byte, addFunc []AdditionalCallFuncInClusterFunc,
) (bodyOut []byte, outContentType string, htmlCode int) {
	startTime := time.Now()
	sr := ServiceResponce{TimeStart: startTime.UnixNano()}

	serviceRequest, ok, failResponce := sc.Unmarshal(prepareCtx, contentType, bodyIn)
	if !ok {
		sr.TimeFinish = time.Now().UnixNano()
		sr.Responce = failResponce

		return sc.Marshal(prepareCtx, "", sr)
	}

	if serviceRequest.CurrentTime > sr.TimeStart {
		sr.TimeFinish = time.Now().UnixNano()
		sr.Responce.Err = GenerateError(10120000, sr.TimeStart, serviceRequest.CurrentTime)
		return sc.Marshal(prepareCtx, "", sr)
	}

	ctxFinishTime := time.Unix(0, serviceRequest.CurrentTime).Add(serviceRequest.WaitDuration).Add(-sc.ResponceDuration)

	if ctxFinishTime.Before(startTime) {
		sr.TimeFinish = time.Now().UnixNano()
		sr.Responce.Err = GenerateError(10120001, sr.TimeStart, serviceRequest.CurrentTime, serviceRequest.WaitDuration, -sc.ResponceDuration)
		return sc.Marshal(prepareCtx, "", sr)
	}

	ctx := context.WithValue(context.Background(), CtxStopTimeName, ctxFinishTime)
	ctx, cancel := context.WithDeadline(ctx, ctxFinishTime)
	defer cancel()

	ok, failResponce = sc.CheckAuth(ctx, &serviceRequest)
	if !ok {
		sr.TimeFinish = time.Now().UnixNano()
		sr.Responce = failResponce

		return sc.Marshal(ctx, "", sr)
	}

	clusterResponce := CallFuncInCluster(ctx, sc.Cluster, serviceRequest.Request, addFunc)

	sr.TimeFinish = time.Now().UnixNano()
	sr.Responce = *clusterResponce

	return sc.Marshal(ctx, serviceRequest.PreferContentType, sr)
}
