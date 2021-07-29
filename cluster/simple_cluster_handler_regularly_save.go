package cluster

import (
	"context"
	"encoding/json"
	"time"

	"github.com/myfantasy/mfs"
	"github.com/myfantasy/mft"
)

func RegularlySaveNewGenerator(
	ctx context.Context,
	cluster Cluster,
	hDescription HandlerDescription,
	idGenerator *mft.G,
) (*HandlerLoadDescription, *mft.Error) {
	hld := &HandlerLoadDescription{
		Name:       hDescription.Name,
		Type:       hDescription.Type,
		Params:     hDescription.Params,
		QueueNames: hDescription.QueueNames,
		UserName:   hDescription.UserName,
	}

	if len(hld.QueueNames) != 1 {
		return nil, GenerateError(10117500, len(hld.QueueNames))
	}

	var rshp RegularlySaveHandlerParams
	er0 := json.Unmarshal(hld.Params, &rshp)
	if er0 != nil {
		return nil, GenerateErrorE(10117501, er0)
	}

	if rshp.Interval <= 0 {
		return nil, GenerateError(10117502, rshp.Interval)
	}
	if rshp.Wait <= 0 {
		return nil, GenerateError(10117503, rshp.Wait)
	}

	return hld, nil
}

func RegularlySaveLoadGenerator(
	ctx context.Context,
	cluster Cluster,
	hDescription *HandlerLoadDescription,
	idGenerator *mft.G,
) (Handler, *mft.Error) {
	if len(hDescription.QueueNames) != 1 {
		return nil, GenerateError(10117600, len(hDescription.QueueNames))
	}

	var rshp RegularlySaveHandlerParams
	er0 := json.Unmarshal(hDescription.Params, &rshp)
	if er0 != nil {
		return nil, GenerateErrorE(10117601, er0)
	}

	rsh := &RegularlySaveHandler{
		Cluster:      cluster,
		QueueName:    hDescription.QueueNames[0],
		Interval:     rshp.Interval,
		Wait:         rshp.Wait,
		UserName:     hDescription.UserName,
		HDescription: hDescription,
	}

	return rsh, nil
}

type RegularlySaveHandlerParams struct {
	// Interval - interval between call
	Interval time.Duration `json:"interval"`
	// Wait - wait save timeout
	Wait time.Duration `json:"wait"`
}

func (hp RegularlySaveHandlerParams) ToJson() json.RawMessage {
	msg, er0 := json.Marshal(hp)
	if er0 != nil {
		panic(GenerateErrorE(10117700, er0))
	}

	return msg
}

type RegularlySaveHandler struct {
	Cluster      Cluster
	QueueName    string
	Interval     time.Duration
	Wait         time.Duration
	UserName     string
	HDescription *HandlerLoadDescription
	mx           mfs.PMutex
	chStop       chan bool
	lastComplete time.Time
	lastError    *mft.Error
}

func (rsh *RegularlySaveHandler) GetName() string {
	return rsh.UserName
}

func (rsh *RegularlySaveHandler) Start(ctx context.Context) (err *mft.Error) {
	rsh.mx.Lock()
	defer rsh.mx.Unlock()
	if rsh.chStop == nil {
		chStop := make(chan bool, 1)
		rsh.chStop = chStop
		go func() {
			for {
				ctxInternal, cancel := context.WithTimeout(context.Background(), rsh.Wait)
				q, exists, err := rsh.Cluster.GetQueue(ctx, rsh, rsh.QueueName)
				if err != nil {
					err = GenerateErrorForClusterUserE(rsh, 10117400, err, rsh.QueueName)
				} else if !exists {
					err = GenerateErrorForClusterUser(rsh, 10117401, rsh.QueueName)
				} else {
					err = q.SaveAll(ctxInternal, rsh)
					if err != nil {
						err = GenerateErrorForClusterUserE(rsh, 10117402, err, rsh.QueueName)
					}
				}

				if err == nil {
					rsh.lastComplete = time.Now()
				} else {
					rsh.lastError = err
					rsh.Cluster.ThrowError(err)
				}

				cancel()
				time.Sleep(rsh.Interval)
				select {
				case <-chStop:
					break
				default:
				}
			}
		}()
	}
	rsh.HDescription.Start = true
	err = rsh.Cluster.OnChange()

	if err != nil {
		return GenerateErrorE(10117403, err, rsh.HDescription.Name)
	}

	return nil
}
func (rsh *RegularlySaveHandler) Stop(ctx context.Context) (err *mft.Error) {
	rsh.mx.Lock()
	defer rsh.mx.Unlock()
	if rsh.chStop != nil {
		rsh.chStop <- true
		rsh.chStop = nil
	}

	rsh.HDescription.Start = false
	err = rsh.Cluster.OnChange()

	if err != nil {
		return GenerateErrorE(10117404, err, rsh.HDescription.Name)
	}

	return nil
}

func (rsh *RegularlySaveHandler) LastComplete(ctx context.Context) (time.Time, *mft.Error) {
	return rsh.lastComplete, nil
}
func (rsh *RegularlySaveHandler) LastError(ctx context.Context) (err *mft.Error) {
	return rsh.lastError
}
func (rsh *RegularlySaveHandler) IsStarted(ctx context.Context) (isStarted bool, err *mft.Error) {
	return rsh.HDescription.Start, nil
}
