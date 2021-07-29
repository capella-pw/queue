package cluster

import (
	"context"
	"encoding/json"
	"time"

	"github.com/capella-pw/queue/queue"
	"github.com/myfantasy/mfs"
	"github.com/myfantasy/mft"
)

func BlockUnloadNewGenerator(
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
		return nil, GenerateError(10118320, len(hld.QueueNames))
	}

	var rshp BlockUnloadHandlerParams
	er0 := json.Unmarshal(hld.Params, &rshp)
	if er0 != nil {
		return nil, GenerateErrorE(10118321, er0)
	}

	if rshp.Interval <= 0 {
		return nil, GenerateError(10118322, rshp.Interval)
	}
	if rshp.Wait <= 0 {
		return nil, GenerateError(10118323, rshp.Wait)
	}

	if rshp.StorageMemoryTime <= 0 {
		return nil, GenerateError(10118324, rshp.StorageMemoryTime)
	}
	if rshp.StorageLastLoadTime <= 0 {
		return nil, GenerateError(10118325, rshp.StorageLastLoadTime)
	}

	return hld, nil
}

func BlockUnloadLoadGenerator(
	ctx context.Context,
	cluster Cluster,
	hDescription *HandlerLoadDescription,
	idGenerator *mft.G,
) (Handler, *mft.Error) {
	if len(hDescription.QueueNames) != 1 {
		return nil, GenerateError(10118340, len(hDescription.QueueNames))
	}

	var rshp BlockUnloadHandlerParams
	er0 := json.Unmarshal(hDescription.Params, &rshp)
	if er0 != nil {
		return nil, GenerateErrorE(10118341, er0)
	}

	rsh := &BlockUnloadHandler{
		Cluster:             cluster,
		QueueName:           hDescription.QueueNames[0],
		Interval:            rshp.Interval,
		Wait:                rshp.Wait,
		UserName:            hDescription.UserName,
		HDescription:        hDescription,
		StorageMemoryTime:   rshp.StorageMemoryTime,
		StorageLastLoadTime: rshp.StorageLastLoadTime,
	}

	return rsh, nil
}

type BlockUnloadHandlerParams struct {
	// Interval - interval between call
	Interval time.Duration `json:"interval"`
	// Wait - wait save timeout
	Wait time.Duration `json:"wait"`
	// StorageMemoryTime - wait timeout to remove block from memory from create
	StorageMemoryTime time.Duration `json:"storage_memory_time"`
	// StorageLastLoadTime - wait tiomeou to remove block from last use
	StorageLastLoadTime time.Duration `json:"storage_last_load_time"`
}

func (hp BlockUnloadHandlerParams) ToJson() json.RawMessage {
	msg, er0 := json.Marshal(hp)
	if er0 != nil {
		panic(GenerateErrorE(10118360, er0))
	}

	return msg
}

type BlockUnloadHandler struct {
	Cluster             Cluster
	QueueName           string
	Interval            time.Duration
	Wait                time.Duration
	UserName            string
	HDescription        *HandlerLoadDescription
	StorageMemoryTime   time.Duration
	StorageLastLoadTime time.Duration
	mx                  mfs.PMutex
	chStop              chan bool
	lastComplete        time.Time
	lastError           *mft.Error
}

func (rsh *BlockUnloadHandler) GetName() string {
	return rsh.UserName
}

func (rsh *BlockUnloadHandler) Start(ctx context.Context) (err *mft.Error) {
	rsh.mx.Lock()
	defer rsh.mx.Unlock()
	if rsh.chStop == nil {
		chStop := make(chan bool, 1)
		rsh.chStop = chStop
		go func() {
			for {
				ctxInternalMark, cancelMark := context.WithTimeout(context.Background(), rsh.Wait)
				q, exists, err := rsh.Cluster.GetQueue(ctx, rsh, rsh.QueueName)
				if err != nil {
					err = GenerateErrorForClusterUserE(rsh, 10118300, err, rsh.QueueName)
				} else if !exists {
					err = GenerateErrorForClusterUser(rsh, 10118301, rsh.QueueName)
				} else if sq, ok := q.(*queue.SimpleQueue); !ok {
					err = GenerateErrorForClusterUser(rsh, 10118302, rsh.QueueName)
				} else {

					dtCheck := time.Now().Add(-rsh.StorageMemoryTime).Add(-sq.TimeLimit)
					dtCheckLoad := time.Now().Add(-rsh.StorageLastLoadTime)

					err = sq.SetUnload(ctxInternalMark, rsh,
						func(ctx context.Context,
							i int, len int,
							q *queue.SimpleQueue, block *queue.SimpleQueueBlock) (needUnload bool, err *mft.Error) {
							if i >= len-1 {
								return false, nil
							}
							if block.Dt.After(dtCheck) {
								return false, nil
							}
							if block.LastGet.Before(dtCheckLoad) {
								return true, nil
							}
							return false, nil
						})

					if err != nil {
						err = GenerateErrorForClusterUserE(rsh, 10118303, err, rsh.QueueName)
					}
				}

				if err == nil {
					rsh.lastComplete = time.Now()
				} else {
					rsh.lastError = err
					rsh.Cluster.ThrowError(err)
				}

				cancelMark()
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
		return GenerateErrorE(10118304, err, rsh.HDescription.Name)
	}

	return nil
}
func (rsh *BlockUnloadHandler) Stop(ctx context.Context) (err *mft.Error) {
	rsh.mx.Lock()
	defer rsh.mx.Unlock()
	if rsh.chStop != nil {
		rsh.chStop <- true
		rsh.chStop = nil
	}

	rsh.HDescription.Start = false
	err = rsh.Cluster.OnChange()

	if err != nil {
		return GenerateErrorE(10118305, err, rsh.HDescription.Name)
	}

	return nil
}

func (rsh *BlockUnloadHandler) LastComplete(ctx context.Context) (time.Time, *mft.Error) {
	return rsh.lastComplete, nil
}
func (rsh *BlockUnloadHandler) LastError(ctx context.Context) (err *mft.Error) {
	return rsh.lastError
}
func (rsh *BlockUnloadHandler) IsStarted(ctx context.Context) (isStarted bool, err *mft.Error) {
	return rsh.HDescription.Start, nil
}
