package cluster

import (
	"context"
	"encoding/json"
	"time"

	"github.com/capella-pw/queue/queue"
	"github.com/myfantasy/mfs"
	"github.com/myfantasy/mft"
)

func BlockDeleteNewGenerator(
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
		return nil, GenerateError(10118220, len(hld.QueueNames))
	}

	var rshp BlockDeleteHandlerParams
	er0 := json.Unmarshal(hld.Params, &rshp)
	if er0 != nil {
		return nil, GenerateErrorE(10118221, er0)
	}

	if rshp.Interval <= 0 {
		return nil, GenerateError(10118222, rshp.Interval)
	}
	if rshp.WaitMark <= 0 {
		return nil, GenerateError(10118223, rshp.WaitMark)
	}
	if rshp.WaitDelete <= 0 {
		return nil, GenerateError(10118224, rshp.WaitDelete)
	}
	if rshp.LimitDelete <= 0 {
		return nil, GenerateError(10118225, rshp.LimitDelete)
	}
	if rshp.StorageTime <= 0 {
		return nil, GenerateError(10118226, rshp.StorageTime)
	}

	return hld, nil
}

func BlockDeleteLoadGenerator(
	ctx context.Context,
	cluster Cluster,
	hDescription *HandlerLoadDescription,
	idGenerator *mft.G,
) (Handler, *mft.Error) {
	if len(hDescription.QueueNames) != 1 {
		return nil, GenerateError(10118240, len(hDescription.QueueNames))
	}

	var rshp BlockDeleteHandlerParams
	er0 := json.Unmarshal(hDescription.Params, &rshp)
	if er0 != nil {
		return nil, GenerateErrorE(10118241, er0)
	}

	rsh := &BlockDeleteHandler{
		Cluster:      cluster,
		QueueName:    hDescription.QueueNames[0],
		Interval:     rshp.Interval,
		UserName:     hDescription.UserName,
		HDescription: hDescription,
		WaitMark:     rshp.WaitMark,
		WaitDelete:   rshp.WaitDelete,
		StorageTime:  rshp.StorageTime,
		LimitDelete:  rshp.LimitDelete,
	}

	return rsh, nil
}

type BlockDeleteHandlerParams struct {
	// Interval - interval between call
	Interval time.Duration `json:"interval"`
	// WaitMark - wait to mark as deleted block timeout
	WaitMark time.Duration `json:"wait_mark"`
	// WaitDelete - wait to delete block timeout
	WaitDelete time.Duration `json:"wait_delete"`
	// StorageTime - time to storage data in queue. For example 3*24*time.Hour
	StorageTime time.Duration `json:"storage_time"`
	// Limit to delete block for one iteration
	LimitDelete int `json:"limit_delete"`
}

func (hp BlockDeleteHandlerParams) ToJson() json.RawMessage {
	msg, er0 := json.Marshal(hp)
	if er0 != nil {
		panic(GenerateErrorE(10118260, er0))
	}

	return msg
}

type BlockDeleteHandler struct {
	Cluster      Cluster
	QueueName    string
	Interval     time.Duration
	WaitMark     time.Duration
	WaitDelete   time.Duration
	UserName     string
	HDescription *HandlerLoadDescription
	StorageTime  time.Duration
	LimitDelete  int
	mx           mfs.PMutex
	chStop       chan bool
	lastComplete time.Time
	lastError    *mft.Error
}

func (rsh *BlockDeleteHandler) GetName() string {
	return rsh.UserName
}

func (rsh *BlockDeleteHandler) Start(ctx context.Context) (err *mft.Error) {
	rsh.mx.Lock()
	defer rsh.mx.Unlock()
	if rsh.chStop == nil {
		chStop := make(chan bool, 1)
		rsh.chStop = chStop
		go func() {
			for {
				var cancelDelete func()
				var ctxInternalDelete context.Context
				ctxInternalMark, cancelMark := context.WithTimeout(context.Background(), rsh.WaitMark)
				q, exists, err := rsh.Cluster.GetQueue(ctx, rsh, rsh.QueueName)
				if err != nil {
					err = GenerateErrorForClusterUserE(rsh, 10118200, err, rsh.QueueName)
				} else if !exists {
					err = GenerateErrorForClusterUser(rsh, 10118201, rsh.QueueName)
				} else if sq, ok := q.(*queue.SimpleQueue); !ok {
					err = GenerateErrorForClusterUser(rsh, 10118202, rsh.QueueName)
				} else {

					dtCheck := time.Now().Add(-rsh.StorageTime).Add(-sq.TimeLimit)

					err = sq.SetDelete(ctxInternalMark,
						func(ctx context.Context,
							i int, len int,
							q *queue.SimpleQueue, block *queue.SimpleQueueBlock,
						) (needDelete bool, err *mft.Error) {
							if block.Dt.Before(dtCheck) {
								return true, nil
							}
							return false, nil
						},
					)
					if err != nil {
						err = GenerateErrorForClusterUserE(rsh, 10118203, err, rsh.QueueName)
					} else {
						ctxInternalDelete, cancelDelete = context.WithTimeout(context.Background(), rsh.WaitDelete)

						err = sq.DeleteBlocks(ctxInternalDelete, rsh.LimitDelete)
						if err != nil {
							err = GenerateErrorForClusterUserE(rsh, 10118204, err, rsh.QueueName)
						}
					}
				}

				if err == nil {
					rsh.lastComplete = time.Now()
				} else {
					rsh.lastError = err
					rsh.Cluster.ThrowError(err)
				}

				cancelMark()
				if cancelDelete != nil {
					cancelDelete()
				}
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
		return GenerateErrorE(10118205, err, rsh.HDescription.Name)
	}

	return nil
}
func (rsh *BlockDeleteHandler) Stop(ctx context.Context) (err *mft.Error) {
	rsh.mx.Lock()
	defer rsh.mx.Unlock()
	if rsh.chStop != nil {
		rsh.chStop <- true
		rsh.chStop = nil
	}

	rsh.HDescription.Start = false
	err = rsh.Cluster.OnChange()

	if err != nil {
		return GenerateErrorE(10118206, err, rsh.HDescription.Name)
	}

	return nil
}

func (rsh *BlockDeleteHandler) LastComplete(ctx context.Context) (time.Time, *mft.Error) {
	return rsh.lastComplete, nil
}
func (rsh *BlockDeleteHandler) LastError(ctx context.Context) (err *mft.Error) {
	return rsh.lastError
}
func (rsh *BlockDeleteHandler) IsStarted(ctx context.Context) (isStarted bool, err *mft.Error) {
	return rsh.HDescription.Start, nil
}
