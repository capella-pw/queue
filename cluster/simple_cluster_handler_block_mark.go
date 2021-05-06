package cluster

import (
	"context"
	"encoding/json"
	"time"

	"github.com/capella-pw/queue/queue"
	"github.com/myfantasy/mfs"
	"github.com/myfantasy/mft"
)

func BlockMarkNewGenerator(
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
		return nil, GenerateError(10118420, len(hld.QueueNames))
	}

	var rshp BlockMarkHandlerParams
	er0 := json.Unmarshal(hld.Params, &rshp)
	if er0 != nil {
		return nil, GenerateErrorE(10118421, er0)
	}

	if rshp.Interval <= 0 {
		return nil, GenerateError(10118422, rshp.Interval)
	}
	if rshp.WaitMark <= 0 {
		return nil, GenerateError(10118423, rshp.WaitMark)
	}
	if rshp.WaitUpdate <= 0 {
		return nil, GenerateError(10118424, rshp.WaitUpdate)
	}
	if rshp.LimitUpdateBlocks <= 0 {
		return nil, GenerateError(10118425, rshp.LimitUpdateBlocks)
	}

	if len(rshp.Conditions) == 0 {
		return nil, GenerateError(10118426)
	}

	for i, condition := range rshp.Conditions {
		if condition.FromTime < 0 {
			return nil, GenerateError(10118427, i, condition.Mark, condition.FromTime)
		}
		if condition.FromTime >= condition.ToTime {
			return nil, GenerateError(10118428, i, condition.Mark, condition.FromTime, condition.ToTime)
		}
	}

	return hld, nil
}

func BlockMarkLoadGenerator(
	ctx context.Context,
	cluster Cluster,
	hDescription *HandlerLoadDescription,
	idGenerator *mft.G,
) (Handler, *mft.Error) {
	if len(hDescription.QueueNames) != 1 {
		return nil, GenerateError(10118440, len(hDescription.QueueNames))
	}

	var rshp BlockMarkHandlerParams
	er0 := json.Unmarshal(hDescription.Params, &rshp)
	if er0 != nil {
		return nil, GenerateErrorE(10118441, er0)
	}

	rsh := &BlockMarkHandler{
		Cluster:           cluster,
		QueueName:         hDescription.QueueNames[0],
		Interval:          rshp.Interval,
		WaitMark:          rshp.WaitMark,
		WaitUpdate:        rshp.WaitUpdate,
		UserName:          hDescription.UserName,
		HDescription:      hDescription,
		Conditions:        rshp.Conditions,
		LimitUpdateBlocks: rshp.LimitUpdateBlocks,
	}

	return rsh, nil
}

type MarkCondition struct {
	// Mark - mark of block
	Mark string `json:"mark"`
	// FromTime - from time age like 10 min (FromTime < ToTime)
	FromTime time.Duration `json:"from_time"`
	// FromTime - from time age like 20 min (ToTime > FromTime)
	ToTime time.Duration `json:"to_time"`
}

type BlockMarkHandlerParams struct {
	// Interval - interval between call
	Interval time.Duration `json:"interval"`
	// WaitMark - wait set new mark for block timeout
	WaitMark time.Duration `json:"wait_mark"`
	// WaitUpdate - wait update mark (move block betweeen marks) timeout
	WaitUpdate time.Duration `json:"wait_update_mark"`
	// Conditions - list of condition to set mark
	Conditions []MarkCondition `json:"conditions"`
	// LimitUpdateBlocks - limit of updates block
	LimitUpdateBlocks int `json:"limit_update_block"`
}

func (hp BlockMarkHandlerParams) ToJson() json.RawMessage {
	msg, er0 := json.Marshal(hp)
	if er0 != nil {
		panic(GenerateErrorE(10118460, er0))
	}

	return msg
}

type BlockMarkHandler struct {
	Cluster           Cluster
	QueueName         string
	Interval          time.Duration
	WaitMark          time.Duration
	WaitUpdate        time.Duration
	UserName          string
	HDescription      *HandlerLoadDescription
	Conditions        []MarkCondition
	LimitUpdateBlocks int
	mx                mfs.PMutex
	chStop            chan bool
	lastComplete      time.Time
	lastError         *mft.Error
}

func (rsh *BlockMarkHandler) GetName() string {
	return rsh.UserName
}

func (rsh *BlockMarkHandler) Start(ctx context.Context) (err *mft.Error) {
	rsh.mx.Lock()
	defer rsh.mx.Unlock()
	if rsh.chStop == nil {
		chStop := make(chan bool, 1)
		rsh.chStop = chStop
		go func() {
			for {
				var cancelUpdate func()
				var ctxInternalUpdate context.Context
				ctxInternalMark, cancelMark := context.WithTimeout(context.Background(), rsh.WaitMark)
				q, exists, err := rsh.Cluster.GetQueue(rsh, rsh.QueueName)
				if err != nil {
					err = GenerateErrorForClusterUserE(rsh, 10118400, err, rsh.QueueName)
				} else if !exists {
					err = GenerateErrorForClusterUser(rsh, 10118401, rsh.QueueName)
				} else if sq, ok := q.(*queue.SimpleQueue); !ok {
					err = GenerateErrorForClusterUser(rsh, 10118402, rsh.QueueName)
				} else {
					err = sq.SetMarks(ctxInternalMark,
						func(ctx context.Context,
							i int, len int,
							q *queue.SimpleQueue, block *queue.SimpleQueueBlock,
						) (needSetMark bool, nextMark string, err *mft.Error) {
							for _, condition := range rsh.Conditions {
								if block.Dt.Before(time.Now().Add(-condition.FromTime)) &&
									block.Dt.After(time.Now().Add(-condition.ToTime)) {
									return true, condition.Mark, nil
								}
							}

							return false, "", nil
						},
					)
					if err != nil {
						err = GenerateErrorForClusterUserE(rsh, 10118403, err, rsh.QueueName)
					} else {
						ctxInternalUpdate, cancelUpdate = context.WithTimeout(context.Background(), rsh.WaitUpdate)

						err = sq.UpdateMarks(ctxInternalUpdate, rsh.LimitUpdateBlocks)
						if err != nil {
							err = GenerateErrorForClusterUserE(rsh, 10118404, err, rsh.QueueName)
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
				if cancelUpdate != nil {
					cancelUpdate()
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
		return GenerateErrorE(10118405, err, rsh.HDescription.Name)
	}

	return nil
}
func (rsh *BlockMarkHandler) Stop(ctx context.Context) (err *mft.Error) {
	rsh.mx.Lock()
	defer rsh.mx.Unlock()
	if rsh.chStop != nil {
		rsh.chStop <- true
		rsh.chStop = nil
	}

	rsh.HDescription.Start = false
	err = rsh.Cluster.OnChange()

	if err != nil {
		return GenerateErrorE(10118406, err, rsh.HDescription.Name)
	}

	return nil
}

func (rsh *BlockMarkHandler) LastComplete(ctx context.Context) (time.Time, *mft.Error) {
	return rsh.lastComplete, nil
}
func (rsh *BlockMarkHandler) LastError(ctx context.Context) (err *mft.Error) {
	return rsh.lastError
}
