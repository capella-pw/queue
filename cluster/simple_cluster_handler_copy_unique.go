package cluster

import (
	"context"
	"encoding/json"
	"time"

	"github.com/capella-pw/queue/queue"
	"github.com/myfantasy/mfs"
	"github.com/myfantasy/mft"
	"github.com/myfantasy/segment"
)

func CopyUniqueNewGenerator(
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

	if len(hld.QueueNames) != 2 {
		return nil, GenerateError(10117900, len(hld.QueueNames))
	}

	var rshp CopyUniqueHandlerParams
	er0 := json.Unmarshal(hld.Params, &rshp)
	if er0 != nil {
		return nil, GenerateErrorE(10117901, er0)
	}

	if rshp.Interval <= 0 {
		return nil, GenerateError(10117902, rshp.Interval)
	}
	if rshp.Wait <= 0 {
		return nil, GenerateError(10117903, rshp.Wait)
	}

	return hld, nil
}

func CopyUniqueLoadGenerator(
	ctx context.Context,
	cluster Cluster,
	hDescription *HandlerLoadDescription,
	idGenerator *mft.G,
) (Handler, *mft.Error) {
	if len(hDescription.QueueNames) != 2 {
		return nil, GenerateError(10118100, len(hDescription.QueueNames))
	}

	var rshp CopyUniqueHandlerParams
	er0 := json.Unmarshal(hDescription.Params, &rshp)
	if er0 != nil {
		return nil, GenerateErrorE(10118101, er0)
	}

	rsh := &CopyUniqueHandler{
		Cluster:      cluster,
		SrcQueueName: hDescription.QueueNames[0],
		DstQueueName: hDescription.QueueNames[1],
		Interval:     rshp.Interval,
		Wait:         rshp.Wait,
		UserName:     hDescription.UserName,
		HDescription: hDescription,

		SaveModeSrc:    rshp.SaveModeSrc,
		SaveModeDst:    rshp.SaveModeDst,
		SubscriberName: rshp.SubscriberName,
		CntLimit:       rshp.CntLimit,
		DoSaveDst:      rshp.DoSaveDst,
		Segments:       rshp.Segments,
	}

	return rsh, nil
}

type CopyUniqueHandlerParams struct {
	// Interval - interval between call
	Interval time.Duration `json:"interval"`
	// Wait - wait save timeout
	Wait time.Duration `json:"wait"`

	SaveModeSrc    int               `json:"src_save_mode"`
	SaveModeDst    int               `json:"dst_save_mode"`
	SubscriberName string            `json:"subscribe_name"`
	CntLimit       int               `json:"cnt_limit"`
	DoSaveDst      bool              `json:"do_save_dst"`
	Segments       *segment.Segments `json:"segments"`
}

func (hp CopyUniqueHandlerParams) ToJson() json.RawMessage {
	msg, er0 := json.Marshal(hp)
	if er0 != nil {
		panic(GenerateErrorE(10117700, er0))
	}

	return msg
}

type CopyUniqueHandler struct {
	Cluster        Cluster
	SrcQueueName   string
	DstQueueName   string
	Interval       time.Duration
	ActiveInterval time.Duration
	Wait           time.Duration
	UserName       string
	HDescription   *HandlerLoadDescription

	SaveModeSrc    int
	SaveModeDst    int
	SubscriberName string
	CntLimit       int
	DoSaveDst      bool
	Segments       *segment.Segments

	mx           mfs.PMutex
	chStop       chan bool
	lastComplete time.Time
	lastError    *mft.Error
}

func (rsh *CopyUniqueHandler) GetName() string {
	return rsh.UserName
}

func (rsh *CopyUniqueHandler) Start(ctx context.Context) (err *mft.Error) {
	rsh.mx.Lock()
	defer rsh.mx.Unlock()
	if rsh.chStop == nil {
		chStop := make(chan bool, 1)

		srcQueue, exists, err := rsh.Cluster.GetQueue(ctx, rsh, rsh.SrcQueueName)
		if err != nil {
			err = GenerateErrorForClusterUserE(rsh, 10118000, err, rsh.SrcQueueName)
			return err
		}
		if !exists {
			err = GenerateErrorForClusterUser(rsh, 10118001, rsh.SrcQueueName)
			return err
		}

		dstQueue, exists, err := rsh.Cluster.GetQueue(ctx, rsh, rsh.DstQueueName)
		if err != nil {
			err = GenerateErrorForClusterUserE(rsh, 10118002, err, rsh.DstQueueName)
			return err
		}
		if !exists {
			err = GenerateErrorForClusterUser(rsh, 10118003, rsh.DstQueueName)
			return err
		}

		rsh.chStop = chStop
		copy := queue.SubscribeCopyUnique(
			srcQueue,
			dstQueue,
			rsh.SaveModeSrc,
			rsh.SaveModeDst,
			rsh.SubscriberName,
			rsh.CntLimit,
			rsh.DoSaveDst,
			rsh.Segments)
		go func() {
			for {
				ctxInternal, cancel := context.WithTimeout(context.Background(), rsh.Wait)

				isEmpty, err := copy(ctxInternal)

				if err == nil {
					rsh.lastComplete = time.Now()
				} else {
					err = GenerateErrorForClusterUserE(rsh, 10118005, err,
						rsh.SrcQueueName,
						rsh.DstQueueName,
						rsh.HDescription.Name)

					rsh.lastError = err
					rsh.Cluster.ThrowError(err)
				}

				cancel()
				if isEmpty {
					time.Sleep(rsh.Interval)
				} else {
					time.Sleep(rsh.ActiveInterval)
				}
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
		return GenerateErrorE(10118006, err, rsh.HDescription.Name)
	}

	return nil
}
func (rsh *CopyUniqueHandler) Stop(ctx context.Context) (err *mft.Error) {
	rsh.mx.Lock()
	defer rsh.mx.Unlock()
	if rsh.chStop != nil {
		rsh.chStop <- true
		rsh.chStop = nil
	}

	rsh.HDescription.Start = false
	err = rsh.Cluster.OnChange()

	if err != nil {
		return GenerateErrorE(10118006, err, rsh.HDescription.Name)
	}

	return nil
}

func (rsh *CopyUniqueHandler) LastComplete(ctx context.Context) (time.Time, *mft.Error) {
	return rsh.lastComplete, nil
}
func (rsh *CopyUniqueHandler) LastError(ctx context.Context) (err *mft.Error) {
	return rsh.lastError
}
func (rsh *CopyUniqueHandler) IsStarted(ctx context.Context) (isStarted bool, err *mft.Error) {
	return rsh.HDescription.Start, nil
}
