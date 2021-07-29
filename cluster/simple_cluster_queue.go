package cluster

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/capella-pw/queue/cn"
	"github.com/capella-pw/queue/queue"
	"github.com/capella-pw/queue/storage"
	"github.com/myfantasy/mfs"
	"github.com/myfantasy/mft"
	"github.com/myfantasy/segment"
)

// SimpleQueueType - simple queue type
var SimpleQueueType = "simple_queue"

// QueueGenerator - queue generator
type QueueGenerator struct {
	mx mfs.PMutex

	qNewGenerator map[string]func(
		ctx context.Context,
		storageGenerator *storage.Generator,
		queueDescription QueueDescription,
		idGenerator *mft.G,
	) (*QueueLoadDescription, *mft.Error)

	qLoadGenerator map[string]func(
		ctx context.Context,
		storageGenerator *storage.Generator,
		queueDescription *QueueLoadDescription,
		idGenerator *mft.G,
	) (queue.Queue, *mft.Error)
}

// QueueLoadDescription description of queue for load
type QueueLoadDescription struct {
	Name         string          `json:"name"`
	Type         string          `json:"type"`
	CreateOnLoad bool            `json:"create_on_load"`
	RelativePath string          `json:"relative_path"`
	Params       json.RawMessage `json:"params"`
	Queue        queue.Queue     `json:"-"`
}

func (qld *QueueLoadDescription) QueueDescription() QueueDescription {
	qd := QueueDescription{
		Name:         qld.Name,
		Type:         qld.Type,
		CreateOnLoad: qld.CreateOnLoad,
		Params:       qld.Params,
	}

	return qd
}

func (qg *QueueGenerator) AddGenerator(
	name string,
	qNewGenerator func(ctx context.Context, storageGenerator *storage.Generator,
		queueDescription QueueDescription, idGenerator *mft.G) (*QueueLoadDescription, *mft.Error),
	qLoadGenerator func(ctx context.Context, storageGenerator *storage.Generator,
		queueDescription *QueueLoadDescription, idGenerator *mft.G) (queue.Queue, *mft.Error),
) {
	qg.mx.Lock()
	defer qg.mx.Unlock()
	qg.qNewGenerator[name] = qNewGenerator
	qg.qLoadGenerator[name] = qLoadGenerator
}

func (qg *QueueGenerator) GetGenerator(
	name string) (
	qNewGenerator func(ctx context.Context, storageGenerator *storage.Generator,
		queueDescription QueueDescription, idGenerator *mft.G) (*QueueLoadDescription, *mft.Error),
	qLoadGenerator func(ctx context.Context, storageGenerator *storage.Generator,
		queueDescription *QueueLoadDescription, idGenerator *mft.G) (queue.Queue, *mft.Error),
	ok bool,
) {
	qg.mx.Lock()
	defer qg.mx.Unlock()
	ng, ok := qg.qNewGenerator[name]

	if !ok {
		return nil, nil, false
	}

	return ng, qg.qLoadGenerator[name], true
}

// AddQueue add queue to cluster
func (sc *SimpleCluster) AddQueue(ctx context.Context, user cn.CapUser, queueDescription QueueDescription) (err *mft.Error) {
	allowed, err := sc.CheckPermission(ctx, user, cn.ClusterSelfObjectType, cn.AddQueueAction, "")
	if err != nil {
		return err
	}
	if !allowed {
		return GenerateErrorForClusterUser(user, 10104000)
	}

	sc.mx.Lock()
	_, ok := sc.Queues[queueDescription.Name]
	sc.mx.Unlock()
	if ok {
		return GenerateErrorForClusterUser(user, 10104002, queueDescription.Name)
	}

	ctx, cancel := context.WithTimeout(context.Background(), sc.ObjectCreateDuration)
	defer cancel()

	cr, ld, ok := sc.QueueGenerator.GetGenerator(queueDescription.Type)
	if !ok {
		return GenerateError(10104001, queueDescription.Type)
	}

	qld, err := cr(ctx, sc.StorageGenerator, queueDescription, sc.IDGenerator)
	if err != nil {
		return err
	}

	q, err := ld(ctx, sc.StorageGenerator, qld, sc.IDGenerator)
	if err != nil {
		return err
	}

	qld.Queue = q

	sc.mx.Lock()
	sc.Queues[qld.Name] = qld
	sc.mx.Unlock()

	err = sc.OnChange()
	if err != nil {
		return err
	}

	return nil
}

func (sc *SimpleCluster) DropQueue(ctx context.Context, user cn.CapUser, name string) (err *mft.Error) {
	allowed, err := sc.CheckPermission(ctx, user, cn.ClusterSelfObjectType, cn.DropQueueAction, "")
	if err != nil {
		return err
	}
	if !allowed {
		return GenerateErrorForClusterUser(user, 10108000)
	}

	sc.mx.Lock()
	_, ok := sc.Queues[name]
	sc.mx.Unlock()
	if !ok {
		return GenerateError(10108001, name)
	}

	sc.mx.Lock()
	delete(sc.Queues, name)
	sc.mx.Unlock()

	err = sc.OnChange()
	if err != nil {
		return err
	}

	return nil
}
func (sc *SimpleCluster) GetQueueDescription(ctx context.Context, user cn.CapUser, name string) (queueDescription QueueDescription, err *mft.Error) {
	allowed, err := sc.CheckPermission(ctx, user, cn.ClusterSelfObjectType, cn.GetQueueDescrAction, "")
	if err != nil {
		return queueDescription, err
	}
	if !allowed {
		return queueDescription, GenerateErrorForClusterUser(user, 10109000)
	}

	sc.mx.RLock()
	qld, ok := sc.Queues[name]
	sc.mx.RUnlock()

	if !ok {
		return queueDescription, GenerateError(10109001, name)
	}

	return qld.QueueDescription(), nil
}
func (sc *SimpleCluster) GetQueuesList(ctx context.Context, user cn.CapUser) (names []string, err *mft.Error) {
	allowed, err := sc.CheckPermission(ctx, user, cn.ClusterSelfObjectType, cn.GetQueueDescrAction, "")
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, GenerateErrorForClusterUser(user, 10110000)
	}

	sc.mx.RLock()
	names = make([]string, 0, len(sc.Queues))
	for name := range sc.Queues {
		names = append(names, name)
	}
	sc.mx.RUnlock()

	return names, nil
}

func (sc *SimpleCluster) GetQueue(ctx context.Context, user cn.CapUser, name string) (queue queue.Queue, exists bool, err *mft.Error) {
	allowed, err := sc.CheckPermission(ctx, user, cn.ClusterSelfObjectType, cn.GetQueueAction, name)
	if idxSp := strings.LastIndex(name, ClusterNameSplitter); idxSp >= 0 {
		subName := name[0:idxSp]
		nextName := name[idxSp+1:]

		subCluster, subExists, err := sc.GetExternalCluster(ctx, user, subName)
		if err != nil {
			return nil, false, GenerateErrorForClusterUserE(user, 10111001, err, subName, nextName)
		}
		if !subExists {
			return nil, false, GenerateErrorForClusterUser(user, 10111002, subName, nextName)
		}

		queue, exists, err = subCluster.GetQueue(ctx, user, nextName)

		if err != nil {
			err = GenerateErrorForClusterUserE(user, 10111003, err, subName, nextName)
		}

		return queue, exists, err
	}

	if err != nil {
		return nil, false, err
	}
	if !allowed {
		return nil, false, GenerateErrorForClusterUser(user, 10111000)
	}

	sc.mx.RLock()
	qld, ok := sc.Queues[name]
	sc.mx.RUnlock()

	if !ok {
		return nil, false, nil
	}

	return qld.Queue, true, nil
}

func QueueGeneratorCreate() *QueueGenerator {
	res := &QueueGenerator{
		qNewGenerator: make(map[string]func(ctx context.Context, storageGenerator *storage.Generator,
			queueDescription QueueDescription, idGenerator *mft.G) (*QueueLoadDescription, *mft.Error)),
		qLoadGenerator: make(map[string]func(ctx context.Context, storageGenerator *storage.Generator,
			queueDescription *QueueLoadDescription, idGenerator *mft.G) (queue.Queue, *mft.Error)),
	}

	res.AddGenerator(SimpleQueueType, SimppleQueueNewGenerator, SimppleQueueLoadGenerator)

	return res
}

type SimpleQueueParams struct {
	CntLimit                        int               `json:"cnt_limit"`
	TimeLimit                       time.Duration     `json:"time_limit"`
	LenLimit                        int               `json:"len_limit"`
	MetaStorageMountName            string            `json:"meta_mount_name"`
	SubscriberStorageMountName      string            `json:"subscriber_mount_name"`
	MarkerBlockDataStorageMountName map[string]string `json:"marker_block_mount_name"`
	Segments                        *segment.Segments `json:"segments"`
	DefaultSaveMode                 int               `json:"default_save_mod"`
	UseDefaultSaveModeForce         bool              `json:"use_default_save_mod_force"`
}

func (sqp SimpleQueueParams) ToJson() json.RawMessage {
	msg, er0 := json.MarshalIndent(sqp, "", "  ")
	if er0 != nil {
		panic(GenerateErrorE(10105100, er0))
	}

	return msg
}

func SimppleQueueNewGenerator(ctx context.Context, storageGenerator *storage.Generator,
	queueDescription QueueDescription, idGenerator *mft.G) (qd *QueueLoadDescription, err *mft.Error) {

	if queueDescription.Name == "" {
		return nil, GenerateError(10105000)
	}

	var sqp SimpleQueueParams

	er0 := json.Unmarshal(queueDescription.Params, &sqp)
	if er0 != nil {
		return nil, GenerateErrorE(10105001, er0)
	}

	qd = &QueueLoadDescription{
		Name:         queueDescription.Name,
		Type:         queueDescription.Type,
		CreateOnLoad: queueDescription.CreateOnLoad,
		RelativePath: queueDescription.Name + "_" + strconv.Itoa(int(idGenerator.RvGetPart())) + "/",
	}

	metaStorage, err := storageGenerator.Create(ctx, sqp.MetaStorageMountName, qd.RelativePath)
	if err != nil {
		return nil, GenerateErrorE(10105002, err, qd.Name)
	}

	var subscriberStorage storage.Storage

	if sqp.SubscriberStorageMountName != "" {
		subscriberStorage, err = storageGenerator.Create(ctx, sqp.SubscriberStorageMountName, qd.RelativePath)
		if err != nil {
			return nil, GenerateErrorE(10105003, err, qd.Name)
		}
	}

	mbs := make(map[string]storage.Storage)

	if len(sqp.MarkerBlockDataStorageMountName) > 0 {
		for name, mountName := range sqp.MarkerBlockDataStorageMountName {
			blockStorage, err := storageGenerator.Create(ctx, mountName, qd.RelativePath)
			if err != nil {
				return nil, GenerateErrorE(10105004, err, qd.Name, name)
			}
			mbs[name] = blockStorage
		}
	}

	sq := queue.CreateSimpleQueue(sqp.CntLimit, sqp.TimeLimit,
		sqp.LenLimit, metaStorage, subscriberStorage, mbs, idGenerator)
	sq.Segments = sqp.Segments
	sq.DefaultSaveMode = sqp.DefaultSaveMode
	sq.UseDefaultSaveModeForce = sqp.UseDefaultSaveModeForce

	err = sq.SaveAll(ctx)
	if err != nil {
		return nil, GenerateErrorE(10105005, err, qd.Name)
	}

	qd.Params = queueDescription.Params
	qd.Queue = sq

	return qd, nil
}
func SimppleQueueLoadGenerator(ctx context.Context, storageGenerator *storage.Generator,
	queueDescription *QueueLoadDescription, idGenerator *mft.G) (queue.Queue, *mft.Error) {

	if queueDescription.CreateOnLoad && queueDescription.Queue != nil {
		return queueDescription.Queue, nil
	}

	if queueDescription.CreateOnLoad {
		queueDescriptionCr := QueueDescription{
			Name:         queueDescription.Name,
			Type:         queueDescription.Type,
			CreateOnLoad: queueDescription.CreateOnLoad,
			Params:       queueDescription.Params,
		}

		qd, err := SimppleQueueNewGenerator(ctx,
			storageGenerator,
			queueDescriptionCr,
			idGenerator)
		if err != nil {
			return nil, err
		}

		if qd.Queue != nil {
			return qd.Queue, nil
		}
		return nil, GenerateError(10106000, qd.Name)
	}

	var sqp SimpleQueueParams

	er0 := json.Unmarshal(queueDescription.Params, &sqp)
	if er0 != nil {
		return nil, GenerateErrorE(10106001, er0)
	}

	metaStorage, err := storageGenerator.Create(ctx, sqp.MetaStorageMountName, queueDescription.RelativePath)
	if err != nil {
		return nil, GenerateErrorE(10106002, err, queueDescription.Name)
	}

	var subscriberStorage storage.Storage

	if sqp.SubscriberStorageMountName != "" {
		subscriberStorage, err = storageGenerator.Create(ctx, sqp.SubscriberStorageMountName, queueDescription.RelativePath)
		if err != nil {
			return nil, GenerateErrorE(10106003, err, queueDescription.Name)
		}
	}

	mbs := make(map[string]storage.Storage)

	if len(sqp.MarkerBlockDataStorageMountName) > 0 {
		for name, mountName := range sqp.MarkerBlockDataStorageMountName {
			blockStorage, err := storageGenerator.Create(ctx, mountName, queueDescription.RelativePath)
			if err != nil {
				return nil, GenerateErrorE(10106004, err, queueDescription.Name, name)
			}
			mbs[name] = blockStorage
		}
	}

	sq, err := queue.LoadSimpleQueue(ctx,
		metaStorage, subscriberStorage, mbs, idGenerator)

	if err != nil {
		return nil, GenerateErrorE(10106005, err, queueDescription.Name)
	}

	return sq, nil
}
