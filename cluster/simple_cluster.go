package cluster

import (
	"context"
	"encoding/json"
	"time"

	"github.com/capella-pw/queue/compress"
	"github.com/capella-pw/queue/storage"
	"github.com/myfantasy/mfs"
	"github.com/myfantasy/mft"
)

// SimpleCluster - simple cluster
type SimpleCluster struct {
	mx mfs.PMutex

	Name string `json:"name"`

	ObjectCreateDuration time.Duration `json:"object_create_duration"`

	RvGeneratorPart int64  `json:"rv_generator_part"`
	IDGenerator     *mft.G `json:"-"`

	Queues           map[string]*QueueLoadDescription           `json:"queues"`
	ExternalClusters map[string]*ExternalClusterLoadDescription `json:"ext_clusters"`
	Handlers         map[string]*HandlerLoadDescription         `json:"handlers"`

	InternalValues map[string]string `json:"internal_values"`

	QueueGenerator           *QueueGenerator           `json:"-"`
	StorageGenerator         *storage.Generator        `json:"-"`
	ExternalClusterGenerator *ExternalClusterGenerator `json:"-"`
	HandlerGenerator         *HandlerGenerator         `json:"-"`
	Compressor               *compress.Generator       `json:"-"`

	EncryptData *EncryptData `json:"-"`

	// case nil then ignore
	CheckPermissionFunc func(user ClusterUser, objectType string, action string, objectName string) (allowed bool, err *mft.Error) `json:"-"`
	// case nil then ignore
	ThrowErrorFunc func(err *mft.Error) bool `json:"-"`

	// OnChange event func (send self)
	OnChangeFunc func(sc *SimpleCluster) (err *mft.Error) `json:"-"`
}

func (sc *SimpleCluster) OnChange() (err *mft.Error) {
	if sc == nil {
		return nil
	}

	if sc.OnChangeFunc == nil {
		return nil
	}

	err = sc.OnChangeFunc(sc)

	if err != nil {
		return GenerateErrorE(10101300, err)
	}

	return nil
}

func (sc *SimpleCluster) CheckPermission(user ClusterUser, objectType string, action string, objectName string) (allowed bool, err *mft.Error) {
	if sc == nil {
		return false, nil
	}

	if sc.CheckPermissionFunc == nil {
		return true, nil
	}

	return sc.CheckPermissionFunc(user, objectType, action, objectName)
}

func (sc *SimpleCluster) ThrowError(err *mft.Error) bool {
	if sc == nil {
		return false
	}
	if sc.ThrowErrorFunc == nil {
		return false
	}
	return sc.ThrowErrorFunc(err)
}

// GetName gets cluster name
func (sc *SimpleCluster) GetName(user ClusterUser) (name string, err *mft.Error) {
	allowed, err := sc.CheckPermission(user, ClusterSelfObjectType, GetNameAction, "")
	if err != nil {
		return name, err
	}
	if !allowed {
		return name, GenerateErrorForClusterUser(user, 10100000)
	}

	return sc.Name, nil
}

// SetName sets cluster name
func (sc *SimpleCluster) SetName(user ClusterUser, name string) (err *mft.Error) {
	allowed, err := sc.CheckPermission(user, ClusterSelfObjectType, SetNameAction, "")
	if err != nil {
		return err
	}
	if !allowed {
		return GenerateErrorForClusterUser(user, 10101000)
	}
	sc.mx.Lock()
	sc.Name = name
	sc.mx.Unlock()

	err = sc.OnChange()

	return err
}

func (sc *SimpleCluster) Ping(user ClusterUser) (err *mft.Error) {
	allowed, err := sc.CheckPermission(user, ClusterSelfObjectType, PingAction, "")
	if err != nil {
		return err
	}
	if !allowed {
		return GenerateErrorForClusterUser(user, 10101010)
	}

	return nil
}
func (sc *SimpleCluster) GetNextId(user ClusterUser) (id int64, err *mft.Error) {
	allowed, err := sc.CheckPermission(user, ClusterSelfObjectType, GetNextIdAction, "")
	if err != nil {
		return id, err
	}
	if !allowed {
		return id, GenerateErrorForClusterUser(user, 10101020)
	}

	return sc.IDGenerator.RvGetPart(), nil
}
func (sc *SimpleCluster) GetNextIds(user ClusterUser, cnt int) (ids []int64, err *mft.Error) {
	allowed, err := sc.CheckPermission(user, ClusterSelfObjectType, GetNextIdAction, "")
	if err != nil {
		return ids, err
	}
	if !allowed {
		return ids, GenerateErrorForClusterUser(user, 10101030)
	}

	for i := 0; i < cnt; i++ {
		ids = append(ids, sc.IDGenerator.RvGetPart())
	}
	return ids, nil
}

func (sc *SimpleCluster) GetFullStruct(user ClusterUser) (data json.RawMessage, err *mft.Error) {
	allowed, err := sc.CheckPermission(user, ClusterSelfObjectType, GetNameAction, "")
	if err != nil {
		return data, err
	}
	if !allowed {
		return data, GenerateErrorForClusterUser(user, 10102000)
	}

	return sc.GetFullStructRaw()
}
func (sc *SimpleCluster) GetFullStructRaw() (data json.RawMessage, err *mft.Error) {
	sc.mx.Lock()
	defer sc.mx.Unlock()

	data, erm := json.MarshalIndent(sc, "", "\t")
	if erm != nil {
		return data, GenerateErrorE(10102001, erm)
	}

	return data, nil
}

func (sc *SimpleCluster) SetValueInternal(name string, value string) (err *mft.Error) {
	sc.mx.Lock()
	sc.InternalValues[name] = value
	sc.mx.Unlock()

	err = sc.OnChange()

	return GenerateErrorE(10101100, err)
}
func (sc *SimpleCluster) GetValueInternal(name string) (value string, ok bool) {
	sc.mx.Lock()
	value, ok = sc.InternalValues[name]
	sc.mx.Unlock()

	return value, ok
}

func (sc *SimpleCluster) LoadFullStruct(user ClusterUser, data json.RawMessage) (err *mft.Error) {
	allowed, err := sc.CheckPermission(user, ClusterSelfObjectType, GetNameAction, "")
	if err != nil {
		return err
	}
	if !allowed {
		return GenerateErrorForClusterUser(user, 10103000)
	}

	return sc.LoadFullStructRaw(data)
}
func (sc *SimpleCluster) LoadFullStructRaw(data json.RawMessage) (err *mft.Error) {
	sc.mx.Lock()

	eru := json.Unmarshal(data, sc)

	if eru != nil {
		sc.mx.Unlock()
		return GenerateErrorE(10103001, eru)
	}

	sc.IDGenerator = &mft.G{AddValue: sc.RvGeneratorPart}
	sc.mx.Unlock()

	ctx := context.Background()
	// Load and run queue
	for name, load := range sc.Queues {
		_, loadGen, ok := sc.QueueGenerator.GetGenerator(load.Type)
		if !ok {
			return GenerateError(10103002, name, load.Type)
		}
		queue, err := loadGen(ctx, sc.StorageGenerator, load, sc.IDGenerator)
		if err != nil {
			return GenerateErrorE(10103003, err, name, load.Type)
		}

		load.Queue = queue
	}

	// Load and run external cluster
	for name, load := range sc.ExternalClusters {
		_, loadGen, ok := sc.ExternalClusterGenerator.GetGenerator(load.Type)
		if !ok {
			return GenerateError(10103004, name, load.Type)
		}
		cluster, err := loadGen(ctx, sc.Compressor, load, sc.IDGenerator, sc.EncryptData)
		if err != nil {
			return GenerateErrorE(10103005, err, name, load.Type)
		}

		load.Cluster = cluster
	}

	// Load and run handler
	for name, load := range sc.Handlers {
		_, loadGen, ok := sc.HandlerGenerator.GetGenerator(load.Type)
		if !ok {
			return GenerateError(10103006, name, load.Type)
		}
		handler, err := loadGen(ctx, sc, load, sc.IDGenerator)
		if err != nil {
			return GenerateErrorE(10103007, err, name, load.Type)
		}

		load.Handler = handler

		if load.Start {
			err = handler.Start(ctx)
			if err != nil {
				return GenerateErrorE(10103008, err, name, load.Type)
			}
		}
	}

	return nil
}
