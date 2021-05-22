package cluster

import (
	"context"
	"time"

	"github.com/capella-pw/queue/compress"
	"github.com/capella-pw/queue/storage"
	"github.com/myfantasy/mft"
)

const SimpleClusterObjectCreateDuration = time.Second * 10
const (
	ClusterFileName = "cluster.json"
)

func SimpleClusterCreate(storageGenerator *storage.Generator,
	throwErrorFunc func(err *mft.Error) bool,
	onChangeFunc func(sc *SimpleCluster) (err *mft.Error),
	checkPermissionFunc func(user ClusterUser, objectType string, action string, objectName string) (allowed bool, err *mft.Error),
	queueGenerator *QueueGenerator, // QueueGeneratorCreate()
	externalClusterGenerator *ExternalClusterGenerator,
	handlerGenerator *HandlerGenerator,
	compressor *compress.Generator,
	encryptData EncryptData,
) *SimpleCluster {
	sc := &SimpleCluster{}

	sc.Queues = make(map[string]*QueueLoadDescription)
	sc.ExternalClusters = make(map[string]*ExternalClusterLoadDescription)
	sc.Handlers = make(map[string]*HandlerLoadDescription)
	sc.QueueGenerator = queueGenerator
	sc.IDGenerator = &mft.G{}
	sc.StorageGenerator = storageGenerator
	sc.HandlerGenerator = handlerGenerator
	sc.ExternalClusterGenerator = externalClusterGenerator
	sc.ThrowErrorFunc = throwErrorFunc
	sc.OnChangeFunc = onChangeFunc
	sc.CheckPermissionFunc = checkPermissionFunc
	sc.ObjectCreateDuration = SimpleClusterObjectCreateDuration
	sc.Compressor = compressor
	sc.EncryptData = &encryptData

	return sc
}

func OnChangeFuncGenerate(timeout time.Duration,
	storageGenerator *storage.Generator,
	mountName string, relativePath string) (f func(sc *SimpleCluster) (err *mft.Error), err *mft.Error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	storage, err := storageGenerator.Create(ctx, mountName, relativePath)

	if err != nil {
		return nil, err
	}

	return func(sc *SimpleCluster) (err *mft.Error) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		data, err := sc.GetFullStructRaw()
		if err != nil {
			return err
		}

		return storage.Save(ctx, ClusterFileName, data)
	}, nil
}

func LoadClusterData(timeout time.Duration,
	storageGenerator *storage.Generator,
	mountName string, relativePath string) (data []byte, err *mft.Error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	storage, err := storageGenerator.Create(ctx, mountName, relativePath)

	if err != nil {
		return nil, err
	}

	return storage.Get(ctx, ClusterFileName)
}
