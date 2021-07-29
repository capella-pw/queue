package cluster

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/capella-pw/queue/cn"
	"github.com/capella-pw/queue/compress"
	"github.com/myfantasy/mfs"
	"github.com/myfantasy/mft"
)

var ClusterNameSplitter = "/"

var DefaultEcNewGenerator map[string]EcNewGenerator
var DefaultEcLoadGenerator map[string]EcLoadGenerator

type EcNewGenerator func(
	ctx context.Context,
	compressor *compress.Generator,
	ecDescription ExternalClusterDescription,
	idGenerator *mft.G,
	encryptData *EncryptData,
) (*ExternalClusterLoadDescription, *mft.Error)

type EcLoadGenerator func(
	ctx context.Context,
	compressor *compress.Generator,
	ecDescription *ExternalClusterLoadDescription,
	idGenerator *mft.G,
	encryptData *EncryptData,
) (Cluster, *mft.Error)

type ExternalClusterGenerator struct {
	mx mfs.PMutex

	ecNewGenerator map[string]EcNewGenerator

	ecLoadGenerator map[string]EcLoadGenerator
}

// ExternalClusterLoadDescription description of external cluster for load
type ExternalClusterLoadDescription struct {
	Name    string          `json:"name"`
	Type    string          `json:"type"`
	Params  json.RawMessage `json:"params"`
	Cluster Cluster         `json:"-"`
}

func (qld *ExternalClusterLoadDescription) ExternalClusterDescription() ExternalClusterDescription {
	qd := ExternalClusterDescription{
		Name:   qld.Name,
		Type:   qld.Type,
		Params: qld.Params,
	}

	return qd
}

func (ecg *ExternalClusterGenerator) AddGenerator(
	name string,
	ecNewGenerator EcNewGenerator,
	ecLoadGenerator EcLoadGenerator,
) {
	ecg.mx.Lock()
	defer ecg.mx.Unlock()
	ecg.ecNewGenerator[name] = ecNewGenerator
	ecg.ecLoadGenerator[name] = ecLoadGenerator
}

func (ecg *ExternalClusterGenerator) GetGenerator(
	name string) (
	ecNewGenerator EcNewGenerator,
	ecLoadGenerator EcLoadGenerator,
	ok bool,
) {
	ecg.mx.Lock()
	defer ecg.mx.Unlock()
	ng, ok := ecg.ecNewGenerator[name]

	if !ok {
		return nil, nil, false
	}

	return ng, ecg.ecLoadGenerator[name], true
}

func (sc *SimpleCluster) AddExternalCluster(ctx context.Context, user cn.CapUser,
	clusterParams ExternalClusterDescription) (err *mft.Error) {
	allowed, err := sc.CheckPermission(ctx, user, cn.ClusterSelfObjectType, cn.AddExternalClusterAction, "")
	if err != nil {
		return err
	}
	if !allowed {
		return GenerateErrorForClusterUser(user, 10112000)
	}

	sc.mx.Lock()
	_, ok := sc.ExternalClusters[clusterParams.Name]
	sc.mx.Unlock()
	if ok {
		return GenerateErrorForClusterUser(user, 10112002, clusterParams.Name)
	}

	ctx, cancel := context.WithTimeout(context.Background(), sc.ObjectCreateDuration)
	defer cancel()

	cr, ld, ok := sc.ExternalClusterGenerator.GetGenerator(clusterParams.Type)
	if !ok {
		return GenerateError(10112001, clusterParams.Type)
	}

	ecld, err := cr(ctx, sc.Compressor, clusterParams, sc.IDGenerator, sc.EncryptData)
	if err != nil {
		return err
	}

	sc.mx.Lock()
	sc.ExternalClusters[ecld.Name] = ecld
	sc.mx.Unlock()

	ec, err := ld(ctx, sc.Compressor, ecld, sc.IDGenerator, sc.EncryptData)
	if err != nil {
		return err
	}

	ecld.Cluster = ec

	err = sc.OnChange()
	if err != nil {
		return err
	}

	return nil
}
func (sc *SimpleCluster) DropExternalCluster(ctx context.Context, user cn.CapUser,
	name string) (err *mft.Error) {

	allowed, err := sc.CheckPermission(ctx, user, cn.ClusterSelfObjectType, cn.DropExternalClusterAction, "")
	if err != nil {
		return err
	}
	if !allowed {
		return GenerateErrorForClusterUser(user, 10113000)
	}

	sc.mx.Lock()
	_, ok := sc.ExternalClusters[name]
	sc.mx.Unlock()
	if !ok {
		return GenerateError(10113001, name)
	}

	sc.mx.Lock()
	delete(sc.ExternalClusters, name)
	sc.mx.Unlock()

	err = sc.OnChange()
	if err != nil {
		return err
	}

	return nil
}
func (sc *SimpleCluster) GetExternalClusterDescription(ctx context.Context, user cn.CapUser,
	name string) (clusterParams ExternalClusterDescription, err *mft.Error) {

	allowed, err := sc.CheckPermission(ctx, user, cn.ClusterSelfObjectType, cn.GetExternalClusterDescrAction, "")
	if err != nil {
		return clusterParams, err
	}
	if !allowed {
		return clusterParams, GenerateErrorForClusterUser(user, 10114000)
	}

	sc.mx.RLock()
	ecld, ok := sc.ExternalClusters[name]
	sc.mx.RUnlock()

	if !ok {
		return clusterParams, GenerateError(10114001, name)
	}

	return ecld.ExternalClusterDescription(), nil
}
func (sc *SimpleCluster) GetExternalClustersList(ctx context.Context, user cn.CapUser) (names []string, err *mft.Error) {

	allowed, err := sc.CheckPermission(ctx, user, cn.ClusterSelfObjectType, cn.GetExternalClusterDescrAction, "")
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, GenerateErrorForClusterUser(user, 10115000)
	}

	sc.mx.RLock()
	names = make([]string, 0, len(sc.ExternalClusters))
	for name := range sc.ExternalClusters {
		names = append(names, name)
	}
	sc.mx.RUnlock()

	return names, nil
}

func (sc *SimpleCluster) GetExternalCluster(ctx context.Context, user cn.CapUser,
	name string) (cluster Cluster, exists bool, err *mft.Error) {

	if idxSp := strings.Index(name, ClusterNameSplitter); idxSp >= 0 {
		subName := name[0:idxSp]
		nextName := name[idxSp+1:]

		subCluster, subExists, err := sc.GetExternalCluster(ctx, user, subName)
		if err != nil {
			return nil, false, GenerateErrorForClusterUserE(user, 10116001, err, subName, nextName)
		}
		if !subExists {
			return nil, false, GenerateErrorForClusterUser(user, 10116002, subName, nextName)
		}

		cluster, exists, err = subCluster.GetExternalCluster(ctx, user, nextName)

		if err != nil {
			err = GenerateErrorForClusterUserE(user, 10116003, err, subName, nextName)
		}

		return cluster, exists, err
	}

	allowed, err := sc.CheckPermission(ctx, user, cn.ClusterSelfObjectType, cn.GetExternalClusterAction, "")
	if err != nil {
		return nil, false, err
	}
	if !allowed {
		return nil, false, GenerateErrorForClusterUser(user, 10116000)
	}

	sc.mx.RLock()
	ecld, ok := sc.ExternalClusters[name]
	sc.mx.RUnlock()

	if !ok {
		return nil, false, nil
	}

	return ecld.Cluster, true, nil
}

func ExternalClusterGeneratorCreate() *ExternalClusterGenerator {
	res := &ExternalClusterGenerator{
		ecNewGenerator:  make(map[string]EcNewGenerator),
		ecLoadGenerator: make(map[string]EcLoadGenerator),
	}

	for k, v := range DefaultEcNewGenerator {
		res.AddGenerator(k, v, DefaultEcLoadGenerator[k])
	}

	return res
}

func init() {
	DefaultEcNewGenerator = make(map[string]EcNewGenerator)
	DefaultEcLoadGenerator = make(map[string]EcLoadGenerator)
}
