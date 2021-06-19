package cluster

import (
	"context"
	"encoding/json"

	"github.com/myfantasy/mfs"
	"github.com/myfantasy/mft"
)

const (
	RegularlySaveHandlerType = "regularly_save"
	CopyUniqueHandlerType    = "copy_unique"
	BlockDeleteHandlerType   = "block_delete"
	BlockUnloadHandlerType   = "block_unload"
	BlockMarkHandlerType     = "block_mark"
)

type HNewGenerator func(
	ctx context.Context,
	cluster Cluster,
	hDescription HandlerDescription,
	idGenerator *mft.G,
) (*HandlerLoadDescription, *mft.Error)

type HLoadGenerator func(
	ctx context.Context,
	cluster Cluster,
	hDescription *HandlerLoadDescription,
	idGenerator *mft.G,
) (Handler, *mft.Error)

type HandlerGenerator struct {
	mx mfs.PMutex

	hNewGenerator  map[string]HNewGenerator
	hLoadGenerator map[string]HLoadGenerator
}

// HandlerLoadDescription description of handler for load
type HandlerLoadDescription struct {
	Name       string          `json:"name"`
	UserName   string          `json:"user_name"`
	Type       string          `json:"type"`
	Params     json.RawMessage `json:"params"`
	QueueNames []string        `json:"queue_names"`
	Start      bool            `json:"start"`
	Handler    Handler         `json:"-"`
}

func (qld *HandlerLoadDescription) HandlerDescription() HandlerDescription {
	qd := HandlerDescription{
		Name:       qld.Name,
		Type:       qld.Type,
		Params:     qld.Params,
		UserName:   qld.UserName,
		QueueNames: qld.QueueNames,
	}

	return qd
}

func (ecg *HandlerGenerator) AddGenerator(
	name string,
	hNewGenerator HNewGenerator,
	hLoadGenerator HLoadGenerator,
) {
	ecg.mx.Lock()
	defer ecg.mx.Unlock()
	ecg.hNewGenerator[name] = hNewGenerator
	ecg.hLoadGenerator[name] = hLoadGenerator
}

func (ecg *HandlerGenerator) GetGenerator(
	name string) (
	hNewGenerator HNewGenerator,
	hLoadGenerator HLoadGenerator,
	ok bool,
) {
	ecg.mx.Lock()
	defer ecg.mx.Unlock()
	ng, ok := ecg.hNewGenerator[name]

	if !ok {
		return nil, nil, false
	}

	return ng, ecg.hLoadGenerator[name], true
}

func (sc *SimpleCluster) AddHandler(user ClusterUser,
	handlerParams HandlerDescription) (err *mft.Error) {
	allowed, err := sc.CheckPermission(user, ClusterSelfObjectType, AddHandlerAction, "")
	if err != nil {
		return err
	}
	if !allowed {
		return GenerateErrorForClusterUser(user, 10117000)
	}

	sc.mx.Lock()
	_, ok := sc.Handlers[handlerParams.Name]
	sc.mx.Unlock()
	if ok {
		return GenerateErrorForClusterUser(user, 10117002, handlerParams.Name)
	}

	ctx, cancel := context.WithTimeout(context.Background(), sc.ObjectCreateDuration)
	defer cancel()

	cr, ld, ok := sc.HandlerGenerator.GetGenerator(handlerParams.Type)
	if !ok {
		return GenerateError(10117001, handlerParams.Type)
	}

	ecld, err := cr(ctx, sc, handlerParams, sc.IDGenerator)
	if err != nil {
		return err
	}

	ec, err := ld(ctx, sc, ecld, sc.IDGenerator)
	if err != nil {
		return err
	}

	ecld.Handler = ec

	sc.mx.Lock()
	sc.Handlers[ecld.Name] = ecld
	sc.mx.Unlock()

	err = sc.OnChange()
	if err != nil {
		return err
	}

	return nil
}
func (sc *SimpleCluster) DropHandler(user ClusterUser,
	name string) (err *mft.Error) {

	allowed, err := sc.CheckPermission(user, ClusterSelfObjectType, DropHandlerAction, "")
	if err != nil {
		return err
	}
	if !allowed {
		return GenerateErrorForClusterUser(user, 10117100)
	}

	sc.mx.Lock()
	_, ok := sc.Handlers[name]
	sc.mx.Unlock()
	if !ok {
		return GenerateError(10117102, name)
	}

	sc.mx.Lock()
	h := sc.Handlers[name]
	delete(sc.Handlers, name)
	sc.mx.Unlock()
	if h != nil && h.Handler != nil {
		err = h.Handler.Stop(context.Background())
		if err != nil {
			return GenerateErrorE(10117101, err)
		}
	}

	err = sc.OnChange()
	if err != nil {
		return err
	}

	return nil
}
func (sc *SimpleCluster) GetHandlerDescription(user ClusterUser,
	name string) (handlerParams HandlerDescription, err *mft.Error) {

	allowed, err := sc.CheckPermission(user, ClusterSelfObjectType, GetHandlerDescrAction, "")
	if err != nil {
		return handlerParams, err
	}
	if !allowed {
		return handlerParams, GenerateErrorForClusterUser(user, 10117200)
	}

	sc.mx.RLock()
	ecld, ok := sc.Handlers[name]
	sc.mx.RUnlock()

	if !ok {
		return handlerParams, GenerateError(10117201, name)
	}

	return ecld.HandlerDescription(), nil
}
func (sc *SimpleCluster) GetHandlersList(user ClusterUser) (names []string, err *mft.Error) {

	allowed, err := sc.CheckPermission(user, ClusterSelfObjectType, GetHandlerDescrAction, "")
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, GenerateErrorForClusterUser(user, 10117300)
	}

	sc.mx.RLock()
	names = make([]string, 0, len(sc.Handlers))
	for name := range sc.Handlers {
		names = append(names, name)
	}
	sc.mx.RUnlock()

	return names, nil
}

func (sc *SimpleCluster) GetHandler(user ClusterUser, name string) (handler Handler, exists bool, err *mft.Error) {
	allowed, err := sc.CheckPermission(user, ClusterSelfObjectType, GetHandlerAction, "")
	if err != nil {
		return nil, false, err
	}
	if !allowed {
		return nil, false, GenerateErrorForClusterUser(user, 10117800)
	}

	sc.mx.RLock()
	hld, ok := sc.Handlers[name]
	sc.mx.RUnlock()

	if !ok {
		return nil, false, nil
	}

	return hld.Handler, true, nil
}

func HandlerGeneratorCreate() *HandlerGenerator {
	res := &HandlerGenerator{
		hNewGenerator:  make(map[string]HNewGenerator),
		hLoadGenerator: make(map[string]HLoadGenerator),
	}

	res.AddGenerator(RegularlySaveHandlerType, RegularlySaveNewGenerator, RegularlySaveLoadGenerator)
	res.AddGenerator(CopyUniqueHandlerType, CopyUniqueNewGenerator, CopyUniqueLoadGenerator)
	res.AddGenerator(BlockDeleteHandlerType, BlockDeleteNewGenerator, BlockDeleteLoadGenerator)
	res.AddGenerator(BlockUnloadHandlerType, BlockUnloadNewGenerator, BlockUnloadLoadGenerator)
	res.AddGenerator(BlockMarkHandlerType, BlockMarkNewGenerator, BlockMarkLoadGenerator)

	return res
}
