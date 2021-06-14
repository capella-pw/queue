package cap

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/capella-pw/queue/cluster"
	"github.com/capella-pw/queue/compress"
	"github.com/capella-pw/queue/queue"
	"github.com/myfantasy/mft"
	"golang.org/x/sync/semaphore"
)

// ConGroup - Cluster connection group
type ConGroup struct {
	ConnectionsInfo map[string]*ClusterConnection `json:"connections"`

	HealthCheck map[string]bool                             `json:"-"`
	Clusters    map[string]*cluster.ExternalAbstractCluster `json:"-"`

	PriorityGroups map[string]*PriorityGroup `json:"priority_groups"`

	mx sync.Mutex
}

type PriorityGroupStep struct {
	StepCallCount     int  `json:"call_count"`
	IgnoreHealthCheck bool `json:"ignore_health_check"`

	ConNames []string `json:"con_names"`

	NextIndex int `json:"-"`
}

type PriorityGroup struct {
	MinSuccess int                 `json:"min_success"`
	Steps      []PriorityGroupStep `json:"step"`
}

func (cg *ConGroup) ToJson() json.RawMessage {
	msg, er0 := json.MarshalIndent(cg, "", "  ")
	if er0 != nil {
		panic(GenerateErrorE(10191000, er0))
	}

	return msg
}

func ConGroupGenerate() (cg *ConGroup) {
	return &ConGroup{
		ConnectionsInfo: make(map[string]*ClusterConnection),
		Clusters:        make(map[string]*cluster.ExternalAbstractCluster),
		HealthCheck:     make(map[string]bool),
		PriorityGroups:  make(map[string]*PriorityGroup),
	}
}

func ConGroupFromJson(data []byte, compressor *compress.Generator) (cg *ConGroup, err *mft.Error) {
	cg = ConGroupGenerate()
	er0 := json.Unmarshal(data, &cg)
	if er0 != nil {
		return nil, GenerateErrorE(10191010, er0)
	}

	cg.Init(compressor)

	return cg, nil
}

func (cg *ConGroup) Init(compressor *compress.Generator) *ConGroup {
	for n, c := range cg.ConnectionsInfo {
		c.Compressor = compressor
		c.CPAuthentificationInfo()
		cg.HealthCheck[n] = false
		c.Init()
		cg.Clusters[n] = c.Cluster()
	}

	return cg
}

func (cg *ConGroup) AddConnection(name string, connection *ClusterConnection) *ConGroup {
	cg.ConnectionsInfo[name] = connection

	return cg
}

func (cg *ConGroup) Ping(errFunc func(err *mft.Error)) {
	for cn, c := range cg.Clusters {
		err := c.Ping(nil)
		if err != nil {
			cg.mx.Lock()
			cg.HealthCheck[cn] = false
			cg.mx.Unlock()
			if errFunc != nil {
				errFunc(err)
			}
		} else {
			cg.mx.Lock()
			cg.HealthCheck[cn] = true
			cg.mx.Unlock()
		}
	}
}

func (cg *ConGroup) FuncDOName(ctx context.Context, conName string,
	doFunc func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error)) (err *mft.Error) {

	c, okCluster := cg.Clusters[conName]

	if !okCluster {
		return GenerateError(10191130, conName)
	}

	err = doFunc(ctx, c)

	return err
}

func (cg *ConGroup) FuncDO(ctx context.Context, pgName string,
	doFunc func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error),
	errFunc func(err *mft.Error)) (err *mft.Error) {
	cntOk := 0
	step := 0
	pg, pgOk := cg.PriorityGroups[pgName]
	if !pgOk {
		return GenerateError(10191103, pgName)
	}
	sended := map[string]struct{}{}
	hasError := false
	skippedCons := int64(0)

	var mx sync.Mutex
	for step < len(pg.Steps) && cntOk < pg.MinSuccess {
		pgs := pg.Steps[step]

		cntCall := pgs.StepCallCount

		if pg.MinSuccess > cntCall {
			cntCall = pg.MinSuccess
		}

		sem := semaphore.NewWeighted(int64(cntCall))

		moveCnt := 0
		idx := pgs.NextIndex

		var wg sync.WaitGroup

		ch := make(chan bool, 2)

		wg.Add(1)

		for moveCnt < len(pgs.ConNames) && cntOk < pg.MinSuccess {
			if idx >= len(pgs.ConNames) {
				idx = 0
			}
			cn := pgs.ConNames[idx]
			idx++
			moveCnt++

			mx.Lock()
			if _, ok := sended[cn]; ok {
				mx.Unlock()
				continue
			}
			mx.Unlock()

			if !pgs.IgnoreHealthCheck {
				cg.mx.Lock()
				isHealth, okHealth := cg.HealthCheck[cn]
				cg.mx.Unlock()

				if okHealth && !isHealth {
					continue
				}
			}

			c, okCluster := cg.Clusters[cn]
			if !okCluster {
				return GenerateError(10191104, cn, cntOk, pg.MinSuccess)
			}

			if er0 := sem.Acquire(ctx, 1); er0 != nil {
				return GenerateErrorE(10191101, er0, cntOk, pg.MinSuccess)
			}

			wg.Add(1)
			go func() {
				errStep := doFunc(ctx, c)
				if errStep == nil {
					mx.Lock()
					_, ok := sended[cn]

					if !ok {
						sended[cn] = struct{}{}
						cntOk++

						if cntOk == pg.MinSuccess {
							ch <- true
						}
					}

					if ok {
						sem.Release(1)
					} else if hasError {
						sem.Release(1)
					} else {
						skippedCons++
					}
					mx.Unlock()
				} else {
					mx.Lock()
					if hasError {
						sem.Release(1)
					} else {
						skippedCons++
						hasError = true
						sem.Release(skippedCons)
					}
					mx.Unlock()

					if errFunc != nil {
						errFunc(GenerateErrorE(10191102, errStep, cn))
					}
				}
				wg.Done()
			}()

		}

		wg.Done()

		go func() {
			wg.Wait()
			ch <- true
		}()

		select {
		case <-ch:
		}

		pgs.NextIndex = idx

		step++
	}

	if cntOk >= pg.MinSuccess {
		return nil
	}

	return GenerateError(10191100, cntOk, pg.MinSuccess)
}

func QueueAddUnique(queueName string,
	message []byte, externalID int64, externalDt int64, source string, segment int64, saveMode int,
	doOnOk func(id int64),
) func(ctx context.Context, cl *cluster.ExternalAbstractCluster) (err *mft.Error) {
	return func(ctx context.Context, cl *cluster.ExternalAbstractCluster) (err *mft.Error) {
		q, exists, err := cl.GetQueue(nil, queueName)
		if err != nil {
			return GenerateErrorE(10191201, err, queueName)
		}

		if !exists {
			return GenerateError(10191200, queueName)
		}

		id, err := q.AddUnique(ctx, message, externalID, externalDt, source, segment, saveMode)

		if err != nil {
			return GenerateErrorE(10191202, err, queueName)
		}

		if doOnOk != nil {
			doOnOk(id)
		}

		return nil
	}
}

func QueueAddUniqueList(queueName string,
	messages []queue.Message, saveMode int,
	doOnOk func(ids []int64),
) func(ctx context.Context, cl *cluster.ExternalAbstractCluster) (err *mft.Error) {
	return func(ctx context.Context, cl *cluster.ExternalAbstractCluster) (err *mft.Error) {
		q, exists, err := cl.GetQueue(nil, queueName)
		if err != nil {
			return GenerateErrorE(10191211, err, queueName)
		}

		if !exists {
			return GenerateError(10191210, queueName)
		}

		ids, err := q.AddUniqueList(ctx, messages, saveMode)

		if err != nil {
			return GenerateErrorE(10191212, err, queueName)
		}

		if doOnOk != nil {
			doOnOk(ids)
		}

		return nil
	}
}
