package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/capella-pw/queue/cluster"
	"github.com/capella-pw/queue/cluster/cap"
	"github.com/capella-pw/queue/queue"
	"github.com/myfantasy/mft"

	log "github.com/sirupsen/logrus"
)

const (
	CapGroupSourceType   string = "caps"
	CapCurrentSourceType string = "cap"
	PGSourceType         string = "pg"
)

type Source struct {
	Type               string        `json:"type"`
	Name               string        `json:"name"`
	QueueName          string        `json:"queue_name"`
	QueueSubscribeName string        `json:"subscribe_name"`
	QueueSaveMode      int           `json:"queue_save_mode"`
	Quantity           int           `json:"qty"`
	GetQuery           string        `json:"get_query"`
	CompleteQuery      string        `json:"complete_query"`
	SetQuery           string        `json:"set_query"`
	Timeout            time.Duration `json:"timeout"`
}

type TransferAction struct {
	From         Source        `json:"from"`
	To           Source        `json:"to"`
	Name         string        `json:"name"`
	Description  string        `json:"descr"`
	TimeoutAny   time.Duration `json:"timeout_any"`
	TimeoutEmpty time.Duration `json:"timeout_empty"`
	RandomSleep  time.Duration `json:"timeout_random_start_sleep"`

	LastStart     time.Time  `json:"-"`
	LastComplete  time.Time  `json:"-"`
	LastFail      time.Time  `json:"-"`
	LastFailError *mft.Error `json:"-"`
	LastFailOp    string     `json:"-"`
}

type CallAction struct {
	Where        Source        `json:"where"`
	Name         string        `json:"name"`
	Description  string        `json:"descr"`
	TimeoutAny   time.Duration `json:"timeout_any"`
	TimeoutEmpty time.Duration `json:"timeout_empty"`
	RandomSleep  time.Duration `json:"timeout_random_start_sleep"`

	LastStart     time.Time  `json:"-"`
	LastComplete  time.Time  `json:"-"`
	LastFail      time.Time  `json:"-"`
	LastFailError *mft.Error `json:"-"`
	LastFailOp    string     `json:"-"`
}

func (ta *TransferAction) LoadMessagesDO(st *Settings) {
	rnd := rand.Intn(100)
	time.Sleep(ta.RandomSleep * time.Duration(rnd) / 100)

	for !st.NeedStop {
		ta.LastStart = time.Now()
		msgs, err := ta.From.LoadMessages(st)
		if err != nil {
			log.Errorf("TA `%v`: LoadMessages: %v/n", ta.Name, err)
			ta.LastFail = time.Now()
			ta.LastFailError = err
			ta.LastFailOp = "LoadMessages"
			time.Sleep(ta.TimeoutEmpty)
			continue
		}

		if len(msgs) == 0 {
			ta.LastComplete = time.Now()
			time.Sleep(ta.TimeoutEmpty)
			continue
		}

		err = ta.To.SetMessages(st, msgs)
		if err != nil {
			log.Errorf("TA `%v`: SetMessages: %v/n", ta.Name, err)
			ta.LastFail = time.Now()
			ta.LastFailError = err
			ta.LastFailOp = "SetMessages"
			time.Sleep(ta.TimeoutEmpty)
			continue
		}

		err = ta.From.MarkCompleteMessages(st, msgs)
		if err != nil {
			log.Errorf("TA `%v`: MarkCompleteMessages: %v/n", ta.Name, err)
			ta.LastFail = time.Now()
			ta.LastFailError = err
			ta.LastFailOp = "MarkCompleteMessages"
			time.Sleep(ta.TimeoutEmpty)
			continue
		}

		ta.LastComplete = time.Now()
		time.Sleep(ta.TimeoutAny)
	}
}

func (ca *CallAction) CallDO(st *Settings) {
	rnd := rand.Intn(100)
	time.Sleep(ca.RandomSleep * time.Duration(rnd) / 100)

	for !st.NeedStop {
		ca.LastStart = time.Now()
		ok, err := ca.Where.CallAction(st)
		if err != nil {
			log.Errorf("CA `%v`: CallAction: %v/n", ca.Name, err)
			ca.LastFail = time.Now()
			ca.LastFailError = err
			ca.LastFailOp = "CallAction"
			time.Sleep(ca.TimeoutEmpty)
			continue
		}

		ca.LastComplete = time.Now()
		if !ok {
			time.Sleep(ca.TimeoutEmpty)
			continue
		}

		time.Sleep(ca.TimeoutAny)
	}
}

func (s *Source) LoadMessages(st *Settings) (msgs []queue.MessageJsonBody, err *mft.Error) {
	if s.Type == CapCurrentSourceType {
		return s.LoadMessageFromCG(st)
	} else if s.Type == PGSourceType {
		return s.LoadMessageFromPG(st)
	}
	return nil, mft.ErrorS(fmt.Sprintf("Type `%v` of source is not allowed for get messages", s.Type))
}

func (s *Source) MarkCompleteMessages(st *Settings, msgs []queue.MessageJsonBody) (err *mft.Error) {
	if s.Type == CapCurrentSourceType {
		return s.MarkCompleteMessagesFromCG(st, msgs)
	} else if s.Type == PGSourceType {
		return s.MarkCompleteMessagesFromPG(st, msgs)
	}
	return mft.ErrorS(fmt.Sprintf("Type `%v` of source is not allowed for complete messages", s.Type))
}

func (s *Source) SetMessages(st *Settings, msgs []queue.MessageJsonBody) (err *mft.Error) {
	if s.Type == CapCurrentSourceType {
		return s.SetMessagesFromCG(st, msgs)
	} else if s.Type == CapGroupSourceType {
		return s.SetMessagesFromCGG(st, msgs)
	} else if s.Type == PGSourceType {
		return s.SetMessagesFromPG(st, msgs)
	}
	return mft.ErrorS(fmt.Sprintf("Type `%v` of source is not allowed for set messages", s.Type))
}

func (s *Source) LoadMessageFromCG(st *Settings) (msgs []queue.MessageJsonBody, err *mft.Error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.Timeout)
	defer cancel()

	var q queue.Queue
	var exists bool
	var messages []*queue.MessageWithMeta
	var lastId int64
	err = st.CG.FuncDOName(ctx, s.Name,
		func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
			q, exists, err = c.GetQueue(ctx, nil, s.QueueName)

			if err != nil {
				return err
			}
			if !exists {
				return err
			}

			lastId, err = q.SubscriberGetLastRead(ctx, s.QueueSubscribeName)
			if err != nil {
				return err
			}

			messages, err = q.Get(ctx, lastId, s.Quantity)
			return err
		})
	if err != nil {
		return msgs, mft.ErrorNew(fmt.Sprintf("Get Queue messages `%v` from `%v`", s.QueueName, s.Name), err)
	}
	if !exists {
		return msgs, mft.ErrorS(fmt.Sprintf("Get Queue messages `%v` from `%v` error: queue does not exists", s.QueueName, s.Name))
	}

	msgs = queue.MWMToMessageJBList(messages)
	return msgs, nil
}

type scanRes struct {
	ExternalID int64     `db:"external_id"`
	ExternalDt time.Time `db:"message_dt"`
	Message    string    `db:"message"`
	Source     string    `db:"source"`
	Segment    int64     `db:"segment"`
}

func (s *Source) LoadMessageFromPG(st *Settings) (msgs []queue.MessageJsonBody, err *mft.Error) {
	pg, ok := st.PG[s.Name]
	if !ok {
		return nil, mft.ErrorS(fmt.Sprintf("PG connection with name `%v` does not exists", s.Name))
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.Timeout)
	defer cancel()

	rows, er0 := pg.db.QueryxContext(ctx, s.GetQuery)
	if er0 != nil {
		return nil, mft.ErrorNew(fmt.Sprintf("PG connection name `%v` call get fail", s.Name), er0)
	}
	defer rows.Close()

	for rows.Next() {
		ok = true
		scres := scanRes{}
		if er0 = rows.StructScan(&scres); er0 != nil {
			return nil, mft.ErrorNew(fmt.Sprintf("PG connection name `%v` parse row fail", s.Name), er0)
		}

		msgs = append(msgs, queue.MessageJsonBody{
			ExternalID: scres.ExternalID,
			ExternalDt: scres.ExternalDt.Unix(),
			Message:    []byte(scres.Message),
			Source:     scres.Source,
			Segment:    scres.Segment,
		})
	}

	return msgs, nil
}

func (s *Source) MarkCompleteMessagesFromCG(st *Settings, msgs []queue.MessageJsonBody) (err *mft.Error) {
	if len(msgs) == 0 {
		return nil
	}
	lastId := msgs[0].ID

	for i := 1; i < len(msgs); i++ {
		if msgs[i].ID > lastId {
			lastId = msgs[i].ID
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.Timeout)
	defer cancel()

	var q queue.Queue
	var exists bool
	err = st.CG.FuncDOName(ctx, s.Name,
		func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
			q, exists, err = c.GetQueue(ctx, nil, s.QueueName)

			if err != nil {
				return err
			}
			if !exists {
				return err
			}

			err = q.SubscriberSetLastRead(ctx, s.QueueSubscribeName, lastId, s.QueueSaveMode)
			return err
		})
	if err != nil {
		return mft.ErrorNew(fmt.Sprintf("Set Queue last read `%v` from `%v`", s.QueueName, s.Name), err)
	}
	if !exists {
		return mft.ErrorS(fmt.Sprintf("Set Queue last read `%v` from `%v` error: queue does not exists", s.QueueName, s.Name))
	}

	return nil
}

func (s *Source) MarkCompleteMessagesFromPG(st *Settings, msgs []queue.MessageJsonBody) (err *mft.Error) {
	if len(msgs) == 0 {
		return nil
	}

	pg, ok := st.PG[s.Name]
	if !ok {
		return mft.ErrorS(fmt.Sprintf("PG connection with name `%v` does not exists", s.Name))
	}

	ids := make([]int64, 0, len(msgs))
	for _, m := range msgs {
		ids = append(ids, m.ExternalID)
	}

	b, er0 := json.Marshal(ids)
	if er0 != nil {
		panic(er0)
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.Timeout)
	defer cancel()

	rows, er0 := pg.db.QueryxContext(ctx, s.CompleteQuery, string(b))
	if er0 != nil {
		log.Tracef("%v :: %v :: %v\n", string(b), s.CompleteQuery, err)
		return mft.ErrorNew(fmt.Sprintf("PG connection name `%v` call fail complete", s.Name), er0)
	}
	defer rows.Close()

	return nil
}

func (s *Source) SetMessagesFromCG(st *Settings, msgs []queue.MessageJsonBody) (err *mft.Error) {
	log.Tracef("Start send msgs to queue `%v` from `%v` cnt %v \n", s.QueueName, s.Name, len(msgs))

	if len(msgs) == 0 {
		return nil
	}

	messages := queue.MJBToMessageList(msgs)

	ctx, cancel := context.WithTimeout(context.Background(), s.Timeout)
	defer cancel()

	var q queue.Queue
	var exists bool
	var ids []int64
	err = st.CG.FuncDOName(ctx, s.Name,
		func(ctx context.Context, c *cluster.ExternalAbstractCluster) (err *mft.Error) {
			q, exists, err = c.GetQueue(ctx, nil, s.QueueName)

			if err != nil {
				return err
			}
			if !exists {
				return err
			}

			ids, err = q.AddUniqueList(ctx, messages, s.QueueSaveMode)
			return err
		})
	if err != nil {
		return mft.ErrorNew(fmt.Sprintf("Add Unique to Queue `%v` from `%v`", s.QueueName, s.Name), err)
	}
	if !exists {
		return mft.ErrorS(fmt.Sprintf("Add Unique to Queue `%v` from `%v` error: queue does not exists", s.QueueName, s.Name))
	}

	log.Tracef("Sended msgs to queue `%v` from `%v` cnt %v \n", s.QueueName, s.Name, len(ids))
	log.Tracef("Sended msgs to queue `%v` from `%v` out %v \n", s.QueueName, s.Name, ids)

	return nil
}

func (s *Source) SetMessagesFromCGG(st *Settings, msgs []queue.MessageJsonBody) (err *mft.Error) {
	log.Tracef("Start send msgs to queue `%v` from `%v` cnt %v \n", s.QueueName, s.Name, len(msgs))

	if len(msgs) == 0 {
		return nil
	}

	messages := queue.MJBToMessageList(msgs)

	ctx, cancel := context.WithTimeout(context.Background(), s.Timeout)
	defer cancel()

	err = st.CG.FuncDO(ctx, s.Name,
		cap.QueueAddUniqueList(s.QueueName, messages, s.QueueSaveMode,
			func(ids []int64) {
				log.Tracef("Sended msgs to queue `%v` from `%v` (GROUP) out %v \n", s.QueueName, s.Name, ids)
			}), func(err *mft.Error) {
			log.Debug("Sended msgs to queue `%v` from `%v` (GROUP) error %v \n", s.QueueName, s.Name, err)
		})

	if err != nil {
		return mft.ErrorNew(fmt.Sprintf("Add Unique to Queue (GROUP) `%v` from `%v`", s.QueueName, s.Name), err)
	}

	log.Tracef("Sended msgs to queue `%v` from `%v` cnt %v \n", s.QueueName, s.Name, len(messages))

	return nil
}

func (s *Source) SetMessagesFromPG(st *Settings, msgs []queue.MessageJsonBody) (err *mft.Error) {
	log.Tracef("Start send msgs to pg `%v` from `%v` cnt %v \n", s.QueueName, s.Name, len(msgs))

	if len(msgs) == 0 {
		return nil
	}

	pg, ok := st.PG[s.Name]
	if !ok {
		return mft.ErrorS(fmt.Sprintf("PG connection with name `%v` does not exists", s.Name))
	}

	b, er0 := json.Marshal(msgs)
	if er0 != nil {
		panic(er0)
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.Timeout)
	defer cancel()

	rows, er0 := pg.db.QueryxContext(ctx, s.SetQuery, string(b))
	if er0 != nil {
		return mft.ErrorNew(fmt.Sprintf("PG connection name `%v` call fail set messages", s.Name), er0)
	}
	defer rows.Close()

	return nil
}

func (s *Source) CallAction(st *Settings) (ok bool, err *mft.Error) {
	if s.Type == PGSourceType {
		return s.CallActioPG(st)
	}
	return false, mft.ErrorS(fmt.Sprintf("Type `%v` of source is not allowed for call action", s.Type))
}

type scanResCA struct {
	OK *bool `db:"ok"`
}

func (s *Source) CallActioPG(st *Settings) (ok bool, err *mft.Error) {
	pg, ok := st.PG[s.Name]
	if !ok {
		return false, mft.ErrorS(fmt.Sprintf("PG connection with name `%v` does not exists", s.Name))
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.Timeout)
	defer cancel()

	rows, er0 := pg.db.QueryxContext(ctx, s.GetQuery)
	if er0 != nil {
		return false, mft.ErrorNew(fmt.Sprintf("PG connection name `%v` call action fail", s.Name), er0)
	}
	defer rows.Close()

	for rows.Next() {
		ok = true
		scres := scanResCA{}
		if er0 = rows.StructScan(&scres); er0 != nil {
			return false, mft.ErrorNew(fmt.Sprintf("PG connection name `%v` parse call action row fail", s.Name), er0)
		}

		if scres.OK == nil || !*scres.OK {
			return false, nil
		}
		return true, nil
	}

	return false, nil
}
