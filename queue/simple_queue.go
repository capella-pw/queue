package queue

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"time"

	"github.com/capella-pw/queue/storage"
	"github.com/myfantasy/mfs"
	"github.com/myfantasy/mft"
)

const (
	// NotSaveSaveMode - not save message to storage (not mark queue as need changed)
	NotSaveSaveMode = 0
	// SaveImmediatelySaveMode - save message immediatly after add element
	SaveImmediatelySaveMode = 1
	// SaveMarkSaveMode - not wait save message after add element (saving do on schedule)
	SaveMarkSaveMode = 2
	// SaveWaitSaveMode - wait save message after add element (saving do on schedule)
	SaveWaitSaveMode = 3
)

// MetaDataFileName - file name with metadata queue info
const MetaDataFileName = "q.json"

// SubscribersFileName - file name with subscribers queue info
const SubscribersFileName = "subscr.json"

// BlockPrefixFileName - prefix file name with queue block
const BlockPrefixFileName = "bl_"

// BlockPostfixFileName - postfix file name with queue block
const BlockPostfixFileName = ".json"

// SimpleQueue - queue MVP
type SimpleQueue struct {
	mx              mfs.PMutex
	mxFileSave      mfs.PMutex
	mxBlockSaveWait mfs.PMutex

	Blocks []*SimpleQueueBlock `json:"blocks"`

	// MetaStorage - storage for metadata (block info)
	MetaStorage storage.Storage `json:"-"`

	// SubscriberStorage - storage for subscriber info
	SubscriberStorage storage.Storage `json:"-"`

	// MarkerBlockDataStorage - storage for block data with marker
	// case nil use MetaStorage
	MarkerBlockDataStorage map[string]storage.Storage `json:"-"`

	IDGenerator *mft.G `json:"-"`

	CntLimit  int           `json:"cnt_limit,omitempty"`
	TimeLimit time.Duration `json:"time_limit,omitempty"`
	LenLimit  int           `json:"len_limit,omitempty"`

	ChangesRv int64 `json:"-"`
	SaveRv    int64 `json:"-"`

	SaveWait []chan bool `json:"-"`

	SaveBlocks map[int64]*SimpleQueueBlock `json:"-"`

	mxExt     mfs.MapMutex
	lastExtID map[string]int64 `json:"-"`

	Subscribers *SimpleQueueSubscribers `json:"-"`

	Source string `json:"-"`
}

// SimpleQueueBlock block with data
type SimpleQueueBlock struct {
	mx         mfs.PMutex
	mxFileSave mfs.PMutex

	ID          int64     `json:"id"`
	Dt          time.Time `json:"dt"`
	Mark        string    `json:"mark"`
	RemoveMarks []string  `json:"rm_marks,omitempty"`
	NextMark    string    `json:"next_mark"`
	NeedDelete  bool      `json:"need_delete"`
	Len         int       `json:"len"`

	Data []*SimpleQueueMessage `json:"-"`

	ChangesRv int64 `json:"-"`
	SaveRv    int64 `json:"-"`

	IsUnload bool `json:"-"`

	SaveWait []chan bool `json:"-"`

	// May use without lock
	LastGet time.Time `json:"-"`
}

//****

// SimpleQueueMessage one message
type SimpleQueueMessage struct {
	ID int64 `json:"id"`
	// ExternalID, source id, when 0 then equal ID
	ExternalID int64     `json:"eid,omitempty"`
	ExternalDt int64     `json:"s_dt,omitempty"`
	Dt         time.Time `json:"dt"`
	Message    []byte    `json:"msg,omitempty"`
	Source     string    `json:"src,omitempty"`
	Segment    int64     `json:"sg,omitempty"`
}

// SimpleQueueSubscribers line subscribers info
type SimpleQueueSubscribers struct {
	mx         mfs.PMutex
	mxFileSave mfs.PMutex

	ChangesRv int64 `json:"-"`
	SaveRv    int64 `json:"-"`

	SaveWait []chan bool `json:"-"`

	SubscribersInfo    map[string]*SimpleQueueSubscriberInfo `json:"si,omitempty"`
	ReplicaSubscribers map[string]struct{}                   `json:"rs,omitempty"`
}

// SimpleQueueSubscriberInfo info aboun one subscriber
type SimpleQueueSubscriberInfo struct {
	LastID  int64     `json:"last_id"`
	StartDt time.Time `json:"start_dt"`
	LastDt  time.Time `json:"last_dt"`
}

// Mutex - returns Mutex
func (q *SimpleQueue) Mutex() *mfs.PMutex {
	return &q.mx
}

// Mutex - returns Mutex
func (block *SimpleQueueBlock) Mutex() *mfs.PMutex {
	return &block.mx
}

// MutexFileSave - returns Mutex for FileSave
func (q *SimpleQueue) MutexFileSave() *mfs.PMutex {
	return &q.mxFileSave
}

// MutexFileSave - returns Mutex for FileSave
func (block *SimpleQueueBlock) MutexFileSave() *mfs.PMutex {
	return &block.mxFileSave
}

// MutexBlockSaveWait - returns Mutex for block save Wait
func (q *SimpleQueue) MutexBlockSaveWait() *mfs.PMutex {
	return &q.mxBlockSaveWait
}

//****

// CreateSimpleQueue creates SimpleQueue
// MetaStorage - storage for metadata (block info)
// MarkerBlockDataStorage - storage for block data with marker
// case nil use MetaStorage
func CreateSimpleQueue(cntLimit int, timeLimit time.Duration, lenLimit int,
	metaStorage storage.Storage, subscriberStorage storage.Storage,
	markerBlockDataStorage map[string]storage.Storage, idGenerator *mft.G) *SimpleQueue {
	if idGenerator == nil {
		idGenerator = &mft.G{}
	}
	return &SimpleQueue{
		MetaStorage:            metaStorage,
		SubscriberStorage:      subscriberStorage,
		MarkerBlockDataStorage: markerBlockDataStorage,
		Blocks:                 make([]*SimpleQueueBlock, 0),
		IDGenerator:            idGenerator,

		CntLimit:  cntLimit,
		TimeLimit: timeLimit,
		LenLimit:  lenLimit,

		SaveBlocks: make(map[int64]*SimpleQueueBlock),

		Subscribers: &SimpleQueueSubscribers{
			SubscribersInfo:    make(map[string]*SimpleQueueSubscriberInfo),
			ReplicaSubscribers: make(map[string]struct{}),
		},

		ChangesRv: idGenerator.RvGetPart(),
	}
}

// LoadSimpleQueue load queue from storage
func LoadSimpleQueue(ctx context.Context, metaStorage storage.Storage, subscriberStorage storage.Storage,
	markerBlockDataStorage map[string]storage.Storage,
	idGenerator *mft.G) (q *SimpleQueue, err *mft.Error) {
	if idGenerator == nil {
		idGenerator = &mft.G{}
	}

	q = &SimpleQueue{
		MetaStorage:            metaStorage,
		SubscriberStorage:      subscriberStorage,
		MarkerBlockDataStorage: markerBlockDataStorage,
		Blocks:                 make([]*SimpleQueueBlock, 0),
		IDGenerator:            idGenerator,

		SaveBlocks: make(map[int64]*SimpleQueueBlock),

		Subscribers: &SimpleQueueSubscribers{
			SubscribersInfo:    make(map[string]*SimpleQueueSubscriberInfo),
			ReplicaSubscribers: make(map[string]struct{}),
		},
	}

	body, err := metaStorage.Get(ctx, MetaDataFileName)

	if err != nil {
		return nil, err
	}

	er0 := json.Unmarshal(body, q)
	if er0 != nil {
		return nil, GenerateErrorE(10030000, er0)
	}

	q.SaveRv = idGenerator.RvGetPart()
	q.ChangesRv = q.SaveRv

	for i := 0; i < len(q.Blocks); i++ {
		q.Blocks[i].SaveRv = q.SaveRv
		q.Blocks[i].ChangesRv = q.SaveRv
	}

	q.Subscribers.SaveRv = q.SaveRv
	q.Subscribers.ChangesRv = q.SaveRv

	if q.SubscriberStorage != nil {
		ok, err := q.SubscriberStorage.Exists(ctx, SubscribersFileName)
		if err != nil {
			return nil, err
		}

		if ok {
			bodyS, err := q.SubscriberStorage.Get(ctx, SubscribersFileName)
			if err != nil {
				return nil, err
			}

			er1 := json.Unmarshal(bodyS, q.Subscribers)
			if er1 != nil {
				return nil, GenerateErrorE(10030001, er1)
			}
		}
	}

	return q, nil
}

// currentBlockForWrite - block for write next message
// need q.mx Rlocked
// case err is not nil -> q.mx Unlocked
// case err is nil -> q.mx Rlocked
func (q *SimpleQueue) currentBlockForWrite(ctx context.Context, saveMode int) (block *SimpleQueueBlock, chWait chan bool, err *mft.Error) {

	if len(q.Blocks) == 0 {
		return q.checkAndAddCurrentBlockForWrite(ctx, saveMode)
	}
	ok, err := q.Blocks[len(q.Blocks)-1].canAppend(ctx, q.CntLimit, q.TimeLimit, q.LenLimit)
	if err != nil {
		q.mx.RUnlock()
		return nil, nil, err
	}

	if !ok {
		return q.checkAndAddCurrentBlockForWrite(ctx, saveMode)
	}

	return q.Blocks[len(q.Blocks)-1], nil, nil
}

// checkAndAddCurrentBlockForWrite - block for write next message
// need q.mx Rlocked
// case err is not nil -> q.mx Unlocked
// case err is nil -> q.mx Rlocked
func (q *SimpleQueue) checkAndAddCurrentBlockForWrite(ctx context.Context, saveMode int) (block *SimpleQueueBlock, chWait chan bool, err *mft.Error) {
	if !q.mx.TryPromoteF(ctx) {
		return nil, nil, GenerateError(10010003)
	}

	if len(q.Blocks) == 0 {
		block := &SimpleQueueBlock{
			ID:   q.IDGenerator.RvGetPart(),
			Dt:   time.Now(),
			Data: make([]*SimpleQueueMessage, 0, 1),
		}
		q.Blocks = append(q.Blocks, block)
		q.ChangesRv = block.ID
	}

	ok, err := q.Blocks[len(q.Blocks)-1].canAppend(ctx, q.CntLimit, q.TimeLimit, q.LenLimit)
	if err != nil {
		q.mx.Unlock()
		return nil, nil, err
	}

	if !ok {
		block := &SimpleQueueBlock{
			ID:   q.IDGenerator.RvGetPart(),
			Dt:   time.Now(),
			Data: make([]*SimpleQueueMessage, 0, 1),
		}
		q.Blocks = append(q.Blocks, block)
		q.ChangesRv = block.ID
	}

	if saveMode == SaveWaitSaveMode {
		chWait = make(chan bool, 1)
		q.SaveWait = append(q.SaveWait, chWait)
	}

	q.mx.Reduce()
	return q.Blocks[len(q.Blocks)-1], chWait, nil
}

// Add message to queue
// externalDt is unix time
// externalID is source id (should be != 0 if set 0 - not set)
func (q *SimpleQueue) Add(ctx context.Context, message []byte, externalID int64, externalDt int64, source string, segment int64, saveMode int) (id int64, err *mft.Error) {
	if externalDt > time.Now().Unix() {
		return id, GenerateError(10010008, externalDt, time.Now())
	}

	if !q.mx.RTryLock(ctx) {
		return id, GenerateError(10010000)
	}
	block, chWaitSaveMeta, err := q.currentBlockForWrite(ctx, saveMode)
	if err != nil {
		return id, err
	}

	id, chWaitBlockSave, err := block.add(ctx, message, externalID, externalDt, source, segment, q.IDGenerator, saveMode)

	if source == "" {
		source = q.Source
	}

	if saveMode != NotSaveSaveMode {
		// mark block as need to save even if there are errors

		q.mxBlockSaveWait.Lock()

		// if !q.mxBlockSaveWait.TryLock(ctx) {
		// 	q.mx.RUnlock()
		// 	return id, GenerateError(10010004)
		// }

		if _, ok := q.SaveBlocks[block.ID]; !ok {
			q.SaveBlocks[block.ID] = block
		}

		q.mxBlockSaveWait.Unlock()
	}

	if err != nil {
		q.mx.RUnlock()
		return id, err
	}

	if externalID != 0 {
		q.SetMaxExtID(source, externalID)
	}

	q.mx.RUnlock()

	if saveMode == SaveImmediatelySaveMode {
		err = q.SaveAll(ctx)
		if err != nil {
			return id, err
		}
		return id, nil
	}

	if saveMode == SaveWaitSaveMode {
		if chWaitSaveMeta != nil {
			select {
			case <-chWaitSaveMeta:
			case <-ctx.Done():
				return id, GenerateError(10010005)
			}
		}

		select {
		case <-chWaitBlockSave:
		case <-ctx.Done():
			return id, GenerateError(10010006)
		}
	}

	return id, nil
}
func (q *SimpleQueue) AddList(ctx context.Context, messages []Message, saveMode int) (ids []int64, err *mft.Error) {
	baseSaveMode := SaveMarkSaveMode
	if saveMode == NotSaveSaveMode {
		baseSaveMode = NotSaveSaveMode
	}
	if len(messages) == 0 {
		return make([]int64, 0), nil
	}
	ids = make([]int64, 0, len(messages))
	for i := 0; i < len(messages)-1; i++ {
		id, err := q.Add(ctx, messages[i].Message,
			messages[i].ExternalID,
			messages[i].ExternalDt,
			messages[i].Source,
			messages[i].Segment,
			baseSaveMode,
		)
		if err != nil {
			return ids, err
		}

		ids = append(ids, id)
	}

	i := len(messages) - 1
	id, err := q.Add(ctx, messages[i].Message,
		messages[i].ExternalID,
		messages[i].ExternalDt,
		messages[i].Source,
		messages[i].Segment,
		saveMode,
	)
	if err != nil {
		return ids, err
	}

	ids = append(ids, id)
	return ids, nil
}

// add message to queue block
// externalDt - unix()
func (block *SimpleQueueBlock) add(ctx context.Context, message []byte, externalID int64, externalDt int64, source string, segment int64, idGen *mft.G, saveMode int) (id int64, chWait chan bool, err *mft.Error) {
	if !block.mx.TryLock(ctx) {
		return id, nil, GenerateError(10010001)
	}

	if block.IsUnload {
		block.mx.Unlock()
		return id, nil, GenerateError(10010007)
	}

	id = idGen.RvGetPart()

	if externalID == 0 {
		externalID = id
	}

	msg := &SimpleQueueMessage{
		ID:         id,
		ExternalID: externalID,
		ExternalDt: externalDt,
		Message:    message,
		Source:     source,
		Segment:    segment,
		Dt:         time.Now(),
	}

	block.Data = append(block.Data, msg)
	block.Len += len(message)
	block.ChangesRv = msg.ID
	block.LastGet = time.Now()

	if saveMode == SaveWaitSaveMode {
		chWait = make(chan bool, 1)
		block.SaveWait = append(block.SaveWait, chWait)
	}

	block.mx.Unlock()
	return id, chWait, nil
}

// canAppend can append message to queue block
func (block *SimpleQueueBlock) canAppend(ctx context.Context, cntLimit int, timeLimit time.Duration, lenLimit int) (ok bool, err *mft.Error) {
	if !block.mx.RTryLock(ctx) {
		return ok, GenerateError(10010002)
	}

	ok = true

	if block.IsUnload {
		ok = false
	}

	if ok && timeLimit > 0 && block.Dt.Sub(time.Now()) >= timeLimit {
		ok = false
	}

	if ok && cntLimit > 0 && len(block.Data) >= cntLimit {
		ok = false
	}

	if ok && lenLimit > 0 && block.Len >= lenLimit {
		ok = false
	}

	block.mx.RUnlock()
	return ok, nil
}

// getBlockForNext gets block where eixts next after idStart available message
// q.mx should be rlocked
func (q *SimpleQueue) getBlockForNext(ctx context.Context, idStart int64) (block []*SimpleQueueBlock, err *mft.Error) {
	if len(q.Blocks) == 0 {
		return nil, nil
	}

	idx := sort.Search(len(q.Blocks), func(i int) bool {
		return q.Blocks[i].ID >= idStart
	})

	if idx > 0 {
		idx--
	}

	for idx < len(q.Blocks) {
		if !q.Blocks[idx].mx.RTryLock(ctx) {
			return nil, GenerateError(10011000)
		}

		if q.Blocks[idx].NeedDelete {
			q.Blocks[idx].mx.RUnlock()
			idx++
			continue
		}

		if q.Blocks[idx].IsUnload {
			err = q.Blocks[idx].load(ctx, q)
			if err != nil {
				return nil, err
			}
		}

		ok := true
		if len(q.Blocks[idx].Data) == 0 {
			ok = false
		} else if q.Blocks[idx].Data[len(q.Blocks[idx].Data)-1].ID <= idStart {
			ok = false
		}

		q.Blocks[idx].mx.RUnlock()

		if ok {
			return q.Blocks[idx:], nil
		}
		idx++
	}

	return nil, nil
}

// getItemsAfter gets items from block where id > idStart
// returns messages == nil when no elements
func (block *SimpleQueueBlock) getItemsAfter(ctx context.Context, q *SimpleQueue, idStart int64, cntLimit int, queueSaveRv int64) (messages []*MessageWithMeta, err *mft.Error) {
	if !block.mx.RTryLock(ctx) {
		return nil, GenerateError(10011001)
	}

	if block.IsUnload {
		err = block.load(ctx, q)
		if err != nil {
			return nil, err
		}
	} else {
		block.LastGet = time.Now()
	}

	if len(block.Data) == 0 {
		block.mx.RUnlock()
		return nil, nil
	}

	idx := sort.Search(len(block.Data), func(i int) bool {
		return block.Data[i].ID > idStart
	})

	for i := 0; (i+idx) < len(block.Data) && i < cntLimit; i++ {
		if block.Data[i+idx].ID > idStart {
			msg := block.Data[i+idx].CopyWM()
			msg.IsSaved = block.ID <= queueSaveRv && msg.ID <= block.SaveRv
			messages = append(messages, msg)
		}
	}

	block.mx.RUnlock()

	return messages, nil
}

// Get - gets messages from queue not more then cntLimit count and id more idStart
// returns messages == nil when no elements
func (q *SimpleQueue) Get(ctx context.Context, idStart int64, cntLimit int) (messages []*MessageWithMeta, err *mft.Error) {

	if !q.mx.RTryLock(ctx) {
		return nil, GenerateError(10011002)
	}

	blocks, err := q.getBlockForNext(ctx, idStart)
	queueSaveRv := q.SaveRv
	q.mx.RUnlock()

	if err != nil {
		return nil, err
	}

	if len(blocks) == 0 {
		return nil, nil
	}

	for i := 0; i < len(blocks); i++ {
		msgs, err := blocks[i].getItemsAfter(ctx, q, idStart, cntLimit-len(messages), queueSaveRv)

		if err != nil {
			return messages, err
		}

		messages = append(messages, msgs...)

		if len(messages) >= cntLimit {
			break
		}
	}

	return messages, nil
}

// Save save meta info of queue
// When MetaStorage == nil returns nil
// When SaveRv == ChangesRv do nothing and returns nil
func (q *SimpleQueue) Save(ctx context.Context) (err *mft.Error) {
	if q.MetaStorage == nil {
		return nil
	}
	if !q.mxFileSave.TryLock(ctx) {
		return GenerateError(10012000)
	}
	defer q.mxFileSave.Unlock()

	if !q.mx.RTryLock(ctx) {
		return GenerateError(10012001)
	}

	if q.SaveRv == q.ChangesRv {
		q.mx.RUnlock()
		return nil
	}

	changesRv := q.ChangesRv
	data, errMarshal := json.MarshalIndent(q, "", "\t")
	chLen := len(q.SaveWait)

	q.mx.RUnlock()

	if errMarshal != nil {
		return GenerateErrorE(10012002, errMarshal)
	}

	err = q.MetaStorage.Save(ctx, MetaDataFileName, data)
	if err != nil {
		return GenerateErrorE(10012003, err, MetaDataFileName)
	}

	if !q.mx.TryLock(ctx) {
		return GenerateError(10012004)
	}
	q.SaveRv = changesRv
	if len(q.SaveWait) > 0 && chLen > 0 {
		saveWait := make([]chan bool, 0)
		for i := chLen; i < len(q.SaveWait); i++ {
			saveWait = append(saveWait, q.SaveWait[i])
		}
		approve := q.SaveWait[0:chLen]

		q.SaveWait = saveWait
		go func() {
			for _, v := range approve {
				v <- true
			}
		}()
	}

	q.mx.Unlock()

	return nil
}

// getStorage by mark name, mx.RLock should be
func (q *SimpleQueue) getStorage(name string) storage.Storage {
	if q.MetaStorage == nil {
		return nil
	}

	if st, ok := q.MarkerBlockDataStorage[name]; ok {
		return st
	}
	return q.MetaStorage
}

// getStorage by mark name, mx.RLock will be locked (used block.mxFileSave)
func (q *SimpleQueue) getStorageLock(ctx context.Context, name string) (st storage.Storage, err *mft.Error) {
	if !q.mx.RTryLock(ctx) {
		return nil, GenerateError(10016000)
	}

	st = q.getStorage(name)
	q.mx.RUnlock()

	return st, nil
}

func (block *SimpleQueueBlock) blockFileName() string {
	return BlockPrefixFileName + strconv.Itoa(int(block.ID)) + BlockPostfixFileName
}

// Save save block of queue
// When q.MetaStorage == nil returns nil
// When block.SaveRv == block.ChangesRv do nothing and returns nil
func (block *SimpleQueueBlock) Save(ctx context.Context, q *SimpleQueue) (err *mft.Error) {
	if q.MetaStorage == nil {
		return nil
	}
	if !block.mxFileSave.TryLock(ctx) {
		return GenerateError(10013000)
	}
	defer block.mxFileSave.Unlock()

	if !block.mx.RTryLock(ctx) {
		return GenerateError(10013001)
	}

	if block.SaveRv == block.ChangesRv {
		block.mx.RUnlock()
		return nil
	}

	changesRv := block.ChangesRv
	data, errMarshal := json.MarshalIndent(block.Data, "", "\t")
	chLen := len(block.SaveWait)

	block.mx.RUnlock()

	if errMarshal != nil {
		return GenerateErrorE(10013002, errMarshal)
	}

	fileName := block.blockFileName()

	st, err := q.getStorageLock(ctx, block.Mark)

	if err != nil {
		return err
	}

	err = st.Save(ctx, fileName, data)
	if err != nil {
		return GenerateErrorE(10013003, err, fileName)
	}

	if !block.mx.TryLock(ctx) {
		return GenerateError(10013004)
	}
	block.SaveRv = changesRv
	if len(block.SaveWait) > 0 && chLen > 0 {
		saveWait := make([]chan bool, 0)
		for i := chLen; i < len(block.SaveWait); i++ {
			saveWait = append(saveWait, block.SaveWait[i])
		}
		approve := block.SaveWait[0:chLen]

		block.SaveWait = saveWait
		go func() {
			for _, v := range approve {
				v <- true
			}
		}()
	}

	block.mx.Unlock()

	return nil
}

// SaveAll save all waiting for save block and metadata
func (q *SimpleQueue) SaveAll(ctx context.Context) (err *mft.Error) {

	if !q.mxBlockSaveWait.TryLock(ctx) {
		return GenerateError(10014000)
	}
	waitSaveBlocks := q.SaveBlocks
	if len(waitSaveBlocks) > 0 {
		q.SaveBlocks = make(map[int64]*SimpleQueueBlock)
	}
	q.mxBlockSaveWait.Unlock()

	err = q.Save(ctx)
	if err != nil {
		q.mxBlockSaveWait.LockF()
		for blockID, block := range waitSaveBlocks {
			q.SaveBlocks[blockID] = block
		}
		q.mxBlockSaveWait.Unlock()

		return err
	}

	failSaveBlocks := make(map[int64]*SimpleQueueBlock)
	for blockID, block := range waitSaveBlocks {
		if err != nil {
			failSaveBlocks[blockID] = block
		}

		err = block.Save(ctx, q)
		if err != nil {
			failSaveBlocks[blockID] = block
		}
	}
	if err != nil {
		q.mxBlockSaveWait.LockF()
		for blockID, block := range failSaveBlocks {
			q.SaveBlocks[blockID] = block
		}
		q.mxBlockSaveWait.Unlock()
		return err
	}

	err = q.SaveSubscribers(ctx)
	if err != nil {
		return err
	}

	return nil
}

// load - load block from Storage
// need block.mx RLocked
// Case err!=nil block.mx is Unlocked
// case err==nil block.mx is RLocked
func (block *SimpleQueueBlock) load(ctx context.Context, q *SimpleQueue) (err *mft.Error) {
	if !block.mxFileSave.TryLock(ctx) {
		return GenerateError(10020000)
	}
	defer block.mxFileSave.Unlock()
	if !block.mx.TryPromote(ctx) {
		return GenerateError(10020001)
	}

	block.LastGet = time.Now()

	fileName := block.blockFileName()

	st, err := q.getStorageLock(ctx, block.Mark)

	if err != nil {
		block.mx.Unlock()
		return err
	}

	body, err := st.Get(ctx, fileName)

	if err != nil {
		block.mx.Unlock()
		return GenerateErrorE(10020003, err, fileName, block.Mark)
	}

	data := make([]*SimpleQueueMessage, 0)

	errJSONUnmarshal := json.Unmarshal(body, &data)

	if errJSONUnmarshal != nil {
		block.mx.Unlock()
		return GenerateErrorE(10020002, errJSONUnmarshal)
	}

	block.Data = data
	block.IsUnload = false
	if len(block.Data) > 0 {
		block.SaveRv = block.Data[len(block.Data)-1].ID
		block.ChangesRv = block.Data[len(block.Data)-1].ID
	} else {
		block.SaveRv = block.ID
		block.ChangesRv = block.ID
	}

	block.mx.Reduce()

	return nil
}

// Unload - unload block from SimpleQueue
func (block *SimpleQueueBlock) Unload(ctx context.Context, q *SimpleQueue) (isNotSave bool, err *mft.Error) {
	if !block.mxFileSave.TryLock(ctx) {
		return false, GenerateError(10019000)
	}
	defer block.mxFileSave.Unlock()
	if !block.mx.TryLock(ctx) {
		return false, GenerateError(10019001)
	}
	defer block.mx.Unlock()

	if block.ChangesRv != block.SaveRv {
		return true, nil
	}

	block.IsUnload = true
	block.Data = nil

	return false, nil
}

func (block *SimpleQueueBlock) setNewStorage(ctx context.Context, q *SimpleQueue, nextMark string) (err *mft.Error) {
	if !block.mxFileSave.TryLock(ctx) {
		return GenerateError(10022000)
	}
	defer block.mxFileSave.Unlock()
	if !q.mx.TryLock(ctx) {
		return GenerateError(10022001)
	}
	defer q.mx.Unlock()

	if !block.mx.TryLock(ctx) {
		return GenerateError(10022002)
	}
	defer block.mx.Unlock()

	if block.NextMark == block.Mark {
		block.NextMark = nextMark
	} else {
		block.RemoveMarks = append(block.RemoveMarks, block.NextMark)
		block.NextMark = nextMark
	}

	q.ChangesRv = q.IDGenerator.RvGetPart()

	return nil
}

func (block *SimpleQueueBlock) setNeedDelete(ctx context.Context, q *SimpleQueue) (err *mft.Error) {
	if !block.mxFileSave.TryLock(ctx) {
		return GenerateError(10023000)
	}
	defer block.mxFileSave.Unlock()
	if !q.mx.TryLock(ctx) {
		return GenerateError(10023001)
	}
	defer q.mx.Unlock()

	if !block.mx.TryLock(ctx) {
		return GenerateError(10023002)
	}
	defer block.mx.Unlock()

	if !block.NeedDelete {
		block.NeedDelete = true

		q.ChangesRv = q.IDGenerator.RvGetPart()
	}

	return nil
}

// moveToNewStorage - move block from Storage to Storage
func (block *SimpleQueueBlock) moveToNewStorage(ctx context.Context, q *SimpleQueue) (err *mft.Error) {
	if !block.mxFileSave.TryLock(ctx) {
		return GenerateError(10018000)
	}
	defer block.mxFileSave.Unlock()
	if !block.mx.RTryLock(ctx) {
		return GenerateError(10018001)
	}

	if block.NextMark == block.Mark {
		block.mx.RUnlock()
		return nil
	}
	if block.IsUnload {
		// block should be load
		block.mx.RUnlock()
		return nil
	}
	fileName := block.blockFileName()

	changesRv := block.ChangesRv
	data, errMarshal := json.MarshalIndent(block.Data, "", "\t")
	chLen := len(block.SaveWait)

	if errMarshal != nil {
		block.mx.RUnlock()
		return GenerateErrorE(10018002, errMarshal)
	}

	st, err := q.getStorageLock(ctx, block.NextMark)

	if err != nil {
		block.mx.RUnlock()
		return err
	}

	err = st.Save(ctx, fileName, data)
	if err != nil {
		block.mx.RUnlock()
		return GenerateErrorE(10018003, err, fileName)
	}

	if !q.mx.TryLock(ctx) {
		block.mx.RUnlock()
		return GenerateError(10018004)
	}

	if !block.mx.TryPromoteF(ctx) {
		q.mx.Unlock()
		return GenerateError(10018005)
	}
	block.SaveRv = changesRv
	block.RemoveMarks = append(block.RemoveMarks, block.Mark)
	block.Mark = block.NextMark

	if len(block.SaveWait) > 0 && chLen > 0 {
		saveWait := make([]chan bool, 0)
		for i := chLen; i < len(block.SaveWait); i++ {
			saveWait = append(saveWait, block.SaveWait[i])
		}
		approve := block.SaveWait[0:chLen]

		block.SaveWait = saveWait
		go func() {
			for _, v := range approve {
				v <- true
			}
		}()
	}
	q.ChangesRv = q.IDGenerator.RvGetPart()

	block.mx.Unlock()
	q.mx.Unlock()

	return nil
}

// clearOldStorage - delete block from Storage in RemoveMarks
func (block *SimpleQueueBlock) clearOldStorage(ctx context.Context, q *SimpleQueue) (notNeed bool, err *mft.Error) {

	if !block.mxFileSave.TryLock(ctx) {
		return false, GenerateError(10017000)
	}
	defer block.mxFileSave.Unlock()

	if len(block.RemoveMarks) == 0 {
		return true, nil
	}

	fileName := block.blockFileName()

	for _, stName := range block.RemoveMarks {
		if block.Mark == stName {
			continue
		}
		st, err := q.getStorageLock(ctx, stName)
		if err != nil {
			return false, err
		}

		err = storage.DeleteIfExists(ctx, st, fileName)
		if err != nil {
			return false, err
		}
	}

	if !q.mx.TryLock(ctx) {
		return false, GenerateError(10017001)
	}

	block.RemoveMarks = make([]string, 0)

	q.ChangesRv = q.IDGenerator.RvGetPart()

	q.mx.Unlock()

	return false, nil
}

// delete - delete block
func (block *SimpleQueueBlock) delete(ctx context.Context, q *SimpleQueue) (err *mft.Error) {

	if !block.mxFileSave.TryLock(ctx) {
		return GenerateError(10015000)
	}
	defer block.mxFileSave.Unlock()

	if !block.mx.RTryLock(ctx) {
		return GenerateError(10015001)
	}

	if !block.NeedDelete {
		block.mx.RUnlock()
		return nil
	}

	fileName := block.blockFileName()

	if !block.mx.TryPromote(ctx) {
		return GenerateError(10015002)
	}

	if block.NextMark != block.Mark {
		st, err := q.getStorageLock(ctx, block.NextMark)

		if err != nil {
			block.mx.Unlock()
			return err
		}

		err = storage.DeleteIfExists(ctx, st, fileName)
		if err != nil {
			block.mx.Unlock()
			return err
		}
	}

	for _, stName := range block.RemoveMarks {
		st, err := q.getStorageLock(ctx, stName)
		if err != nil {
			block.mx.Unlock()
			return err
		}

		err = storage.DeleteIfExists(ctx, st, fileName)
		if err != nil {
			block.mx.Unlock()
			return err
		}
	}

	{
		st, err := q.getStorageLock(ctx, block.Mark)

		if err != nil {
			block.mx.Unlock()
			return err
		}

		err = storage.DeleteIfExists(ctx, st, fileName)
		if err != nil {
			block.mx.Unlock()
			return err
		}
	}
	block.mx.Unlock()

	if !q.mx.TryLock(ctx) {
		return GenerateError(10015003)
	}

	idx := sort.Search(len(q.Blocks), func(i int) bool {
		return q.Blocks[i].ID >= block.ID
	})
	if q.Blocks[idx].ID == block.ID {
		newBlocks := make([]*SimpleQueueBlock, 0, len(q.Blocks)-1)
		for i, b := range q.Blocks {
			if i != idx {
				newBlocks = append(newBlocks, b)
			}
		}
		q.Blocks = newBlocks
		q.ChangesRv = q.IDGenerator.RvGetPart()
	}
	q.mx.Unlock()

	return nil
}

// SetUnload - find and set blocks as unload
func (q *SimpleQueue) SetUnload(ctx context.Context, setBlockUnload func(ctx context.Context, i int, len int, q *SimpleQueue, block *SimpleQueueBlock) (needUnload bool, err *mft.Error)) (err *mft.Error) {

	blocks := make([]*SimpleQueueBlock, 0)

	if !q.mx.RTryLock(ctx) {
		return GenerateError(10021000)
	}

	for i := 0; i < len(q.Blocks); i++ {
		blocks = append(blocks, q.Blocks[i])
	}

	q.mx.RUnlock()

	for i, block := range blocks {
		needUnload, err := setBlockUnload(ctx, i, len(blocks), q, block)
		if err != nil {
			return err
		}
		if needUnload {
			_, err = block.Unload(ctx, q)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// SetMarks - find and set blocks marks
// it needs to save q (q.save(ctx)) after done
func (q *SimpleQueue) SetMarks(ctx context.Context, setBlockMark func(ctx context.Context, i int, len int, q *SimpleQueue, block *SimpleQueueBlock) (needSetMark bool, nextMark string, err *mft.Error)) (err *mft.Error) {

	blocks := make([]*SimpleQueueBlock, 0)

	if !q.mx.RTryLock(ctx) {
		return GenerateError(10024000)
	}

	for i := 0; i < len(q.Blocks); i++ {
		blocks = append(blocks, q.Blocks[i])
	}

	q.mx.RUnlock()

	for i, block := range blocks {
		needSetMark, nextMark, err := setBlockMark(ctx, i, len(blocks), q, block)
		if err != nil {
			return err
		}
		if needSetMark {
			err = block.setNewStorage(ctx, q, nextMark)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// SetDelete - find and set blocks to delete
// it needs to save q (q.save(ctx)) after done
// find first false (dunc returns false) block and all balcks befor false are need to delete
func (q *SimpleQueue) SetDelete(ctx context.Context, setNeedDelete func(ctx context.Context, i int, len int, q *SimpleQueue, block *SimpleQueueBlock) (needDelete bool, err *mft.Error)) (err *mft.Error) {

	blocks := make([]*SimpleQueueBlock, 0)

	if !q.mx.RTryLock(ctx) {
		return GenerateError(10025000)
	}

	for i := 0; i < len(q.Blocks); i++ {
		blocks = append(blocks, q.Blocks[i])
	}

	q.mx.RUnlock()

	blocksToDelete := make([]*SimpleQueueBlock, 0)

	for i, block := range blocks {
		needDelete, err := setNeedDelete(ctx, i, len(blocks), q, block)
		if err != nil {
			return err
		}
		if needDelete {
			blocksToDelete = append(blocksToDelete, block)
		} else {
			break
		}
	}

	for _, block := range blocksToDelete {
		err = block.setNeedDelete(ctx, q)
		if err != nil {
			return err
		}
	}

	return nil
}

// UpdateMarks move from storage to storage and clear OldStorages
// it needs to save q (q.save(ctx)) after done
// blocksCount = 0 - unlimited
func (q *SimpleQueue) UpdateMarks(ctx context.Context, blocksCount int) (err *mft.Error) {
	blocks := make([]*SimpleQueueBlock, 0)

	if !q.mx.RTryLock(ctx) {
		return GenerateError(10026000)
	}

	for i := 0; i < len(q.Blocks); i++ {
		blocks = append(blocks, q.Blocks[i])
	}

	q.mx.RUnlock()

	changes := 0
	for _, block := range blocks {
		if block.NextMark != block.Mark {

			if !block.mx.RTryLock(ctx) {
				return GenerateError(10026000)
			}
			if block.IsUnload {
				err = block.load(ctx, q)
				if err != nil {
					return err
				}
			}
			block.mx.RUnlock()

			err = block.moveToNewStorage(ctx, q)
			if err != nil {
				return err
			}
			_, err = block.clearOldStorage(ctx, q)
			if err != nil {
				return err
			}
			changes++
		} else {
			notNeed, err := block.clearOldStorage(ctx, q)
			if err != nil {
				return err
			}
			if !notNeed {
				changes++
			}
		}

		if changes >= blocksCount && blocksCount > 0 {
			break
		}
	}

	return nil
}

// DeleteBlocks dete blocks (NeedDelete true)
// blocksCount = 0 - unlimited
func (q *SimpleQueue) DeleteBlocks(ctx context.Context, blocksCount int) (err *mft.Error) {
	blocks := make([]*SimpleQueueBlock, 0)

	if !q.mx.RTryLock(ctx) {
		return GenerateError(10026000)
	}

	for i := 0; i < len(q.Blocks); i++ {
		blocks = append(blocks, q.Blocks[i])
	}

	q.mx.RUnlock()

	changes := 0
	for _, block := range blocks {
		if block.NeedDelete {
			err = block.delete(ctx, q)
			if err != nil {
				return err
			}
			changes++
		}

		if changes >= blocksCount && blocksCount > 0 {
			break
		}
	}

	return nil
}

// SetMaxExtID - set max external id
func (q *SimpleQueue) SetMaxExtID(source string, extID int64) {
	q.mxExt.DoGlobal(func() {
		if q.lastExtID == nil {
			q.lastExtID = make(map[string]int64)
		}
		v, ok := q.lastExtID[source]
		if !ok {
			q.lastExtID[source] = extID
		} else if v < extID {
			q.lastExtID[source] = extID
		}
	})
}

// getCurrentMaxExtID - get current saved max external id
func (q *SimpleQueue) getCurrentMaxExtID(source string) (extID int64, ok bool) {
	q.mxExt.DoGlobal(func() {
		if q.lastExtID != nil {
			extID, ok = q.lastExtID[source]
		}
	})

	return extID, ok
}

// searchMaxExtID - search maximum ext id
// need mxExt lock
func (q *SimpleQueue) searchMaxExtID(ctx context.Context, source string, dt time.Time) (extID int64, ok bool, err *mft.Error) {
	blocks := make([]*SimpleQueueBlock, 0)

	if !q.mx.RTryLock(ctx) {
		return extID, false, GenerateError(10027000)
	}

	for i := len(q.Blocks) - 1; i >= 0; i-- {
		blocks = append(blocks, q.Blocks[i])
		if q.Blocks[i].Dt.Unix() < dt.Unix() {
			break
		}
	}

	q.mx.RUnlock()
	maxID := int64(0)
	for _, block := range blocks {
		if !block.mx.RTryLock(ctx) {
			return extID, false, GenerateError(10027001)
		}

		if block.IsUnload {
			err = block.load(ctx, q)
			if err != nil {
				return extID, false, err
			}
		} else {
			block.LastGet = time.Now()
		}

		if len(block.Data) == 0 {
			block.mx.RUnlock()
			continue
		}

		for i := len(block.Data) - 1; i >= 0; i-- {
			if block.Data[i].Source == source {
				if maxID == 0 {
					maxID = block.Data[i].ExternalID
				} else if maxID < block.Data[i].ExternalID {
					maxID = block.Data[i].ExternalID
				}
				block.mx.RUnlock()
			}
		}

		block.mx.RUnlock()
	}
	if maxID != 0 {
		return maxID, true, nil
	}

	return extID, false, nil
}

// searchMaxExtID - search ext id
// need mxExt lock
func (q *SimpleQueue) searchExtID(ctx context.Context, source string, extID int64, dt int64) (id int64, ok bool, err *mft.Error) {
	blocks := make([]*SimpleQueueBlock, 0)

	if !q.mx.RTryLock(ctx) {
		return id, false, GenerateError(10028000)
	}

	for i := len(q.Blocks) - 1; i >= 0; i-- {
		blocks = append(blocks, q.Blocks[i])
		if q.Blocks[i].Dt.Unix() <= dt {
			break
		}
	}

	q.mx.RUnlock()

	for _, block := range blocks {
		if !block.mx.RTryLock(ctx) {
			return id, false, GenerateError(10028001)
		}

		if block.IsUnload {
			err = block.load(ctx, q)
			if err != nil {
				return id, false, err
			}
		} else {
			block.LastGet = time.Now()
		}

		if len(block.Data) == 0 {
			block.mx.RUnlock()
			continue
		}

		for i := len(block.Data) - 1; i >= 0; i-- {
			if block.Data[i].Source == source && block.Data[i].ExternalID == extID {

				id = block.Data[i].ID
				block.mx.RUnlock()

				return id, true, nil
			}
		}

		block.mx.RUnlock()
	}

	return id, false, nil
}

// AddUnique message to queue
// externalDt is unix time
// externalID is source id (should be != 0 !!!!)
func (q *SimpleQueue) AddUnique(ctx context.Context, message []byte, externalID int64, externalDt int64, source string, segment int64, saveMode int) (id int64, err *mft.Error) {
	if externalID == 0 {
		return id, GenerateError(10029000)
	}

	if !q.mxExt.TryLock(ctx, source) {
		return id, GenerateError(10029001)
	}
	defer q.mxExt.Unlock(source)

	extID, ok := q.getCurrentMaxExtID(source)

	if !ok || extID >= externalID {
		id, ok, err = q.searchExtID(ctx, source, externalID, externalDt)
		if err != nil {
			return id, err
		}
		if ok {
			return id, nil
		}
	}

	return q.Add(ctx, message, externalID, externalDt, source, segment, saveMode)

}
func (q *SimpleQueue) AddUniqueList(ctx context.Context, messages []Message, saveMode int) (ids []int64, err *mft.Error) {
	baseSaveMode := SaveMarkSaveMode
	if saveMode == NotSaveSaveMode {
		baseSaveMode = NotSaveSaveMode
	}
	if len(messages) == 0 {
		return make([]int64, 0), nil
	}
	ids = make([]int64, 0, len(messages))
	for i := 0; i < len(messages)-1; i++ {
		id, err := q.AddUnique(ctx, messages[i].Message,
			messages[i].ExternalID,
			messages[i].ExternalDt,
			messages[i].Source,
			messages[i].Segment,
			baseSaveMode,
		)
		if err != nil {
			return ids, err
		}

		ids = append(ids, id)
	}

	i := len(messages) - 1
	id, err := q.AddUnique(ctx, messages[i].Message,
		messages[i].ExternalID,
		messages[i].ExternalDt,
		messages[i].Source,
		messages[i].Segment,
		saveMode,
	)
	if err != nil {
		return ids, err
	}

	ids = append(ids, id)
	return ids, nil
}

// SaveSubscribers save subscribers info of queue
// When MetaStorage == nil returns nil
// When SaveRv == ChangesRv do nothing and returns nil
func (q *SimpleQueue) SaveSubscribers(ctx context.Context) (err *mft.Error) {
	if q.SubscriberStorage == nil {
		return nil
	}
	if !q.Subscribers.mxFileSave.TryLock(ctx) {
		return GenerateError(10031000)
	}
	defer q.Subscribers.mxFileSave.Unlock()

	if !q.Subscribers.mx.RTryLock(ctx) {
		return GenerateError(10031001)
	}

	if q.Subscribers.SaveRv == q.Subscribers.ChangesRv {
		q.Subscribers.mx.RUnlock()
		return nil
	}

	changesRv := q.Subscribers.ChangesRv
	data, errMarshal := json.MarshalIndent(q.Subscribers, "", "\t")
	chLen := len(q.Subscribers.SaveWait)

	q.Subscribers.mx.RUnlock()

	if errMarshal != nil {
		return GenerateErrorE(10031002, errMarshal)
	}

	err = q.SubscriberStorage.Save(ctx, SubscribersFileName, data)
	if err != nil {
		return GenerateErrorE(10031003, err, SubscribersFileName)
	}

	if !q.Subscribers.mx.TryLock(ctx) {
		return GenerateError(10031004)
	}
	q.Subscribers.SaveRv = changesRv
	if len(q.Subscribers.SaveWait) > 0 && chLen > 0 {
		saveWait := make([]chan bool, 0)
		for i := chLen; i < len(q.Subscribers.SaveWait); i++ {
			saveWait = append(saveWait, q.Subscribers.SaveWait[i])
		}
		approve := q.Subscribers.SaveWait[0:chLen]

		q.Subscribers.SaveWait = saveWait
		go func() {
			for _, v := range approve {
				v <- true
			}
		}()
	}

	q.Subscribers.mx.Unlock()

	return nil
}

// SubscriberSetLastRead - set last read info
// if id == 0 remove subscribe
func (q *SimpleQueue) SubscriberSetLastRead(ctx context.Context, subscriber string, id int64, saveMode int) (err *mft.Error) {

	if !q.Subscribers.mx.TryLock(ctx) {
		return GenerateError(10032000)
	}

	isChanged := false
	var chWait chan bool
	v, ok := q.Subscribers.SubscribersInfo[subscriber]
	if !ok && id != 0 {
		v = &SimpleQueueSubscriberInfo{
			StartDt: time.Now(),
			LastID:  id,
			LastDt:  time.Now(),
		}
		q.Subscribers.SubscribersInfo[subscriber] = v

		isChanged = true
	} else if v.LastID < id && id != 0 {
		v.LastID = id
		v.LastDt = time.Now()

		isChanged = true
	} else if ok && id == 0 {
		delete(q.Subscribers.SubscribersInfo, subscriber)
	}

	if !isChanged {
		q.Subscribers.mx.Unlock()
		return nil
	}

	if saveMode == SaveWaitSaveMode {
		chWait = make(chan bool, 1)
		q.Subscribers.SaveWait = append(q.Subscribers.SaveWait, chWait)
	}

	if saveMode == SaveMarkSaveMode {
		q.Subscribers.ChangesRv = q.IDGenerator.RvGetPart()
	}

	q.Subscribers.mx.Unlock()

	if saveMode == SaveImmediatelySaveMode {
		err = q.SaveSubscribers(ctx)
		if err != nil {
			return err
		}
		return nil
	}

	if saveMode == SaveWaitSaveMode {
		if chWait != nil {
			select {
			case <-chWait:
			case <-ctx.Done():
				return GenerateError(10032001)
			}
		}
	}

	return nil
}

// SubscriberGetLastRead - get last read info
func (q *SimpleQueue) SubscriberGetLastRead(ctx context.Context, subscriber string) (id int64, err *mft.Error) {

	if !q.Subscribers.mx.RTryLock(ctx) {
		return 0, GenerateError(10033000)
	}
	defer q.Subscribers.mx.RUnlock()

	v, ok := q.Subscribers.SubscribersInfo[subscriber]
	if !ok {
		return 0, nil
	}

	return v.LastID, nil
}

// SubscriberGetLastRead - get last read info
func (q *SimpleQueue) SubscriberAddReplicaMember(ctx context.Context, subscriber string) (err *mft.Error) {

	if !q.Subscribers.mx.TryLock(ctx) {
		return GenerateError(10033100)
	}

	q.Subscribers.ReplicaSubscribers[subscriber] = struct{}{}

	q.Subscribers.mx.Unlock()

	err = q.SaveSubscribers(ctx)

	if err != nil {
		GenerateErrorE(10033101, err)
	}

	return err
}

// SubscriberGetLastRead - get last read info
func (q *SimpleQueue) SubscriberRemoveReplicaMember(ctx context.Context, subscriber string) (err *mft.Error) {

	if !q.Subscribers.mx.TryLock(ctx) {
		return GenerateError(10033200)
	}

	delete(q.Subscribers.ReplicaSubscribers, subscriber)

	q.Subscribers.mx.Unlock()

	err = q.SaveSubscribers(ctx)

	if err != nil {
		GenerateErrorE(10033201, err)
	}

	return err
}

// SubscriberGetLastRead - get last read info
func (q *SimpleQueue) SubscriberGetReplicaCount(ctx context.Context, id int64) (cnt int, err *mft.Error) {

	if !q.Subscribers.mx.RTryLock(ctx) {
		return 0, GenerateError(10033300)
	}
	defer q.Subscribers.mx.RUnlock()

	for k := range q.Subscribers.ReplicaSubscribers {
		v, ok := q.Subscribers.SubscribersInfo[k]
		if ok {
			if v.LastID >= id {
				cnt++
			}
		}
	}

	return cnt, nil
}
