package queue

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/capella-pw/queue/cn"
	"github.com/capella-pw/queue/storage"
	"github.com/myfantasy/mft"
)

func TestSimpleQueue_NoSave_full(t *testing.T) {
	stor := storage.CreateMapSorage()
	q := CreateSimpleQueue(5, 0, 0, stor, nil, nil, nil)

	// Add msgs
	{
		for i := 0; i < 10; i++ {
			_, err := q.Add(context.Background(), nil, []byte("test text"), int64(i)+1, 0, "", 0, cn.NotSaveSaveMode)
			if err != nil {
				t.Error(err)
			}
		}

		if len(q.Blocks) != 2 {
			t.Errorf("SimpleQueue.Add should add only 2 blocks not %v", len(q.Blocks))
		}
	}

	// Get messages one by one
	{
		id := int64(0)
		msgs, err := q.Get(context.Background(), nil, id, 1)
		extID := int64(0)
		for len(msgs) != 0 {
			extID++
			if err != nil {
				t.Error(err)
				err = nil
				break
			}

			if msgs[0].ExternalID != extID {
				t.Errorf("SimpleQueue.Get external ids are not equal expect %v != %v actual", extID, msgs[0].ExternalID)
				break
			}

			if msgs[0].IsSaved {
				t.Errorf("SimpleQueue.Get value should be not saved")
				break
			}

			id = msgs[0].ID

			msgs, err = q.Get(context.Background(), nil, id, 1)
		}

		if err != nil {
			t.Error(err)
		}
	}

	// save
	{
		if len(q.SaveBlocks) > 0 {
			t.Errorf("SimpleQueue.SaveBlocks should be empty not %v", len(q.SaveBlocks))
		}

		if len(q.SaveWait) > 0 {
			t.Errorf("SimpleQueue.SaveWait should be empty not %v", len(q.SaveWait))
		}
	}

}

func TestSimpleQueue_SaveImmediately_full(t *testing.T) {
	stor := storage.CreateMapSorage()
	q := CreateSimpleQueue(5, 0, 0, stor, nil, nil, nil)

	// Add msgs
	{
		for i := 0; i < 10; i++ {
			_, err := q.Add(context.Background(), nil, []byte("test text"), int64(i)+1, 0, "", 0, cn.SaveImmediatelySaveMode)
			if err != nil {
				t.Error(err)
			}
		}

		if len(q.Blocks) != 2 {
			t.Errorf("SimpleQueue.Add should add only 2 blocks not %v", len(q.Blocks))
		}
	}

	// Get messages one by one
	{
		id := int64(0)
		msgs, err := q.Get(context.Background(), nil, id, 1)
		extID := int64(0)
		for len(msgs) != 0 {
			extID++
			if err != nil {
				t.Error(err)
				err = nil
				break
			}

			if msgs[0].ExternalID != extID {
				t.Errorf("SimpleQueue.Get external ids are not equal expect %v != %v actual", extID, msgs[0].ExternalID)
				break
			}

			if !msgs[0].IsSaved {
				t.Errorf("SimpleQueue.Get value should be saved")
				break
			}

			id = msgs[0].ID

			msgs, err = q.Get(context.Background(), nil, id, 1)
		}

		if err != nil {
			t.Error(err)
		}
	}

	// save
	{
		if len(q.SaveBlocks) > 0 {
			t.Errorf("SimpleQueue.SaveBlocks should be empty not %v", len(q.SaveBlocks))
		}

		if len(q.SaveWait) > 0 {
			t.Errorf("SimpleQueue.SaveWait should be empty not %v", len(q.SaveWait))
		}

		_, err := stor.Get(context.Background(), MetaDataFileName)
		if err != nil {
			t.Error(err)
		}

	}

}

func TestSimpleQueue_SaveMarkSaveMode_full(t *testing.T) {
	stor := storage.CreateMapSorage()
	q := CreateSimpleQueue(5, 0, 0, stor, nil, nil, nil)

	// Add msgs
	{
		for i := 0; i < 10; i++ {
			_, err := q.Add(context.Background(), nil, []byte("test text"), int64(i)+1, 0, "", 0, cn.SaveMarkSaveMode)
			if err != nil {
				t.Error(err)
			}
		}

		if len(q.Blocks) != 2 {
			t.Errorf("SimpleQueue.Add should add only 2 blocks not %v", len(q.Blocks))
		}
	}

	// Get messages one by one
	{
		id := int64(0)
		msgs, err := q.Get(context.Background(), nil, id, 1)
		extID := int64(0)
		for len(msgs) != 0 {
			extID++
			if err != nil {
				t.Error(err)
				err = nil
				break
			}

			if msgs[0].ExternalID != extID {
				t.Errorf("SimpleQueue.Get external ids are not equal expect %v != %v actual", extID, msgs[0].ExternalID)
				break
			}

			id = msgs[0].ID

			msgs, err = q.Get(context.Background(), nil, id, 1)
		}

		if err != nil {
			t.Error(err)
		}
	}

	// save
	{
		if len(q.SaveBlocks) != 2 {
			t.Errorf("SimpleQueue.SaveBlocks should be 2 not %v", len(q.SaveBlocks))
		}

		if len(q.SaveWait) > 0 {
			t.Errorf("SimpleQueue.SaveWait should be empty not %v", len(q.SaveWait))
		}

		ok, err := stor.Exists(context.Background(), MetaDataFileName)
		if err != nil {
			t.Error(err)
		}
		if ok {
			t.Errorf("SimpleQueue (cn.SaveMarkSaveMode) torage should be empty")
		}

		err = q.SaveAll(context.Background(), nil)
		if err != nil {
			t.Error(err)
		}

		if len(q.SaveBlocks) > 0 {
			t.Errorf("SimpleQueue.SaveBlocks should be empty not %v", len(q.SaveBlocks))
		}

		_, err = stor.Get(context.Background(), MetaDataFileName)
		if err != nil {
			t.Error(err)
		}

	}

}

func TestSimpleQueue_SaveWaitSaveMode_full(t *testing.T) {
	stor := storage.CreateMapSorage()
	q := CreateSimpleQueue(5, 0, 0, stor, nil, nil, nil)

	waitMsgs := 0
	// Add msgs
	{
		var mx sync.Mutex

		var mxDeg sync.Mutex

		for i := 0; i < 10; i++ {
			iIx := i
			// Lets order messages
			time.Sleep(50 * time.Microsecond)
			mx.Lock()
			go func() {
				waitMsgs++
				mx.Unlock()
				_, err := q.Add(context.Background(), nil, []byte("test text"), int64(iIx)+1, 0, "", 0, cn.SaveWaitSaveMode)
				if err != nil {
					t.Error(err)
				}
				mxDeg.Lock()
				waitMsgs--
				mxDeg.Unlock()
			}()
		}

		time.Sleep(10 * time.Millisecond)

		if len(q.Blocks) != 2 {
			t.Errorf("SimpleQueue.Add should add only 2 blocks not %v", len(q.Blocks))
		}
	}

	// Get messages one by one
	{
		id := int64(0)
		msgs, err := q.Get(context.Background(), nil, id, 1)
		extID := int64(0)
		for len(msgs) != 0 {
			extID++
			if err != nil {
				t.Error(err)
				err = nil
				break
			}

			if msgs[0].ExternalID != extID {
				t.Errorf("SimpleQueue.Get external ids are not equal expect %v != %v actual", extID, msgs[0].ExternalID)
				break
			}

			id = msgs[0].ID

			msgs, err = q.Get(context.Background(), nil, id, 1)
		}

		if err != nil {
			t.Error(err)
		}
	}

	// save
	{
		if waitMsgs != 10 {
			t.Errorf("SimpleQueue waitMsgs should be 10 not %v", waitMsgs)
		}

		if len(q.SaveBlocks) != 2 {
			t.Errorf("SimpleQueue.SaveBlocks should be 2 not %v", len(q.SaveBlocks))
		}

		if len(q.Blocks[0].SaveWait) != 5 {
			t.Errorf("SimpleQueue.Blocks[0].SaveWait should be 5 not %v", len(q.Blocks[0].SaveWait))
		}
		if len(q.Blocks[1].SaveWait) != 5 {
			t.Errorf("SimpleQueue.Blocks[1].SaveWait should be 5 not %v", len(q.Blocks[1].SaveWait))
		}

		if len(q.SaveWait) != 2 {
			t.Errorf("SimpleQueue.SaveWait should be 2 not %v", len(q.SaveWait))
		}

		ok, err := stor.Exists(context.Background(), MetaDataFileName)
		if err != nil {
			t.Error(err)
		}
		if ok {
			t.Errorf("SimpleQueue (cn.SaveMarkSaveMode) torage should be empty")
		}

		err = q.SaveAll(context.Background(), nil)
		if err != nil {
			t.Error(err)
		}

		time.Sleep(10 * time.Millisecond)

		if len(q.Blocks[0].SaveWait) != 0 {
			t.Errorf("SimpleQueue.Blocks[0].SaveWait should be empty not %v", len(q.Blocks[0].SaveWait))
		}
		if len(q.Blocks[1].SaveWait) != 0 {
			t.Errorf("SimpleQueue.Blocks[1].SaveWait should be empty not %v", len(q.Blocks[1].SaveWait))
		}

		if waitMsgs != 0 {
			t.Errorf("SimpleQueue waitMsgs after save should be 0 not %v", waitMsgs)
		}

		if len(q.SaveBlocks) > 0 {
			t.Errorf("SimpleQueue.SaveBlocks should be empty not %v", len(q.SaveBlocks))
		}

		_, err = stor.Get(context.Background(), MetaDataFileName)
		if err != nil {
			t.Error(err)
		}

	}

}

func TestSimpleQueue_SaveMarkSaveMode_Size(t *testing.T) {
	stor := storage.CreateMapSorage()
	q := CreateSimpleQueue(500, 0, 0, stor, nil, nil, nil)

	// Add msgs
	{
		for i := 0; i < 10000; i++ {
			_, err := q.Add(context.Background(), nil, []byte("test text"), int64(i)+1, 0, "", 0, cn.SaveMarkSaveMode)
			if err != nil {
				t.Error(err)
			}
		}
	}

	// save
	{
		err := q.SaveAll(context.Background(), nil)
		if err != nil {
			t.Error(err)
		}

		body, err := stor.Get(context.Background(), MetaDataFileName)
		if err != nil {
			t.Error(err)
		}

		if len(string(body)) > 100 {

		}
	}

}

func TestSimpleQueue_SaveMarkSaveMode_unload_and_load(t *testing.T) {

	stor := storage.CreateMapSorage()
	q := CreateSimpleQueue(5, 0, 0, stor, nil, nil, nil)

	// Add msgs
	{
		for i := 0; i < 10; i++ {
			_, err := q.Add(context.Background(), nil, []byte("test text"), int64(i)+1, 0, "", 0, cn.SaveMarkSaveMode)
			if err != nil {
				t.Error(err)
			}
		}

		if len(q.Blocks) != 2 {
			t.Errorf("SimpleQueue.Add should add only 2 blocks not %v", len(q.Blocks))
		}
	}

	// Get messages one by one
	{
		id := int64(0)
		msgs, err := q.Get(context.Background(), nil, id, 1)
		extID := int64(0)
		for len(msgs) != 0 {
			extID++
			if err != nil {
				t.Error(err)
				err = nil
				break
			}

			if msgs[0].ExternalID != extID {
				t.Errorf("SimpleQueue.Get external ids are not equal expect %v != %v actual", extID, msgs[0].ExternalID)
				break
			}

			id = msgs[0].ID

			msgs, err = q.Get(context.Background(), nil, id, 1)
		}

		if err != nil {
			t.Error(err)
		}
	}

	// unload part 1
	{
		isNotSave, err := q.Blocks[0].Unload(context.Background(), q)
		if err != nil {
			t.Error(err)
		}
		if !isNotSave {
			t.Errorf("SimpleQueue.Blocks[0].unloadBlock should not do expect isNotSave == false but actual isNotSave = %v", isNotSave)
		}
	}

	// save
	{
		err := q.SaveAll(context.Background(), nil)
		if err != nil {
			t.Error(err)
		}

	}

	// unload part 2
	{
		isNotSave, err := q.Blocks[0].Unload(context.Background(), q)
		if err != nil {
			t.Error(err)
		}
		if isNotSave {
			t.Errorf("SimpleQueue.Blocks[0].Unload should do expect isNotSave == true but actual isNotSave = %v", isNotSave)
		}

		if len(q.Blocks[0].Data) > 0 {
			t.Errorf("SimpleQueue.Blocks[0].Unload should be empty but len is %v", len(q.Blocks[0].Data))
		}

	}

	// Get messages one by one
	{
		id := int64(0)
		msgs, err := q.Get(context.Background(), nil, id, 1)
		extID := int64(0)
		for len(msgs) != 0 {
			extID++
			if err != nil {
				t.Error(err)
				err = nil
				break
			}

			if msgs[0].ExternalID != extID {
				t.Errorf("SimpleQueue.Get external ids are not equal expect %v != %v actual", extID, msgs[0].ExternalID)
				break
			}

			id = msgs[0].ID

			msgs, err = q.Get(context.Background(), nil, id, 1)
		}

		if err != nil {
			t.Error(err)
		}

		if len(q.Blocks[0].Data) != 5 {
			t.Errorf("SimpleQueue.Blocks[0].load should be len = 5 but len is %v", len(q.Blocks[0].Data))
		}
	}

}

func TestSimpleQueue_SaveMarkSaveMode_delete(t *testing.T) {

	stor := storage.CreateMapSorage()
	q := CreateSimpleQueue(5, 0, 0, stor, nil, nil, nil)

	// Add msgs
	{
		for i := 0; i < 100; i++ {
			_, err := q.Add(context.Background(), nil, []byte("test text"), int64(i)+1, 0, "", 0, cn.SaveMarkSaveMode)
			if err != nil {
				t.Error(err)
			}
		}

		if len(q.Blocks) != 20 {
			t.Errorf("SimpleQueue.Add should add only 20 blocks not %v", len(q.Blocks))
		}
	}

	// Get messages one by one
	{
		id := int64(0)
		msgs, err := q.Get(context.Background(), nil, id, 1)
		extID := int64(0)
		for len(msgs) != 0 {
			extID++
			if err != nil {
				t.Error(err)
				err = nil
				break
			}

			if msgs[0].ExternalID != extID {
				t.Errorf("SimpleQueue.Get external ids are not equal expect %v != %v actual", extID, msgs[0].ExternalID)
				break
			}

			id = msgs[0].ID

			msgs, err = q.Get(context.Background(), nil, id, 1)
		}

		if err != nil {
			t.Error(err)
		}
	}

	// save
	{
		err := q.SaveAll(context.Background(), nil)
		if err != nil {
			t.Error(err)
		}

	}

	// delete first 3 blocks
	{
		err := q.SetDelete(context.Background(), nil, func(ctx context.Context, i, len int, q *SimpleQueue, block *SimpleQueueBlock) (needDelete bool, err *mft.Error) {
			if i < 3 || i == 5 {
				return true, nil
			}
			return false, nil
		})
		if err != nil {
			t.Error(err)
		}

		if q.Blocks[5].NeedDelete {
			t.Errorf("SimpleQueue.Blocks[5].NeedDelete should be false, but value is %v", q.Blocks[5].NeedDelete)
		}

		if !q.Blocks[2].NeedDelete {
			t.Errorf("SimpleQueue.Blocks[2].NeedDelete should be true, but value is %v", q.Blocks[2].NeedDelete)
		}

		err = q.DeleteBlocks(context.Background(), nil, 2)
		if err != nil {
			t.Error(err)
		}

		if !q.Blocks[0].NeedDelete {
			t.Errorf("SimpleQueue.Blocks[0].NeedDelete after delete should be true, but value is %v", q.Blocks[0].NeedDelete)
		}
		if q.Blocks[1].NeedDelete {
			t.Errorf("SimpleQueue.Blocks[1].NeedDelete after delete should be false, but value is %v", q.Blocks[1].NeedDelete)
		}

		if q.Blocks[0].Data[0].ExternalID != 11 {
			t.Errorf("SimpleQueue.Blocks[0].Data[0].ExternalID after delete should be 11, but value is %v", q.Blocks[0].Data[0].ExternalID)
		}

		id := int64(0)
		msgs, err := q.Get(context.Background(), nil, id, 1)
		if err != nil {
			t.Error(err)
		}

		if msgs[0].ExternalID != 16 {
			t.Errorf("SimpleQueue.Get()[0].ExternalID after delete should be 16, but value is %v", msgs[0].ExternalID)
		}

		err = q.DeleteBlocks(context.Background(), nil, 0)
		if err != nil {
			t.Error(err)
		}

		if q.Blocks[0].Data[0].ExternalID != 16 {
			t.Errorf("SimpleQueue.Blocks[0].Data[0].ExternalID after delete 2 should be 16, but value is %v", q.Blocks[0].Data[0].ExternalID)
		}
	}

}

func TestSimpleQueue_SaveMarkSaveMode_moveStorage(t *testing.T) {

	stor := storage.CreateMapSorage()
	storA := storage.CreateMapSorage()
	storB := storage.CreateMapSorage()
	q := CreateSimpleQueue(5, 0, 0, stor, nil, map[string]storage.Storage{"a": storA, "b": storB}, nil)

	// Add msgs
	{
		for i := 0; i < 100; i++ {
			_, err := q.Add(context.Background(), nil, []byte("test text"), int64(i)+1, 0, "", 0, cn.SaveMarkSaveMode)
			if err != nil {
				t.Error(err)
			}
		}

		if len(q.Blocks) != 20 {
			t.Errorf("SimpleQueue.Add should add only 20 blocks not %v", len(q.Blocks))
		}
	}

	// Get messages one by one
	{
		id := int64(0)
		msgs, err := q.Get(context.Background(), nil, id, 1)
		extID := int64(0)
		for len(msgs) != 0 {
			extID++
			if err != nil {
				t.Error(err)
				err = nil
				break
			}

			if msgs[0].ExternalID != extID {
				t.Errorf("SimpleQueue.Get external ids are not equal expect %v != %v actual", extID, msgs[0].ExternalID)
				break
			}

			id = msgs[0].ID

			msgs, err = q.Get(context.Background(), nil, id, 1)
		}

		if err != nil {
			t.Error(err)
		}
	}

	// save
	{
		err := q.SaveAll(context.Background(), nil)
		if err != nil {
			t.Error(err)
		}

		for _, block := range q.Blocks {
			fname := block.blockFileName()
			ok, err := stor.Exists(context.Background(), fname)
			if err != nil {
				t.Error(err)
			}
			if !ok {
				t.Errorf("SimpleQueue.Blocks should be saved in storage, but it not exists file name is %v", fname)
			}
		}

	}

	// move
	{
		st, err := q.getStorageLock(context.Background(), "a")
		if err != nil {
			t.Error(err)
		}
		if st != storA {
			t.Errorf("SimpleQueue.getStorageLock(\"a\") should be equal storA")
		}

		err = q.SetMarks(context.Background(), nil, func(ctx context.Context, i, len int, q *SimpleQueue, block *SimpleQueueBlock) (needSetMark bool, nextMark string, err *mft.Error) {
			if i < 3 {
				return true, "a", nil
			}
			if i < 6 {
				return true, "a", nil
			}
			return false, "", nil
		})
		if err != nil {
			t.Error(err)
		}

		if q.Blocks[0].NextMark != "a" {
			t.Errorf("SimpleQueue.Blocks[0].NextMark should be \"a\", but value is \"%v\"", q.Blocks[0].NextMark)
		}

		if q.Blocks[0].Mark != "" {
			t.Errorf("SimpleQueue.Blocks[0].Mark should be \"\", but value is \"%v\"", q.Blocks[0].Mark)
		}

		err = q.UpdateMarks(context.Background(), nil, 2)
		if err != nil {
			t.Error(err)
		}

		{
			fname := q.Blocks[0].blockFileName()
			ok, err := stor.Exists(context.Background(), fname)
			if err != nil {
				t.Error(err)
			}
			if ok {
				t.Errorf("SimpleQueue.Blocks[0] should not be saved in storage, but it exists file name is %v", fname)
			}

			ok, err = storA.Exists(context.Background(), fname)
			if err != nil {
				t.Error(err)
			}
			if !ok {
				t.Errorf("SimpleQueue.Blocks[0] should be saved in storA, but it not exists file name is %v", fname)
			}
		}

		if q.Blocks[0].NextMark != "a" {
			t.Errorf("SimpleQueue.Blocks[0].NextMark after update should be \"a\", but value is \"%v\"", q.Blocks[0].NextMark)
		}

		if q.Blocks[0].Mark != "a" {
			t.Errorf("SimpleQueue.Blocks[0].Mark after update should be \"a\", but value is \"%v\"", q.Blocks[0].Mark)
		}

		if q.Blocks[2].NextMark != "a" {
			t.Errorf("SimpleQueue.Blocks[2].NextMark should be \"a\", but value is \"%v\"", q.Blocks[2].NextMark)
		}

		if q.Blocks[2].Mark != "" {
			t.Errorf("SimpleQueue.Blocks[2].Mark should be \"\", but value is \"%v\"", q.Blocks[2].Mark)
		}

		err = q.SetUnload(context.Background(), nil, func(ctx context.Context, i, len int, q *SimpleQueue, block *SimpleQueueBlock) (needUnload bool, err *mft.Error) {
			return true, nil
		})
		if err != nil {
			t.Error(err)
		}

		err = q.UpdateMarks(context.Background(), nil, 2)
		if err != nil {
			t.Error(err)
		}

		if q.Blocks[2].NextMark != "a" {
			t.Errorf("SimpleQueue.Blocks[2].NextMark after update should be \"a\", but value is \"%v\"", q.Blocks[2].NextMark)
		}

		if q.Blocks[2].Mark != "a" {
			t.Errorf("SimpleQueue.Blocks[2].Mark after update should be \"a\", but value is \"%v\"", q.Blocks[2].Mark)
		}

		if q.Blocks[2].IsUnload {
			t.Errorf("SimpleQueue.Blocks[2].IsUnload after update should be false, but value is %v", q.Blocks[2].IsUnload)
		}

		if !q.Blocks[0].IsUnload {
			t.Errorf("SimpleQueue.Blocks[0].IsUnload after update 2 should be true, but value is %v", q.Blocks[0].IsUnload)
		}
	}

	// Get messages one by one step 2
	{
		id := int64(0)
		msgs, err := q.Get(context.Background(), nil, id, 1)
		extID := int64(0)
		for len(msgs) != 0 {
			extID++
			if err != nil {
				t.Error(err)
				err = nil
				break
			}

			if msgs[0].ExternalID != extID {
				t.Errorf("SimpleQueue.Get external ids are not equal expect %v != %v actual", extID, msgs[0].ExternalID)
				break
			}

			id = msgs[0].ID

			msgs, err = q.Get(context.Background(), nil, id, 1)
		}

		if err != nil {
			t.Error(err)
		}
	}

	// move step 2
	{
		err := q.UpdateMarks(context.Background(), nil, 0)
		if err != nil {
			t.Error(err)
		}

		if q.Blocks[5].NextMark != "a" {
			t.Errorf("SimpleQueue.Blocks[2].NextMark after update should be \"a\", but value is \"%v\"", q.Blocks[5].NextMark)
		}

		if q.Blocks[5].Mark != "a" {
			t.Errorf("SimpleQueue.Blocks[2].Mark after update should be \"a\", but value is \"%v\"", q.Blocks[5].Mark)
		}

	}

}

func TestSimpleQueue_SaveMarkSaveMode_saveUnique(t *testing.T) {

	stor := storage.CreateMapSorage()
	q := CreateSimpleQueue(5, 0, 0, stor, nil, nil, nil)

	// Add msgs
	{
		for i := 0; i < 100; i++ {
			source := "A"
			if i%3 == 1 {
				source = "B"
			}
			_, err := q.AddUnique(context.Background(), nil, []byte("test text"), int64(i)+1, 0, source, 0, cn.SaveMarkSaveMode)
			if err != nil {
				t.Error(err)
			}
		}

		if len(q.Blocks) != 20 {
			t.Errorf("SimpleQueue.Add should add only 20 blocks not %v", len(q.Blocks))
		}

		if q.lastExtID["B"] != 98 {
			t.Errorf("SimpleQueue.lastExtID[A] should be 98 not %v", q.lastExtID["B"])
		}
	}

	// Get messages one by one
	{
		id := int64(0)
		msgs, err := q.Get(context.Background(), nil, id, 1)
		extID := int64(0)
		for len(msgs) != 0 {
			extID++
			if err != nil {
				t.Error(err)
				err = nil
				break
			}

			if msgs[0].ExternalID != extID {
				t.Errorf("SimpleQueue.Get external ids are not equal expect %v != %v actual", extID, msgs[0].ExternalID)
				break
			}

			id = msgs[0].ID

			msgs, err = q.Get(context.Background(), nil, id, 1)
		}

		if err != nil {
			t.Error(err)
		}
	}

	// Add msgs
	{
		for i := 0; i < 100; i++ {
			source := "A"
			_, err := q.AddUnique(context.Background(), nil, []byte("test text"), int64(i)+1, 0, source, 0, cn.SaveMarkSaveMode)
			if err != nil {
				t.Error(err)
			}
		}

		if len(q.Blocks) != 27 {
			t.Errorf("SimpleQueue.Add should add only 27 blocks not %v", len(q.Blocks))
		}

		if q.lastExtID["A"] != 100 {
			t.Errorf("SimpleQueue.lastExtID[A] should be 100 not %v", q.lastExtID["A"])
		}
	}

}

func TestSimpleQueue_SaveMarkSaveMode_load(t *testing.T) {
	stor := storage.CreateMapSorage()
	q := CreateSimpleQueue(5, 0, 0, stor, nil, nil, nil)

	// Add msgs
	{
		for i := 0; i < 10; i++ {
			_, err := q.Add(context.Background(), nil, []byte("test text"), int64(i)+1, 0, "", 0, cn.SaveMarkSaveMode)
			if err != nil {
				t.Error(err)
			}
		}

		if len(q.Blocks) != 2 {
			t.Errorf("SimpleQueue.Add should add only 2 blocks not %v", len(q.Blocks))
		}
	}

	// Get messages one by one
	{
		id := int64(0)
		msgs, err := q.Get(context.Background(), nil, id, 1)
		extID := int64(0)
		for len(msgs) != 0 {
			extID++
			if err != nil {
				t.Error(err)
				err = nil
				break
			}

			if msgs[0].ExternalID != extID {
				t.Errorf("SimpleQueue.Get external ids are not equal expect %v != %v actual", extID, msgs[0].ExternalID)
				break
			}

			id = msgs[0].ID

			msgs, err = q.Get(context.Background(), nil, id, 1)
		}

		if err != nil {
			t.Error(err)
		}
	}

	// save
	{
		err := q.SaveAll(context.Background(), nil)
		if err != nil {
			t.Error(err)
		}

	}

	// load
	q2, err := LoadSimpleQueue(context.Background(), stor, nil, nil, nil)
	if err != nil {
		t.Error(err)
	}

	// Get messages one by one q2
	{
		id := int64(0)
		msgs, err := q2.Get(context.Background(), nil, id, 1)
		extID := int64(0)
		for len(msgs) != 0 {
			extID++
			if err != nil {
				t.Error(err)
				err = nil
				break
			}

			if msgs[0].ExternalID != extID {
				t.Errorf("SimpleQueue.Get (q2) external ids are not equal expect %v != %v actual", extID, msgs[0].ExternalID)
				break
			}

			id = msgs[0].ID

			msgs, err = q2.Get(context.Background(), nil, id, 1)
		}

		if err != nil {
			t.Error(err)
		}

		if len(q2.Blocks) != 2 {
			t.Errorf("SimpleQueue.Blocks should be 2 blocks (q2) not %v", len(q2.Blocks))
		}
	}

}
