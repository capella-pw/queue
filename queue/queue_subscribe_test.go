package queue

import (
	"context"
	"testing"

	"github.com/capella-pw/queue/cn"
	"github.com/capella-pw/queue/storage"
)

func TestSubscribeCopy_base(t *testing.T) {
	stor1 := storage.CreateMapSorage()
	q1 := CreateSimpleQueue(5, 0, 0, stor1, nil, nil, nil)

	stor2 := storage.CreateMapSorage()
	q2 := CreateSimpleQueue(5, 0, 0, stor2, nil, nil, nil)

	q1.Source = "55"
	q2.Source = "66"

	copy := SubscribeCopyUnique(q1, q2, nil, nil, cn.SaveMarkSaveMode, cn.SaveMarkSaveMode, "q2_subscr", 3, false, nil)

	ctx := context.Background()

	isEmpty, err := copy(ctx)

	if err != nil {
		t.Errorf("SubscribeCopy return error %v", err)
	}

	if !isEmpty {
		t.Errorf("SubscribeCopy should return empty = true, not %v", isEmpty)
	}

	// Add msgs
	{
		for i := 0; i < 10; i++ {
			_, err := q1.Add(context.Background(), nil, []byte("test text"), int64(i)+1, 0, "", 0, cn.NotSaveSaveMode)
			if err != nil {
				t.Error(err)
			}
		}

		if len(q1.Blocks) != 2 {
			t.Errorf("SimpleQueue.Add should add only 2 blocks not %v", len(q1.Blocks))
		}
	}

	for i := 0; i < 3; i++ {
		isEmpty, err := copy(ctx)

		if err != nil {
			t.Errorf("SubscribeCopy 2 return error %v", err)
		}

		if isEmpty {
			t.Errorf("SubscribeCopy 2 should return empty = false, not %v", isEmpty)
		}
	}

	// Get messages one by one
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
				t.Errorf("SimpleQueue.Get external ids are not equal expect %v != %v actual", extID, msgs[0].ExternalID)
				break
			}

			if msgs[0].IsSaved {
				t.Errorf("SimpleQueue.Get value should be not saved")
				break
			}

			id = msgs[0].ID

			msgs, err = q2.Get(context.Background(), nil, id, 1)
		}

		if err != nil {
			t.Error(err)
		}

		if extID != 9 {
			t.Errorf("SubscribeCopy: extID should be 9, but: %v", extID)
		}
	}

	// copy last row
	{
		isEmpty, err := copy(ctx)

		if err != nil {
			t.Errorf("SubscribeCopy 3 return error %v", err)
		}

		if isEmpty {
			t.Errorf("SubscribeCopy 3 should return empty = false, not %v", isEmpty)
		}
	}

	// Get messages one by one
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
				t.Errorf("SimpleQueue2.Get external ids are not equal expect %v != %v actual", extID, msgs[0].ExternalID)
				break
			}

			if msgs[0].IsSaved {
				t.Errorf("SimpleQueue2.Get value should be not saved")
				break
			}

			id = msgs[0].ID

			msgs, err = q2.Get(context.Background(), nil, id, 1)
		}

		if err != nil {
			t.Error(err)
		}

		if extID != 10 {
			t.Errorf("SubscribeCopy2: extID should be 10, but: %v", extID)
		}
	}

	// copy last row
	{
		isEmpty, err := copy(ctx)

		if err != nil {
			t.Errorf("SubscribeCopy 4 return error %v", err)
		}

		if !isEmpty {
			t.Errorf("SubscribeCopy 4 should return empty = true, not %v", isEmpty)
		}
	}
}
