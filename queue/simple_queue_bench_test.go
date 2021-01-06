package queue

import (
	"context"
	"testing"

	"github.com/capella-pw/queue/storage"
)

func BenchmarkSimpleQueue_SaveMarkSaveMode(b *testing.B) {
	ctx := context.Background()
	stor := storage.CreateMapSorage()
	q := CreateSimpleQueue(5, 0, 0, stor, nil, nil)

	for i := 0; i < b.N; i++ {
		_, err := q.Add(ctx, []byte("test  text"), int64(i)+1, 0, "", SaveMarkSaveMode)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkSimpleQueue_SaveImmediatelySaveMode(b *testing.B) {
	ctx := context.Background()
	stor := storage.CreateMapSorage()
	q := CreateSimpleQueue(5, 0, 0, stor, nil, nil)

	for i := 0; i < b.N; i++ {
		_, err := q.Add(ctx, []byte("test  text"), int64(i)+1, 0, "", SaveImmediatelySaveMode)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkSimpleQueue_SaveMarkSaveMode_and_Save_500(b *testing.B) {
	ctx := context.Background()
	stor := storage.CreateMapSorage()
	q := CreateSimpleQueue(500, 0, 0, stor, nil, nil)

	for i := 0; i < b.N; i++ {
		_, err := q.Add(ctx, []byte("test  text"), int64(i)+1, 0, "", SaveMarkSaveMode)
		if err != nil {
			b.Error(err)
		}
		err = q.SaveAll(ctx)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkSimpleQueue_SaveMarkSaveMode_and_Save_50(b *testing.B) {
	ctx := context.Background()
	stor := storage.CreateMapSorage()
	q := CreateSimpleQueue(50, 0, 0, stor, nil, nil)

	for i := 0; i < b.N; i++ {
		_, err := q.Add(ctx, []byte("test  text"), int64(i)+1, 0, "", SaveMarkSaveMode)
		if err != nil {
			b.Error(err)
		}
		err = q.SaveAll(ctx)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkSimpleQueue_SaveMarkSaveMode_and_Save_50_lage(b *testing.B) {
	ctx := context.Background()
	stor := storage.CreateMapSorage()
	q := CreateSimpleQueue(50, 0, 0, stor, nil, nil)

	t := "test  text"
	for i := 0; i < 10; i++ {
		t += t
	}

	for i := 0; i < b.N; i++ {
		_, err := q.Add(ctx, []byte(t), int64(i)+1, 0, "", SaveMarkSaveMode)
		if err != nil {
			b.Error(err)
		}
		err = q.SaveAll(ctx)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkSimpleQueue_SaveMarkSaveMode_and_Save_5_lage(b *testing.B) {
	ctx := context.Background()
	stor := storage.CreateMapSorage()
	q := CreateSimpleQueue(5, 0, 0, stor, nil, nil)

	t := "test  text"
	for i := 0; i < 10; i++ {
		t += t
	}

	for i := 0; i < b.N; i++ {
		_, err := q.Add(ctx, []byte(t), int64(i)+1, 0, "", SaveMarkSaveMode)
		if err != nil {
			b.Error(err)
		}
		err = q.SaveAll(ctx)
		if err != nil {
			b.Error(err)
		}
	}
}
