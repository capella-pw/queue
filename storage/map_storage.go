package storage

import (
	"context"
	"sync"

	"github.com/myfantasy/mft"
)

// MapSorage - storage that saves to Map (memory)
type MapSorage struct {
	storage map[string][]byte

	mx sync.RWMutex
}

// CreateMapSorage - creates map storange
func CreateMapSorage() *MapSorage {
	return &MapSorage{
		storage: make(map[string][]byte),
	}
}

// Exists name in storage
func (s *MapSorage) Exists(ctx context.Context, name string) (ok bool, err *mft.Error) {
	s.mx.RLock()
	defer s.mx.RUnlock()

	_, ok = s.storage[name]
	return ok, nil
}

// Get data from storage
func (s *MapSorage) Get(ctx context.Context, name string) (body []byte, err *mft.Error) {
	s.mx.RLock()
	defer s.mx.RUnlock()

	var ok bool
	if body, ok = s.storage[name]; ok {
		return body, nil
	}

	return nil, GenerateError(10000000, name)
}

// Save write data into storage
func (s *MapSorage) Save(ctx context.Context, name string, body []byte) *mft.Error {
	s.mx.Lock()
	defer s.mx.Unlock()

	s.storage[name] = body

	return nil
}

// Delete delete data from storage
func (s *MapSorage) Delete(ctx context.Context, name string) *mft.Error {
	s.mx.Lock()
	defer s.mx.Unlock()

	delete(s.storage, name)

	return nil
}

// Rename rename file from oldName to newName
func (s *MapSorage) Rename(ctx context.Context, oldName string, newName string) *mft.Error {
	s.mx.Lock()
	defer s.mx.Unlock()

	b, ok := s.storage[oldName]
	_, ok2 := s.storage[newName]
	if ok && !ok2 {
		s.storage[newName] = b
		delete(s.storage, oldName)

		return nil
	} else if ok2 {
		return GenerateError(10000001, newName)
	}

	return GenerateError(10000000, oldName)

}
