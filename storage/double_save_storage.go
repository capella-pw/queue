package storage

import (
	"context"

	"github.com/myfantasy/mft"
)

// DoubleSaveSorage - storage that saves to Map (memory)
type DoubleSaveSorage struct {
	storage Storage
}

// CreateDoubleSaveSorage - creates double_save_storage storange
func CreateDoubleSaveSorage(storage Storage) *DoubleSaveSorage {
	return &DoubleSaveSorage{
		storage: storage,
	}
}

// Exists name in storage
func (s *DoubleSaveSorage) Exists(ctx context.Context, name string) (ok bool, err *mft.Error) {
	pathOld := name + ".old"
	pathNew := name + ".new"

	ok, err = s.storage.Exists(ctx, name)
	if err != nil {
		return ok, err
	}
	if ok {
		return ok, err
	}

	ok, err = s.storage.Exists(ctx, pathNew)
	if err != nil {
		return ok, err
	}
	if ok {
		return ok, err
	}

	ok, err = s.storage.Exists(ctx, pathOld)
	if err != nil {
		return ok, err
	}
	return ok, err
}

// Get data from storage
func (s *DoubleSaveSorage) Get(ctx context.Context, name string) (body []byte, err *mft.Error) {
	pathOld := name + ".old"
	pathNew := name + ".new"

	ok, err := s.storage.Exists(ctx, name)
	if err != nil {
		return nil, err
	}
	if ok {
		return s.storage.Get(ctx, name)
	}

	ok, err = s.storage.Exists(ctx, pathNew)
	if err != nil {
		return nil, err
	}
	if ok {
		return s.storage.Get(ctx, pathNew)
	}

	ok, err = s.storage.Exists(ctx, pathOld)
	if err != nil {
		return nil, err
	}
	return s.storage.Get(ctx, pathOld)
}

// Save write data into storage
func (s *DoubleSaveSorage) Save(ctx context.Context, name string, body []byte) *mft.Error {
	pathOld := name + ".old"
	pathNew := name + ".new"

	ok, err := s.storage.Exists(ctx, name)
	if err != nil {
		return err
	}
	if !ok {
		err := s.storage.Save(ctx, name, body)
		if err != nil {
			return err
		}

		ok, err := s.storage.Exists(ctx, pathNew)
		if err != nil {
			return nil
		}
		if ok {
			err := s.storage.Delete(ctx, pathNew)
			if err != nil {
				return err
			}
		}

		ok, err = s.storage.Exists(ctx, pathOld)
		if err != nil {
			return nil
		}
		if ok {
			err := s.storage.Delete(ctx, pathOld)
			if err != nil {
				return err
			}
		}
	}

	ok, err = s.storage.Exists(ctx, pathOld)
	if err != nil {
		return nil
	}
	if ok {
		err := s.storage.Delete(ctx, pathOld)
		if err != nil {
			return err
		}
	}

	err = s.storage.Rename(ctx, name, pathOld)
	if err != nil {
		return nil
	}

	ok, err = s.storage.Exists(ctx, pathNew)
	if err != nil {
		return nil
	}
	if ok {
		err := s.storage.Delete(ctx, pathNew)
		if err != nil {
			return err
		}
	}

	err = s.storage.Save(ctx, pathNew, body)
	if err != nil {
		return err
	}

	err = s.storage.Delete(ctx, name)
	if err != nil {
		return err
	}

	err = s.storage.Rename(ctx, pathNew, name)
	if err != nil {
		return nil
	}

	err = s.storage.Delete(ctx, pathNew)
	if err != nil {
		return err
	}

	return nil
}

// Delete delete data from storage
func (s *DoubleSaveSorage) Delete(ctx context.Context, name string) *mft.Error {
	pathOld := name + ".old"
	pathNew := name + ".new"

	ok, err := s.storage.Exists(ctx, pathNew)
	if err != nil {
		return nil
	}
	if ok {
		err := s.storage.Delete(ctx, pathNew)
		if err != nil {
			return err
		}
	}

	ok, err = s.storage.Exists(ctx, pathOld)
	if err != nil {
		return nil
	}
	if ok {
		err := s.storage.Delete(ctx, pathOld)
		if err != nil {
			return err
		}
	}

	ok, err = s.storage.Exists(ctx, name)
	if err != nil {
		return err
	}
	if ok {
		err := s.storage.Delete(ctx, name)
		if err != nil {
			return err
		}
	}

	return nil
}

// Rename rename file from oldName to newName
func (s *DoubleSaveSorage) Rename(ctx context.Context, oldName string, newName string) *mft.Error {
	return s.storage.Rename(ctx, oldName, newName)
}
