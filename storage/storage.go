package storage

import (
	"context"

	"github.com/myfantasy/mft"
)

// Storage - Save data into storage
type Storage interface {
	// Exists name in storage
	Exists(ctx context.Context, name string) (ok bool, err *mft.Error)
	// Get data from storage
	Get(ctx context.Context, name string) (body []byte, err *mft.Error)
	// Save write data into storage
	Save(ctx context.Context, name string, body []byte) *mft.Error
	// Delete delete data from storage
	Delete(ctx context.Context, name string) *mft.Error
	// Rename rename file from oldName to newName
	Rename(ctx context.Context, oldName string, newName string) *mft.Error
	// Rename make directory
	MkDirIfNotExists(ctx context.Context, name string) *mft.Error
}

// DeleteIfExists delete file if exists
func DeleteIfExists(ctx context.Context, st Storage, name string) (err *mft.Error) {

	ok, err := st.Exists(ctx, name)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	err = st.Delete(ctx, name)
	return err
}
