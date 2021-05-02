package storage

import (
	"context"

	"github.com/capella-pw/queue/compress"
	"github.com/myfantasy/mft"
)

// DoubleSaveSorage - storage that saves to Map (memory)
type ZipSaveSorage struct {
	storage       Storage
	compressor    *compress.Generator
	alghoritm     string
	fileExtention string
}

// CreateDoubleSaveSorage - creates double_save_storage storange
func CreateZipSaveSorage(storage Storage, compressor *compress.Generator, alghoritm string, fileExtention string) *ZipSaveSorage {
	return &ZipSaveSorage{
		storage:       storage,
		compressor:    compressor,
		alghoritm:     alghoritm,
		fileExtention: fileExtention,
	}
}

func (s *ZipSaveSorage) Path(name string) string {
	return name + s.fileExtention
}

// Exists name in storage
func (s *ZipSaveSorage) Exists(ctx context.Context, name string) (ok bool, err *mft.Error) {
	return s.storage.Exists(ctx, s.Path(name))
}

// Get data from storage
func (s *ZipSaveSorage) Get(ctx context.Context, name string) (body []byte, err *mft.Error) {
	body, err = s.storage.Get(ctx, s.Path(name))

	_, body, err = s.compressor.Restore(ctx, s.alghoritm, body, nil)

	return body, err
}

// Save write data into storage
func (s *ZipSaveSorage) Save(ctx context.Context, name string, body []byte) (err *mft.Error) {
	_, body, err = s.compressor.Compress(ctx, true, s.alghoritm, body, nil)
	if err != nil {
		return err
	}
	return s.storage.Save(ctx, s.Path(name), body)
}

// Delete delete data from storage
func (s *ZipSaveSorage) Delete(ctx context.Context, name string) *mft.Error {
	return s.storage.Delete(ctx, s.Path(name))
}

// Rename rename file from oldName to newName
func (s *ZipSaveSorage) Rename(ctx context.Context, oldName string, newName string) *mft.Error {
	return s.storage.Rename(ctx, s.Path(oldName), s.Path(newName))
}

// MkDirIfNotExists make directory
func (s *ZipSaveSorage) MkDirIfNotExists(ctx context.Context, name string) *mft.Error {
	return s.storage.MkDirIfNotExists(ctx, name)
}
