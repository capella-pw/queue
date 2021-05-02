package storage

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/myfantasy/mft"
)

// FileSorage file on disk profider
type FileSorage struct {
	FolderPerm os.FileMode
	FilePerm   os.FileMode
	Folder     string
}

// CreateFileSorageParams params for create file storage
type CreateFileSorageParams struct {
	Folder string `json:"folder"`
}

// CreateFileSorage creates simple FileOnDisk with perms 0760 & 0660
func CreateFileSorage(ctx context.Context, params CreateFileSorageParams) (*FileSorage, *mft.Error) {
	res := &FileSorage{
		FolderPerm: 0760,
		FilePerm:   0660,
		Folder:     params.Folder,
	}

	if len(res.Folder) > 0 && res.Folder[len(res.Folder)-1] == '/' {
		err := res.MkDirIfNotExists(ctx, "")
		if err != nil {
			return nil, err
		}
	}

	return res, nil
}

// Exists name in storage
func (s *FileSorage) Exists(ctx context.Context, name string) (ok bool, err *mft.Error) {
	path := filepath.FromSlash(s.Folder + name)

	_, er0 := os.Stat(path)
	if er0 == nil {
		return true, nil
	} else if os.IsNotExist(er0) {
		return false, nil
	}
	return false, GenerateError(10000002, er0)
}

// Get data from storage
func (s *FileSorage) Get(ctx context.Context, name string) (body []byte, err *mft.Error) {
	path := filepath.FromSlash(s.Folder + name)

	data, er0 := ioutil.ReadFile(path)

	if er0 == nil {
		return data, nil
	}
	return data, GenerateError(10000002, er0)
}

// Save write data into storage
func (s *FileSorage) Save(ctx context.Context, name string, body []byte) *mft.Error {
	path := filepath.FromSlash(s.Folder + name)

	er0 := ioutil.WriteFile(path, body, s.FilePerm)
	if er0 == nil {
		return nil
	}
	return GenerateError(10000002, er0)
}

// Delete delete data from storage
func (s *FileSorage) Delete(ctx context.Context, name string) *mft.Error {
	path := filepath.FromSlash(s.Folder + name)

	er0 := os.RemoveAll(path)
	if er0 == nil {
		return nil
	}
	return GenerateError(10000002, er0)
}

// Rename rename file from oldName to newName
func (s *FileSorage) Rename(ctx context.Context, oldName string, newName string) *mft.Error {
	pathOld := filepath.FromSlash(s.Folder + oldName)
	pathNew := filepath.FromSlash(s.Folder + newName)

	er0 := os.Rename(pathOld, pathNew)
	if er0 == nil {
		return nil
	}
	return GenerateError(10000002, er0)
}

// MkDirIfNotExists make directory
func (s *FileSorage) MkDirIfNotExists(ctx context.Context, name string) *mft.Error {
	path := filepath.FromSlash(s.Folder + name)

	ok, err := s.Exists(ctx, name)
	if err != nil {
		return err
	}
	if !ok {
		er0 := os.MkdirAll(path, s.FolderPerm)
		if er0 != nil {
			return GenerateErrorE(10000003, er0, path)
		}
	}

	return nil
}
