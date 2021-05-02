package storage

import (
	"context"

	"github.com/capella-pw/queue/compress"
	"github.com/myfantasy/mfs"
	"github.com/myfantasy/mft"
)

// StorageMAPType - storage map type
var StorageMAPType = "map"

// StorageFileType - storage file type (folder := homePath + folder)
var StorageFileType = "file"

// StorageFileDoubleSaveType - storage file type with double (folder := homePath + folder)
var StorageFileDoubleSaveType = "file_dbl_save"

// StorageFileDoubleSaveType - storage file type with double (folder := homePath + folder)
var StorageFileDoubleSaveTypeGZip = "file_dbl_save_gzip"

// Generator - storage cluster
type Generator struct {
	mx            mfs.PMutex
	storGenerator map[string]func(ctx context.Context, params Mount, relativePath string) (Storage, *mft.Error)
	GeneratorInfo GeneratorInfo
}

type GeneratorInfo struct {
	Mounts map[string]Mount `json:"mounts"`
}

type Mount struct {
	ProviderType string `json:"provider"`
	HomePath     string `json:"home_path"`

	CompressAlg   string `json:"compress_alg,omitempty"`
	FileExtention string `json:"file_extention,omitempty"`

	Params map[string]string `json:"params"`
}

// CreateGenerator create storage cluster
func CreateGenerator(generatorInfo GeneratorInfo, compressor *compress.Generator) *Generator {
	res := &Generator{
		GeneratorInfo: generatorInfo,
	}

	res.AddStorGenerator(StorageMAPType, func(ctx context.Context, params Mount, relativePath string) (Storage, *mft.Error) {
		return CreateMapSorage(), nil
	})
	res.AddStorGenerator(StorageFileType, func(ctx context.Context, params Mount, relativePath string) (Storage, *mft.Error) {
		cp := CreateFileSorageParams{}
		cp.Folder = params.HomePath + relativePath

		return CreateFileSorage(ctx, cp)
	})
	res.AddStorGenerator(StorageFileDoubleSaveType, func(ctx context.Context, params Mount, relativePath string) (Storage, *mft.Error) {
		cp := CreateFileSorageParams{}
		cp.Folder = params.HomePath + relativePath

		storage, er := CreateFileSorage(ctx, cp)

		if er != nil {
			return nil, er
		}

		return CreateDoubleSaveSorage(storage), nil
	})
	res.AddStorGenerator(StorageFileDoubleSaveTypeGZip, func(ctx context.Context, params Mount, relativePath string) (Storage, *mft.Error) {
		cp := CreateFileSorageParams{}
		cp.Folder = params.HomePath + relativePath

		storage, er := CreateFileSorage(ctx, cp)

		if er != nil {
			return nil, er
		}

		return CreateDoubleSaveSorage(CreateZipSaveSorage(storage, compressor, params.CompressAlg, params.FileExtention)), nil
	})

	return res
}

// AddStorGenerator add storage generator
func (s *Generator) AddStorGenerator(name string, generator func(ctx context.Context, params Mount, relativePath string) (Storage, *mft.Error)) {
	s.mx.Lock()
	defer s.mx.Unlock()

	if s.storGenerator == nil {
		s.storGenerator = make(map[string]func(ctx context.Context, params Mount, relativePath string) (Storage, *mft.Error))
	}

	s.storGenerator[name] = generator
}

// Create create new storage
func (s *Generator) Create(ctx context.Context, mountName string, relativePath string) (Storage, *mft.Error) {
	if !s.mx.RTryLock(ctx) {
		return nil, GenerateError(10001000)
	}
	defer s.mx.RUnlock()

	mount, ok := s.GeneratorInfo.Mounts[mountName]
	if !ok {
		return nil, GenerateError(10001002, mountName)
	}

	f, ok := s.storGenerator[mount.ProviderType]

	if !ok {
		return nil, GenerateError(10001001, mount.ProviderType)
	}

	return f(ctx, mount, relativePath)
}
