package compress

import (
	"context"

	"github.com/myfantasy/mfs"
	"github.com/myfantasy/mft"
)

// Algs
const (
	NoCompression = ""
	Zip           = "gzip"
	Zip1          = "gzip1"
	Zip9          = "gzip9"

	// Aes - AES 256 alg
	Aes = "aes"
)

type CompressFunc func(ctx context.Context, algorithm string, body []byte, encryptKey []byte) (algorithmUsed string, result []byte, err *mft.Error)
type RestoreFunc func(ctx context.Context, algorithm string, body []byte, decryptKey []byte) (algorithmUsed string, result []byte, err *mft.Error)

// Generator - compressor
type Generator struct {
	mx          mfs.PMutex
	compressors map[string]CompressFunc
	restores    map[string]RestoreFunc
}

func (g *Generator) Add(name string, compressor CompressFunc, restore RestoreFunc) {
	g.mx.Lock()
	defer g.mx.Unlock()

	g.compressors[name] = compressor
	g.restores[name] = restore
}

func (g *Generator) Init() {
	g.mx.Lock()
	defer g.mx.Unlock()
	g.compressors = make(map[string]CompressFunc)
	g.restores = make(map[string]RestoreFunc)
}

// GeneratorCreate - generate with default algs
func GeneratorCreate(gzipDefaultLevel int) *Generator {
	g := &Generator{}
	g.Init()

	g.Add(Zip, GZipCompressGenerator(gzipDefaultLevel), GZipRestore)
	g.Add(Zip1, GZipCompressGenerator(1), GZipRestore)
	g.Add(Zip9, GZipCompressGenerator(9), GZipRestore)
	g.Add(Aes, AesEncrypt, AesDecrypt)

	return g
}

func (g *Generator) Compress(ctx context.Context, must bool, algorithm string, body []byte, encryptKey []byte) (algorithmUsed string, result []byte, err *mft.Error) {
	g.mx.RLock()
	defer g.mx.RUnlock()

	if algorithm == "" {
		return "", body, nil
	}

	c, ok := g.compressors[algorithm]
	if !ok || c == nil {
		if len(encryptKey) == 0 && !must {
			return "", body, nil
		}

		return "", nil, GenerateError(10200000, algorithm)
	}

	return c(ctx, algorithm, body, encryptKey)
}
func (g *Generator) Restore(ctx context.Context, algorithm string, body []byte, decryptKey []byte) (algorithmUsed string, result []byte, err *mft.Error) {
	g.mx.RLock()
	defer g.mx.RUnlock()

	if algorithm == "" {
		return "", body, nil
	}

	r, ok := g.restores[algorithm]
	if !ok || r == nil {
		return "", nil, GenerateError(10200001, algorithm)
	}

	return r(ctx, algorithm, body, decryptKey)
}
