package compress

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"

	"github.com/myfantasy/mft"
)

var LimitZipRestore int64 = 1e8

func GZipRestore(ctx context.Context, algorithm string, body []byte, decryptKey []byte) (algorithmUsed string, result []byte, err *mft.Error) {
	buf := bytes.NewBuffer(body)

	zr, er0 := gzip.NewReader(buf)
	if er0 != nil {
		return algorithm, nil, GenerateErrorE(10201000, er0)
	}

	rLimit := io.LimitReader(zr, LimitZipRestore)

	result, er0 = io.ReadAll(rLimit)
	if er0 != nil {
		return algorithm, nil, GenerateErrorE(10201001, er0)
	}

	if len(result) >= int(LimitZipRestore)-1 {
		return algorithm, nil, GenerateError(10201002)
	}

	if er0 = zr.Close(); er0 != nil {
		return algorithm, nil, GenerateErrorE(10201003, er0)
	}

	return algorithm, result, nil
}

func GZipCompressGenerator(level int) CompressFunc {
	return func(ctx context.Context, algorithm string, body []byte, encryptKey []byte) (algorithmUsed string, result []byte, err *mft.Error) {
		return GZipCompress(ctx, level, algorithm, body, encryptKey)
	}
}
func GZipCompress(ctx context.Context, level int, algorithm string, body []byte, encryptKey []byte) (algorithmUsed string, result []byte, err *mft.Error) {
	var buf bytes.Buffer
	zw, er0 := gzip.NewWriterLevel(&buf, level)
	if er0 != nil {
		return algorithm, nil, GenerateErrorE(10201100, er0)
	}

	_, er0 = zw.Write(body)
	if er0 != nil {
		return algorithm, nil, GenerateErrorE(10201101, er0)
	}

	if er0 := zw.Close(); er0 != nil {
		return algorithm, nil, GenerateErrorE(10201102, er0)
	}

	return Zip, buf.Bytes(), nil
}
