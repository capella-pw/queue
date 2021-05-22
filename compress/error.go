package compress

import (
	"fmt"

	"github.com/myfantasy/mft"
)

// Errors codes and description
var Errors map[int]string = map[int]string{
	10200000: "Generator.Compress: algorithm %v is not exists and encrypt key is not null",
	10200001: "Generator.Restore: algorithm %v is not exists",

	10201000: "GZipRestore: gzip reader not created with error",
	10201001: "GZipRestore: read gzip error",
	10201002: "GZipRestore: readresult is large",
	10201003: "GZipRestore: close gzip error",

	10201100: "GZipCompress: gzip writer fail on create",
	10201101: "GZipCompress: gzip writer fail on write",
	10201102: "GZipCompress: gzip writer fail on close",

	10202000: "AesEncrypt: input key len %v != 32",
	10202001: "AesEncrypt: NewCipher fail",
	10202002: "AesEncrypt: NewGCM fail",
	10202003: "AesEncrypt: nonce fill fail",

	10202100: "AesDecrypt: input key len %v != 32",
	10202101: "AesDecrypt: NewCipher fail",
	10202102: "AesDecrypt: NewGCM fail",
	10202103: "AesDecrypt: decrypt fail",
}

// GenerateError -
func GenerateError(key int, a ...interface{}) *mft.Error {
	if text, ok := Errors[key]; ok {
		return mft.ErrorCS(key, fmt.Sprintf(text, a...))
	}
	panic(fmt.Sprintf("compress.GenerateError, error not found code:%v", key))
}

// GenerateErrorE -
func GenerateErrorE(key int, err error, a ...interface{}) *mft.Error {
	if text, ok := Errors[key]; ok {
		return mft.ErrorCSE(key, fmt.Sprintf(text, a...), err)
	}
	panic(fmt.Sprintf("compress.GenerateErrorE, error not found code:%v error:%v", key, err))
}
