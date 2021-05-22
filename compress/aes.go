package compress

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"

	"github.com/myfantasy/mft"
)

// AesKeyGenerate generates a random 32 byte (256 bit) key for AES-256
func AesKeyGenerate() []byte {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		panic(err.Error())
	}

	return bytes
}

func AesEncrypt(ctx context.Context, algorithm string, body []byte, encryptKey []byte,
) (algorithmUsed string, result []byte, err *mft.Error) {

	if len(encryptKey) != 32 {
		return algorithm, nil, GenerateError(10202000, len(encryptKey))
	}

	block, er0 := aes.NewCipher(encryptKey)
	if er0 != nil {
		return algorithm, nil, GenerateError(10202001, er0)
	}

	aesGCM, er0 := cipher.NewGCM(block)
	if er0 != nil {
		return algorithm, nil, GenerateError(10202002, er0)
	}

	nonce := make([]byte, aesGCM.NonceSize())
	if _, er0 = io.ReadFull(rand.Reader, nonce); er0 != nil {
		return algorithm, nil, GenerateError(10202003, er0)
	}

	ciphertext := aesGCM.Seal(nonce, nonce, body, nil)

	return algorithm, ciphertext, nil
}

func AesDecrypt(ctx context.Context, algorithm string, body []byte, decryptKey []byte,
) (algorithmUsed string, result []byte, err *mft.Error) {
	if len(decryptKey) != 32 {
		return algorithm, nil, GenerateError(10202100, len(decryptKey))
	}

	block, er0 := aes.NewCipher(decryptKey)
	if er0 != nil {
		return algorithm, nil, GenerateError(10202101, er0)
	}

	aesGCM, er0 := cipher.NewGCM(block)
	if er0 != nil {
		return algorithm, nil, GenerateError(10202102, er0)
	}

	nonceSize := aesGCM.NonceSize()

	nonce, ciphertext := body[:nonceSize], body[nonceSize:]

	plaintext, er0 := aesGCM.Open(nil, nonce, ciphertext, nil)
	if er0 != nil {
		return algorithm, nil, GenerateError(10202103, er0)
	}

	return algorithm, plaintext, nil
}
