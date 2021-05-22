package compress

import (
	"context"
	"testing"
)

func TestCompressZip(t *testing.T) {
	compressor := GeneratorCreate(7)

	ctx := context.Background()

	msg := "hello world"

	algOut, bodyOut, err := compressor.Compress(ctx, true, Zip, []byte(msg), nil)
	if err != nil {
		t.Error(err)
		return
	}

	_, result, err := compressor.Restore(ctx, algOut, bodyOut, nil)
	if err != nil {
		t.Error(err)
		return
	}

	if string(result) != msg {
		t.Errorf("Message encrypt decrypt error msg: %v result: %v", msg, string(result))
		return
	}
}

func TestCompressAes(t *testing.T) {
	compressor := GeneratorCreate(7)

	ctx := context.Background()

	key := AesKeyGenerate()
	msg := "hello world"

	algOut, bodyOut, err := compressor.Compress(ctx, true, Aes, []byte(msg), key)
	if err != nil {
		t.Error(err)
		return
	}

	_, result, err := compressor.Restore(ctx, algOut, bodyOut, key)
	if err != nil {
		t.Error(err)
		return
	}

	if string(result) != msg {
		t.Errorf("Message encrypt decrypt error msg: %v result: %v", msg, string(result))
		return
	}
}
