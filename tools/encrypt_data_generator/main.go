package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/capella-pw/queue/cluster"
	"github.com/capella-pw/queue/compress"
)

var fConfigEncrypt = flag.String("cfge", "encrypt.json",
	"Sets encrypt config file path")

func main() {

	flag.Parse()

	encryptData := cluster.EncryptData{
		EncryptAlg: compress.Aes,
		DecryptAlg: compress.Aes,
		EncryptKey: compress.AesKeyGenerate(),
	}
	encryptData.DecryptKey = encryptData.EncryptKey

	body, err := json.Marshal(encryptData)
	if err != nil {
		log.Fatalf("Marshal EncryptData fail %v", err)
		os.Exit(1)
	}

	path := filepath.FromSlash(*fConfigEncrypt)
	err = ioutil.WriteFile(path, body, 0600)
	if err != nil {
		log.Fatalf("Write file %v fail %v", path, err)
		os.Exit(1)
	}
}
