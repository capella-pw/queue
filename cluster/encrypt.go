package cluster

// EncryptData - info for encrypt and decrypt data
type EncryptData struct {
	EncryptAlg string `json:"enc_alg"`
	EncryptKey []byte `json:"enc_key"`
	DecryptAlg string `json:"dec_alg"`
	DecryptKey []byte `json:"dec_key"`
}
