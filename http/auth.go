package http

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
)

// BasicAuthName name of basic authentification
const BasicAuthName = "basic"

// AuthMethods - authentication methods
var AuthMethods map[string]func(header string) (ok bool, user User, err error)

// User user info
type User struct {
	Name string
}

// BasicAuthRequest - basic authentification request
type BasicAuthRequest struct {
	Name string `json:"n,omitempty"`
	Pwd  string `json:"p,omitempty"`
}

// BasicCheckMap basic authentification check map
var BasicCheckMap map[string]string

// BasicKeyGenerate generate storaged key
func BasicKeyGenerate(r BasicAuthRequest) string {
	h := sha256.New()
	h.Write([]byte(r.Name + ":" + r.Pwd))
	encoded := base64.StdEncoding.EncodeToString(h.Sum(nil))

	return encoded
}

// HeaderGenerate generate header
func (r *BasicAuthRequest) HeaderGenerate() string {
	b, er0 := json.Marshal(r)
	if er0 != nil {
		panic(er0)
	}

	result := base64.StdEncoding.EncodeToString(b)

	return result
}

// BasicAuthCheckMethod auth method for basic authentification
func BasicAuthCheckMethod(header string) (ok bool, user User, err error) {
	if len(header) == 0 {
		return false, user, nil
	}

	ubytes, er0 := base64.StdEncoding.DecodeString(header)
	if er0 != nil {
		return false, user, er0
	}

	var r BasicAuthRequest
	er1 := json.Unmarshal(ubytes, &r)
	if er1 != nil {
		return false, user, er1
	}

	key := BasicKeyGenerate(r)
	if k, ok := BasicCheckMap[r.Name]; ok && k == key {
		user.Name = r.Name
		return true, user, nil
	}

	return false, user, nil
}
