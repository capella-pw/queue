package http

import "encoding/json"

func init() {
	BasicCheckMap = make(map[string]string)
	AuthMethods = make(map[string]func(header string) (ok bool, user User, err error))
	Handlers = make(map[string]func(object string, segment int64,
		action string, requestData json.RawMessage,
		okUser bool, user User) (resultData json.RawMessage))
	AuthMethods[BasicAuthName] = BasicAuthCheckMethod

	Handlers["echo"] = Echo
	Handlers["ping"] = Ping

}
