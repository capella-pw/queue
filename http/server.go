package http

import (
	"encoding/json"
	"strings"

	"github.com/valyala/fasthttp"
)

// Handlers - handlers list `resource`
var Handlers map[string]func(object string, segment int64,
	action string, requestData json.RawMessage, okUser bool, user User) (resultData json.RawMessage)

// ErrorHandler error handler
var ErrorHandler func(err error)

func sendError(err error) {
	if ErrorHandler != nil {
		ErrorHandler(err)
	}
}

// FastHTTPHandler - fasthttp fast http handler
func FastHTTPHandler(ctx *fasthttp.RequestCtx) {

	headers := make(map[string]string)

	ctx.Request.Header.VisitAll(func(key, value []byte) {
		headers[string(key)] = string(value)
	})

	var okUser bool
	var user User

	authType, okAuth := headers["authtype"]
	if okAuth && authType == BasicAuthName {
		var er0 error
		okUser, user, er0 = BasicAuthCheckMethod(headers["auth"])
		if er0 != nil {
			sendError(er0)
			ctx.Response.SetStatusCode(400)
			return
		}
	}

	path := string(ctx.URI().Path())

	pathSegments := strings.Split(path, "/")

	resource := ""
	object := ""

	var segment int64
	var action string

	if len(pathSegments) > 1 {
		resource = pathSegments[1]
	}
	if len(pathSegments) > 2 {
		object = pathSegments[2]
	}

	if f, ok := Handlers[resource]; ok {
		resultData := f(object, segment, action, ctx.Request.Body(), okUser, user)
		ctx.Write(resultData)
		ctx.Response.SetStatusCode(200)
		return
	}

	ctx.Response.SetStatusCode(500)
	return
}

// Echo returns requestData
func Echo(object string, segment int64,
	action string, requestData json.RawMessage, okUser bool, user User) (resultData json.RawMessage) {
	return requestData
}

// Ping returns input params
func Ping(object string, segment int64,
	action string, requestData json.RawMessage, okUser bool, user User) (resultData json.RawMessage) {

	m := map[string]interface{}{
		"object":      object,
		"segment":     segment,
		"action":      action,
		"requestData": requestData,
		"okUser":      okUser,
		"user":        user,
	}

	b, er0 := json.MarshalIndent(m, "", "\t")

	if er0 != nil {
		panic(er0)
	}

	return b
}
