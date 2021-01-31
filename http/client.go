package http

import (
	"crypto/tls"
	"encoding/json"
	"strconv"
	"time"

	"github.com/valyala/fasthttp"
)

// Connection - to host
type Connection struct {
	Server              string        `json:"server,omitempty"`
	IgnoreSSLValidation bool          `json:"ignore_ssql_validation,omitempty"`
	QueryWait           time.Duration `json:"query_wait,omitempty"`

	MaxConnsPerHost     int           `json:"max_conn,omitempty"`
	MaxIdleConnDuration time.Duration `json:"max_idle_duration,omitempty"`

	BasicAuth *BasicAuthRequest `json:"basic_auth,omitempty"`

	client *fasthttp.Client `json:"-"`
}

// Init connection
func (c *Connection) Init() {
	c.client = &fasthttp.Client{
		MaxConnsPerHost:     c.MaxConnsPerHost,
		MaxIdleConnDuration: c.MaxIdleConnDuration,
		TLSConfig: &tls.Config{
			InsecureSkipVerify: c.IgnoreSSLValidation,
		},
	}
}

// DoRawQuery do some query
func (c *Connection) DoRawQuery(path string, query []byte) (body []byte, statusCode int, err error) {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)   // <- do not forget to release
	defer fasthttp.ReleaseResponse(resp) // <- do not forget to release

	req.SetRequestURI(c.Server + path)
	req.SetBody(query)
	req.Header.SetMethod("POST")

	err = c.client.DoTimeout(req, resp, c.QueryWait)
	if err != nil {
		return body, 0, err
	}

	body = resp.Body()

	return body, resp.StatusCode(), err
}

// DoQuery do resource query
func (c *Connection) DoQuery(
	resource string,
	object string,
	segment int64,
	action string,
	requestData json.RawMessage) (
	resultData json.RawMessage,
	statusCode int,
	err error) {

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)   // <- do not forget to release
	defer fasthttp.ReleaseResponse(resp) // <- do not forget to release

	path := resource + "/" + object + "/" + strconv.Itoa(int(segment)) + "/" + action

	req.SetRequestURI(c.Server + path)
	req.SetBody(requestData)
	req.Header.SetMethod("POST")

	if c.BasicAuth != nil {
		req.Header.Add("authtype", BasicAuthName)
		req.Header.Add("auth", c.BasicAuth.HeaderGenerate())
	}

	err = c.client.DoTimeout(req, resp, c.QueryWait)
	if err != nil {
		return nil, 0, err
	}

	body := resp.Body()

	return body, resp.StatusCode(), err
}
