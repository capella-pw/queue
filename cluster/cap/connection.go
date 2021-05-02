package cap

import (
	"crypto/tls"
	"encoding/json"
	"time"

	"github.com/myfantasy/mft"
	"github.com/valyala/fasthttp"
)

// Connection - to host
type Connection struct {
	Server              string        `json:"server,omitempty"`
	IgnoreSSLValidation bool          `json:"ignore_ssql_validation,omitempty"`
	QueryWait           time.Duration `json:"query_wait,omitempty"`

	MaxConnsPerHost     int           `json:"max_conn,omitempty"`
	MaxIdleConnDuration time.Duration `json:"max_idle_duration,omitempty"`

	client *fasthttp.Client `json:"-"`
}

func CreateConnection(server string,
	ignoreSSLValidation bool,
	queryWait time.Duration,
	maxConnsPerHost int,
	maxIdleConnDuration time.Duration) (c *Connection) {

	c = &Connection{
		Server:              server,
		IgnoreSSLValidation: ignoreSSLValidation,
		QueryWait:           queryWait,
		MaxConnsPerHost:     maxConnsPerHost,
		MaxIdleConnDuration: maxIdleConnDuration,
	}

	return c
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

func ConnectionFromJson(body []byte) (c *Connection, err *mft.Error) {
	c = &Connection{}
	er0 := json.Unmarshal(body, c)
	if er0 != nil {
		return nil, GenerateErrorE(10190000, er0)
	}
	return c, nil
}

// DoRawQuery do some query
func (c *Connection) DoRawQuery(queryWait time.Duration, path string, headersIn map[string]string, query []byte) (body []byte, headersOut map[string]string, statusCode int, err error) {
	if queryWait == 0 {
		queryWait = c.QueryWait
	}

	headersOut = map[string]string{}
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)   // <- do not forget to release
	defer fasthttp.ReleaseResponse(resp) // <- do not forget to release

	req.SetRequestURI(c.Server + path)
	req.SetBody(query)
	req.Header.SetMethod("POST")
	if headersIn != nil {
		for k, v := range headersIn {
			req.Header.Add(k, v)
		}
	}

	err = c.client.DoTimeout(req, resp, queryWait)
	if err != nil {
		return body, nil, 0, GenerateErrorE(10190001, err, c.Server)
	}

	body = resp.Body()

	resp.Header.VisitAll(func(key []byte, value []byte) {
		headersOut[string(key)] = string(value)
	})

	return body, headersOut, resp.StatusCode(), nil
}
