package http_service

import (
	"context"

	"github.com/capella-pw/queue/cluster"
	"github.com/capella-pw/queue/cluster/cap"
	"github.com/valyala/fasthttp"
)

// FastHTTPHandler - fasthttp fast http handler
func FastHTTPHandler(sc *cluster.ClusterService,
	prepareCtxGenerator func() (ctx context.Context, doOnCompete func()),
	addFunc []cluster.AdditionalCallFuncInClusterFunc,
) func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		decompressAlg := ""
		ctx.Request.Header.VisitAll(func(key []byte, value []byte) {
			if string(key) == cap.CompressTypeHeader {
				decompressAlg = string(value)
			}
		})

		ctxPrep, doOnCompete := prepareCtxGenerator()
		defer doOnCompete()

		bodyOut, outContentType, htmlCode := sc.Call(ctxPrep, decompressAlg, ctx.Request.Body(), addFunc)

		ctx.Response.SetStatusCode(htmlCode)
		ctx.Response.Header.Add(cap.CompressTypeHeader, outContentType)
		ctx.Response.SetBody(bodyOut)
	}
}
