/*--------------------------------------------------------*\
|                                                          |
|                          hprose                          |
|                                                          |
| Official WebSite: https://hprose.com                     |
|                                                          |
| rpc/core/context.go                                      |
|                                                          |
| LastModified: Feb 21, 2021                               |
| Author: Ma Bingyao <andot@hprose.com>                    |
|                                                          |
\*________________________________________________________*/

package core

import (
	"context"
)

type contextKeyT string

// contextKey 用指针而非字符串值：ctx.Value(key any) 会把 key 装箱，
// 字符串 key 每次调用都要堆分配（FromContext 是每请求最热的分配点），
// 指针 key 直接存入 interface 的数据字，零分配。
var contextKey = new(contextKeyT)

// Context for RPC.
type Context interface {
	Items() Dict
	HasRequestHeaders() bool
	RequestHeaders() Dict
	HasResponseHeaders() bool
	ResponseHeaders() Dict
	Clone() Context
}

type rpcContext struct {
	items           Dict
	requestHeaders  Dict
	responseHeaders Dict
}

// NewContext returns a core.Context.
func NewContext() Context {
	return &rpcContext{}
}

func (c *rpcContext) Items() Dict {
	if c.items == nil {
		c.items = NewDict(nil)
	}
	return c.items
}

func (c *rpcContext) HasRequestHeaders() bool {
	return c.requestHeaders != nil && !c.requestHeaders.Empty()
}

func (c *rpcContext) RequestHeaders() Dict {
	if c.requestHeaders == nil {
		c.requestHeaders = NewDict(nil)
	}
	return c.requestHeaders
}

func (c *rpcContext) HasResponseHeaders() bool {
	return c.responseHeaders != nil && !c.responseHeaders.Empty()
}

func (c *rpcContext) ResponseHeaders() Dict {
	if c.responseHeaders == nil {
		c.responseHeaders = NewDict(nil)
	}
	return c.responseHeaders
}

func (c *rpcContext) Clone() Context {
	clone := &rpcContext{}
	if c.items != nil {
		clone.items = NewDict(nil)
		c.items.CopyTo(clone.items)
	}
	if c.requestHeaders != nil {
		clone.requestHeaders = NewDict(nil)
		c.requestHeaders.CopyTo(clone.requestHeaders)
	}
	if c.responseHeaders != nil {
		clone.responseHeaders = NewDict(nil)
		c.responseHeaders.CopyTo(clone.responseHeaders)
	}
	return clone
}

// WithContext returns a copy of the parent context and associates it with a core.Context.
func WithContext(ctx context.Context, rpcContext Context) context.Context {
	return context.WithValue(ctx, contextKey, rpcContext)
}

// adoptRequestHeaders 直接采用解码得到的 map 作为请求头。
// 解码器总是新建 map，而服务端上下文每次请求都是新的（requestHeaders 为 nil），
// 因此可以直接接管，省掉 NewDict + CopyTo 的一次 map 分配与逐项写入。
func (c *rpcContext) adoptRequestHeaders(m map[string]interface{}) {
	if c.requestHeaders == nil {
		c.requestHeaders = dict(m)
		return
	}
	for k, v := range m {
		c.requestHeaders.Set(k, v)
	}
}

// adoptResponseHeaders 语义同 adoptRequestHeaders，用于客户端响应头。
func (c *rpcContext) adoptResponseHeaders(m map[string]interface{}) {
	if c.responseHeaders == nil {
		c.responseHeaders = dict(m)
		return
	}
	for k, v := range m {
		c.responseHeaders.Set(k, v)
	}
}

// FromContext returns the core.Context bound to the context.
func FromContext(ctx context.Context) (Context, bool) {
	c, ok := ctx.Value(contextKey).(Context)
	return c, ok
}
