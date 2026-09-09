/*--------------------------------------------------------*\
|                                                          |
|                          hprose                          |
|                                                          |
| Official WebSite: https://hprose.com                     |
|                                                          |
| rpc/http/transport.go                                    |
|                                                          |
| LastModified: Mar 7, 2022                                |
| Author: Ma Bingyao <andot@hprose.com>                    |
|                                                          |
\*________________________________________________________*/

package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"

	"github.com/uouuou/hprose-golang/v3/rpc/core"
	"github.com/uouuou/hprose-golang/v3/rpc/http/cookie"
)

// maxHTTP2Connections 是 HTTP/2 默认连接分片数的上限。
// HTTP/2 客户端在单条连接内串行刷帧（每个请求写完 HEADERS/DATA 各 Flush 一次，
// 都在连接级写锁下），单连接会成为高并发吞吐的平台期；把帧流水线分散到多条
// 连接上可近线性提升吞吐（回环实测 2 条 1.45x、4 条 2.6x、8 条 3.0x）。
const maxHTTP2Connections = 4

func defaultHTTP2Connections() int {
	n := runtime.GOMAXPROCS(0)
	if n > maxHTTP2Connections {
		n = maxHTTP2Connections
	}
	if n < 1 {
		n = 1
	}
	return n
}

type Transport struct {
	DisableHTTPHeader bool
	Header            http.Header
	HTTPClient        http.Client
	// h2cClient 明文 HTTP/2 客户端，仅 h2c:// scheme 使用；
	// http:// 请求仍走 HTTP/1.1，https:// 由 ForceAttemptHTTP2 经 ALPN 自动协商 h2。
	h2cClient http.Client

	// HTTP/2 连接分片：httpShards/h2cShards 是不含上面两个基准 client 的额外分片。
	// 基准 client 的 Transport 被用户替换（httpBase/h2cBase 不再匹配）时自动退化为
	// 单连接，避免与用户的连接管理配置冲突。
	httpShards []http.Client
	h2cShards  []http.Client
	shardNext  atomic.Uint64
	httpBase   http.RoundTripper
	h2cBase    http.RoundTripper
}

// pickHTTP 返回一个 https/h2 用的 client（轮询分片）。
func (trans *Transport) pickHTTP() *http.Client {
	if len(trans.httpShards) == 0 || trans.HTTPClient.Transport != trans.httpBase {
		return &trans.HTTPClient
	}
	i := trans.shardNext.Add(1) % uint64(len(trans.httpShards)+1)
	if i == 0 {
		return &trans.HTTPClient
	}
	return &trans.httpShards[i-1]
}

// pickH2C 返回一个 h2c 用的 client（轮询分片）。
func (trans *Transport) pickH2C() *http.Client {
	if len(trans.h2cShards) == 0 || trans.h2cClient.Transport != trans.h2cBase {
		return &trans.h2cClient
	}
	i := trans.shardNext.Add(1) % uint64(len(trans.h2cShards)+1)
	if i == 0 {
		return &trans.h2cClient
	}
	return &trans.h2cShards[i-1]
}

func (trans *Transport) Transport(ctx context.Context, request []byte) ([]byte, error) {
	clientContext := core.GetClientContext(ctx)
	target := clientContext.URL.String()
	client := &trans.HTTPClient
	switch clientContext.URL.Scheme {
	case "h2c": // 明文 HTTP/2
		target = "http" + strings.TrimPrefix(target, "h2c")
		client = trans.pickH2C()
	case "h2": // TLS HTTP/2，等价 https（ALPN 自动协商）
		target = "https" + strings.TrimPrefix(target, "h2")
		client = trans.pickHTTP()
	case "https":
		client = trans.pickHTTP()
	}
	req, err := http.NewRequestWithContext(ctx, "POST", target, bytes.NewReader(request))
	if err != nil {
		return nil, err
	}
	if !trans.DisableHTTPHeader {
		if trans.Header != nil {
			addHeader(req.Header, trans.Header)
		}
		if header, ok := clientContext.Items().GetInterface("httpRequestHeaders").(http.Header); ok {
			addHeader(req.Header, header)
		}
	}
	var resp *http.Response
	resp, err = client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	clientContext.Items().Set("httpStatusCode", resp.StatusCode)
	clientContext.Items().Set("httpStatusText", http.StatusText(resp.StatusCode))
	switch resp.StatusCode {
	case http.StatusOK:
		if !trans.DisableHTTPHeader {
			clientContext.Items().Set("httpResponseHeaders", resp.Header)
		}
		return readAll(resp.Body, resp.ContentLength)
	case http.StatusRequestEntityTooLarge:
		return nil, core.ErrRequestEntityTooLarge
	default:
		return nil, errors.New(resp.Status)
	}
}

func (trans *Transport) Abort() {
}

// eachHTTPTransport 对基准 client 与所有 http 分片的 *http.Transport 执行 fn，
// 保证 setter 同时作用于全部分片，避免分片间配置不一致。
func (trans *Transport) eachHTTPTransport(fn func(*http.Transport)) {
	if t, ok := trans.HTTPClient.Transport.(*http.Transport); ok {
		fn(t)
	}
	for i := range trans.httpShards {
		if t, ok := trans.httpShards[i].Transport.(*http.Transport); ok {
			fn(t)
		}
	}
}

// CookieManagerOption returns the CookieManagerOption
func (trans *Transport) CookieManagerOption() cookie.CookieManagerOption {
	switch trans.HTTPClient.Jar {
	case nil:
		return cookie.NoCookieManager
	case globalFastJar:
		return cookie.GlobalCookieManager
	default:
		return cookie.ClientCookieManager
	}
}

// SetCookieManagerOption sets the CookieManagerOption
func (trans *Transport) SetCookieManagerOption(option cookie.CookieManagerOption) {
	apply := func(set func(*http.Client)) {
		set(&trans.HTTPClient)
		set(&trans.h2cClient)
		for i := range trans.httpShards {
			set(&trans.httpShards[i])
		}
		for i := range trans.h2cShards {
			set(&trans.h2cShards[i])
		}
	}
	switch option {
	case cookie.NoCookieManager:
		apply(func(c *http.Client) { c.Jar = nil })
	case cookie.GlobalCookieManager:
		apply(func(c *http.Client) { c.Jar = globalFastJar })
	default:
		apply(func(c *http.Client) {
			c.Jar, _ = cookiejar.New(nil)
		})
	}
}

// TLSClientConfig returns the tls.Config
func (trans *Transport) TLSClientConfig() *tls.Config {
	if t, ok := trans.HTTPClient.Transport.(*http.Transport); ok {
		return t.TLSClientConfig
	}
	return nil
}

// SetTLSClientConfig sets the tls.Config
func (trans *Transport) SetTLSClientConfig(config *tls.Config) {
	trans.eachHTTPTransport(func(t *http.Transport) {
		t.TLSClientConfig = config
	})
}

// KeepAlive returns the keepalive status
func (trans *Transport) KeepAlive() bool {
	if t, ok := trans.HTTPClient.Transport.(*http.Transport); ok {
		return !t.DisableKeepAlives
	}
	return false
}

// SetKeepAlive sets the keepalive status
func (trans *Transport) SetKeepAlive(enable bool) {
	trans.eachHTTPTransport(func(t *http.Transport) {
		t.DisableKeepAlives = !enable
	})
}

// Compression returns the compression status
func (trans *Transport) Compression() bool {
	if t, ok := trans.HTTPClient.Transport.(*http.Transport); ok {
		return !t.DisableCompression
	}
	return false
}

// SetCompression sets the compression status
func (trans *Transport) SetCompression(enable bool) {
	trans.eachHTTPTransport(func(t *http.Transport) {
		t.DisableCompression = !enable
	})
}

// fastCookieJar 包装标准库 cookiejar：在第一次收到 Set-Cookie 之前，
// Cookies() 直接返回 nil。http.Client 每请求都会调用 Jar.Cookies()，而标准库
// cookiejar 每次都要计算 eTLD+1（publicsuffix 查表）并抢一把全局互斥锁——
// 在 HTTP/2 高并发下这是可观的串行开销。无 cookie 写入时语义等价于空 jar。
type fastCookieJar struct {
	jar     http.CookieJar
	hasData atomic.Bool
}

func newFastCookieJar(jar http.CookieJar) *fastCookieJar {
	return &fastCookieJar{jar: jar}
}

func (j *fastCookieJar) Cookies(u *url.URL) []*http.Cookie {
	if !j.hasData.Load() {
		return nil
	}
	return j.jar.Cookies(u)
}

func (j *fastCookieJar) SetCookies(u *url.URL, cookies []*http.Cookie) {
	if len(cookies) == 0 {
		return
	}
	j.hasData.Store(true)
	j.jar.SetCookies(u, cookies)
}

var globalCookieJar, _ = cookiejar.New(nil)

// globalFastJar 是默认 cookie 管理器，包装 globalCookieJar 但跳过空 jar 的每请求开销。
var globalFastJar = newFastCookieJar(globalCookieJar)

type transportFactory struct {
	schemes []string
}

func (factory transportFactory) Schemes() []string {
	return factory.schemes
}

func (factory transportFactory) New() core.Transport {
	transport := &Transport{}
	dialer := &net.Dialer{
		Timeout:   time.Second,
		KeepAlive: time.Second * 30,
		DualStack: true,
	}
	newHTTPTransport := func() *http.Transport {
		return &http.Transport{
			DialContext:           dialer.DialContext,
			MaxIdleConnsPerHost:   100,
			IdleConnTimeout:       time.Minute,
			TLSHandshakeTimeout:   time.Second,
			ExpectContinueTimeout: time.Millisecond * 500,
			// https 经 ALPN 自动协商 HTTP/2；http 明文保持 HTTP/1.1（明文 h2 走 h2c:// scheme）
			ForceAttemptHTTP2: true,
		}
	}
	newH2CTransport := func() *http2.Transport {
		return &http2.Transport{
			AllowHTTP:       true,
			TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
			// 明文直连：h2c 不经 TLS，DialTLSContext 直接返回 TCP 连接
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return dialer.DialContext(ctx, network, addr)
			},
		}
	}
	transport.HTTPClient.Transport = newHTTPTransport()
	transport.HTTPClient.Jar = globalFastJar
	transport.h2cClient = http.Client{
		Transport: newH2CTransport(),
		Jar:       globalFastJar,
	}
	transport.httpBase = transport.HTTPClient.Transport
	transport.h2cBase = transport.h2cClient.Transport
	// 额外分片：每条分片持有独立的 Transport 实例，因而拥有独立的连接池，
	// 帧流水线不再挤在同一条 HTTP/2 连接上。
	if n := defaultHTTP2Connections(); n > 1 {
		transport.httpShards = make([]http.Client, 0, n-1)
		transport.h2cShards = make([]http.Client, 0, n-1)
		for i := 1; i < n; i++ {
			httpClient := transport.HTTPClient
			httpClient.Transport = newHTTPTransport()
			transport.httpShards = append(transport.httpShards, httpClient)
			h2cClient := transport.h2cClient
			h2cClient.Transport = newH2CTransport()
			transport.h2cShards = append(transport.h2cShards, h2cClient)
		}
	}
	return transport
}

func RegisterTransport() {
	core.RegisterTransport("http", transportFactory{[]string{"http", "https", "h2c", "h2"}})
}
