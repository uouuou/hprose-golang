<p align="center"><img src="http://hprose.com/banner.@2x.png" alt="Hprose" title="Hprose" width="650" height="200" /></p>

# Hprose 3.0 for Golang

[![test](https://github.com/uouuou/hprose-golang/actions/workflows/go.yml/badge.svg)](https://github.com/uouuou/hprose-golang/actions)
[![GoDoc](https://godoc.org/github.com/uouuou/hprose-golang?status.svg&style=flat)](https://godoc.org/github.com/uouuou/hprose-golang)
[![License](https://img.shields.io/github/license/hprose/hprose-golang.svg)](http://opensource.org/licenses/MIT)

[中文](README.md) | **English**

## Introduction

Hprose (High Performance Remote Object Service Engine) is a lightweight,
cross-language, cross-platform, high-performance remote dynamic communication
middleware. With Hprose you can conveniently and efficiently intercommunicate
between many programming languages:

AAuto Quicker, ActionScript, ASP, C++, Dart, Delphi/Free Pascal, dotNET (C#/VB...), Golang, Java, JavaScript, Node.js,
Objective-C, Perl, PHP, Python,
Ruby, and more.

This repository is the Golang implementation of Hprose 3.0 (a fork, module
`github.com/uouuou/hprose-golang/v3`). On top of the official v3 it adds:

- **Transport-layer performance leap**: batched writes (writev coalescing) on
  both the socket client and server, plus buffered reads and queue buffering —
  ~21× higher QPS for small messages at 128 concurrency on a single connection (see [Performance](#performance));
- **Transparent HTTP/2 upgrade on the HTTP channel**: `http://`/`https://`
  negotiate automatically (TLS ALPN), with explicit `h2c://` (cleartext HTTP/2)
  and `h2://` (TLS HTTP/2) entry points. The server side is built on the
  standard library `http.Server.Protocols` (`golang.org/x/net/http2/h2c` is
  deprecated);
- All transports and the client/server APIs are unchanged from upstream —
  zero migration for existing code.

## Table of Contents

- [Quick Start](#quick-start)
- [Transport Protocols](#transport-protocols)
- [Server](#server)
- [Client](#client)
- [Codec Configuration](#codec-configuration)
- [Plugin System](#plugin-system)
- [HTTP/2 Support](#http2-support)
- [Performance](#performance)
- [Testing](#testing)
- [License](#license)

## Quick Start

A minimal TCP example (server + client):

```go
package main

import (
	"fmt"
	"net"
	"time"

	"github.com/uouuou/hprose-golang/v3/rpc/core"
)

func main() {
	// ---- Server ----
	service := core.NewService()
	service.AddFunction(func(name string) string {
		return "hello " + name
	}, "hello")
	server, err := net.Listen("tcp", "127.0.0.1:8412")
	if err != nil {
		panic(err)
	}
	if err = service.Bind(server); err != nil {
		panic(err)
	}
	defer server.Close()

	time.Sleep(5 * time.Millisecond)

	// ---- Client ----
	client := core.NewClient("tcp://127.0.0.1/") // default port 8412
	var proxy struct {
		Hello func(name string) (string, error)
	}
	client.UseService(&proxy)
	result, err := proxy.Hello("world")
	fmt.Println(result, err) // hello world <nil>
}
```

## Transport Protocols

The client selects a transport via the URL scheme; codecs, plugins and proxies
stay identical across transports.

| transport     | schemes                                                                            | connection model                                               | notes                                                            |
|---------------|------------------------------------------------------------------------------------|----------------------------------------------------------------|------------------------------------------------------------------|
| socket        | `tcp`/`tcp4`/`tcp6`, `tls`/`tls4`/`tls6`, `ssl`/`ssl4`/`ssl6`, `unix`/`unixpacket` | single-connection multiplexing (index frames + batched writes) | **default choice**; best latency & throughput; default port 8412 |
| websocket     | `ws`/`wss`                                                                         | single-connection multiplexing                                 | subprotocol `hprose`; good for browsers/gateways                 |
| udp           | `udp`/`udp4`/`udp6`                                                                | connectionless, one frame per datagram                         | ≤ 65507 bytes per packet; good for retryable status reports      |
| http          | `http`/`https`                                                                     | one HTTP transaction per call (keep-alive pool)                | `https` auto-upgrades to HTTP/2 via ALPN                         |
| http (HTTP/2) | `h2c`/`h2`                                                                         | HTTP/2 single connection, many streams                         | `h2c` cleartext, `h2` TLS; for gateway-style high concurrency    |
| mock          | `mock://address`                                                                   | in-process direct call                                         | for unit tests, zero network cost                                |

## Server

### Create and bind

```go
service := core.NewService()

// Bind to different server types:
service.Bind(tcpListener) // *net.TCPListener / *net.UnixListener / tls.Listener
service.Bind(udpConn)            // *net.UDPConn
service.Bind(httpServer)         // *http.Server (HTTP/2 enabled automatically, see HTTP/2)
service.Bind(fasthttpServer) // *fasthttp.Server
service.Bind(mock.Server{Address: "test"}) // in-process mock
```

### Method registration (7 ways)

```go
// 1. Function + aliases (multiple allowed)
service.AddFunction(func (a int, b int) int { return a + b }, "add", "sum")

// 2. Bind named methods of an object
service.AddMethod("Hello", &obj{}) // method name "Hello"
service.AddMethods([]string{"A", "B"}, &obj{}) // several methods
service.AddInstanceMethods(&obj{}) // all exported methods, optional namespace
service.AddAllMethods(&obj{}) // including unexported (lowercase) methods

// 3. Missing-method handler (dynamic dispatch, context variant available)
service.AddMissingMethod(func (name string, args []interface{}) ([]interface{}, error) {
return []interface{}{name, args}, nil
})

// 4. net/rpc style
service.AddNetRPCMethods(&obj{})
```

### Concurrency and resource control

```go
service.MaxRequestLength = 1 << 20 // max bytes per request; oversized -> RequestEntityTooLarge

// Business worker pool (default: one goroutine per request)
type myPool struct{}
func (p *myPool) Submit(f func ()) { /* dispatch to your own workers */ }
socketHandler := service.GetHandler("socket").(*socket.Handler)
socketHandler.Pool = &myPool{}

// Connection lifecycle callbacks per transport handler:
// socket.Handler:    OnAccept / OnClose / OnError / Pool
// udp.Handler:       OnClose / OnError
// websocket.Handler: same as socket (over *websocket.Conn)
```

## Client

### Create and proxy

```go
// Multiple addresses: used in order / per the load-balancing strategy
client := core.NewClient("tcp://host1:8412/", "tcp://host2:8412/")
client.ShuffleURLs() // shuffle the address list

// Struct proxy: the function signature determines the call semantics
var stub struct {
Hello      func (name string) (string, error)   // unary call
Notify     func (msg string) `context:"oneway"` // one-way (no response)
Echo       func (ctx context.Context, data []byte) ([]byte, error)
RetryHello func (name string) string `context:"idempotent,retry:3"` // idempotent + retry 3
AuthHello  func (ctx context.Context, name string) string `header:"token"` // extra header
}
client.UseService(&stub)
```

### Call context and headers

```go
clientContext := core.NewClientContext()
clientContext.RequestHeaders().Set("userid", "u1") // request headers
clientContext.Timeout = 5 * time.Second // per-call timeout
ctx := core.WithContext(context.Background(), clientContext)
result, err := stub.Hello(ctx, "world")
// Response headers:
if token, ok := clientContext.ResponseHeaders().GetInterface("token").(string); ok { /* ... */ }
```

### Timeout, cancellation and Abort

```go
client.Timeout = 10 * time.Second // global default timeout (default 30s)
client.Abort() // abort all in-flight calls and close connections
// Per-call cancellation:
ctx, cancel := context.WithTimeout(context.Background(), time.Second)
_, err := stub.Hello(ctx, "world")
cancel()
```

### Transport customization

```go
// Access each transport instance (rpc package provides accessors)
trans := rpc.SocketTransport(client) // *socket.Transport
trans.OnConnect = func (c net.Conn) net.Conn { /* handshake/wrap, e.g. TLS */ return c }
trans.OnClose = func (c net.Conn) { /* disconnect cleanup */ }

rpc.HTTPTransport(client).SetTLSClientConfig(&tls.Config{...})
rpc.WebSocketTransport(client) // *websocket.Transport
rpc.UDPTransport(client) // *udp.Transport
rpc.FastHTTPTransport(client) // *fasthttp.Transport
```

## Codec Configuration

The default is the hprose binary protocol. The client and server can be
configured independently, but **both ends must match**:

```go
client.Codec = rpc.NewClientCodec(
rpc.WithLongType(io.LongTypeUint64), // int64 -> uint64
rpc.WithRealType(io.RealTypeFloat64), // float -> float64
rpc.WithMapType(io.MapTypeIIMap), // map[string]interface{}
rpc.WithStructType(io.StructTypeValue), // structs by value
rpc.WithListType(io.ListTypeISlice), // slices -> []interface{}
rpc.WithSimple(true),                // simple mode (no reference counting)
rpc.WithDebug(true), // debug logging
)
service.Codec = rpc.NewServiceCodec( /* same options */)
```

You can also swap in a JSON-RPC codec (`rpc/codec/jsonrpc` package, built on
json-iterator):

```go
import jsoniter "github.com/json-iterator/go"

client.Codec = jsonrpc.NewClientCodec(jsoniter.ConfigDefault)
service.Codec = jsonrpc.NewServiceCodec(jsoniter.ConfigDefault)
```

## Plugin System

Plugins are onion-model middleware, split into two kinds attached to the call
chain:

- **invoke handler**: `func(ctx, name, args, next) (result, err)` — method-call level;
- **io handler**: `func(ctx, request []byte, next) (response []byte, err)` — raw bytes level.

`client.Use(...)` / `service.Use(...)` sort handlers by type automatically.
All available plugins:

### Logging — log

```go
client.Use(log.Plugin) // ready-made logging plugin
client.Use(log.IOHandler, log.InvokeHandler)  // or attach individually
client.Use(log.New(func(v ...interface{}) { /* custom output */ }))
```

### Server-side execution timeout — timeout

```go
service.Use(timeout.New(5 * time.Millisecond)) // methods slower than 5ms time out
```

### One-way calls — oneway

```go
client.Use(oneway.Oneway{}) // all calls become one-way
// or per method: func Notify(msg string) `context:"oneway"`
```

### Rate/concurrency limiting — limiter

```go
// Concurrent limiter: at most 3 in-flight requests; extra calls wait 1ns then time out
client.Use(limiter.NewConcurrentLimiter(3, time.Nanosecond))
client.Use(limiter.NewConcurrentLimiter(3)) // no timeout -> queue up

// Rate limiter: 1000 permits per second
client.Use(limiter.NewRateLimiter(1000, limiter.WithMaxPermits(2), limiter.WithTimeout(time.Millisecond*80)))
```

### Circuit breaker — circuitbreaker

```go
client.Use(circuitbreaker.New(
circuitbreaker.WithThreshold(3), // open after 3 consecutive failures
circuitbreaker.WithRecoverTime(10*time.Millisecond), // recovery duration
circuitbreaker.WithMockService(func(ctx context.Context, name string, args []interface{}) ([]interface{}, error) {
return []interface{}{"circuit open fallback"}, nil
}),
))
```

### Cluster strategies — cluster

```go
// Failtry: retry on the same address (idempotency required)
client.Use(cluster.New(cluster.FailtryConfig(
cluster.WithIdempotent(true), // only idempotent methods may retry (pair with context:"idempotent")
cluster.WithRetry(3), // retry count (default 10)
cluster.WithMinInterval(100*time.Millisecond),
cluster.WithMaxInterval(2*time.Second),
)))

// Failover: rotate across addresses
client.Use(cluster.New(cluster.FailoverConfig()))

// Failfast: fail immediately with a callback
client.Use(cluster.New(cluster.FailfastConfig(func(ctx context.Context) {
println("failed")
})))

// Fan-out / broadcast (multi-address scenarios)
client.Use(cluster.Forking) // all in parallel
client.Use(cluster.Broadcast) // broadcast and aggregate
```

### Load balancing — loadbalance (7 strategies, multi-address)

```go
client.Use(loadbalance.NewRandomLoadBalance())
client.Use(loadbalance.NewRoundRobinLoadBalance())
client.Use(loadbalance.NewLeastActiveLoadBalance())
client.Use(loadbalance.NewNginxRoundRobinLoadBalance(map[string]int{"tcp://a:8412/": 1, "tcp://b:8412/": 2}))
client.Use(loadbalance.NewWeightedRandomLoadBalance(map[string]int{...}))
client.Use(loadbalance.NewWeightedRoundRobinLoadBalance(map[string]int{...}))
client.Use(loadbalance.NewWeightedLeastActiveLoadBalance(map[string]int{...}))
```

### Forwarding — forward (gateway/proxy)

```go
// Server A forwards unknown methods to server B
fw := forward.New("tcp://127.0.0.1:8402/")
serviceA.AddMissingMethod(fw.Forward) // invoke-level forwarding
serviceA.Use(fw.IOHandler) // or byte-level forwarding
```

### Messaging / push — push

```go
// ---- Server: create a Broker; registers methods "+"/"-"/">"/">?"/">*"/"?"/"|" ----
broker := push.NewBroker(service)
broker.Push(data, "topic") // broadcast to topic
broker.Push(data, "topic", "user1") // push to one subscriber
broker.Unicast(ctx, data, "topic", "user1", "") // unicast
broker.Multicast(ctx, data, "topic", []string{"u1", "u2"}, "")
broker.Broadcast(ctx, data, "topic", "")
broker.Exists("topic", "user1") // is subscriber subscribed?
broker.IdList("topic") // subscriber list

// ---- Client: Prosumer manages subscriptions ----
prosumer := push.NewProsumer(client, "user1")
prosumer.OnSubscribe = func (topic string) { println("subscribed", topic) }
prosumer.OnUnsubscribe = func (topic string) { println("unsubscribed", topic) }
// Subscribe to a topic (symbolic method "+"):
client.Invoke("+", []interface{}{"topic"}) // or declare it in a stub
```

### Reverse calls — reverse (server dispatches to clients)

```go
// ---- Server: register a Caller ----
caller := reverse.NewCaller(service)
// Methods callable on the client side follow the same signatures as server methods.

// ---- Client: Provider receives calls initiated by the server ----
provider := reverse.NewProvider(client, "user1")
// Register callable methods on the provider via AddFunction/AddMethod
provider.Listen() // start long-polling for calls (connection must stay alive)
```

### In-process mock (testing)

```go
service := core.NewService()
service.AddFunction(func (a int) int { return a * 2 }, "double")
service.Bind(mock.Server{Address: "testService"})
defer mock.Server{Address: "testService"}.Close()

client := core.NewClient("mock://testService")
var stub struct {
Double func (a int) (int, error)
}
client.UseService(&stub)
result, _ := stub.Double(21) // 42, no network cost
```

## HTTP/2 Support

The HTTP channel upgrades to HTTP/2 transparently **without changing the
scheme or URL**:

- **Server**: binding an `*http.Server` enables it automatically —
  `http.Server.Protocols` turns on HTTP/1.1, TLS HTTP/2 and cleartext h2c at
  the same time; cleartext connections are probed for the HTTP/2 preface and
  fall back to HTTP/1.1 otherwise, so **existing clients are unaffected**
  (`golang.org/x/net/http2/h2c` is deprecated and no longer used):

```go
httpServer := &http.Server{Addr: ":8080"}
service.Bind(httpServer) // Protocols configured + Handler bound
go httpServer.ListenAndServe() // cleartext: h2c + HTTP/1.1 compatible
// TLS: go httpServer.ListenAndServeTLS("cert.pem", "key.pem") // ALPN negotiates h2
```

- **Client**: `https://` negotiates HTTP/2 via ALPN automatically; `h2c://`
  explicitly uses cleartext HTTP/2 (for high-concurrency intranet gateways):

```go
client := core.NewClient("h2c://127.0.0.1:8080/") // cleartext HTTP/2
client2 := core.NewClient("https://example.com/") // TLS, auto h2
```

- Notes: plain `http://` stays HTTP/1.1 by default (no forced h2c, so pure
  HTTP/1.1 servers keep working); use `h2c://` for explicit cleartext h2.
  fasthttp servers do not support HTTP/2 (the HTTP/1.1 path is unaffected).

## Performance

After the transport frame-scheduling optimization (batched writes + buffered
reads, wire protocol unchanged), measured on the 127.0.0.1 loopback (Go 1.26, single-connection multiplexing, median of
two rounds):

| Scenario               | Before (official v3.0.16) |                     After |                                  Ratio |
|------------------------|--------------------------:|--------------------------:|---------------------------------------:|
| 64B × 1 concurrency    |   6,307 QPS (p50 0.157ms) |  17,939 QPS (p50 0.048ms) |                                   2.8× |
| 64B × 16 concurrency   |                17,804 QPS |               109,051 QPS |                                   6.1× |
| 64B × 128 concurrency  |   20,821 QPS (p50 5.92ms) | 492,048 QPS (p50 0.234ms) |                                  23.6× |
| 1KB × 128 concurrency  |                21,104 QPS |               380,190 QPS |                                  18.0× |
| 64KB × 128 concurrency |                 9,703 QPS |                10,782 QPS | 1.1× (no regression on large payloads) |

For reference, gRPC reaches 209,092 QPS in the same 128-concurrency 64B
scenario — the optimized single-connection hprose-tcp is ~2.4× faster.
Full reports live in the ShuHeSdk repository under
`docs/rpc-optimization-report.md` and `docs/rpc-benchmark-report.md`.

## Testing

```bash
go build ./... && go vet ./...
go test -race -count=1 ./...     # full suite (concurrency correctness, h2c/h2 round-trips)
```

The `rpc/mock` package provides an in-process mock transport for fast unit
tests in CI.

## License

MIT License
