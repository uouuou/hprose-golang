<p align="center"><img src="http://hprose.com/banner.@2x.png" alt="Hprose" title="Hprose" width="650" height="200" /></p>

# Hprose 3.0 for Golang

[![GoDoc](https://godoc.org/github.com/uouuou/hprose-golang?status.svg&style=flat)](https://godoc.org/github.com/uouuou/hprose-golang)
[![License](https://img.shields.io/github/license/hprose/hprose-golang.svg)](http://opensource.org/licenses/MIT)

**中文** | [English](README_EN.md)

## 简介

Hprose（High Performance Remote Object Service Engine）是一个轻量、跨语言、跨平台的
高性能远程动态通信中间件。通过 Hprose，你可以方便高效地在不同编程语言之间互通：

支持的语言包括 AAuto Quicker、ActionScript、ASP、C++、Dart、Delphi/Free Pascal、
dotNET (C#/VB...)、Golang、Java、JavaScript、Node.js、Objective-C、Perl、PHP、Python、Ruby 等。

本仓库是 Hprose 3.0 的 Golang 实现（fork，模块路径 `github.com/uouuou/hprose-golang/v3`），
在官方 v3 基础上新增/增强了：

- **传输层性能跃迁**：socket 客户端与服务端双侧批量写（writev 合并）+ 双侧读缓冲 +
  队列缓冲，单连接 128 并发小消息 QPS 提升约 21 倍（详见[性能](#性能)）；
- **HTTP 通道透明升级 HTTP/2**：`http://`/`https://` 自动协商（TLS ALPN），
  新增 `h2c://`（明文 HTTP/2）与 `h2://`（TLS HTTP/2）显式入口，服务端基于标准库
  `http.Server.Protocols`（`golang.org/x/net/http2/h2c` 已弃用）；
- 全部传输协议与服务端/客户端 API 与原版保持一致，存量代码零迁移。

## 目录

- [快速开始](#快速开始)
- [传输协议总览](#传输协议总览)
- [服务端](#服务端)
- [客户端](#客户端)
- [编解码配置](#编解码配置)
- [插件系统](#插件系统)
- [HTTP/2 支持](#http2-支持)
- [性能](#性能)
- [测试](#测试)
- [License](#license)

## 快速开始

一个 TCP 通道的最小示例（服务端 + 客户端）：

```go
package main

import (
  "fmt"
  "net"
  "time"

  "github.com/uouuou/hprose-golang/v3/rpc/core"
)

func main() {
  // ---- 服务端 ----
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

  // ---- 客户端 ----
  client := core.NewClient("tcp://127.0.0.1/") // 默认端口 8412
  var proxy struct {
    Hello func(name string) (string, error)
  }
  client.UseService(&proxy)
  result, err := proxy.Hello("world")
  fmt.Println(result, err) // hello world <nil>
}
```

## 传输协议总览

客户端通过 URL scheme 选择传输协议，同一套编解码/插件/代理代码无需改动。

| transport      | schemes                                                                            | 连接模型                                  | 说明                                                 |
|----------------|------------------------------------------------------------------------------------|-------------------------------------------|------------------------------------------------------|
| socket         | `tcp`/`tcp4`/`tcp6`、`tls`/`tls4`/`tls6`、`ssl`/`ssl4`/`ssl6`、`unix`/`unixpacket` | 单连接多路复用（index 帧复用 + 批量写）   | **默认推荐**；低延迟高吞吐全场景最优；缺省端口 8412  |
| websocket      | `ws`/`wss`                                                                         | 单连接多路复用                            | 子协议 `hprose`；适合浏览器/网关穿透                 |
| udp            | `udp`/`udp4`/`udp6`                                                                | 无连接，每报文一帧                        | 单包 ≤ 65507 字节；适合可重试的状态上报              |
| http           | `http`/`https`                                                                     | 每请求一个 HTTP 事务（keep-alive 连接池） | `https` 经 ALPN 自动升级 HTTP/2                      |
| http（HTTP/2） | `h2c`/`h2`                                                                         | HTTP/2 单连接多流                         | `h2c` 明文 HTTP/2，`h2` TLS HTTP/2；适合网关型高并发 |
| mock           | `mock://地址`                                                                      | 进程内直接调用                            | 单元测试用，零网络开销                               |

## 服务端

### 创建与绑定

```go
service := core.NewService()

// 绑定到不同的服务器类型：
service.Bind(tcpListener) // *net.TCPListener / *net.UnixListener / tls.Listener
service.Bind(udpConn)            // *net.UDPConn
service.Bind(httpServer)         // *http.Server（自动启用 HTTP/2，见 HTTP/2 章节）
service.Bind(fasthttpServer) // *fasthttp.Server
service.Bind(mock.Server{Address: "test"}) // 进程内 mock
```

### 方法注册（7 种方式）

```go
// 1. 函数 + 别名（可多个别名）
service.AddFunction(func (a int, b int) int { return a + b }, "add", "sum")

// 2. 绑定具名方法到对象
service.AddMethod("Hello", &obj{}) // 方法名 "Hello"
service.AddMethods([]string{"A", "B"}, &obj{}) // 多个方法
service.AddInstanceMethods(&obj{}) // 全部导出方法，可选命名空间
service.AddAllMethods(&obj{}) // 含非导出（小写）方法

// 3. 缺失方法处理器（动态分发，含 context 版本）
service.AddMissingMethod(func (name string, args []interface{}) ([]interface{}, error) {
return []interface{}{name, args}, nil
})

// 4. 兼容 net/rpc 风格
service.AddNetRPCMethods(&obj{})
```

### 并发与资源控制

```go
service.MaxRequestLength = 1 << 20 // 单请求最大字节数，超限返回 RequestEntityTooLarge

// 业务处理线程池（默认每请求一个 goroutine）
type myPool struct{}
func (p *myPool) Submit(f func ()) { /* 投递到自己的 worker 池 */ }
socketHandler := service.GetHandler("socket").(*socket.Handler)
socketHandler.Pool = &myPool{}

// 各协议 Handler 均可定制连接生命周期回调
// socket.Handler:     OnAccept / OnClose / OnError / Pool
// udp.Handler:        OnClose / OnError
// websocket.Handler:  同 socket（面向 *websocket.Conn）
```

## 客户端

### 创建与代理

```go
// 多地址：自动按顺序/负载均衡策略使用
client := core.NewClient("tcp://host1:8412/", "tcp://host2:8412/")
client.ShuffleURLs() // 随机打乱地址顺序

// 结构体代理：函数签名的返回值决定调用语义
var stub struct {
Hello      func (name string) (string, error)   // 一元调用
Notify     func (msg string) `context:"oneway"` // 单向（不等响应）
Echo       func (ctx context.Context, data []byte) ([]byte, error)
RetryHello func (name string) string `context:"idempotent,retry:3"` // 幂等 + 重试 3 次
AuthHello  func (ctx context.Context, name string) string `header:"token"` // 附加请求头
}
client.UseService(&stub)
```

### 调用上下文与头信息

```go
clientContext := core.NewClientContext()
clientContext.RequestHeaders().Set("userid", "u1") // 请求头
clientContext.Timeout = 5 * time.Second // 单次调用超时
ctx := core.WithContext(context.Background(), clientContext)
result, err := stub.Hello(ctx, "world")
// 响应头：
if token, ok := clientContext.ResponseHeaders().GetInterface("token").(string); ok { /* ... */ }
```

### 超时、取消与 Abort

```go
client.Timeout = 10 * time.Second // 全局默认超时（默认 30s）
client.Abort() // 中断所有在途调用并关闭连接
// 单次取消：
ctx, cancel := context.WithTimeout(context.Background(), time.Second)
_, err := stub.Hello(ctx, "world")
cancel()
```

### 传输层定制

```go
// 获取各传输实例（rpc 包提供访问器）
trans := rpc.SocketTransport(client) // *socket.Transport
trans.OnConnect = func (c net.Conn) net.Conn { /* 握手/包装，如 TLS 包装 */ return c }
trans.OnClose = func (c net.Conn) { /* 断连清理 */ }

rpc.HTTPTransport(client).SetTLSClientConfig(&tls.Config{...})
rpc.WebSocketTransport(client) // *websocket.Transport
rpc.UDPTransport(client) // *udp.Transport
rpc.FastHTTPTransport(client) // *fasthttp.Transport
```

## 编解码配置

默认使用 hprose 二进制协议；客户端与服务端可独立配置编解码选项， **两端必须一致**：

```go
client.Codec = rpc.NewClientCodec(
rpc.WithLongType(io.LongTypeUint64), // int64 -> uint64
rpc.WithRealType(io.RealTypeFloat64), // float -> float64
rpc.WithMapType(io.MapTypeIIMap), // map[string]interface{}
rpc.WithStructType(io.StructTypeValue), // 结构体按值
rpc.WithListType(io.ListTypeISlice), // 切片 -> []interface{}
rpc.WithSimple(true),                // 简化模式（省去引用计数）
rpc.WithDebug(true), // 调试日志
)
service.Codec = rpc.NewServiceCodec( /* 同样的选项 */)
```

也支持替换为 JSON-RPC 编解码（`rpc/codec/jsonrpc` 包，基于 json-iterator）：

```go
import jsoniter "github.com/json-iterator/go"

client.Codec = jsonrpc.NewClientCodec(jsoniter.ConfigDefault)
service.Codec = jsonrpc.NewServiceCodec(jsoniter.ConfigDefault)
```

## 插件系统

插件是洋葱模型的中间件，分两类挂在调用链上：

- **invoke handler**：`func(ctx, name, args, next) (result, err)` —— 方法调用级；
- **io handler**：`func(ctx, request []byte, next) (response []byte, err)` —— 传输字节级。

`client.Use(...)` / `service.Use(...)` 按 handler 类型自动归类。以下为全部可用插件：

### 日志 log

```go
client.Use(log.Plugin) // 预置的日志插件
client.Use(log.IOHandler, log.InvokeHandler)  // 或分别挂
client.Use(log.New(func(v ...interface{}) { /* 自定义输出 */ }))
```

### 服务端执行超时 timeout

```go
service.Use(timeout.New(5 * time.Millisecond)) // 方法执行超过 5ms 返回超时错误
```

### 单向调用 oneway

```go
client.Use(oneway.Oneway{}) // 全部调用改为单向（不等响应）
// 或按方法标注：func Notify(msg string) `context:"oneway"`
```

### 限流 limiter

```go
// 并发限流：最多 3 个在途请求，超出等待 1ns 后返回超时
client.Use(limiter.NewConcurrentLimiter(3, time.Nanosecond))
client.Use(limiter.NewConcurrentLimiter(3)) // 不设超时则排队等待

// 速率限流：每秒 1000 次
client.Use(limiter.NewRateLimiter(1000, limiter.WithMaxPermits(2), limiter.WithTimeout(time.Millisecond*80)))
```

### 熔断 circuitbreaker

```go
client.Use(circuitbreaker.New(
circuitbreaker.WithThreshold(3), // 连续失败 3 次熔断
circuitbreaker.WithRecoverTime(10*time.Millisecond), // 熔断持续时间
circuitbreaker.WithMockService(func(ctx context.Context, name string, args []interface{}) ([]interface{}, error) {
return []interface{}{"服务熔断降级"}, nil
}),
))
```

### 集群策略 cluster

```go
// Failtry：同一地址重试（幂等要求）
client.Use(cluster.New(cluster.FailtryConfig(
cluster.WithIdempotent(true), // 仅幂等方法可重试（配合 context:"idempotent" 使用）
cluster.WithRetry(3), // 重试次数（默认 10）
cluster.WithMinInterval(100*time.Millisecond),
cluster.WithMaxInterval(2*time.Second),
)))

// Failover：多地址轮换
client.Use(cluster.New(cluster.FailoverConfig()))

// Failfast：立即失败回调
client.Use(cluster.New(cluster.FailfastConfig(func(ctx context.Context) {
println("失败")
})))

// 扇出/广播（多地址场景）
client.Use(cluster.Forking) // 并发全部
client.Use(cluster.Broadcast) // 广播聚合
```

### 负载均衡 loadbalance（7 种，多地址场景）

```go
client.Use(loadbalance.NewRandomLoadBalance())
client.Use(loadbalance.NewRoundRobinLoadBalance())
client.Use(loadbalance.NewLeastActiveLoadBalance())
client.Use(loadbalance.NewNginxRoundRobinLoadBalance(map[string]int{"tcp://a:8412/": 1, "tcp://b:8412/": 2}))
client.Use(loadbalance.NewWeightedRandomLoadBalance(map[string]int{...}))
client.Use(loadbalance.NewWeightedRoundRobinLoadBalance(map[string]int{...}))
client.Use(loadbalance.NewWeightedLeastActiveLoadBalance(map[string]int{...}))
```

### 转发 forward（网关/代理）

```go
// 服务端 A 收到未知方法时转发到服务端 B
fw := forward.New("tcp://127.0.0.1:8402/")
serviceA.AddMissingMethod(fw.Forward) // invoke 级转发
serviceA.Use(fw.IOHandler) // 或字节级转发
```

### 消息推送 push

```go
// ---- 服务端：创建 Broker，自动注册方法 "+"/"-"/">"/">?"/">*"/"?"/"|" ----
broker := push.NewBroker(service)
// 推送（按订阅者 id / 主题）
broker.Push(data, "topic") // 广播到主题
broker.Push(data, "topic", "user1") // 指定订阅者
broker.Unicast(ctx, data, "topic", "user1", "") // 单播
broker.Multicast(ctx, data, "topic", []string{"u1", "u2"}, "")
broker.Broadcast(ctx, data, "topic", "")
broker.Exists("topic", "user1") // 订阅者是否存在
broker.IdList("topic") // 订阅者列表

// ---- 客户端：Prosumer 自动订阅 ----
prosumer := push.NewProsumer(client, "user1")
prosumer.OnSubscribe = func (topic string) { println("subscribed", topic) }
prosumer.OnUnsubscribe = func (topic string) { println("unsubscribed", topic) }
// 订阅主题（符号方法 "+"）：
client.Invoke("+", []interface{}{"topic"}) // 或通过 stub 声明
```

### 反向调用 reverse（服务端调度客户端）

```go
// ---- 服务端：注册 Caller，客户端需声明可被调用的方法 ----
caller := reverse.NewCaller(service)
// 客户端侧回调方法的入参/返回值与普通服务端方法一致

// ---- 客户端：Provider 承接服务端发起的调用 ----
provider := reverse.NewProvider(client, "user1")
// 通过 AddFunction/AddMethod 在 provider 上注册可被服务端调用的方法
provider.Listen() // 开始长轮询接收调用（需保持连接）
```

### 进程内 mock（测试）

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
result, _ := stub.Double(21) // 42，无网络开销
```

## HTTP/2 支持

HTTP 通道在 **不改变 scheme 与 URL** 的前提下透明升级 HTTP/2：

- **服务端**：`service.Bind(*http.Server)` 时自动启用——`http.Server.Protocols`
  同时打开 HTTP/1.1、TLS HTTP/2 与明文 h2c；明文连接先做 HTTP/2 preface 探测，
  非 h2 连接自动回落 HTTP/1.1， **老客户端零影响**（`golang.org/x/net/http2/h2c`
  已弃用，不再使用）：

```go
httpServer := &http.Server{Addr: ":8080"}
service.Bind(httpServer) // Protocols 自动配置 + Handler 绑定
go httpServer.ListenAndServe() // 明文：h2c + HTTP/1.1 兼容
// TLS：go httpServer.ListenAndServeTLS("cert.pem", "key.pem") // ALPN 自动协商 h2
```

- **客户端**：`https://` 经 ALPN 自动协商 HTTP/2；`h2c://` 显式走明文 HTTP/2
  （内网高并发网关场景）：

```go
client := core.NewClient("h2c://127.0.0.1:8080/") // 明文 HTTP/2
client2 := core.NewClient("https://example.com/") // TLS，自动 h2
```

- 说明：`http://` 明文默认保持 HTTP/1.1（不强制 h2c，保证与纯 HTTP/1.1 服务端兼容）；
  显式明文 h2 请使用 `h2c://`。fasthttp 服务器不支持 HTTP/2（HTTP/1.1 路径不受影响）。

## 性能

传输层帧调度优化（批量写 + 读缓冲，协议字节不变）后，127.0.0.1 回环压测
（Go 1.26，单连接多路复用，两轮中位数）：

| 场景            |   优化前（官方 v3.0.16） |                     优化后 |                 倍数 |
|-----------------|-------------------------:|---------------------------:|---------------------:|
| 64B × 1 并发    | 6,307 QPS（p50 0.157ms） |  17,939 QPS（p50 0.048ms） |                 2.8× |
| 64B × 16 并发   |               17,804 QPS |                109,051 QPS |                 6.1× |
| 64B × 128 并发  | 20,821 QPS（p50 5.92ms） | 492,048 QPS（p50 0.234ms） |                23.6× |
| 1KB × 128 并发  |               21,104 QPS |                380,190 QPS |                18.0× |
| 64KB × 128 并发 |                9,703 QPS |                 10,782 QPS | 1.1×（大载荷无回归） |

对照：同场景 gRPC 128 并发 64B 为 209,092 QPS——优化后单连接 hprose-tcp 约为 gRPC 的
2.4 倍。完整报告见 ShuHeSdk 仓库的 `docs/rpc-optimization-report.md` 与 `docs/rpc-benchmark-report.md`。

## 测试

```bash
go build ./... && go vet ./...
go test -race -count=1 ./...     # 全量测试（含并发正确性、h2c/h2 往返）
```

`rpc/mock` 提供进程内 mock 传输，便于在 CI 中快速编写单元测试。

## License

MIT License
