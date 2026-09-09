/*--------------------------------------------------------*\
|                                                          |
|                          hprose                          |
|                                                          |
| Official WebSite: https://hprose.com                     |
|                                                          |
| rpc/socket/transport.go                                  |
|                                                          |
| LastModified: May 22, 2021                               |
| Author: Ma Bingyao <andot@hprose.com>                    |
|                                                          |
\*________________________________________________________*/

package socket

import (
	"bufio"
	"context"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/uouuou/hprose-golang/v3/rpc/core"
)

type conn struct {
	net.Conn
	reader   *bufio.Reader // 读缓冲：头与体常在同一 TCP 段内，合并为一次系统调用
	requests chan data
	results  map[int]chan data
	lock     sync.Mutex
	counter  int32
	onClose  func(net.Conn)
	once     sync.Once
	// pool 复用每请求的结果通道（cap 1，约 96B/次）。通道可能残留超时请求的
	// 迟到响应，等待侧用完整 index 校验丢弃，因此复用是安全的。
	pool sync.Pool
	// 以下字段仅 Send goroutine 访问，用于批量写的缓冲复用。
	// bufs 是固定 scratch 数组：net.Buffers.WriteTo 会消费（前移并置 nil）切片，
	// 因此只能把它的副本交给 WriteTo，数组本身才能跨批次复用。
	bufs    [2 * maxBatchFrames][]byte
	headers [maxBatchFrames][12]byte
}

func (c *conn) getResultChan() chan data {
	if v := c.pool.Get(); v != nil {
		return v.(chan data)
	}
	return make(chan data, 1)
}

// putResultChan 归还前尽力清空残留值；即便清理与并发投递竞争，
// 复用方也会用 index 校验丢弃不属于自己的响应。
func (c *conn) putResultChan(resultChan chan data) {
	for {
		select {
		case <-resultChan:
		default:
			c.pool.Put(resultChan)
			return
		}
	}
}

func dial(ctx context.Context) (net.Conn, error) {
	u := core.GetClientContext(ctx).URL
	var d net.Dialer
	switch u.Scheme {
	case "tcp", "tcp4", "tcp6", "tls", "tls4", "tls6", "ssl", "ssl4", "ssl6":
		address := u.Host
		if u.Port() == "" {
			address += ":8412"
		}
		return d.DialContext(ctx, "tcp", address)
	case "unix", "unixpacket":
		return d.DialContext(ctx, "unix", u.Path)
	}
	return nil, core.UnsupportedProtocolError{Scheme: u.Scheme}
}

func newConn(ctx context.Context, onConnect func(net.Conn) net.Conn, onClose func(net.Conn)) (*conn, error) {
	c, err := dial(ctx)
	if err != nil {
		return nil, err
	}
	wrapped := onConnect(c)
	return &conn{
		Conn:     wrapped,
		reader:   bufio.NewReaderSize(wrapped, readBufferSize),
		requests: make(chan data, 256),
		onClose:  onClose,
		results:  make(map[int]chan data),
	}, nil
}

func (c *conn) store(index int, resultChan chan data) {
	c.lock.Lock()
	c.results[index] = resultChan
	c.lock.Unlock()
}

func (c *conn) delete(index int) {
	c.lock.Lock()
	delete(c.results, index)
	c.lock.Unlock()
}

func (c *conn) loadAndDelete(index int) (resultChan chan data, loaded bool) {
	c.lock.Lock()
	if resultChan, loaded = c.results[index]; loaded {
		delete(c.results, index)
	}
	c.lock.Unlock()
	return
}

func (c *conn) rangeAndClean(f func(index int, resultChan chan data)) {
	c.lock.Lock()
	for len(c.results) > 0 {
		results := c.results
		c.results = make(map[int]chan data)
		c.lock.Unlock()
		for index, resultChan := range results {
			f(index, resultChan)
		}
		runtime.Gosched()
		c.lock.Lock()
	}
	c.lock.Unlock()
}

func (c *conn) Transport(ctx context.Context, request []byte) (response []byte, err error) {
	index := int(atomic.AddInt32(&c.counter, 1) & 0x7fffffff)
	resultChan := c.getResultChan()
	c.store(index, resultChan)
	select {
	case <-ctx.Done():
		c.delete(index)
		return nil, ctx.Err()
	case c.requests <- data{
		Index: index,
		Body:  request,
	}:
	}
	for {
		select {
		case <-ctx.Done():
			// 超时后不归还通道：仍可能有迟到响应投递进来，交给 GC 更简单安全。
			c.delete(index)
			return nil, ctx.Err()
		case res := <-resultChan:
			if res.Index == index {
				c.putResultChan(resultChan)
				return res.Body, res.Error
			}
			// 池化复用的通道上可能残留上一个请求的迟到响应，丢弃后继续等待。
		}
	}
}

func (c *conn) Exit(onExit func(), err error) {
	onExit()
	if e := recover(); e != nil {
		err = core.NewPanicError(e)
	}
	if err != nil {
		c.Close(err)
	}
}

// sendBatch 批量写请求：阻塞取首帧后，把就绪的后续帧合并为一次系统调用写出。
// bufs/headers 仅 Send goroutine 访问，跨批次复用，无额外分配。
func (c *conn) sendBatch(first data) error {
	request := first
	count, total := 0, 0
	for {
		putHeader(c.headers[count][:], len(request.Body), request.Index)
		c.bufs[2*count] = c.headers[count][:]
		c.bufs[2*count+1] = request.Body
		count++
		total += len(request.Body)
		if count >= maxBatchFrames || total >= maxBatchBytes {
			break
		}
		select {
		case request = <-c.requests:
		default:
			return c.writeBatch(count)
		}
	}
	return c.writeBatch(count)
}

// writeBatch 把 scratch 数组前 count 帧交给 WriteTo。传切片副本，
// 使 WriteTo 内部的消费只作用于副本，c.bufs 跨批次保持可用。
func (c *conn) writeBatch(count int) error {
	buffers := net.Buffers(c.bufs[:2*count])
	_, err := buffers.WriteTo(c.Conn)
	return err
}

func (c *conn) Send(ctx context.Context, onExit func()) {
	var err error
	defer func() {
		c.Exit(onExit, err)
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case request := <-c.requests:
			if err = c.sendBatch(request); err != nil {
				return
			}
		}
	}
}

func (c *conn) receive() (err error) {
	var header [12]byte
	if _, err = io.ReadAtLeast(c.reader, header[:], 12); err != nil {
		return
	}
	length, index, ok := parseHeader(header)
	if length == 0 && index == -1 && !ok {
		err = core.InvalidResponseError{}
		return
	}
	body := make([]byte, length)
	if _, err = io.ReadAtLeast(c.reader, body, length); err != nil {
		return
	}
	if !ok {
		if string(body) == core.RequestEntityTooLarge {
			err = core.ErrRequestEntityTooLarge
		} else {
			err = core.InvalidResponseError{Response: body}
		}
		return
	}
	if resultChan, loaded := c.loadAndDelete(index); loaded {
		resultChan <- data{
			Index: index,
			Body:  body,
		}
	}
	return
}

func (c *conn) Receive(ctx context.Context, onExit func()) {
	var err error
	defer func() {
		c.Exit(onExit, err)
	}()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err = c.receive(); err != nil {
				return
			}
		}
	}
}

func (c *conn) Close(err error) {
	c.once.Do(func() {
		c.onClose(c.Conn)
		_ = c.Conn.Close()
	})
	c.rangeAndClean(func(index int, resultChan chan data) {
		resultChan <- data{
			Index: index,
			Error: err,
		}
	})
}

type Transport struct {
	OnConnect func(net.Conn) net.Conn
	OnClose   func(net.Conn)
	conns     map[string]*conn
	lock      sync.RWMutex
}

func (trans *Transport) getConn(ctx context.Context) (conn *conn, err error) {
	u := core.GetClientContext(ctx).URL
	key := u.String()
	trans.lock.RLock()
	if conn = trans.conns[key]; conn != nil {
		trans.lock.RUnlock()
		return
	}
	trans.lock.RUnlock()
	trans.lock.Lock()
	defer trans.lock.Unlock()
	if conn = trans.conns[key]; conn != nil {
		return
	}
	conn, err = newConn(ctx, trans.onConnect, trans.onClose)
	if err != nil {
		return
	}
	trans.conns[key] = conn
	ctx, cancel := context.WithCancel(context.Background())
	onExit := func() {
		trans.lock.Lock()
		if trans.conns[key] == conn {
			delete(trans.conns, key)
		}
		trans.lock.Unlock()
		// 必须无条件取消本连接的上下文：Abort()/重连会整体替换 conns，
		// 若只在命中映射项时取消，Send goroutine 会永久阻塞在请求队列上无法退出。
		cancel()
	}
	go conn.Send(ctx, onExit)
	go conn.Receive(ctx, onExit)
	return
}

func (trans *Transport) onConnect(conn net.Conn) net.Conn {
	if trans.OnConnect != nil {
		return trans.OnConnect(conn)
	}
	return conn
}

func (trans *Transport) onClose(conn net.Conn) {
	if trans.OnClose != nil {
		trans.OnClose(conn)
	}
}

func (trans *Transport) Transport(ctx context.Context, request []byte) ([]byte, error) {
	conn, err := trans.getConn(ctx)
	if err != nil {
		return nil, err
	}
	return conn.Transport(ctx, request)
}

func (trans *Transport) Abort() {
	trans.lock.Lock()
	conns := trans.conns
	trans.conns = make(map[string]*conn)
	trans.lock.Unlock()
	for _, conn := range conns {
		conn.Close(core.ErrClosed)
	}
}

type transportFactory struct {
	schemes []string
}

func (factory transportFactory) Schemes() []string {
	return factory.schemes
}

func (factory transportFactory) New() core.Transport {
	transport := &Transport{
		conns: make(map[string]*conn),
	}
	return transport
}

func RegisterTransport() {
	core.RegisterTransport("socket", transportFactory{[]string{"tcp", "tcp4", "tcp6", "tls", "tls4", "tls6", "ssl", "ssl4", "ssl6", "unix", "unixpacket"}})
}
