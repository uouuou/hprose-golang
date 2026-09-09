/*--------------------------------------------------------*\
|                                                          |
|                          hprose                          |
|                                                          |
| Official WebSite: https://hprose.com                     |
|                                                          |
| rpc/socket/common.go                                     |
|                                                          |
| LastModified: Mar 18, 2022                               |
| Author: Ma Bingyao <andot@hprose.com>                    |
|                                                          |
\*________________________________________________________*/

package socket

import (
	"hash/crc32"
	"net"
	"time"

	"github.com/uouuou/hprose-golang/v3/rpc/core"
)

type data struct {
	Index int
	Body  []byte
	Error error
}

// 单次批量写的帧数与字节上限：把同一连接上就绪的多个帧合并为一次系统调用
// （writev/WSASend 多缓冲），上限防止大消息饿死同连接上的小消息。
const (
	maxBatchFrames = 32
	maxBatchBytes  = 64 * 1024
)

// putHeader 把 12 字节帧头写入 dst（要求 len(dst) >= 12）。
// 直接写入目标缓冲而非返回值：返回值形式每次调用都会在堆上分配一个 [12]byte。
func putHeader(dst []byte, length int, index int) {
	dst[11] = byte(index & 0xff)
	dst[10] = byte(index >> 8 & 0xff)
	dst[9] = byte(index >> 16 & 0xff)
	dst[8] = byte(index >> 24 & 0xff)
	dst[7] = byte(length & 0xff)
	dst[6] = byte(length >> 8 & 0xff)
	dst[5] = byte(length >> 16 & 0xff)
	dst[4] = byte((length >> 24 & 0xff) | 0x80)
	crc := crc32.ChecksumIEEE(dst[4:12])
	dst[3] = byte(crc & 0xff)
	dst[2] = byte(crc >> 8 & 0xff)
	dst[1] = byte(crc >> 16 & 0xff)
	dst[0] = byte(crc >> 24 & 0xff)
}

func parseHeader(header [12]byte) (length int, index int, ok bool) {
	index = int(header[11]) | int(header[10])<<8 | int(header[9])<<16 | int(header[8])<<24
	length = int(header[7]) | int(header[6])<<8 | int(header[5])<<16 | int(header[4]&0x7F)<<24
	crc := uint32(header[3]) | uint32(header[2])<<8 | uint32(header[1])<<16 | uint32(header[0])<<24
	if crc32.ChecksumIEEE(header[4:]) != crc {
		index = -1
		length = 0
		return
	}
	if ok = (header[8]&0x80 == 0); !ok {
		index &= 0x7fffffff
	}
	return
}

func nextTempDelay(err error, onError func(net.Conn, error), tempDelay time.Duration) time.Duration {
	if core.IsTemporaryError(err) {
		if tempDelay == 0 {
			tempDelay = 5 * time.Millisecond
		} else {
			tempDelay *= 2
		}
		if max := 1 * time.Second; tempDelay > max {
			tempDelay = max
		}
		onError(nil, err)
		time.Sleep(tempDelay)
		return tempDelay
	}
	return 0
}
