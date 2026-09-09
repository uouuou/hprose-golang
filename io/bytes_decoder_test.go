/*--------------------------------------------------------*\
|                                                          |
|                          hprose                          |
|                                                          |
| Official WebSite: https://hprose.com                     |
|                                                          |
| io/bytes_decoder_test.go                                 |
|                                                          |
| LastModified: Apr 27, 2021                               |
| Author: Ma Bingyao <andot@hprose.com>                    |
|                                                          |
\*________________________________________________________*/

package io_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	. "github.com/uouuou/hprose-golang/v3/io"
)

func TestDecodeBytes(t *testing.T) {
	sb := new(strings.Builder)
	enc := NewEncoder(sb)
	enc.Encode("字")
	enc.Encode("Pokémon")
	enc.Encode("中文")
	enc.Encode("🐱🐶")
	enc.Encode("👩‍👩‍👧‍👧")
	enc.Encode("")
	enc.Encode(nil)
	enc.Encode([]byte{1, 2, 3, 4, 5})
	enc.Encode([]int{1, 2, 3, 4, 5})
	enc.Encode(1)
	dec := NewDecoder(([]byte)(sb.String()))
	var b []byte
	dec.Decode(&b)
	assert.Equal(t, []byte("字"), b) // "字"
	dec.Decode(&b)
	assert.Equal(t, []byte("Pokémon"), b) // "Pokémon"
	dec.Decode(&b)
	assert.Equal(t, []byte("中文"), b) // "中文"
	dec.Decode(&b)
	assert.Equal(t, []byte("🐱🐶"), b) // "🐱🐶"
	dec.Decode(&b)
	assert.Equal(t, []byte("👩‍👩‍👧‍👧"), b) // "👩‍👩‍👧‍👧"
	dec.Decode(&b)
	assert.Equal(t, []byte(""), b) // ""
	dec.Decode(&b)
	assert.Equal(t, []byte(nil), b) // nil
	dec.Decode(&b)
	assert.Equal(t, []byte{1, 2, 3, 4, 5}, b) // []byte{1, 2, 3, 4, 5}
	dec.Decode(&b)
	assert.Equal(t, []byte{1, 2, 3, 4, 5}, b) // []int{1, 2, 3, 4, 5}
	dec.Decode(&b)
	assert.EqualError(t, dec.Error, `hprose/io: can not cast int to []uint8`) // 1
}

func TestDecodeBytesPtr(t *testing.T) {
	sb := new(strings.Builder)
	enc := NewEncoder(sb)
	enc.Encode("字")
	enc.Encode("Pokémon")
	enc.Encode("中文")
	enc.Encode("🐱🐶")
	enc.Encode("👩‍👩‍👧‍👧")
	enc.Encode("")
	enc.Encode(nil)
	enc.Encode([]byte{1, 2, 3, 4, 5})
	enc.Encode([]int{1, 2, 3, 4, 5})
	enc.Encode(1)
	dec := NewDecoder(([]byte)(sb.String()))
	var b *[]byte
	dec.Decode(&b)
	assert.Equal(t, []byte("字"), *b) // "字"
	dec.Decode(&b)
	assert.Equal(t, []byte("Pokémon"), *b) // "Pokémon"
	dec.Decode(&b)
	assert.Equal(t, []byte("中文"), *b) // "中文"
	dec.Decode(&b)
	assert.Equal(t, []byte("🐱🐶"), *b) // "🐱🐶"
	dec.Decode(&b)
	assert.Equal(t, []byte("👩‍👩‍👧‍👧"), *b) // "👩‍👩‍👧‍👧"
	dec.Decode(&b)
	assert.Equal(t, []byte(""), *b) // ""
	dec.Decode(&b)
	assert.Equal(t, (*[]byte)(nil), b) // nil
	dec.Decode(&b)
	assert.Equal(t, []byte{1, 2, 3, 4, 5}, *b) // []byte{1, 2, 3, 4, 5}
	dec.Decode(&b)
	assert.Equal(t, []byte{1, 2, 3, 4, 5}, *b) // []int{1, 2, 3, 4, 5}
	dec.Decode(&b)
	assert.EqualError(t, dec.Error, `hprose/io: can not cast int to *[]uint8`) // 1
}
