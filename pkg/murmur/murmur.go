// Copyright 2019 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package murmur

import (
	"hash"
	"unsafe"
)

const (
	c1    int64 = -8663945395140668459 // 0x87c37b91114253d5
	c2    int64 = 5545529020109919103  // 0x4cf5ad432745937f
	fmix1 int64 = -49064778989728563   // 0xff51afd7ed558ccd
	fmix2 int64 = -4265267296055464877 // 0xc4ceb9fe1a85ec53
)

func fmix(n int64) int64 {
	// cast to unsigned for logical right bitshift (to match C* MM3 implementation)
	n ^= int64(uint64(n) >> 33)
	n *= fmix1
	n ^= int64(uint64(n) >> 33)
	n *= fmix2
	n ^= int64(uint64(n) >> 33)

	return n
}

func block(p byte) int64 {
	return int64(int8(p))
}

func rotl(x int64, r uint8) int64 {
	// cast to unsigned for logical right bitshift (to match C* MM3 implementation)
	return (x << r) | (int64)((uint64(x) >> (64 - r)))
}

func Murmur3H1(data []byte) int64 {
	length := len(data)

	var h1, h2, k1, k2 int64

	// body
	nBlocks := length / 16
	for i := 0; i < nBlocks; i++ {
		k1, k2 = getBlock(data, i)

		k1 *= c1
		k1 = rotl(k1, 31)
		k1 *= c2
		h1 ^= k1

		h1 = rotl(h1, 27)
		h1 += h2
		h1 = h1*5 + 0x52dce729

		k2 *= c2
		k2 = rotl(k2, 33)
		k2 *= c1
		h2 ^= k2

		h2 = rotl(h2, 31)
		h2 += h1
		h2 = h2*5 + 0x38495ab5
	}

	// tail
	tail := data[nBlocks*16:]
	k1 = 0
	k2 = 0
	switch length & 15 {
	case 15:
		k2 ^= block(tail[14]) << 48
		fallthrough
	case 14:
		k2 ^= block(tail[13]) << 40
		fallthrough
	case 13:
		k2 ^= block(tail[12]) << 32
		fallthrough
	case 12:
		k2 ^= block(tail[11]) << 24
		fallthrough
	case 11:
		k2 ^= block(tail[10]) << 16
		fallthrough
	case 10:
		k2 ^= block(tail[9]) << 8
		fallthrough
	case 9:
		k2 ^= block(tail[8])

		k2 *= c2
		k2 = rotl(k2, 33)
		k2 *= c1
		h2 ^= k2

		fallthrough
	case 8:
		k1 ^= block(tail[7]) << 56
		fallthrough
	case 7:
		k1 ^= block(tail[6]) << 48
		fallthrough
	case 6:
		k1 ^= block(tail[5]) << 40
		fallthrough
	case 5:
		k1 ^= block(tail[4]) << 32
		fallthrough
	case 4:
		k1 ^= block(tail[3]) << 24
		fallthrough
	case 3:
		k1 ^= block(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= block(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= block(tail[0])

		k1 *= c1
		k1 = rotl(k1, 31)
		k1 *= c2
		h1 ^= k1
	}

	h1 ^= int64(length)
	h2 ^= int64(length)

	h1 += h2
	h2 += h1

	h1 = fmix(h1)
	h2 = fmix(h2)

	h1 += h2
	// the following is extraneous since h2 is discarded
	// h2 += h1

	return h1
}

type Hash struct {
	h1, h2, k1, k2 int64

	tail     [16]byte
	tailLen  int
	totalLen int
}

func (h *Hash) Sum(b []byte) []byte {
	out := Murmur3H1(b)
	return (*(*[16]byte)(unsafe.Pointer(&out)))[:]
}

func (h *Hash) Reset() {
	h.h1 = 0
	h.h2 = 0
	h.k1 = 0
	h.k2 = 0
	h.tailLen = 0
	h.totalLen = 0
}

func (h *Hash) Size() int {
	return 16
}

func (h *Hash) BlockSize() int {
	return 16
}

func (h *Hash) Sum64() uint64 {
	h1, h2, k1, k2 := h.h1, h.h2, int64(0), int64(0)

	switch h.tailLen & 15 {
	case 15:
		k2 ^= block(h.tail[14]) << 48
		fallthrough
	case 14:
		k2 ^= block(h.tail[13]) << 40
		fallthrough
	case 13:
		k2 ^= block(h.tail[12]) << 32
		fallthrough
	case 12:
		k2 ^= block(h.tail[11]) << 24
		fallthrough
	case 11:
		k2 ^= block(h.tail[10]) << 16
		fallthrough
	case 10:
		k2 ^= block(h.tail[9]) << 8
		fallthrough
	case 9:
		k2 ^= block(h.tail[8])

		k2 *= c2
		k2 = rotl(k2, 33)
		k2 *= c1
		h2 ^= k2

		fallthrough
	case 8:
		k1 ^= block(h.tail[7]) << 56
		fallthrough
	case 7:
		k1 ^= block(h.tail[6]) << 48
		fallthrough
	case 6:
		k1 ^= block(h.tail[5]) << 40
		fallthrough
	case 5:
		k1 ^= block(h.tail[4]) << 32
		fallthrough
	case 4:
		k1 ^= block(h.tail[3]) << 24
		fallthrough
	case 3:
		k1 ^= block(h.tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= block(h.tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= block(h.tail[0])

		k1 *= c1
		k1 = rotl(k1, 31)
		k1 *= c2
		h1 ^= k1
	}

	h1 ^= int64(h.totalLen)
	h2 ^= int64(h.totalLen)

	h1 += h2
	h2 += h1

	h1 = fmix(h1)
	h2 = fmix(h2)

	h1 += h2
	// the following is extraneous since h2 is discarded
	// h2 += h1

	return uint64(h1)
}

var _ hash.Hash64 = (*Hash)(nil)

func (h *Hash) Write(data []byte) (n int, err error) {
	h.Write2(data...)
	return len(data), nil
}

func (h *Hash) Write2(data ...byte) {
	originalLen := len(data)
	h.totalLen += originalLen

	h1, h2, k1, k2 := h.h1, h.h2, h.k1, h.k2

	if h.tailLen > 0 {
		n := copy(h.tail[h.tailLen:], data)
		h.tailLen += n
		if h.tailLen < 16 {
			return
		}
		k1, k2 = getBlock(h.tail[:], 0)
		k1 *= c1
		k1 = rotl(k1, 31)
		k1 *= c2
		h1 ^= k1

		h1 = rotl(h1, 27)
		h1 += h2
		h1 = h1*5 + 0x52dce729

		k2 *= c2
		k2 = rotl(k2, 33)
		k2 *= c1
		h2 ^= k2

		h2 = rotl(h2, 31)
		h2 += h1
		h2 = h2*5 + 0x38495ab5

		h.tailLen = 0
		data = data[n:]
	}
	length := len(data)

	// body
	nBlocks := length / 16
	for i := 0; i < nBlocks; i++ {
		k1, k2 = getBlock(data, i)

		k1 *= c1
		k1 = rotl(k1, 31)
		k1 *= c2
		h1 ^= k1

		h1 = rotl(h1, 27)
		h1 += h2
		h1 = h1*5 + 0x52dce729

		k2 *= c2
		k2 = rotl(k2, 33)
		k2 *= c1
		h2 ^= k2

		h2 = rotl(h2, 31)
		h2 += h1
		h2 = h2*5 + 0x38495ab5
	}

	h.h1, h.h2, h.k1, h.k2 = h1, h2, k1, k2

	tail := length - nBlocks*16
	if tail > 0 {
		h.tailLen = copy(h.tail[:], data[nBlocks*16:])
	}
	return
}
