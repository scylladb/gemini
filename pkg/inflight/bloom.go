// Copyright 2025 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package inflight

import (
	"sync/atomic"
)

type BoomFilterInflight struct {
	bits []atomic.Uint64
	mask uint64
}

func NewBloom(mBits uint64) *BoomFilterInflight {
	return &BoomFilterInflight{
		bits: make([]atomic.Uint64, mBits>>6),
		mask: mBits - 1,
	}
}

func (b *BoomFilterInflight) AddIfNotPresent(value uint64) bool {
	idx := murmurMix64(value) & b.mask // fast "mod m"
	word := idx >> 6                   // /64
	mask := uint64(1) << (idx & 63)    // %64
	partition := &b.bits[word]
	for {
		old := partition.Load()
		if old&mask != 0 {
			return false // bit already set ⇒ duplicate
		}

		if partition.CompareAndSwap(old, old|mask) {
			return true // successful insertion
		}
	}
}

func (b *BoomFilterInflight) Delete(value uint64) {
	idx := murmurMix64(value) & b.mask
	word := idx >> 6
	mask := uint64(1) << (idx & 63)
	partition := &b.bits[word]

	for {
		old := partition.Load()
		newValue := old &^ mask
		if partition.CompareAndSwap(old, newValue) {
			return
		}
	}
}

func (b *BoomFilterInflight) Has(value uint64) bool {
	idx := murmurMix64(value) & b.mask
	word := idx >> 6
	mask := uint64(1) << (idx & 63)
	return b.bits[word].Load()&mask != 0
}

func murmurMix64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	return x ^ (x >> 33)
}
