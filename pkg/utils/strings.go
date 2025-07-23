// Copyright 2025 ScyllaDB
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

package utils

import (
	"encoding/hex"
	"math"
	"sync"
	"unsafe"

	"github.com/shirou/gopsutil/v4/mem"
)

const (
	uint64Size = int(unsafe.Sizeof(uint64(0)))
	hextable   = "0123456789abcdef"
)

var (
	randomString     string
	randomBytes      []byte
	randomStringOnce sync.Once
)

func PreallocateRandomString(rnd Random, ln int) {
	randomStringOnce.Do(func() {
		vmStat, err := mem.VirtualMemory()
		if err != nil {
			panic(err)
		}

		length := ln
		if val := length % uint64Size; val != 0 {
			length += uint64Size - val
		}

		if uint64(2*length) >= vmStat.Total/2 {
			panic("PreallocateRandomString: requested length is too large, you'll be using more then half of the available memory")
		}

		data := make([]byte, ln)

		for i := 0; i < length; i += uint64Size {
			number := rnd.Uint64()
			for j := range uint64Size {
				data[i+j] = byte(number >> j & 0xFF)
			}
		}

		randomBytes = data

		if ln&1 == 1 {
			ln++ // Ensure the length is even
		}

		randomString = hex.EncodeToString(data)[:ln]
	})
}

func RandString(rnd Random, ln int, even bool) string {
	val := len(randomString) - ln - 1
	if val < 1 {
		val = int(math.Abs(float64(val)))
	}
	start := rnd.IntN(val)
	end := start + ln

	if even && start&1 == 1 {
		end-- // Ensure the string is an even length
	}

	return randomString[start:end]
}

func RandomBytes(rnd Random, ln int) []byte {
	val := len(randomBytes) - ln - 1
	if val < 1 {
		val = int(math.Abs(float64(val)))
	}
	start := rnd.IntN(val)
	end := start + ln

	return randomBytes[start:end]
}
