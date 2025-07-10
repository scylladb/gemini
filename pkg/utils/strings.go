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
	"sync"
	"unsafe"
)

const (
	uint64Size = int(unsafe.Sizeof(uint64(0)))
	hextable   = "0123456789abcdef"
)

var (
	randomString     string
	randomStringOnce sync.Once
)

func PreallocateRandomString(rnd Random, ln int) {
	randomStringOnce.Do(func() {
		length := ln
		if val := length % uint64Size; val != 0 {
			length += uint64Size - val
		}

		data := make([]byte, ln)

		for i := 0; i < length; i += uint64Size {
			number := rnd.Uint64()
			for j := range uint64Size {
				data[i+j] = hextable[(number>>j)&0x0f]
			}
		}

		randomString = unsafe.String(unsafe.SliceData(data), length)
	})
}

func RandString(rnd Random, ln int) string {
	start := rnd.IntN(len(randomString) - ln - 1)
	return randomString[start : start+ln]
}
