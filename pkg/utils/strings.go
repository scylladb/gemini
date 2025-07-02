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
	"strconv"
	"unsafe"

	"golang.org/x/exp/constraints"
)

const (
	uint64Size = int(unsafe.Sizeof(uint64(0)))
	hextable   = "0123456789abcdef"
)

func RandString(rnd Random, ln int) string {
	length := ln
	if val := length % uint64Size; val != 0 {
		length += uint64Size - val
	}

	binBuff := make([]byte, length)

	for i := 0; i < length; i += uint64Size {
		number := rnd.Int64()
		for j := 0; j < uint64Size; j++ {
			binBuff[i+j] = hextable[(number>>j)&0x0f]
		}
	}

	return unsafe.String(unsafe.SliceData(binBuff), length)[:ln]
}

func FormatString[T constraints.Integer](dst []byte, integer T) string {
	dst = dst[:0]
	conv := strconv.AppendInt(dst, int64(integer), 10)

	return unsafe.String(unsafe.SliceData(conv), len(conv))
}
