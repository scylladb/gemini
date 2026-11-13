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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/shirou/gopsutil/v4/mem"
)

// KeyspaceTableFromQuery extracts the "keyspace.table" target of a
// machine-generated mutation (INSERT INTO <kt> ..., UPDATE <kt> SET ...,
// DELETE FROM <kt> WHERE ...). Returns "" when it cannot be determined.
//
// The store and statement logger are constructed once but shared across every
// table in the schema, so any per-table logic (compensation targeting, _logs
// routing) must derive the table from the statement's own query — the only
// thing that identifies which table it hit.
func KeyspaceTableFromQuery(query string) string {
	if kt, ok := keyspaceTableCache.Load(query); ok {
		return mustString(kt)
	}

	kt := parseKeyspaceTable(query)
	if keyspaceTableCacheSize.Load() < keyspaceTableCacheLimit {
		if _, loaded := keyspaceTableCache.LoadOrStore(query, kt); !loaded {
			keyspaceTableCacheSize.Add(1)
		}
	}

	return kt
}

// keyspaceTableCache maps a query to its "keyspace.table". Gemini binds its
// values, so a run has only a few dozen distinct query strings, and every
// logged statement looks one up. keyspaceTableCacheLimit stops it from growing
// when queries carry literals, as DDL statements do.
var (
	keyspaceTableCache     sync.Map
	keyspaceTableCacheSize atomic.Int64
)

const keyspaceTableCacheLimit = 4096

func mustString(v any) string {
	s, ok := v.(string)
	if !ok {
		panic(fmt.Sprintf("keyspaceTableCache holds %T, want string", v))
	}

	return s
}

func parseKeyspaceTable(query string) string {
	fields := strings.Fields(query)
	for i, f := range fields {
		switch strings.ToUpper(f) {
		case "INTO", "UPDATE", "FROM":
			if i+1 < len(fields) {
				kt := fields[i+1]
				// INSERT renders the table glued to its column list: "ks.t(c0,..".
				if p := strings.IndexByte(kt, '('); p >= 0 {
					kt = kt[:p]
				}
				return kt
			}
		}
	}
	return ""
}

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

		data := make([]byte, length)
		fillRandomBytes(rnd, data)

		data = data[:ln]

		randomBytes = data

		if ln&1 == 1 {
			ln++ // Ensure the length is even
		}

		// Encode only the bytes the string needs. Slicing a full-length
		// encoding would keep its whole backing array, twice the buffer size,
		// alive for the run. The result is the same: ln/2 bytes give the
		// first ln hex characters.
		randomString = hex.EncodeToString(data[:ln/2])
	})
}

// fillRandomBytes fills data, whose length is a multiple of 8, with the
// little-endian bytes of successive rnd.Uint64 values, so every byte carries
// 8 fresh random bits.
func fillRandomBytes(rnd Random, data []byte) {
	for i := 0; i < len(data); i += uint64Size {
		binary.LittleEndian.PutUint64(data[i:], rnd.Uint64())
	}
}

func RandString(rnd Random, ln int, even bool) string {
	// Ensure randomString is initialized before accessing
	if len(randomString) == 0 {
		return ""
	}

	val := len(randomString) - ln - 1
	if val < 1 {
		val = int(math.Abs(float64(val)))
	}

	// Ensure we don't exceed bounds
	if val <= 0 {
		val = 1
	}

	start := rnd.IntN(val)
	end := start + ln

	// Ensure end doesn't exceed randomString length
	if end > len(randomString) {
		end = len(randomString)
		start = max(end-ln, 0)
	}

	if even && start&1 == 1 {
		end-- // Ensure the string is an even length
		// Re-check bounds after adjustment
		if end <= start {
			end = start + 1
		}
	}

	return randomString[start:end]
}

func RandomBytes(rnd Random, ln int) []byte {
	// Ensure randomBytes is initialized before accessing
	if len(randomBytes) == 0 {
		return []byte{}
	}

	val := len(randomBytes) - ln - 1
	if val < 1 {
		val = int(math.Abs(float64(val)))
	}

	// Ensure we don't exceed bounds
	if val <= 0 {
		val = 1
	}

	start := rnd.IntN(val)
	end := start + ln

	// Ensure end doesn't exceed randomBytes length
	if end > len(randomBytes) {
		end = len(randomBytes)
		start = max(end-ln, 0)
	}

	return randomBytes[start:end]
}
