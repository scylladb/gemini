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

package utils_test

import (
	"encoding/hex"
	randv1 "math/rand"
	randv2 "math/rand/v2"
	"strconv"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/utils"
)

func getRandom(seed ...uint64) *randv2.Rand {
	if len(seed) > 0 {
		return randv2.New(randv2.NewPCG(seed[0], seed[0]))
	}
	currentTime := uint64(time.Now().UnixNano())
	return randv2.New(randv2.NewPCG(currentTime, currentTime))
}

func TestRandString(t *testing.T) {
	t.Parallel()

	rnd := getRandom()
	utils.PreallocateRandomString(rnd, 3*1024*1024)

	for _, ln := range []int{1, 3, 5, 16, 45, 100, 1000} {
		out := utils.RandString(rnd, ln, false)
		if len(out) != ln {
			t.Fatalf("%d != %d", ln, len(out))
		}
	}
}

func TestRandomBytes(t *testing.T) {
	t.Parallel()

	rnd := getRandom()
	utils.PreallocateRandomString(rnd, 1000) // Preallocate with sufficient size

	tests := []struct {
		expected func(t *testing.T, result []byte, length int)
		name     string
		length   int
		seed     int64
	}{
		{
			name:   "returns correct length",
			length: 10,
			seed:   42,
			expected: func(t *testing.T, result []byte, length int) {
				t.Helper()
				assert.Len(t, result, length)
			},
		},
		{
			name:   "returns correct length for larger size",
			length: 100,
			seed:   42,
			expected: func(t *testing.T, result []byte, length int) {
				t.Helper()
				assert.Len(t, result, length)
			},
		},
		{
			name:   "returns single byte",
			length: 1,
			seed:   42,
			expected: func(t *testing.T, result []byte, length int) {
				t.Helper()
				assert.Len(t, result, 1)
			},
		},
		{
			name:   "different seeds produce different results",
			length: 10,
			seed:   123,
			expected: func(t *testing.T, result []byte, length int) {
				t.Helper()
				// This will be compared with other results in a separate test
				assert.Len(t, result, length)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := getRandom()
			result := utils.RandomBytes(r, tt.length)

			require.NotNil(t, result)
			tt.expected(t, result, tt.length)
		})
	}
}

func TestRandomBytes_Deterministic(t *testing.T) {
	t.Parallel()
	utils.PreallocateRandomString(getRandom(42), 1000)

	rnd1 := getRandom(100)
	rnd2 := getRandom(100)

	result1 := utils.RandomBytes(rnd1, 50)
	result2 := utils.RandomBytes(rnd2, 50)

	// Same seed should produce same random positions, thus same slices
	assert.Equal(t, result1, result2, "Same seed should produce identical results")
}

func TestRandomBytes_DifferentSeeds(t *testing.T) {
	t.Parallel()

	utils.PreallocateRandomString(getRandom(42), 1000)

	rnd1 := getRandom(100)
	rnd2 := getRandom(200)

	result1 := utils.RandomBytes(rnd1, 50)
	result2 := utils.RandomBytes(rnd2, 50)

	assert.NotEqual(t, result1, result2, "Different seeds should produce different results")
}

func RandomStringGeminiV186(rnd *randv1.Rand, ln int) string {
	buffLen := ln
	if buffLen > 32 {
		buffLen = 32
	}
	binBuff := make([]byte, buffLen/2+1)
	_, _ = rnd.Read(binBuff)
	buff := hex.EncodeToString(binBuff)[:buffLen]
	if ln <= 32 {
		return buff
	}
	out := make([]byte, ln)
	for idx := 0; idx < ln; idx += buffLen {
		copy(out[idx:], buff)
	}
	return string(out[:ln])
}

const (
	uint64Size = int(unsafe.Sizeof(uint64(0)))
	hextable   = "0123456789abcdef"
)

func CurrentImplementation(rnd utils.Random, ln int) string {
	length := ln
	if val := length % uint64Size; val != 0 {
		length += uint64Size - val
	}

	binBuff := make([]byte, length)

	for i := 0; i < length; i += uint64Size {
		number := rnd.Uint64()
		for j := 0; j < uint64Size; j++ {
			binBuff[i+j] = hextable[(number>>j)&0x0f]
		}
	}

	return unsafe.String(unsafe.SliceData(binBuff), length)[:ln]
}

func BenchmarkRandString(b *testing.B) {
	b.ReportAllocs()

	sizes := []int{1, 5, 10, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536}

	b.Run("Gemini_V186", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for _, size := range sizes {
			b.Run("Size_"+strconv.FormatInt(int64(size), 10), func(b *testing.B) {
				rand := randv1.New(randv1.NewSource(1))

				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					RandomStringGeminiV186(rand, size)
				}
			})
		}
	})

	b.Run("Current_Implementation", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for _, size := range sizes {
			b.Run("Size_"+strconv.FormatInt(int64(size), 10), func(b *testing.B) {
				rand := randv2.New(randv2.NewChaCha8([32]byte{}))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					CurrentImplementation(rand, size)
				}
			})
		}
	})

	b.Run("Fast_Implementation", func(b *testing.B) {
		utils.PreallocateRandomString(randv2.New(randv2.NewChaCha8([32]byte{})), 100*1024*1024) // Preallocate a large enough string
		b.ReportAllocs()
		b.ResetTimer()
		for _, size := range sizes {
			b.Run("Size_"+strconv.FormatInt(int64(size), 10), func(b *testing.B) {
				rand := randv2.New(randv2.NewChaCha8([32]byte{}))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					utils.RandString(rand, size, false)
				}
			})
		}
	})
}
