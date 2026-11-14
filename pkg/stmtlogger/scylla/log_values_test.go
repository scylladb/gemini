// Copyright 2026 ScyllaDB
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

package scylla

import (
	"math/big"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/samber/mo"
	"github.com/stretchr/testify/require"
	"gopkg.in/inf.v0"
)

// textListType is the frozen<list<text>> type of the values column.
var textListType = gocql.CollectionType{
	NativeType: gocql.NewNativeType(4, gocql.TypeList),
	Elem:       gocql.NewNativeType(4, gocql.TypeVarchar),
}

// TestEncodeLogValuesMatchesDriver checks that encodeLogValues gives the same
// bytes that gocql writes for the []string of prepareValuesOptimized. The log
// rows must not change when the committer binds the pre-encoded column.
func TestEncodeLogValuesMatchesDriver(t *testing.T) {
	t.Parallel()

	tests := []struct {
		values mo.Either[[]any, []byte]
		name   string
	}{
		{name: "nil", values: mo.Left[[]any, []byte](nil)},
		{name: "empty", values: mo.Left[[]any, []byte]([]any{})},
		{name: "raw bytes", values: mo.Right[[]any, []byte]([]byte("raw data"))},
		{name: "empty raw bytes", values: mo.Right[[]any, []byte]([]byte{})},
		{name: "every fast type", values: mo.Left[[]any, []byte]([]any{
			nil, "quoted \"text\"\n",
			[]byte{0xde, 0xad},
			true,
			int(-1), int8(2), int16(-3), int32(4), int64(-5),
			float32(1.5), float64(-2.25),
			gocql.TimeUUID(), 3 * time.Second,
			time.Date(2026, 11, 14, 10, 0, 0, 5, time.UTC),
			big.NewInt(-12345678901234), (*big.Int)(nil),
			inf.NewDec(31415, 4),
		})},
		{name: "fallback type", values: mo.Left[[]any, []byte]([]any{[]int{1, 2}, map[string]int{"a": 1}})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			want, err := gocql.Marshal(textListType, prepareValuesOptimized(tt.values))
			require.NoError(t, err)

			got, err := gocql.Marshal(textListType, encodeLogValues(tt.values))
			require.NoError(t, err)

			require.Equal(t, want, got)
		})
	}
}

func BenchmarkLogValues(b *testing.B) {
	values := mo.Left[[]any, []byte]([]any{
		[]any{"set element one", "set element two", "three"},
		int64(42), "some text value", gocql.TimeUUID(), int32(7),
		[]byte{1, 2, 3, 4, 5, 6, 7, 8},
		true, time.Now(), int16(3),
	})

	b.Run("strings", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := gocql.Marshal(textListType, prepareValuesOptimized(values)); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("encoded", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := gocql.Marshal(textListType, encodeLogValues(values)); err != nil {
				b.Fatal(err)
			}
		}
	})
}
