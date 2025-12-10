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

package store

import (
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/go-set/strset"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/inf.v0"

	"github.com/scylladb/gemini/pkg/typedef"
)

func Test_formatRows_AllTypes(t *testing.T) {
	t.Parallel()

	var sb stringsBuilder

	// Float32/64
	assert.Equal(t, "f32=3.14", formatRows(sb.reset(), "f32", float32(3.14)))
	assert.Equal(t, "f64=3.14", formatRows(sb.reset(), "f64", float64(3.14)))

	// Various ints / uints
	assert.Equal(t, "i8=8", formatRows(sb.reset(), "i8", int8(8)))
	assert.Equal(t, "i16=16", formatRows(sb.reset(), "i16", int16(16)))
	assert.Equal(t, "i=42", formatRows(sb.reset(), "i", int(42)))
	assert.Equal(t, "i32=32", formatRows(sb.reset(), "i32", int32(32)))
	assert.Equal(t, "i64=64", formatRows(sb.reset(), "i64", int64(64)))

	assert.Equal(t, "u=7", formatRows(sb.reset(), "u", uint(7)))
	assert.Equal(t, "u8=8", formatRows(sb.reset(), "u8", uint8(8)))
	assert.Equal(t, "u16=16", formatRows(sb.reset(), "u16", uint16(16)))
	assert.Equal(t, "u32=32", formatRows(sb.reset(), "u32", uint32(32)))
	assert.Equal(t, "u64=64", formatRows(sb.reset(), "u64", uint64(64)))

	// UUID
	uuid := gocql.TimeUUID()
	assert.Equal(t, "id="+uuid.String(), formatRows(sb.reset(), "id", uuid))

	// time.Time
	ts := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	assert.Equal(t, "ts=2025-01-02 03:04:05", formatRows(sb.reset(), "ts", ts))

	// *big.Int
	bi := big.NewInt(1234567890)
	assert.Equal(t, "bi=1234567890", formatRows(sb.reset(), "bi", bi))

	// *inf.Dec
	dec := inf.NewDec(1234, 2) // 12.34
	assert.Equal(t, "dec=12.34", formatRows(sb.reset(), "dec", dec))

	// nil
	assert.Equal(t, "n=", formatRows(sb.reset(), "n", nil))

	// []byte
	assert.Equal(t, "b=abc", formatRows(sb.reset(), "b", []byte("abc")))

	// string
	assert.Equal(t, "s=hello", formatRows(sb.reset(), "s", "hello"))
}

func Test_pks_WithAndWithoutClusteringKeys(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		PartitionKeys:  []typedef.ColumnDef{{Name: "pk0", Type: typedef.TypeInt}},
		ClusteringKeys: []typedef.ColumnDef{{Name: "ck0", Type: typedef.TypeInt}},
	}

	rows := Rows{
		NewRow([]string{"pk0", "ck0", "v"}, []any{1, 10, "a"}),
		NewRow([]string{"pk0", "ck0", "v"}, []any{2, 20, "b"}),
	}

	set := pks(table, rows)
	require.IsType(t, &strset.Set{}, set)
	// The set should contain composite keys for each row
	assert.Equal(t, 2, set.Size(), "Expected 2 unique composite keys")
	assert.True(t, set.Has("pk0=1,ck0=10"))
	assert.True(t, set.Has("pk0=2,ck0=20"))

	// Without clustering keys
	table2 := &typedef.Table{PartitionKeys: []typedef.ColumnDef{{Name: "pk0", Type: typedef.TypeInt}}}
	set2 := pks(table2, rows)
	assert.Equal(t, 2, set2.Size(), "Expected 2 unique partition keys")
	assert.True(t, set2.Has("pk0=1"))
	assert.True(t, set2.Has("pk0=2"))
}

// stringsBuilder is a tiny helper to reuse a builder in tests
type stringsBuilder struct{ b strings.Builder }

func (s *stringsBuilder) reset() *strings.Builder { s.b.Reset(); return &s.b }
