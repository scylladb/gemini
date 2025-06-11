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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/go-set/strset"
	"github.com/stretchr/testify/assert"
	"gopkg.in/inf.v0"

	"github.com/scylladb/gemini/pkg/typedef"
)

func TestPks(t *testing.T) {
	// Define test cases
	tests := []struct {
		table    *typedef.Table
		expected *strset.Set
		name     string
		rows     Rows
	}{
		{
			name: "empty rows",
			table: &typedef.Table{
				PartitionKeys:  typedef.Columns{},
				ClusteringKeys: typedef.Columns{},
			},
			rows:     Rows{},
			expected: nil,
		},
		{
			name: "partition keys only",
			table: &typedef.Table{
				PartitionKeys: typedef.Columns{
					{Name: "pk0"},
					{Name: "pk1"},
				},
				ClusteringKeys: typedef.Columns{},
			},
			rows: Rows{
				{"pk0": "value1", "pk1": 42},
			},
			expected: func() *strset.Set {
				s := strset.New()
				s.Add("pk0=value1")
				s.Add("pk1=42")
				return s
			}(),
		},
		{
			name: "clustering keys only",
			table: &typedef.Table{
				PartitionKeys: typedef.Columns{},
				ClusteringKeys: typedef.Columns{
					{Name: "ck0"},
					{Name: "ck1"},
				},
			},
			rows: Rows{
				{"ck0": "valueA", "ck1": 24},
			},
			expected: func() *strset.Set {
				s := strset.New()
				s.Add("ck0=valueA")
				s.Add("ck1=24")
				return s
			}(),
		},
		{
			name: "both partition and clustering keys",
			table: &typedef.Table{
				PartitionKeys: typedef.Columns{
					{Name: "pk0"},
				},
				ClusteringKeys: typedef.Columns{
					{Name: "ck0"},
				},
			},
			rows: Rows{
				{"pk0": "value1", "ck0": "valueA"},
			},
			expected: func() *strset.Set {
				s := strset.New()
				s.Add("pk0=value1")
				s.Add("ck0=valueA")
				return s
			}(),
		},
		{
			name: "multiple rows",
			table: &typedef.Table{
				PartitionKeys: typedef.Columns{
					{Name: "pk0"},
				},
				ClusteringKeys: typedef.Columns{
					{Name: "ck0"},
				},
			},
			rows: Rows{
				{"pk0": "value1", "ck0": "valueA"},
				{"pk0": "value2", "ck0": "valueB"},
			},
			expected: func() *strset.Set {
				s := strset.New()
				s.Add("pk0=value1")
				s.Add("pk0=value2")
				s.Add("ck0=valueA")
				s.Add("ck0=valueB")
				return s
			}(),
		},
		{
			name: "different data types",
			table: &typedef.Table{
				PartitionKeys: typedef.Columns{
					{Name: "pk_string"},
					{Name: "pk_int"},
					{Name: "pk_float"},
					{Name: "pk_bool"},
				},
				ClusteringKeys: typedef.Columns{},
			},
			rows: Rows{
				{
					"pk_string": "test",
					"pk_int":    42,
					"pk_float":  3.14,
					"pk_bool":   true,
				},
			},
			expected: func() *strset.Set {
				s := strset.New()
				s.Add("pk_string=test")
				s.Add("pk_int=42")
				s.Add("pk_float=3.14")
				s.Add("pk_bool=true")
				return s
			}(),
		},
		{
			name: "missing key in row",
			table: &typedef.Table{
				PartitionKeys: typedef.Columns{
					{Name: "pk0"},
					{Name: "missing"},
				},
				ClusteringKeys: typedef.Columns{},
			},
			rows: Rows{
				{"pk0": "value1"},
			},
			expected: func() *strset.Set {
				s := strset.New()
				s.Add("pk0=value1")
				s.Add("missing=")
				return s
			}(),
		},
		{
			name: "zero values",
			table: &typedef.Table{
				PartitionKeys: typedef.Columns{
					{Name: "pk_int"},
					{Name: "pk_string"},
					{Name: "pk_bool"},
				},
				ClusteringKeys: typedef.Columns{},
			},
			rows: Rows{
				{
					"pk_int":    0,
					"pk_string": "",
					"pk_bool":   false,
				},
			},
			expected: func() *strset.Set {
				s := strset.New()
				s.Add("pk_int=0")
				s.Add("pk_string=")
				s.Add("pk_bool=false")
				return s
			}(),
		},
		{
			name: "byte slice value",
			table: &typedef.Table{
				PartitionKeys: typedef.Columns{
					{Name: "pk_bytes"},
				},
				ClusteringKeys: typedef.Columns{},
			},
			rows: Rows{
				{"pk_bytes": []byte("hello")},
			},
			expected: func() *strset.Set {
				s := strset.New()
				s.Add("pk_bytes=hello")
				return s
			}(),
		},
	}

	// Run tests
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pks(tt.table, tt.rows)

			if tt.expected == nil {
				assert.Nil(t, result)
				return
			}

			assert.NotNil(t, result)

			// Check that all expected keys are in the result
			for _, key := range tt.expected.List() {
				assert.True(t, result.Has(key), "expected key '%s' not found in result", key)
			}

			// Check that result doesn't have extra keys
			assert.Equal(t, tt.expected.Size(), result.Size(), "result has different number of keys than expected")

			// Alternative check that sets are equal
			assert.True(t, tt.expected.IsEqual(result), "result set is not equal to expected set")
		})
	}
}

func TestFormatRows(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    any
		expected string
	}{
		{
			name:     "string value",
			key:      "mykey",
			value:    "test",
			expected: "mykey=test",
		},
		{
			name:     "int value",
			key:      "id",
			value:    42,
			expected: "id=42",
		},
		{
			name:     "int32 value",
			key:      "id32",
			value:    int32(42),
			expected: "id32=42",
		},
		{
			name:     "int64 value",
			key:      "id64",
			value:    int64(9223372036854775807),
			expected: "id64=9223372036854775807",
		},
		{
			name:     "byte slice value",
			key:      "data",
			value:    []byte("hello"),
			expected: "data=hello",
		},
		{
			name:     "empty byte slice",
			key:      "empty",
			value:    []byte{},
			expected: "empty=",
		},
		{
			name:     "bool true value",
			key:      "flag",
			value:    true,
			expected: "flag=true",
		},
		{
			name:     "bool false value",
			key:      "flag",
			value:    false,
			expected: "flag=false",
		},
		{
			name:     "nil value",
			key:      "nothing",
			value:    nil,
			expected: "nothing=",
		},
		{
			name:     "float value",
			key:      "pi",
			value:    3.14159,
			expected: "pi=3.14159",
		},
		// Adding tests for all the types in formatRows function
		{
			name:     "int8 value",
			key:      "int8",
			value:    int8(127),
			expected: "int8=127",
		},
		{
			name:     "int16 value",
			key:      "int16",
			value:    int16(32767),
			expected: "int16=32767",
		},
		{
			name:     "uint value",
			key:      "uint",
			value:    uint(42),
			expected: "uint=42",
		},
		{
			name:     "uint8 value",
			key:      "uint8",
			value:    uint8(255),
			expected: "uint8=255",
		},
		{
			name:     "uint16 value",
			key:      "uint16",
			value:    uint16(65535),
			expected: "uint16=65535",
		},
		{
			name:     "uint32 value",
			key:      "uint32",
			value:    uint32(4294967295),
			expected: "uint32=4294967295",
		},
		{
			name:     "uint64 value",
			key:      "uint64",
			value:    uint64(18446744073709551615),
			expected: "uint64=18446744073709551615",
		},
		{
			name:     "float32 value",
			key:      "float32",
			value:    float32(3.14),
			expected: "float32=3.14",
		},
		{
			name: "gocql.UUID value",
			key:  "uuid",
			value: func() gocql.UUID {
				uuid, _ := gocql.ParseUUID("550e8400-e29b-41d4-a716-446655440000")
				return uuid
			}(),
			expected: "uuid=550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:     "time.Time value",
			key:      "timestamp",
			value:    time.Date(2025, 6, 11, 12, 34, 56, 0, time.UTC),
			expected: "timestamp=2025-06-11 12:34:56",
		},
		{
			name:     "*big.Int value",
			key:      "bigint",
			value:    big.NewInt(9223372036854775807),
			expected: "bigint=9223372036854775807",
		},
		{
			name: "*inf.Dec value",
			key:  "decimal",
			value: func() *inf.Dec {
				d := new(inf.Dec)
				d.SetString("123456.789")
				return d
			}(),
			expected: "decimal=123456.789",
		},
		{
			name:     "complex struct",
			key:      "struct",
			value:    struct{ Name string }{Name: "test"},
			expected: "struct={test}",
		},
		{
			name:     "slice of ints",
			key:      "slice",
			value:    []int{1, 2, 3},
			expected: "slice=[1 2 3]",
		},
		{
			name:     "map value",
			key:      "map",
			value:    map[string]int{"a": 1, "b": 2},
			expected: "map=map[a:1 b:2]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sb strings.Builder
			result := formatRows(&sb, tt.key, tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func BenchmarkPks(b *testing.B) {
	// Create test data
	table := &typedef.Table{
		PartitionKeys: typedef.Columns{
			{Name: "pk0"},
			{Name: "pk1"},
		},
		ClusteringKeys: typedef.Columns{
			{Name: "ck0"},
			{Name: "ck1"},
		},
	}

	rows := make(Rows, 100)
	for i := 0; i < 100; i++ {
		rows[i] = Row{
			"pk0": "value" + strconv.Itoa(i),
			"pk1": i,
			"ck0": "cluster" + strconv.Itoa(i),
			"ck1": float64(i) * 1.5,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pks(table, rows)
	}
}

func BenchmarkFormatRows(b *testing.B) {
	var sb strings.Builder
	sb.Grow(64)

	b.Run("string", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = formatRows(&sb, "key", "value")
		}
	})

	b.Run("int", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = formatRows(&sb, "key", 42)
		}
	})

	b.Run("int8", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = formatRows(&sb, "key", int8(42))
		}
	})

	b.Run("int16", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = formatRows(&sb, "key", int16(42))
		}
	})

	b.Run("int32", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = formatRows(&sb, "key", int32(42))
		}
	})

	b.Run("int64", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = formatRows(&sb, "key", int64(42))
		}
	})

	b.Run("uint", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = formatRows(&sb, "key", uint(42))
		}
	})

	b.Run("uint8", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = formatRows(&sb, "key", uint8(42))
		}
	})

	b.Run("uint16", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = formatRows(&sb, "key", uint16(42))
		}
	})

	b.Run("uint32", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = formatRows(&sb, "key", uint32(42))
		}
	})

	b.Run("uint64", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = formatRows(&sb, "key", uint64(42))
		}
	})

	b.Run("float32", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = formatRows(&sb, "key", float32(3.14159))
		}
	})

	b.Run("float64", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = formatRows(&sb, "key", 3.14159)
		}
	})

	b.Run("bytes", func(b *testing.B) {
		bytes := []byte("hello world")
		for i := 0; i < b.N; i++ {
			_ = formatRows(&sb, "key", bytes)
		}
	})

	b.Run("gocql.UUID", func(b *testing.B) {
		uuid, _ := gocql.ParseUUID("550e8400-e29b-41d4-a716-446655440000")
		for i := 0; i < b.N; i++ {
			_ = formatRows(&sb, "key", uuid)
		}
	})

	b.Run("time.Time", func(b *testing.B) {
		t := time.Date(2025, 6, 11, 12, 34, 56, 0, time.UTC)
		for i := 0; i < b.N; i++ {
			_ = formatRows(&sb, "key", t)
		}
	})

	b.Run("*big.Int", func(b *testing.B) {
		bi := big.NewInt(9223372036854775807)
		for i := 0; i < b.N; i++ {
			_ = formatRows(&sb, "key", bi)
		}
	})

	b.Run("*inf.Dec", func(b *testing.B) {
		d := new(inf.Dec)
		d.SetString("123456.789")
		for i := 0; i < b.N; i++ {
			_ = formatRows(&sb, "key", d)
		}
	})

	b.Run("struct", func(b *testing.B) {
		s := struct{ Name string }{Name: "test"}
		for i := 0; i < b.N; i++ {
			_ = formatRows(&sb, "key", s)
		}
	})

	b.Run("nil", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = formatRows(&sb, "key", nil)
		}
	})
}
