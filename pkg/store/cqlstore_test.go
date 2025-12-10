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

//go:build testing

package store

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gopkg.in/inf.v0"

	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/testutils"
	"github.com/scylladb/gemini/pkg/typedef"
)

func Test_DuplicateValuesWithCompare(t *testing.T) {
	t.Parallel()

	assert := require.New(t)
	scyllaContainer := testutils.TestContainers(t)
	keyspace := testutils.GenerateUniqueKeyspaceName(t)

	assert.NoError(scyllaContainer.Test.Query(
		fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", keyspace),
	).Exec())
	assert.NoError(scyllaContainer.Test.Query(
		fmt.Sprintf("CREATE TABLE %s.table_1 (id timeuuid PRIMARY KEY, value list<text>)", keyspace),
	).Exec())
	assert.NoError(scyllaContainer.Oracle.Query(
		fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", keyspace),
	).Exec())
	assert.NoError(scyllaContainer.Oracle.Query(
		fmt.Sprintf("CREATE TABLE %s.table_1 (id timeuuid PRIMARY KEY, value list<text>)", keyspace),
	).Exec())

	schema := &typedef.Schema{
		Keyspace: typedef.Keyspace{Name: keyspace},
		Config: typedef.SchemaConfig{
			ReplicationStrategy:              replication.NewSimpleStrategy(),
			OracleReplicationStrategy:        replication.NewSimpleStrategy(),
			AsyncObjectStabilizationAttempts: 10,
			AsyncObjectStabilizationDelay:    10 * time.Millisecond,
		},
		Tables: []*typedef.Table{{
			Name: "table_1",
			Columns: typedef.Columns{
				{Name: "id", Type: typedef.TypeUuid},
				{Name: "values", Type: &typedef.Collection{
					ComplexType: typedef.TypeList,
					ValueType:   typedef.TypeText,
					Frozen:      false,
				}},
			},
		}},
	}

	store := &delegatingStore{
		oracleStore:        newCQLStoreWithSession(scyllaContainer.OracleCluster, schema, zap.NewNop(), "", "oracle"),
		testStore:          newCQLStoreWithSession(scyllaContainer.TestCluster, schema, zap.NewNop(), "", "test"),
		logger:             zap.NewNop(),
		mutationRetrySleep: 100 * time.Millisecond,
		mutationRetries:    10,
	}

	assert.NoError(store.oracleStore.Init())
	assert.NoError(store.testStore.Init())

	uuid := gocql.TimeUUID()

	insert := typedef.SimpleStmt(
		fmt.Sprintf("INSERT INTO %s.table_1(id, value) VALUES(%s, ['%s', '%s'])", keyspace, uuid, "test1", "test2"),
		typedef.InsertStatementType,
	)

	insert2 := typedef.SimpleStmt(
		fmt.Sprintf("INSERT INTO %s.table_1(id, value) VALUES(%s, ['%s', '%s', '%s'])", keyspace, uuid, "test1", "test2", "test3"),
		typedef.InsertStatementType,
	)
	timestamp := mo.Some(time.Now())

	// Try to make it insert a double
	for range 1000 {
		go func() {
			_ = store.testStore.mutate(t.Context(), insert2, timestamp)
		}()
	}

	check := typedef.SimpleStmt(
		fmt.Sprintf("SELECT * FROM %s.table_1 WHERE id = %s", keyspace, uuid),
		typedef.InsertStatementType,
	)
	assert.NoError(store.Mutate(t.Context(), insert))
	count, err := store.Check(t.Context(), schema.Tables[0], check, 1)

	assert.NoError(err)
	assert.Equal(1, count, "Expected only one row to be inserted with duplicate values")
}

func TestDeepCopyValue(t *testing.T) {
	t.Parallel()

	t.Run("nil value", func(t *testing.T) {
		t.Parallel()
		result := deepCopyValue(nil)
		assert.Nil(t, result)
	})

	t.Run("byte slice", func(t *testing.T) {
		t.Parallel()
		original := []byte("hello world")
		copied := deepCopyValue(original)

		require.IsType(t, []byte{}, copied)
		copiedBytes := copied.([]byte)

		assert.Equal(t, original, copiedBytes)

		// Verify it's a deep copy by modifying original
		original[0] = 'H'
		assert.NotEqual(t, original, copiedBytes, "copied value should not be affected by changes to original")
	})

	t.Run("nil byte slice", func(t *testing.T) {
		t.Parallel()
		var original []byte
		copied := deepCopyValue(original)
		assert.Nil(t, copied)
	})

	t.Run("primitive types", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			value any
			name  string
		}{
			{name: "string", value: "test string"},
			{name: "bool", value: true},
			{name: "int", value: 42},
			{name: "int8", value: int8(8)},
			{name: "int16", value: int16(16)},
			{name: "int32", value: int32(32)},
			{name: "int64", value: int64(64)},
			{name: "uint", value: uint(42)},
			{name: "uint8", value: uint8(8)},
			{name: "uint16", value: uint16(16)},
			{name: "uint32", value: uint32(32)},
			{name: "uint64", value: uint64(64)},
			{name: "float32", value: float32(3.14)},
			{name: "float64", value: float64(3.14159)},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				copied := deepCopyValue(tt.value)
				assert.Equal(t, tt.value, copied)
			})
		}
	})

	t.Run("big.Int", func(t *testing.T) {
		t.Parallel()
		original := big.NewInt(123456789)
		copied := deepCopyValue(original)

		require.IsType(t, &big.Int{}, copied)
		copiedBig := copied.(*big.Int)

		assert.Equal(t, original, copiedBig)
		assert.NotSame(t, original, copiedBig, "should be different instances")

		// Verify deep copy
		original.SetInt64(999)
		assert.NotEqual(t, original, copiedBig, "copied value should not be affected")
	})

	t.Run("nil big.Int", func(t *testing.T) {
		t.Parallel()
		var original *big.Int
		copied := deepCopyValue(original)
		assert.Nil(t, copied)
	})

	t.Run("inf.Dec", func(t *testing.T) {
		t.Parallel()
		original := inf.NewDec(12345, 2) // 123.45
		copied := deepCopyValue(original)

		require.IsType(t, &inf.Dec{}, copied)
		copiedDec := copied.(*inf.Dec)

		assert.Equal(t, 0, original.Cmp(copiedDec))
		assert.NotSame(t, original, copiedDec, "should be different instances")

		// Verify deep copy
		original.SetUnscaled(999)
		assert.NotEqual(t, 0, original.Cmp(copiedDec), "copied value should not be affected")
	})

	t.Run("nil inf.Dec", func(t *testing.T) {
		t.Parallel()
		var original *inf.Dec
		copied := deepCopyValue(original)
		assert.Nil(t, copied)
	})

	t.Run("time.Time", func(t *testing.T) {
		t.Parallel()
		original := time.Now()
		copied := deepCopyValue(original)

		require.IsType(t, time.Time{}, copied)
		copiedTime := copied.(time.Time)

		assert.Equal(t, original, copiedTime)
	})
}

func TestLoadIterNoMapReuse(t *testing.T) {
	t.Parallel()

	// This test verifies that the iterator doesn't have slice reuse issues
	// by collecting multiple rows and ensuring each has independent data

	iter := RowIterator(func(yield func(Row, error) bool) {
		// Simulate what gocql does - reuse the same backing array
		backingData := []byte("original")
		columnNames := []string{"id", "data"}

		for i := range 3 {
			// Simulate gocql modifying the backing array
			backingData[0] = byte('0' + i)

			// Create row with values that need deep copying
			values := []any{i, backingData}

			// Deep copy the values before creating the row
			copiedValues := make([]any, len(values))
			for j, v := range values {
				copiedValues[j] = deepCopyValue(v)
			}

			row := NewRow(columnNames, copiedValues)

			if !yield(row, nil) {
				return
			}
		}
	})

	rows, err := iter.Collect()
	require.NoError(t, err)
	require.Len(t, rows, 3)

	// Verify each row has independent data
	assert.Equal(t, []byte("0riginal"), rows[0].Get("data"))
	assert.Equal(t, []byte("1riginal"), rows[1].Get("data"))
	assert.Equal(t, []byte("2riginal"), rows[2].Get("data"))
}

func BenchmarkDeepCopyValue(b *testing.B) {
	testCases := []struct {
		value any
		name  string
	}{
		{name: "byte_slice_small", value: []byte("hello")},
		{name: "byte_slice_large", value: make([]byte, 1024)},
		{name: "string", value: "test string value"},
		{name: "int64", value: int64(123456789)},
		{name: "big_int", value: big.NewInt(123456789)},
		{name: "inf_dec", value: inf.NewDec(12345, 2)},
		{name: "time", value: time.Now()},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			for range b.N {
				_ = deepCopyValue(tc.value)
			}
		})
	}
}

func BenchmarkScanVsMapScan(b *testing.B) {
	// This benchmark compares the performance difference between
	// using Scan with slice vs MapScan with map

	b.Run("row_allocation", func(b *testing.B) {
		colCount := 10
		columnNames := make([]string, colCount)
		for i := range columnNames {
			columnNames[i] = "col" + string(rune('0'+i))
		}
		b.ResetTimer()
		for range b.N {
			values := make([]any, colCount)
			_ = NewRow(columnNames, values)
		}
	})

	b.Run("slice_allocation", func(b *testing.B) {
		colCount := 10
		b.ResetTimer()
		for range b.N {
			_ = make([]any, colCount)
		}
	})

	b.Run("slice_reuse", func(b *testing.B) {
		colCount := 10
		scanDest := make([]any, colCount)
		b.ResetTimer()
		for range b.N {
			// Simulate reusing the slice
			for i := range scanDest {
				scanDest[i] = i
			}
		}
	})
}
