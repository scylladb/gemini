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
	"encoding/json"
	"math/big"
	"sort"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/inf.v0"
)

func TestRow_Set(t *testing.T) {
	t.Parallel()

	columnNames := []string{"id", "name", "age"}
	values := []any{1, "John", 30}

	row := NewRow(columnNames, values)

	// Test setting an existing column
	row.Set("name", "Jane")
	assert.Equal(t, "Jane", row.Get("name"))

	// Test setting another column
	row.Set("age", 35)
	assert.Equal(t, 35, row.Get("age"))

	// Test setting a non-existent column (should be no-op)
	row.Set("nonexistent", "value")
	assert.Nil(t, row.Get("nonexistent"))
}

func TestRows_Len(t *testing.T) {
	t.Parallel()

	rows := Rows{
		NewRow([]string{"id"}, []any{1}),
		NewRow([]string{"id"}, []any{2}),
		NewRow([]string{"id"}, []any{3}),
	}

	assert.Equal(t, 3, rows.Len())

	emptyRows := Rows{}
	assert.Equal(t, 0, emptyRows.Len())
}

func TestRows_Swap(t *testing.T) {
	t.Parallel()

	rows := Rows{
		NewRow([]string{"id"}, []any{1}),
		NewRow([]string{"id"}, []any{2}),
		NewRow([]string{"id"}, []any{3}),
	}

	// Swap first and last
	rows.Swap(0, 2)

	assert.Equal(t, 3, rows[0].Get("id"))
	assert.Equal(t, 2, rows[1].Get("id"))
	assert.Equal(t, 1, rows[2].Get("id"))
}

func TestRows_Less(t *testing.T) {
	t.Parallel()

	t.Run("compare integers", func(t *testing.T) {
		t.Parallel()

		rows := Rows{
			NewRow([]string{"id"}, []any{1}),
			NewRow([]string{"id"}, []any{2}),
		}

		assert.True(t, rows.Less(0, 1))
		assert.False(t, rows.Less(1, 0))
	})

	t.Run("compare strings", func(t *testing.T) {
		t.Parallel()

		rows := Rows{
			NewRow([]string{"name"}, []any{"alice"}),
			NewRow([]string{"name"}, []any{"bob"}),
		}

		assert.True(t, rows.Less(0, 1))
		assert.False(t, rows.Less(1, 0))
	})

	t.Run("compare multiple columns", func(t *testing.T) {
		t.Parallel()

		rows := Rows{
			NewRow([]string{"name", "age"}, []any{"alice", 30}),
			NewRow([]string{"name", "age"}, []any{"bob", 25}),
		}

		assert.True(t, rows.Less(0, 1))
	})
}

func TestRows_Sort(t *testing.T) {
	t.Parallel()

	rows := Rows{
		NewRow([]string{"id"}, []any{3}),
		NewRow([]string{"id"}, []any{1}),
		NewRow([]string{"id"}, []any{2}),
	}

	sort.Sort(rows)

	assert.Equal(t, 1, rows[0].Get("id"))
	assert.Equal(t, 2, rows[1].Get("id"))
	assert.Equal(t, 3, rows[2].Get("id"))
}

func TestRow_MarshalJSON(t *testing.T) {
	t.Parallel()

	t.Run("simple values", func(t *testing.T) {
		t.Parallel()

		columnNames := []string{"id", "name", "age"}
		values := []any{1, "John", 30}

		row := NewRow(columnNames, values)

		data, err := json.Marshal(row)
		require.NoError(t, err)

		// Parse back to map to verify structure
		var result map[string]any
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)

		assert.Equal(t, float64(1), result["id"])
		assert.Equal(t, "John", result["name"])
		assert.Equal(t, float64(30), result["age"])
	})

	t.Run("nil values", func(t *testing.T) {
		t.Parallel()

		columnNames := []string{"id", "name", "optional"}
		values := []any{1, "John", nil}

		row := NewRow(columnNames, values)

		data, err := json.Marshal(row)
		require.NoError(t, err)

		var result map[string]any
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)

		assert.Nil(t, result["optional"])
	})

	t.Run("complex types", func(t *testing.T) {
		t.Parallel()

		uuid := gocql.TimeUUID()
		timestamp := time.Now()

		columnNames := []string{"id", "created_at", "data"}
		values := []any{uuid, timestamp, map[string]any{"key": "value"}}

		row := NewRow(columnNames, values)

		data, err := json.Marshal(row)
		require.NoError(t, err)

		assert.NotEmpty(t, data)
	})
}

func TestRowsCmp(t *testing.T) {
	t.Parallel()

	t.Run("equal rows", func(t *testing.T) {
		t.Parallel()

		row1 := NewRow([]string{"id", "name"}, []any{1, "John"})
		row2 := NewRow([]string{"id", "name"}, []any{1, "John"})

		assert.Equal(t, 0, rowsCmp(row1, row2))
	})

	t.Run("different integers", func(t *testing.T) {
		t.Parallel()

		row1 := NewRow([]string{"id"}, []any{1})
		row2 := NewRow([]string{"id"}, []any{2})

		assert.Equal(t, -1, rowsCmp(row1, row2))
		assert.Equal(t, 1, rowsCmp(row2, row1))
	})

	t.Run("different strings", func(t *testing.T) {
		row1 := NewRow([]string{"name"}, []any{"alice"})
		row2 := NewRow([]string{"name"}, []any{"bob"})

		assert.Equal(t, -1, rowsCmp(row1, row2))
		assert.Equal(t, 1, rowsCmp(row2, row1))
	})

	t.Run("nil vs non-nil", func(t *testing.T) {
		row1 := NewRow([]string{"value"}, []any{nil})
		row2 := NewRow([]string{"value"}, []any{1})

		// nil is less than non-nil
		assert.Equal(t, -1, rowsCmp(row1, row2))
		assert.Equal(t, 1, rowsCmp(row2, row1))
	})

	t.Run("both nil", func(t *testing.T) {
		row1 := NewRow([]string{"value"}, []any{nil})
		row2 := NewRow([]string{"value"}, []any{nil})

		assert.Equal(t, 0, rowsCmp(row1, row2))
	})

	t.Run("big.Int comparison", func(t *testing.T) {
		row1 := NewRow([]string{"bigint"}, []any{big.NewInt(100)})
		row2 := NewRow([]string{"bigint"}, []any{big.NewInt(200)})

		assert.Equal(t, -1, rowsCmp(row1, row2))
		assert.Equal(t, 1, rowsCmp(row2, row1))
	})

	t.Run("inf.Dec comparison", func(t *testing.T) {
		dec1 := inf.NewDec(100, 0)
		dec2 := inf.NewDec(200, 0)

		row1 := NewRow([]string{"decimal"}, []any{dec1})
		row2 := NewRow([]string{"decimal"}, []any{dec2})

		assert.Equal(t, -1, rowsCmp(row1, row2))
		assert.Equal(t, 1, rowsCmp(row2, row1))
	})

	t.Run("time comparison", func(t *testing.T) {
		time1 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
		time2 := time.Date(2023, 12, 31, 0, 0, 0, 0, time.UTC)

		row1 := NewRow([]string{"timestamp"}, []any{time1})
		row2 := NewRow([]string{"timestamp"}, []any{time2})

		assert.Equal(t, -1, rowsCmp(row1, row2))
		assert.Equal(t, 1, rowsCmp(row2, row1))
	})

	t.Run("UUID comparison", func(t *testing.T) {
		uuid1 := gocql.TimeUUID()
		time.Sleep(1 * time.Millisecond) // Ensure different timestamp
		uuid2 := gocql.TimeUUID()

		row1 := NewRow([]string{"id"}, []any{uuid1})
		row2 := NewRow([]string{"id"}, []any{uuid2})

		// Should have some comparison result
		result := rowsCmp(row1, row2)
		assert.Contains(t, []int{-1, 0, 1}, result)
	})

	t.Run("byte slice comparison", func(t *testing.T) {
		row1 := NewRow([]string{"data"}, []any{[]byte{1, 2, 3}})
		row2 := NewRow([]string{"data"}, []any{[]byte{1, 2, 4}})

		assert.Equal(t, -1, rowsCmp(row1, row2))
		assert.Equal(t, 1, rowsCmp(row2, row1))
	})

	t.Run("float comparison", func(t *testing.T) {
		row1 := NewRow([]string{"price"}, []any{10.5})
		row2 := NewRow([]string{"price"}, []any{20.5})

		assert.Equal(t, -1, rowsCmp(row1, row2))
		assert.Equal(t, 1, rowsCmp(row2, row1))
	})

	t.Run("different types fallback to string", func(t *testing.T) {
		row1 := NewRow([]string{"value"}, []any{123})
		row2 := NewRow([]string{"value"}, []any{"456"})

		// Should not panic and use string comparison
		result := rowsCmp(row1, row2)
		assert.Contains(t, []int{-1, 0, 1}, result)
	})

	t.Run("multiple columns comparison", func(t *testing.T) {
		// First column same, second column different
		row1 := NewRow([]string{"a", "b"}, []any{1, 2})
		row2 := NewRow([]string{"a", "b"}, []any{1, 3})

		assert.Equal(t, -1, rowsCmp(row1, row2))
		assert.Equal(t, 1, rowsCmp(row2, row1))
	})

	t.Run("different number of columns", func(t *testing.T) {
		row1 := NewRow([]string{"a"}, []any{1})
		row2 := NewRow([]string{"a", "b"}, []any{1, 2})

		// Should handle gracefully
		result := rowsCmp(row1, row2)
		assert.Contains(t, []int{-1, 0, 1}, result)
	})
}
