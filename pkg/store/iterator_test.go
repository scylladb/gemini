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
	"errors"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/typedef"
)

func TestRowIterator_Collect(t *testing.T) {
	t.Parallel()

	t.Run("collect empty iterator", func(t *testing.T) {
		t.Parallel()

		iter := RowIterator(func(_ func(Row, error) bool) {
			// Empty iterator
		})

		rows, err := iter.Collect()
		assert.NoError(t, err)
		assert.Empty(t, rows)
	})

	t.Run("collect multiple rows", func(t *testing.T) {
		t.Parallel()

		expectedRows := Rows{
			NewRow([]string{"pk0", "col1"}, []any{"key1", "value1"}),
			NewRow([]string{"pk0", "col1"}, []any{"key2", "value2"}),
			NewRow([]string{"pk0", "col1"}, []any{"key3", "value3"}),
		}

		iter := RowIterator(func(yield func(Row, error) bool) {
			for _, row := range expectedRows {
				if !yield(row, nil) {
					return
				}
			}
		})

		rows, err := iter.Collect()
		assert.NoError(t, err)
		assert.Equal(t, expectedRows, rows)
	})

	t.Run("collect with error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("test error")

		iter := RowIterator(func(yield func(Row, error) bool) {
			yield(NewRow([]string{"pk0"}, []any{"key1"}), nil)
			yield(Row{}, expectedErr)
		})

		rows, err := iter.Collect()
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Len(t, rows, 1)
	})

	t.Run("early stop", func(t *testing.T) {
		t.Parallel()

		iter := RowIterator(func(yield func(Row, error) bool) {
			yield(NewRow([]string{"pk0"}, []any{"key1"}), nil)
			if !yield(NewRow([]string{"pk0"}, []any{"key2"}), nil) {
				return
			}
			yield(NewRow([]string{"pk0"}, []any{"key3"}), nil) // Should not be reached
		})

		count := 0
		for _, err := range iter {
			if err != nil {
				t.Fatal(err)
			}
			count++
			if count == 2 {
				break
			}
		}

		assert.Equal(t, 2, count)
	})
}

func TestRowIterator_Count(t *testing.T) {
	t.Parallel()

	t.Run("count empty iterator", func(t *testing.T) {
		t.Parallel()

		iter := RowIterator(func(_ func(Row, error) bool) {})

		count, err := iter.Count()
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("count multiple rows", func(t *testing.T) {
		t.Parallel()

		iter := RowIterator(func(yield func(Row, error) bool) {
			for i := range 5 {
				if !yield(NewRow([]string{"pk0"}, []any{i}), nil) {
					return
				}
			}
		})

		count, err := iter.Count()
		assert.NoError(t, err)
		assert.Equal(t, 5, count)
	})

	t.Run("count with error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("count error")

		iter := RowIterator(func(yield func(Row, error) bool) {
			yield(NewRow([]string{"pk0"}, []any{"key1"}), nil)
			yield(Row{}, expectedErr)
		})

		count, err := iter.Count()
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Equal(t, 1, count)
	})
}

func TestCompareCollectedRows(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		PartitionKeys: []typedef.ColumnDef{
			{Name: "pk0", Type: typedef.TypeText},
		},
	}

	t.Run("identical rows", func(t *testing.T) {
		t.Parallel()

		testRows := Rows{
			NewRow([]string{"pk0", "col1"}, []any{"key1", "value1"}),
			NewRow([]string{"pk0", "col1"}, []any{"key2", "value2"}),
		}
		oracleRows := Rows{
			NewRow([]string{"pk0", "col1"}, []any{"key1", "value1"}),
			NewRow([]string{"pk0", "col1"}, []any{"key2", "value2"}),
		}

		result := CompareCollectedRows(table, testRows, oracleRows)

		assert.NoError(t, result.ToError())
		assert.Equal(t, 2, result.MatchCount)
		assert.Empty(t, result.DifferentRows)
		assert.Empty(t, result.TestOnlyRows)
		assert.Empty(t, result.OracleOnlyRows)
	})

	t.Run("both empty", func(t *testing.T) {
		t.Parallel()

		result := CompareCollectedRows(table, Rows{}, Rows{})

		assert.NoError(t, result.ToError())
		assert.Equal(t, 0, result.MatchCount)
	})

	t.Run("different row counts", func(t *testing.T) {
		t.Parallel()

		testRows := Rows{
			NewRow([]string{"pk0", "col1"}, []any{"key1", "value1"}),
			NewRow([]string{"pk0", "col1"}, []any{"key2", "value2"}),
		}
		oracleRows := Rows{
			NewRow([]string{"pk0", "col1"}, []any{"key1", "value1"}),
		}

		result := CompareCollectedRows(table, testRows, oracleRows)

		assert.Error(t, result.ToError())
		assert.Equal(t, 0, result.MatchCount)
		assert.Len(t, result.TestOnlyRows, 1)
		assert.Empty(t, result.OracleOnlyRows)
	})

	t.Run("different values", func(t *testing.T) {
		t.Parallel()

		testRows := Rows{
			NewRow([]string{"pk0", "col1"}, []any{"key1", "value1_test"}),
		}
		oracleRows := Rows{
			NewRow([]string{"pk0", "col1"}, []any{"key1", "value1_oracle"}),
		}

		result := CompareCollectedRows(table, testRows, oracleRows)

		assert.Error(t, result.ToError())
		assert.Equal(t, 0, result.MatchCount)
		assert.Len(t, result.DifferentRows, 1)
		assert.Contains(t, result.DifferentRows[0].Diff, "value1")
	})

	t.Run("unordered rows are sorted before comparison", func(t *testing.T) {
		t.Parallel()

		testRows := Rows{
			NewRow([]string{"pk0", "col1"}, []any{"key2", "value2"}),
			NewRow([]string{"pk0", "col1"}, []any{"key1", "value1"}),
		}
		oracleRows := Rows{
			NewRow([]string{"pk0", "col1"}, []any{"key1", "value1"}),
			NewRow([]string{"pk0", "col1"}, []any{"key2", "value2"}),
		}

		result := CompareCollectedRows(table, testRows, oracleRows)

		assert.NoError(t, result.ToError())
		assert.Equal(t, 2, result.MatchCount)
	})
}

func TestZipAndCompare(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		PartitionKeys: []typedef.ColumnDef{
			{Name: "pk0", Type: typedef.TypeText},
		},
	}

	t.Run("compare matching iterators", func(t *testing.T) {
		t.Parallel()

		testIter := RowIterator(func(yield func(Row, error) bool) {
			yield(NewRow([]string{"pk0", "col1"}, []any{"key1", "value1"}), nil)
			yield(NewRow([]string{"pk0", "col1"}, []any{"key2", "value2"}), nil)
		})

		oracleIter := RowIterator(func(yield func(Row, error) bool) {
			yield(NewRow([]string{"pk0", "col1"}, []any{"key1", "value1"}), nil)
			yield(NewRow([]string{"pk0", "col1"}, []any{"key2", "value2"}), nil)
		})

		result := ZipAndCompare(t.Context(), table, testIter, oracleIter)

		assert.NoError(t, result.ToError())
		assert.Equal(t, 2, result.MatchCount)
	})

	t.Run("test iterator has error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("test error")

		testIter := RowIterator(func(yield func(Row, error) bool) {
			yield(Row{}, expectedErr)
		})

		oracleIter := RowIterator(func(yield func(Row, error) bool) {
			yield(NewRow([]string{"pk0"}, []any{"key1"}), nil)
		})

		result := ZipAndCompare(t.Context(), table, testIter, oracleIter)

		assert.Error(t, result.ToError())
		assert.Equal(t, expectedErr, result.TestError)
	})

	t.Run("oracle iterator has error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("oracle error")

		testIter := RowIterator(func(yield func(Row, error) bool) {
			yield(NewRow([]string{"pk0"}, []any{"key1"}), nil)
		})

		oracleIter := RowIterator(func(yield func(Row, error) bool) {
			yield(Row{}, expectedErr)
		})

		result := ZipAndCompare(t.Context(), table, testIter, oracleIter)

		assert.Error(t, result.ToError())
		assert.Equal(t, expectedErr, result.OracleError)
	})
}

func TestComparisonResult_ToError(t *testing.T) {
	t.Parallel()

	t.Run("no error on match", func(t *testing.T) {
		t.Parallel()

		result := ComparisonResult{
			MatchCount: 5,
		}

		err := result.ToError()
		assert.NoError(t, err)
	})

	t.Run("error on test error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("test error")
		result := ComparisonResult{
			TestError: expectedErr,
		}

		err := result.ToError()
		assert.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("error on oracle error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("oracle error")
		result := ComparisonResult{
			OracleError: expectedErr,
		}

		err := result.ToError()
		assert.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("error on missing rows", func(t *testing.T) {
		t.Parallel()

		result := ComparisonResult{
			MatchCount:     1,
			TestOnlyRows:   Rows{NewRow([]string{"pk0"}, []any{"key1"})},
			OracleOnlyRows: Rows{NewRow([]string{"pk0"}, []any{"key2"})},
		}

		err := result.ToError()
		require.Error(t, err)

		var rowDiffErr ErrorRowDifference
		assert.ErrorAs(t, err, &rowDiffErr)
	})

	t.Run("error on different values", func(t *testing.T) {
		t.Parallel()

		result := ComparisonResult{
			DifferentRows: []RowDifference{
				{
					TestRow:   NewRow([]string{"pk0", "col1"}, []any{"key1", "test"}),
					OracleRow: NewRow([]string{"pk0", "col1"}, []any{"key1", "oracle"}),
					Diff:      "col1 differs",
				},
			},
		}

		err := result.ToError()
		require.Error(t, err)

		var rowDiffErr ErrorRowDifference
		assert.ErrorAs(t, err, &rowDiffErr)
	})
}

func BenchmarkRowIterator_Collect(b *testing.B) {
	// Create a large iterator
	iter := func() RowIterator {
		return RowIterator(func(yield func(Row, error) bool) {
			for i := range 1000 {
				if !yield(NewRow([]string{"pk0", "col1", "col2"}, []any{i, i * 2, i * 3}), nil) {
					return
				}
			}
		})
	}

	b.ResetTimer()
	for range b.N {
		rows, err := iter().Collect()
		if err != nil {
			b.Fatal(err)
		}
		if len(rows) != 1000 {
			b.Fatalf("expected 1000 rows, got %d", len(rows))
		}
	}
}

func BenchmarkRowIterator_Count(b *testing.B) {
	// Create a large iterator
	iter := func() RowIterator {
		return RowIterator(func(yield func(Row, error) bool) {
			for i := range 1000 {
				if !yield(NewRow([]string{"pk0"}, []any{i}), nil) {
					return
				}
			}
		})
	}

	b.ResetTimer()
	for range b.N {
		count, err := iter().Count()
		if err != nil {
			b.Fatal(err)
		}
		if count != 1000 {
			b.Fatalf("expected 1000, got %d", count)
		}
	}
}

func BenchmarkCompareCollectedRows(b *testing.B) {
	table := &typedef.Table{
		PartitionKeys: []typedef.ColumnDef{
			{Name: "pk0", Type: typedef.TypeInt},
		},
	}

	// Create test data
	testRows := make(Rows, 100)
	oracleRows := make(Rows, 100)
	for i := range 100 {
		testRows[i] = NewRow([]string{"pk0", "col1"}, []any{i, i * 2})
		oracleRows[i] = NewRow([]string{"pk0", "col1"}, []any{i, i * 2})
	}

	b.ResetTimer()
	for range b.N {
		result := CompareCollectedRows(table, testRows, oracleRows)
		if result.MatchCount != 100 {
			b.Fatalf("expected 100 matches, got %d", result.MatchCount)
		}
	}
}

func TestCompareCollectedRows_ContentVsPointerEquality(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		PartitionKeys: []typedef.ColumnDef{{Name: "pk0", Type: typedef.TypeInt}},
	}

	// Same logical values, but different representations (pointer vs value)
	s := "110.198.234.8"
	bval := false

	oracleRows := Rows{NewRow([]string{"pk0", "col1", "col2"}, []any{1, &s, &bval})}
	testRows := Rows{NewRow([]string{"pk0", "col1", "col2"}, []any{1, s, bval})}

	result := CompareCollectedRows(table, testRows, oracleRows)

	require.NoError(t, result.ToError())
	assert.Equal(t, 1, result.MatchCount)
	assert.Empty(t, result.DifferentRows)
}

func TestCompareCollectedRows_CleanUnifiedDiff(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name:           "table1",
		PartitionKeys:  []typedef.ColumnDef{{Name: "pk0", Type: typedef.TypeInt}},
		ClusteringKeys: []typedef.ColumnDef{{Name: "ck0", Type: typedef.TypeInt}},
	}

	// Oracle row without an extra column, test row has an extra column and a changed value
	oracle := NewRow([]string{"pk0", "ck0", "col1"}, []any{1, 2, "oracle"})
	test := NewRow([]string{"pk0", "ck0", "col1", "extra_col"}, []any{1, 2, "test", true})

	result := CompareCollectedRows(table, Rows{test}, Rows{oracle})

	// Verify we get a single difference with our unified diff format
	err := result.ToError()
	require.Error(t, err)

	var rowDiffErr ErrorRowDifference
	require.ErrorAs(t, err, &rowDiffErr)
	require.NotEmpty(t, rowDiffErr.Diff)

	diff := rowDiffErr.Diff
	// Contains header with pk context
	assert.Contains(t, diff, "pk:")
	assert.Contains(t, diff, "pk0=1")
	assert.Contains(t, diff, "ck0=2")
	// Shows changed value as -/+ lines
	assert.Contains(t, diff, "- col1: oracle")
	assert.Contains(t, diff, "+ col1: test")
	// Shows extra column present only on test side
	assert.Contains(t, diff, "+ extra_col: true")

	// And must not contain raw pointer addresses typical of %#v like 0xc0...
	assert.NotContains(t, diff, "0xc0")
}

func TestCompareCollectedRows_MissingInTestOnly(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name:           "table1",
		PartitionKeys:  []typedef.ColumnDef{{Name: "pk0", Type: typedef.TypeInt}},
		ClusteringKeys: []typedef.ColumnDef{{Name: "ck0", Type: typedef.TypeInt}},
	}

	// Oracle has an extra column that is missing on test side
	oracle := NewRow([]string{"pk0", "ck0", "col1", "missing_col"}, []any{1, 2, "same", true})
	test := NewRow([]string{"pk0", "ck0", "col1"}, []any{1, 2, "same"})

	result := CompareCollectedRows(table, Rows{test}, Rows{oracle})

	err := result.ToError()
	require.Error(t, err)

	var rowDiffErr ErrorRowDifference
	require.ErrorAs(t, err, &rowDiffErr)
	require.NotEmpty(t, rowDiffErr.Diff)

	diff := rowDiffErr.Diff
	// Header present
	assert.Contains(t, diff, "pk:")
	// Shows only '-' line for the column missing on test side
	assert.Contains(t, diff, "- missing_col: true")
	assert.NotContains(t, diff, "+ missing_col:")
}

func TestCompareCollectedRows_MissingInOracleOnly(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name:           "table1",
		PartitionKeys:  []typedef.ColumnDef{{Name: "pk0", Type: typedef.TypeInt}},
		ClusteringKeys: []typedef.ColumnDef{{Name: "ck0", Type: typedef.TypeInt}},
	}

	// Test has an extra column that is missing on oracle side
	oracle := NewRow([]string{"pk0", "ck0", "col1"}, []any{1, 2, "same"})
	test := NewRow([]string{"pk0", "ck0", "col1", "extra_col"}, []any{1, 2, "same", true})

	result := CompareCollectedRows(table, Rows{test}, Rows{oracle})

	err := result.ToError()
	require.Error(t, err)

	var rowDiffErr ErrorRowDifference
	require.ErrorAs(t, err, &rowDiffErr)
	require.NotEmpty(t, rowDiffErr.Diff)

	diff := rowDiffErr.Diff
	// Header present
	assert.Contains(t, diff, "pk:")
	// Shows only '+' line for the column missing on oracle side
	assert.Contains(t, diff, "+ extra_col: true")
	assert.NotContains(t, diff, "- extra_col:")
}

func TestCompareCollectedRows_BothEmpty_NoError(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{PartitionKeys: []typedef.ColumnDef{{Name: "pk0", Type: typedef.TypeInt}}}

	result := CompareCollectedRows(table, nil, nil)
	assert.NoError(t, result.ToError())
	assert.Equal(t, 0, result.MatchCount)
	assert.Empty(t, result.DifferentRows)
	assert.Empty(t, result.TestOnlyRows)
	assert.Empty(t, result.OracleOnlyRows)
}

func TestCompareCollectedRows_IdenticalRows_NoDiff(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{PartitionKeys: []typedef.ColumnDef{{Name: "pk0", Type: typedef.TypeInt}}}

	r1 := NewRow([]string{"pk0", "col"}, []any{1, "A"})
	r2 := NewRow([]string{"pk0", "col"}, []any{2, "B"})

	result := CompareCollectedRows(table, Rows{r1, r2}, Rows{r1, r2})
	assert.NoError(t, result.ToError())
	assert.Equal(t, 2, result.MatchCount)
	assert.Empty(t, result.DifferentRows)
}

func TestCompareCollectedRows_UnorderedRows_AreSortedAndMatch(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{PartitionKeys: []typedef.ColumnDef{{Name: "pk0", Type: typedef.TypeInt}}}

	// Same rows but in different order on each side
	a := NewRow([]string{"pk0", "v"}, []any{1, "x"})
	b := NewRow([]string{"pk0", "v"}, []any{2, "y"})

	testRows := Rows{b, a}
	oracleRows := Rows{a, b}

	result := CompareCollectedRows(table, testRows, oracleRows)
	assert.NoError(t, result.ToError())
	assert.Equal(t, 2, result.MatchCount)
	assert.Empty(t, result.DifferentRows)
}

func TestCompareCollectedRows_RowCountMismatch_ExtraInOracle(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{PartitionKeys: []typedef.ColumnDef{{Name: "pk0", Type: typedef.TypeInt}}}

	r1 := NewRow([]string{"pk0", "v"}, []any{1, "x"})
	r2 := NewRow([]string{"pk0", "v"}, []any{2, "y"})

	// Oracle has one extra row
	result := CompareCollectedRows(table, Rows{r1}, Rows{r1, r2})

	// Validate result fields
	assert.Len(t, result.OracleOnlyRows, 1)
	assert.Equal(t, r2, result.OracleOnlyRows[0])
	assert.Empty(t, result.TestOnlyRows)

	err := result.ToError()
	require.Error(t, err)
	var rowDiffErr ErrorRowDifference
	require.ErrorAs(t, err, &rowDiffErr)
	// With only row-count mismatch, ToError reports counts without matches
	// (MatchCount is not included). Since test has 1 row and oracle has 2
	// rows, the error aggregates will reflect only the unmatched counts.
	assert.Equal(t, 0, rowDiffErr.TestRows)
	assert.Equal(t, 1, rowDiffErr.OracleRows)
}

func TestCompareCollectedRows_RowCountMismatch_ExtraInTest(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{PartitionKeys: []typedef.ColumnDef{{Name: "pk0", Type: typedef.TypeInt}}}

	r1 := NewRow([]string{"pk0", "v"}, []any{1, "x"})
	r2 := NewRow([]string{"pk0", "v"}, []any{2, "y"})

	// Test has one extra row
	result := CompareCollectedRows(table, Rows{r1, r2}, Rows{r1})

	// Validate result fields
	assert.Len(t, result.TestOnlyRows, 1)
	assert.Equal(t, r2, result.TestOnlyRows[0])
	assert.Empty(t, result.OracleOnlyRows)

	err := result.ToError()
	require.Error(t, err)
	var rowDiffErr ErrorRowDifference
	require.ErrorAs(t, err, &rowDiffErr)
	// With only row-count mismatch, ToError reports counts without matches
	// (MatchCount is not included). Since test has 2 rows and oracle has 1,
	// unmatched counts are: test=1, oracle=0.
	assert.Equal(t, 1, rowDiffErr.TestRows)
	assert.Equal(t, 0, rowDiffErr.OracleRows)
}

func TestCompareCollectedRows_MultipleColumnDifferences(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		PartitionKeys:  []typedef.ColumnDef{{Name: "pk0", Type: typedef.TypeInt}},
		ClusteringKeys: []typedef.ColumnDef{{Name: "ck0", Type: typedef.TypeInt}},
	}

	oracle := NewRow([]string{"pk0", "ck0", "col1", "col2"}, []any{1, 1, "A", 10})
	test := NewRow([]string{"pk0", "ck0", "col1", "col3"}, []any{1, 1, "B", "x"})

	result := CompareCollectedRows(table, Rows{test}, Rows{oracle})
	err2 := result.ToError()
	require.Error(t, err2)

	var rowDiffErr ErrorRowDifference
	require.ErrorAs(t, err2, &rowDiffErr)
	diff := rowDiffErr.Diff

	// col1 changed
	assert.Contains(t, diff, "- col1: A")
	assert.Contains(t, diff, "+ col1: B")
	// col2 missing on test
	assert.Contains(t, diff, "- col2: 10")
	// col3 added on test
	assert.Contains(t, diff, "+ col3: x")
}

func TestCompareCollectedRows_CanonicalValueRendering(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		PartitionKeys:  []typedef.ColumnDef{{Name: "pk0", Type: typedef.TypeInt}},
		ClusteringKeys: []typedef.ColumnDef{{Name: "ck0", Type: typedef.TypeInt}},
	}

	// Use deterministic time
	ts1 := time.Date(2023, 1, 2, 15, 4, 5, 0, time.UTC)
	ts2 := ts1.Add(time.Hour)

	// Use fixed UUIDs
	uuid1, err := gocql.ParseUUID("11111111-1111-1111-1111-111111111111")
	require.NoError(t, err)
	uuid2, err := gocql.ParseUUID("22222222-2222-2222-2222-222222222222")
	require.NoError(t, err)

	oracle := NewRow(
		[]string{"pk0", "ck0", "bytes", "ts", "uuid", "nilcol"},
		[]any{1, 1, []byte{0xAB, 0xCD}, ts1, uuid1, nil},
	)
	test := NewRow(
		[]string{"pk0", "ck0", "bytes", "ts", "uuid", "nilcol"},
		[]any{1, 1, []byte{0x01}, ts2, uuid2, "x"},
	)

	result := CompareCollectedRows(table, Rows{test}, Rows{oracle})
	err = result.ToError()
	require.Error(t, err)

	var rowDiffErr ErrorRowDifference
	require.ErrorAs(t, err, &rowDiffErr)
	diff := rowDiffErr.Diff

	// bytes as hex
	assert.Contains(t, diff, "- bytes: 0xabcd")
	assert.Contains(t, diff, "+ bytes: 0x01")
	// time formatted using time.DateTime
	assert.Contains(t, diff, "- ts: 2023-01-02 15:04:05")
	assert.Contains(t, diff, "+ ts: 2023-01-02 16:04:05")
	// uuid string
	assert.Contains(t, diff, "- uuid: 11111111-1111-1111-1111-111111111111")
	assert.Contains(t, diff, "+ uuid: 22222222-2222-2222-2222-222222222222")
	// nil rendering
	assert.Contains(t, diff, "- nilcol: null")
	assert.Contains(t, diff, "+ nilcol: x")
}

// Helper iterator constructors
func iterFromRows(rows Rows, err error) RowIterator {
	return func(yield func(Row, error) bool) {
		for _, r := range rows {
			if !yield(r, nil) {
				return
			}
		}
		if err != nil {
			_ = yield(Row{}, err)
		}
	}
}

func TestZipAndCompare_ErrorShortCircuit(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{PartitionKeys: []typedef.ColumnDef{{Name: "pk0", Type: typedef.TypeInt}}}
	testErr := errors.New("test-iter-error")
	oracleErr := errors.New("oracle-iter-error")

	// Only test side errors
	r1 := ZipAndCompare(t.Context(), table, iterFromRows(nil, testErr), iterFromRows(nil, nil))
	assert.ErrorIs(t, r1.TestError, testErr)
	assert.NoError(t, r1.OracleError)
	require.Error(t, r1.ToError())
	assert.ErrorIs(t, r1.ToError(), testErr)

	// Only oracle side errors
	r2 := ZipAndCompare(t.Context(), table, iterFromRows(nil, nil), iterFromRows(nil, oracleErr))
	assert.NoError(t, r2.TestError)
	assert.ErrorIs(t, r2.OracleError, oracleErr)
	require.Error(t, r2.ToError())
	assert.ErrorIs(t, r2.ToError(), oracleErr)

	// Both errors
	r3 := ZipAndCompare(t.Context(), table, iterFromRows(nil, testErr), iterFromRows(nil, oracleErr))
	assert.ErrorIs(t, r3.TestError, testErr)
	assert.ErrorIs(t, r3.OracleError, oracleErr)
	err := r3.ToError()
	require.Error(t, err)
	assert.ErrorIs(t, err, testErr)
	assert.ErrorIs(t, err, oracleErr)
}

func TestBuildRowMap_CreatesCompositePKs(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		PartitionKeys:  []typedef.ColumnDef{{Name: "pk0", Type: typedef.TypeInt}},
		ClusteringKeys: []typedef.ColumnDef{{Name: "ck0", Type: typedef.TypeInt}, {Name: "ck1", Type: typedef.TypeInt}},
	}

	r := NewRow([]string{"pk0", "ck0", "ck1", "v"}, []any{1, 2, 3, "x"})
	m := buildRowMap(table, Rows{r})
	// Expect formatted keys like "pk0=1ck0=2ck1=3" (formatRows includes key names and '='; concatenated in order)
	// We don't know exact concatenation without delimiters, so verify map has single entry and value is r.
	require.Len(t, m, 1)
	for _, got := range m {
		assert.Equal(t, r, got)
	}
}

func TestRowsToStrings_AndFormatRowForError(t *testing.T) {
	t.Parallel()

	rWithPK := NewRow([]string{"pk0", "v"}, []any{"key1", 10})
	rNoPK := NewRow([]string{"other"}, []any{"x"})

	list := rowsToStrings([]Row{rWithPK, rNoPK})
	require.Len(t, list, 2)
	assert.Equal(t, `{"pk0":"key1","v":10}`, list[0])
	assert.Equal(t, `{"other":"x"}`, list[1])
}

func TestCanonicalValueString_Edges(t *testing.T) {
	t.Parallel()

	// Nested pointers
	v := 7
	pv := &v
	ppv := &pv
	pppv := &ppv
	assert.Equal(t, "7", canonicalValueString(pppv))

	// Empty bytes -> 0x
	assert.Equal(t, "0x", canonicalValueString([]byte{}))

	// Zero time formatting
	var zt time.Time
	assert.Contains(t, canonicalValueString(zt), "0001-01-01 00:00:00")

	// UUID
	uid := gocql.TimeUUID()
	assert.Equal(t, uid.String(), canonicalValueString(uid))

	// Nil pointer
	var ip *int
	assert.Equal(t, "null", canonicalValueString(ip))
}

func TestCompareCollectedRows_DisjointPKSets(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{PartitionKeys: []typedef.ColumnDef{{Name: "pk0", Type: typedef.TypeInt}}}
	t1 := NewRow([]string{"pk0", "v"}, []any{1, "a"})
	o1 := NewRow([]string{"pk0", "v"}, []any{2, "b"})

	res := CompareCollectedRows(table, Rows{t1}, Rows{o1})
	// Equal counts but different PKs lead to a value diff (pk0 changes)
	require.Len(t, res.DifferentRows, 1)
	diff := res.DifferentRows[0].Diff
	assert.Contains(t, diff, "- pk0: 2")
	assert.Contains(t, diff, "+ pk0: 1")
}

func TestCompareCollectedRows_DuplicateRows_AreDeduplicatedBeforeComparison(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		PartitionKeys:  []typedef.ColumnDef{{Name: "pk0", Type: typedef.TypeInt}},
		ClusteringKeys: []typedef.ColumnDef{{Name: "ck0", Type: typedef.TypeInt}},
	}

	// Create identical rows
	row1 := NewRow([]string{"pk0", "ck0", "v"}, []any{1, 10, "value1"})
	row2 := NewRow([]string{"pk0", "ck0", "v"}, []any{1, 10, "value1"}) // duplicate

	// Oracle has the row once, test has it twice (simulating eventual consistency)
	testRows := Rows{row1, row2}
	oracleRows := Rows{row1}

	res := CompareCollectedRows(table, testRows, oracleRows)

	// After deduplication, both sides should have 1 row and match
	assert.Equal(t, 1, res.MatchCount, "Expected rows to match after deduplication")
	assert.Empty(t, res.TestOnlyRows, "Expected no test-only rows after deduplication")
	assert.Empty(t, res.OracleOnlyRows, "Expected no oracle-only rows after deduplication")
	assert.Empty(t, res.DifferentRows, "Expected no different rows after deduplication")
}

func TestCompareCollectedRows_DuplicateRows_BothSidesDeduplicatedCorrectly(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		PartitionKeys:  []typedef.ColumnDef{{Name: "pk0", Type: typedef.TypeInt}},
		ClusteringKeys: []typedef.ColumnDef{{Name: "ck0", Type: typedef.TypeInt}},
	}

	// Create rows
	row1 := NewRow([]string{"pk0", "ck0", "v"}, []any{1, 10, "value1"})
	row2 := NewRow([]string{"pk0", "ck0", "v"}, []any{2, 20, "value2"})

	// Both sides have duplicates
	testRows := Rows{row1, row1, row2}   // row1 appears twice
	oracleRows := Rows{row1, row2, row2} // row2 appears twice

	res := CompareCollectedRows(table, testRows, oracleRows)

	// After deduplication, both sides should have 2 unique rows and match
	assert.Equal(t, 2, res.MatchCount, "Expected 2 rows to match after deduplication")
	assert.Empty(t, res.TestOnlyRows, "Expected no test-only rows after deduplication")
	assert.Empty(t, res.OracleOnlyRows, "Expected no oracle-only rows after deduplication")
	assert.Empty(t, res.DifferentRows, "Expected no different rows after deduplication")
}
