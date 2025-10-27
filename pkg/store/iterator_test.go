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
