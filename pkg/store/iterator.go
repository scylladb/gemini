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
	"context"
	"slices"
	"strings"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/multierr"

	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

// RowIterator is a function that yields rows one by one
type RowIterator func(yield func(Row, error) bool)

// ComparisonResult represents the result of comparing two row iterators
type ComparisonResult struct {
	TestError      error
	OracleError    error
	TestOnlyRows   []Row
	OracleOnlyRows []Row
	DifferentRows  []RowDifference
	MatchCount     int
}

// RowDifference represents a difference between two rows
type RowDifference struct {
	Diff      string
	TestRow   Row
	OracleRow Row
}

// Collect drains the iterator and returns all rows and any error encountered
func (ri RowIterator) Collect() (Rows, error) {
	var rows Rows
	var lastErr error

	for row, err := range ri {
		if err != nil {
			lastErr = err
			break
		}
		rows = append(rows, row)
	}

	return rows, lastErr
}

// Count returns the number of rows without storing them in memory
func (ri RowIterator) Count() (int, error) {
	count := 0
	var lastErr error

	for _, err := range ri {
		if err != nil {
			lastErr = err
			break
		}
		count++
	}

	return count, lastErr
}

// ZipAndCompare compares two row iterators row-by-row and returns detailed comparison results
// This version sorts both sides before comparison to handle unordered results
func ZipAndCompare(_ context.Context, table *typedef.Table, testIter, oracleIter RowIterator) ComparisonResult {
	result := ComparisonResult{}

	// Collect both sides - we need to sort them for comparison
	testRows, testErr := testIter.Collect()
	if testErr != nil {
		result.TestError = testErr
	}

	oracleRows, oracleErr := oracleIter.Collect()
	if oracleErr != nil {
		result.OracleError = oracleErr
	}

	// If either side has errors, return early
	if testErr != nil || oracleErr != nil {
		return result
	}

	return CompareCollectedRows(table, testRows, oracleRows)
}

// CompareCollectedRows compares already collected rows from both sides
func CompareCollectedRows(table *typedef.Table, testRows, oracleRows Rows) ComparisonResult {
	result := ComparisonResult{}

	// Handle empty result sets
	if len(testRows) == 0 && len(oracleRows) == 0 {
		return result
	}

	// Sort both sides
	if len(testRows) > 1 {
		slices.SortStableFunc(testRows, rowsCmp)
	}
	if len(oracleRows) > 1 {
		slices.SortStableFunc(oracleRows, rowsCmp)
	}

	// Compare row counts and find missing rows
	if len(testRows) != len(oracleRows) {
		testSet := pks(table, testRows)
		oracleSet := pks(table, oracleRows)

		// Build a map of pk strings to rows for efficient lookup
		testRowMap := buildRowMap(table, testRows)
		oracleRowMap := buildRowMap(table, oracleRows)

		// Find rows only in oracle (missing from test)
		for _, pk := range oracleSet.List() {
			if !testSet.Has(pk) {
				if row, ok := oracleRowMap[pk]; ok {
					result.OracleOnlyRows = append(result.OracleOnlyRows, row)
				}
			}
		}

		// Find rows only in test (missing from oracle)
		for _, pk := range testSet.List() {
			if !oracleSet.Has(pk) {
				if row, ok := testRowMap[pk]; ok {
					result.TestOnlyRows = append(result.TestOnlyRows, row)
				}
			}
		}

		return result
	}

	// Compare rows one by one
	for i := range len(testRows) {
		testRow := testRows[i]
		oracleRow := oracleRows[i]

		if diff := cmp.Diff(oracleRow, testRow, comparers...); diff != "" {
			result.DifferentRows = append(result.DifferentRows, RowDifference{
				TestRow:   testRow,
				OracleRow: oracleRow,
				Diff:      diff,
			})
		} else {
			result.MatchCount++
		}
	}

	return result
}

// buildRowMap creates a map from pk string to Row for efficient lookup
func buildRowMap(table *typedef.Table, rows Rows) map[string]Row {
	result := make(map[string]Row, len(rows))

	for _, row := range rows {
		var sb strings.Builder
		sb.Grow(64)

		// Build composite key from all partition and clustering keys
		for _, pk := range table.PartitionKeys {
			formatRows(&sb, pk.Name, row.Get(pk.Name))
		}
		for _, ck := range table.ClusteringKeys {
			formatRows(&sb, ck.Name, row.Get(ck.Name))
		}

		pkStr := sb.String()
		result[pkStr] = row
	}

	return result
}

// ToError converts ComparisonResult to an error if there are differences
func (cr ComparisonResult) ToError() error {
	if cr.TestError != nil || cr.OracleError != nil {
		return multierr.Combine(cr.TestError, cr.OracleError)
	}

	var err error

	if len(cr.TestOnlyRows) > 0 || len(cr.OracleOnlyRows) > 0 {
		err = multierr.Append(err, ErrorRowDifference{
			MissingInTest:   rowsToStrings(cr.OracleOnlyRows),
			MissingInOracle: rowsToStrings(cr.TestOnlyRows),
			TestRows:        cr.MatchCount + len(cr.TestOnlyRows),
			OracleRows:      cr.MatchCount + len(cr.OracleOnlyRows),
		})
	}

	for _, diff := range cr.DifferentRows {
		err = multierr.Append(err, ErrorRowDifference{
			Diff:      diff.Diff,
			OracleRow: diff.OracleRow,
			TestRow:   diff.TestRow,
		})
	}

	return err
}

// rowsToStrings converts rows to string representations for error reporting
func rowsToStrings(rows []Row) []string {
	result := make([]string, len(rows))
	for i, row := range rows {
		// Simple string representation - could be enhanced
		result[i] = formatRowForError(row)
	}
	return result
}

// formatRowForError formats a row for error messages
func formatRowForError(row Row) string {
	// Simple implementation - just return pk0 if available
	pk0 := row.Get("pk0")
	if pk0 != nil {
		return formatValue(pk0)
	}
	return "unknown"
}

// formatValue formats a value for display in errors
func formatValue(v any) string {
	switch val := v.(type) {
	case []byte:
		return utils.UnsafeString(val)
	case string:
		return val
	default:
		return ""
	}
}
