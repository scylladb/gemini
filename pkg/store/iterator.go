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

	"github.com/scylladb/gemini/pkg/typedef"
)

// RowIterator is a function that yields rows one by one
type RowIterator func(yield func(Row, error) bool)

// ComparisonResult represents the result of comparing two row iterators
type ComparisonResult struct {
	Table          *typedef.Table
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
	result := ComparisonResult{Table: table}

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
