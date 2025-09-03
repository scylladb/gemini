// Copyright 2025 ScyllaDB
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

package stmtlogger

import (
	"slices"
	"testing"

	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
)

// TestValuesNullIssue reproduces the issue where statement logger
// shows 'null' for values instead of the actual values
func TestValuesNullIssue(t *testing.T) {
	t.Parallel()
	
	testCases := []struct {
		name           string
		values         mo.Either[[]any, []byte]
		expectedJSON   string
		description    string
	}{
		{
			name:         "insert_with_values",
			values:       mo.Left[[]any, []byte]([]any{1, "test", 3.14}),
			expectedJSON: `[1,"test",3.14]`,
			description:  "INSERT statement with actual values",
		},
		{
			name:         "delete_with_pk_values", 
			values:       mo.Left[[]any, []byte]([]any{42, "key"}),
			expectedJSON: `[42,"key"]`,
			description:  "DELETE statement with partition key values",
		},
		{
			name:         "statement_with_nil_values",
			values:       mo.Left[[]any, []byte](nil),
			expectedJSON: "[]", // Fixed: nil now becomes empty array
			description:  "Statement with nil values array (e.g., DDL statements)",
		},
		{
			name:         "statement_with_empty_values",
			values:       mo.Left[[]any, []byte]([]any{}),
			expectedJSON: "[]",
			description:  "Statement with empty values array",
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test the prepareValues function which is what ScyllaLogger uses
			// This simulates what happens when observers.go passes values to the logger
			preparedValue := prepareValues(tc.values)
			
			// After the fix, all values should match expected
			assert.Equal(t, tc.expectedJSON, preparedValue,
				"Values should match expected for case: %s", tc.description)
		})
	}
}

// TestPrepareValuesFunction tests the prepareValues function behavior
func TestPrepareValuesFunction(t *testing.T) {
	t.Parallel()
	
	testCases := []struct {
		name           string
		values         mo.Either[[]any, []byte]
		expectedOutput string
		description    string
	}{
		{
			name:           "normal_values",
			values:         mo.Left[[]any, []byte]([]any{1, "test", true}),
			expectedOutput: `[1,"test",true]`,
			description:    "Normal values should be JSON encoded",
		},
		{
			name:           "nil_values",
			values:         mo.Left[[]any, []byte](nil),
			expectedOutput: "[]", // Fixed: nil values now become empty array
			description:    "Nil values now become empty array (fixed)",
		},
		{
			name:           "empty_values",
			values:         mo.Left[[]any, []byte]([]any{}),
			expectedOutput: "[]",
			description:    "Empty array should be preserved",
		},
		{
			name:           "byte_values",
			values:         mo.Right[[]any, []byte]([]byte("raw bytes")),
			expectedOutput: "raw bytes",
			description:    "Byte values should be returned as-is",
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := prepareValues(tc.values)
			assert.Equal(t, tc.expectedOutput, result, 
				"prepareValues output mismatch for: %s", tc.description)
		})
	}
}

// TestObserverValueHandling simulates how observers.go creates Items with values
func TestObserverValueHandling(t *testing.T) {
	t.Parallel()
	
	// Simulate what happens in observers.go when query.Values or batch.Values[i] is nil
	simulateObserverBehavior := func(queryValues []any) mo.Either[[]any, []byte] {
		// This is what observers.go does: 
		// Values: mo.Left[[]any, []byte](slices.Clone(query.Values))
		// When query.Values is nil, slices.Clone returns nil
		return mo.Left[[]any, []byte](slices.Clone(queryValues))
	}
	
	// Test cases representing different query scenarios
	testCases := []struct {
		name        string
		queryValues []any // What gocql provides
		statement   string
		expectNull  bool
	}{
		{
			name:        "insert_with_placeholders",
			queryValues: []any{1, "value", 3.14},
			statement:   "INSERT INTO table1 (pk0, col1, col2) VALUES (?, ?, ?)",
			expectNull:  false,
		},
		{
			name:        "delete_with_where",
			queryValues: []any{42, "key"},
			statement:   "DELETE FROM table1 WHERE pk0=? AND pk1=?",
			expectNull:  false,
		},
		{
			name:        "statement_without_placeholders",
			queryValues: nil, // gocql sets this to nil when no placeholders
			statement:   "CREATE TABLE test (id int PRIMARY KEY)",
			expectNull:  false, // Fixed: no longer produces 'null'
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			values := simulateObserverBehavior(tc.queryValues)
			prepared := prepareValues(values)
			
			if tc.expectNull {
				assert.Equal(t, "null", prepared, 
					"Statement without placeholders should result in 'null' string: %s", tc.statement)
			} else {
				assert.NotEqual(t, "null", prepared,
					"Statement should not produce 'null': %s", tc.statement)
			}
			// Additional check: all values should now be arrays
			assert.True(t, prepared == "[]" || (len(prepared) > 0 && prepared[0] == '['),
				"All values should be JSON arrays after fix, got: %s", prepared)
		})
	}
}
