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
	"testing"

	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
)

// TestPrepareValuesDesiredBehavior tests what the prepareValues function
// SHOULD do (this test will FAIL until the bug is fixed)
func TestPrepareValuesDesiredBehavior(t *testing.T) {
	t.Parallel()
	
	testCases := []struct {
		name           string
		values         mo.Either[[]any, []byte]
		expectedOutput string
		description    string
	}{
		{
			name:           "normal_values_should_work",
			values:         mo.Left[[]any, []byte]([]any{1, "test", true}),
			expectedOutput: `[1,"test",true]`,
			description:    "Normal values should be JSON encoded",
		},
		{
			name:           "nil_values_should_be_empty_array",
			values:         mo.Left[[]any, []byte](nil),
			expectedOutput: "[]", // DESIRED behavior - empty array instead of "null"
			description:    "Nil values should become empty array, not 'null'",
		},
		{
			name:           "empty_values_should_be_empty_array",
			values:         mo.Left[[]any, []byte]([]any{}),
			expectedOutput: "[]",
			description:    "Empty array should be preserved",
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := prepareValues(tc.values)
			assert.Equal(t, tc.expectedOutput, result, 
				"prepareValues should handle %s correctly", tc.description)
		})
	}
}

// TestStatementLoggerNeverShowsNull tests that the logger should never
// output the literal string "null" for values field
func TestStatementLoggerNeverShowsNull(t *testing.T) {
	t.Parallel()
	
	// Test various scenarios that currently produce "null"
	scenarios := []struct {
		name   string
		values mo.Either[[]any, []byte]
	}{
		{
			name:   "nil_values",
			values: mo.Left[[]any, []byte](nil),
		},
		{
			name:   "DDL_statement_simulation",
			values: mo.Left[[]any, []byte](nil), // DDL statements have no values
		},
	}
	
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			result := prepareValues(scenario.values)
			
			// This assertion will FAIL with current implementation
			assert.NotEqual(t, "null", result, 
				"Statement logger should NEVER output literal 'null' for values field")
			
			// Additional check: result should be valid JSON array
			assert.True(t, result == "[]" || (len(result) > 0 && result[0] == '['), 
				"Values should always be a JSON array, got: %s", result)
		})
	}
}
