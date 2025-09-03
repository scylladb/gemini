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

package stmtlogger

import (
	"testing"

	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
)

func TestPrepareValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		values   mo.Either[[]any, []byte]
		expected string
	}{
		{
			name:     "nil values should return empty array",
			values:   mo.Left[[]any, []byte](nil),
			expected: "[]",
		},
		{
			name:     "empty slice should return empty array",
			values:   mo.Left[[]any, []byte]([]any{}),
			expected: "[]",
		},
		{
			name:     "values with data should be marshaled",
			values:   mo.Left[[]any, []byte]([]any{1, "test", true}),
			expected: `[1,"test",true]`,
		},
		{
			name:     "byte array should be returned as string",
			values:   mo.Right[[]any, []byte]([]byte("test data")),
			expected: "test data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := prepareValues(tt.values)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestPrepareValuesNilHandling specifically tests the nil handling fix
// This test ensures that nil values return '[]' instead of 'null'
func TestPrepareValuesNilHandling(t *testing.T) {
	t.Parallel()
	
	// Test the specific fix for nil values
	nilValues := mo.Left[[]any, []byte](nil)
	result := prepareValues(nilValues)
	
	// After the fix, nil values should return '[]' not 'null'
	assert.Equal(t, "[]", result, "nil values should return empty array '[]'")
	
	t.Logf("Fixed behavior for nil values: %s (returns empty array instead of null)", result)
}
