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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/samber/mo"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/typedef"
)

// TestItemJSONSerialization tests that Item serializes/deserializes correctly
// without requiring testcontainers
func TestItemJSONSerialization(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	// Create a test item with various Scylla types
	testCases := []struct {
		name   string
		values []any
	}{
		{"integers", []any{int32(42), int64(123), int16(456)}},
		{"floats", []any{float32(3.14), float64(2.718)}},
		{"strings", []any{"text", "ascii", "varchar"}},
		{"booleans", []any{true, false}},
		{"mixed", []any{42, "test", true, 3.14}},
		{"with_nil", []any{42, nil, "test"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			item := Item{
				Start:         Time{Time: time.Now()},
				Error:         mo.Right[error, string](""),
				Statement:     fmt.Sprintf("INSERT INTO test_%s VALUES (?)", tc.name),
				Host:          "test_host",
				Type:          TypeTest,
				Values:        mo.Left[[]any, []byte](tc.values),
				Duration:      Duration{Duration: time.Millisecond * 100},
				Attempt:       1,
				GeminiAttempt: 1,
				StatementType: typedef.InsertStatementType,
			}

			// Test JSON marshaling
			jsonData, err := json.Marshal(item)
			assert.NoError(err, "Should marshal to JSON successfully")
			assert.NotEmpty(jsonData, "JSON should not be empty")

			// Test JSON unmarshaling
			var result map[string]any
			err = json.Unmarshal(jsonData, &result)
			assert.NoError(err, "Should unmarshal from JSON successfully")

			// Verify key fields are present with correct abbreviated names
			assert.Contains(result, "s", "Should have start time field")
			assert.Contains(result, "q", "Should have statement field")
			assert.Contains(result, "h", "Should have host field")
			assert.Contains(result, "v", "Should have values field")
			assert.Contains(result, "d", "Should have duration field")
			assert.Contains(result, "d_a", "Should have attempt field")

			// Verify field values
			assert.Equal(item.Statement, result["q"], "Statement should match")
			assert.Equal(item.Host, result["h"], "Host should match")
			assert.Equal(float64(item.Attempt), result["d_a"], "Attempt should match")

			// Verify values field exists (regardless of exact structure)
			vField := result["v"]
			assert.NotNil(vField, "Values field should not be nil")

			// Handle mo.Either structure - values might be nested in a map
			if vMap, ok := vField.(map[string]interface{}); ok {
				// Look for "left" field in mo.Either structure
				if leftField, hasLeft := vMap["left"]; hasLeft {
					// Convert to string and check it contains our expected values
					vStr := fmt.Sprintf("%v", leftField)
					for _, val := range tc.values {
						if val != nil {
							valStr := fmt.Sprintf("%v", val)
							assert.Contains(vStr, valStr, "Values should contain %v", val)
						}
					}
				}
			} else {
				// Fallback to direct string conversion
				vStr := fmt.Sprintf("%v", vField)
				for _, val := range tc.values {
					if val != nil {
						valStr := fmt.Sprintf("%v", val)
						assert.Contains(vStr, valStr, "Values should contain %v", val)
					}
				}
			}

			t.Logf("Test %s: JSON serialization successful, values field type: %T",
				tc.name, vField)
		})
	}
}

// TestScyllaTypesBasic tests basic serialization of all Scylla types
func TestScyllaTypesBasic(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	// Test basic types that should serialize without issues
	basicTypes := map[string]any{
		"int32":   int32(42),
		"int64":   int64(123456789),
		"float32": float32(3.14159),
		"float64": float64(2.718281828),
		"string":  "Hello Scylla",
		"bool":    true,
		"nil":     nil,
	}

	for typeName, value := range basicTypes {
		t.Run(typeName, func(t *testing.T) {
			t.Parallel()
			item := Item{
				Start:         Time{Time: time.Now()},
				Error:         mo.Right[error, string](""),
				Statement:     fmt.Sprintf("INSERT INTO test_%s VALUES (?)", typeName),
				Host:          "test_host",
				Type:          TypeTest,
				Values:        mo.Left[[]any, []byte]([]any{value}),
				Duration:      Duration{Duration: time.Millisecond * 50},
				Attempt:       1,
				GeminiAttempt: 1,
				StatementType: typedef.InsertStatementType,
			}

			// Test that we can serialize and deserialize without errors
			jsonData, err := json.Marshal(item)
			assert.NoError(err, "Should serialize %s type successfully", typeName)

			var result map[string]any
			err = json.Unmarshal(jsonData, &result)
			assert.NoError(err, "Should deserialize %s type successfully", typeName)

			// Verify the values field exists
			assert.Contains(result, "v", "Should have values field for %s", typeName)

			t.Logf("Type %s: Successfully serialized/deserialized", typeName)
		})
	}
}
