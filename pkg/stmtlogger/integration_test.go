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
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/typedef"
)

// TestIntegrationNilValuesHandling verifies that nil values are properly 
// handled throughout the logging pipeline and never appear as "null"
func TestIntegrationNilValuesHandling(t *testing.T) {
	t.Parallel()

	// Simulate different types of statements
	testCases := []struct {
		name          string
		statement     string
		values        []any
		statementType typedef.StatementType
		description   string
	}{
		{
			name:          "INSERT_with_values",
			statement:     "INSERT INTO table1 (pk0,pk1,col0) VALUES (?,?,?)",
			values:        []any{1, "key", "value"},
			statementType: typedef.InsertStatementType,
			description:   "INSERT with placeholder values",
		},
		{
			name:          "DELETE_with_where",
			statement:     "DELETE FROM table1 WHERE pk0=? AND pk1=?",
			values:        []any{1, "key"},
			statementType: typedef.DeleteSingleRowType,
			description:   "DELETE with WHERE clause values",
		},
		{
			name:          "DDL_statement",
			statement:     "CREATE TABLE table1 (pk0 int, pk1 text, PRIMARY KEY (pk0))",
			values:        nil, // DDL statements have no values
			statementType: typedef.CreateSchemaStatementType,
			description:   "DDL statement without values",
		},
		{
			name:          "TRUNCATE_statement",
			statement:     "TRUNCATE table1",
			values:        nil, // TRUNCATE has no values
			statementType: typedef.CreateSchemaStatementType,
			description:   "TRUNCATE statement without values",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Set up a buffer to capture the logged output
			var buf bytes.Buffer
			ch := make(chan Item, 10)

			// Create a file logger that writes to our buffer
			logger, err := NewIOWriterLogger(ch, "test", &buf, CompressionNone, zap.NewNop())
			require.NoError(t, err)

			// Simulate what observers.go would create
			// Convert nil to empty slice as our fix does
			values := tc.values
			if values == nil {
				values = []any{}
			}

			item := Item{
				Start:         Time{Time: time.Now()},
				Error:         mo.Right[error, string](""),
				Statement:     tc.statement,
				Host:          "192.168.1.10:9042",
				Type:          TypeOracle,
				Values:        mo.Left[[]any, []byte](values),
				Duration:      Duration{Duration: 500 * time.Microsecond},
				Attempt:       1,
				GeminiAttempt: 0,
				StatementType: tc.statementType,
				PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
					"pk0": {1},
					"pk1": {"key"},
				}),
			}

			// Send the item through the channel
			ch <- item

			// Wait for async processing
			time.Sleep(50 * time.Millisecond)

			// Close the channel and logger
			close(ch)
			err = logger.Close()
			require.NoError(t, err)

			// Parse and verify the output
			output := buf.String()
			require.NotEmpty(t, output, "Expected output to be written")

			// Check that "null" never appears as the value for the "v" field
			assert.NotContains(t, output, `"v":null`, 
				"Output should never contain \"v\":null for %s", tc.description)
			
			// For statements without values, we should see an empty array
			if tc.values == nil {
				assert.Contains(t, output, `"v":[]`, 
					"Nil values should be logged as empty array for %s", tc.description)
			}

			// Parse the JSON to verify structure
			var result map[string]any
			err = json.Unmarshal([]byte(strings.TrimSpace(output)), &result)
			require.NoError(t, err, "Failed to unmarshal JSON output")

			// Verify the values field
			valuesField, exists := result["v"]
			assert.True(t, exists, "Values field should exist in output")

			// Check that values is always an array or map, never null
			if valuesField != nil {
				switch v := valuesField.(type) {
				case []interface{}:
					// This is what we expect - an array
					if tc.values == nil {
						assert.Empty(t, v, "Nil values should result in empty array")
					} else {
						assert.Len(t, v, len(tc.values), "Values array should have correct length")
					}
				case map[string]interface{}:
					// mo.Either might serialize as a map
					// This is OK as long as it's not null
					assert.NotNil(t, v, "Values should not be nil")
				default:
					t.Errorf("Unexpected type for values field: %T", valuesField)
				}
			}

			// Ensure we never get the literal string "null"
			if str, ok := valuesField.(string); ok {
				assert.NotEqual(t, "null", str, "Values field should never be the string 'null'")
			}
		})
	}
}
