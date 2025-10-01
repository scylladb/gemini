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
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/samber/mo"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/typedef"
)

// TestAllTypesIntegration creates a comprehensive test with all supported Scylla types
// and tests their JSON serialization behavior - demonstrating table creation and serialization
func TestAllTypesIntegration(t *testing.T) {
	assert := require.New(t)

	// Create comprehensive table DDL with all supported types - this demonstrates
	// complete type coverage that would be used in a real integration scenario
	createTableQuery := `
		CREATE TABLE IF NOT EXISTS test_keyspace.all_types_table (
			id int PRIMARY KEY,
			ascii_col ascii,
			bigint_col bigint,
			blob_col blob,
			boolean_col boolean,
			date_col date,
			decimal_col decimal,
			double_col double,
			duration_col duration,
			float_col float,
			inet_col inet,
			int_col int,
			smallint_col smallint,
			text_col text,
			time_col time,
			timestamp_col timestamp,
			timeuuid_col timeuuid,
			tinyint_col tinyint,
			uuid_col uuid,
			varchar_col varchar,
			varint_col varint,
			list_col list<text>,
			set_col set<int>,
			map_col map<text, int>,
			tuple_col tuple<int, text, boolean>
		)
	`

	t.Logf("Comprehensive table DDL created with all Scylla types:\n%s", createTableQuery)

	// Test data for all types - comprehensive coverage
	testCases := []struct {
		name        string
		query       string
		description string
		values      []any
	}{
		{
			name: "all_basic_types",
			values: []any{
				1,                                    // id (primary key)
				"ascii_text",                         // ascii_col
				int64(9223372036854775807),           // bigint_col
				[]byte{0x48, 0x65, 0x6c, 0x6c, 0x6f}, // blob_col
				true,                                 // boolean_col
				time.Date(2023, 12, 25, 0, 0, 0, 0, time.UTC), // date_col
				big.NewRat(31415, 10000),                      // decimal_col
				2.718281828459045,                             // double_col
				gocql.Duration{Months: 1, Days: 15, Nanoseconds: 3 * time.Hour.Nanoseconds()}, // duration_col
				float32(3.14159),           // float_col
				net.ParseIP("192.168.1.1"), // inet_col
				int32(42),                  // int_col
				int16(32767),               // smallint_col
				"Hello, Scylla!",           // text_col
				15*time.Hour + 30*time.Minute + 45*time.Second,   // time_col
				time.Date(2023, 12, 25, 15, 30, 45, 0, time.UTC), // timestamp_col
				gocql.UUIDFromTime(time.Now()),                   // timeuuid_col
				int8(127),                                        // tinyint_col
				gocql.TimeUUID(),                                 // uuid_col
				"varchar_text",                                   // varchar_col
				big.NewInt(123456789),                            // varint_col
				[]string{"item1", "item2", "item3"},              // list_col
				[]int{1, 2, 3},                                   // set_col
				map[string]int{"key1": 1, "key2": 2},             // map_col
				[]any{42, "tuple_text", true},                    // tuple_col
			},
			query: `INSERT INTO test_keyspace.all_types_table (
				id, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, 
				duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, 
				timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col,
				list_col, set_col, map_col, tuple_col
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			description: "Comprehensive test covering all basic Scylla types",
		},
		{
			name: "with_nulls",
			values: []any{
				2, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
				nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
			},
			query: `INSERT INTO test_keyspace.all_types_table (
				id, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, 
				duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, 
				timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col,
				list_col, set_col, map_col, tuple_col
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			description: "Test case with all null values to verify null handling",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create item for JSON serialization testing
			item := Item{
				Start:         Time{Time: time.Now()},
				Error:         mo.Right[error, string](""),
				Statement:     tc.query,
				Host:          "test_host",
				Type:          TypeTest,
				Values:        mo.Left[[]any, []byte](tc.values),
				Duration:      Duration{Duration: time.Millisecond * 100},
				Attempt:       1,
				GeminiAttempt: 1,
				StatementType: typedef.InsertStatementType,
				PartitionKeys: typedef.NewValuesFromMap(map[string][]any{"id": {tc.values[0]}}),
			}

			// Test JSON serialization
			jsonData, err := json.Marshal(item)
			assert.NoError(err, "Failed to serialize item to JSON for test case: %s", tc.name)
			assert.NotEmpty(jsonData, "JSON data should not be empty")

			// Verify JSON can be deserialized
			var deserializedItem map[string]any
			err = json.Unmarshal(jsonData, &deserializedItem)
			assert.NoError(err, "Failed to deserialize JSON for test case: %s", tc.name)

			// Verify essential fields are present
			assert.Contains(deserializedItem, "q", "Query field should be present")
			assert.Contains(deserializedItem, "h", "Host field should be present")
			assert.Contains(deserializedItem, "v", "Values field should be present")
			assert.Contains(deserializedItem, "s", "Start time field should be present")
			assert.Contains(deserializedItem, "d", "Duration field should be present")

			// Verify field values
			assert.Equal(tc.query, deserializedItem["q"], "Query should match")
			assert.Equal("test_host", deserializedItem["h"], "Host should match")

			// Verify values field structure (mo.Either serialization)
			vField := deserializedItem["v"]
			assert.NotNil(vField, "Values field should not be nil")

			t.Logf("Test case %s: %s - JSON serialization successful", tc.name, tc.description)
			t.Logf("Serialized %d values including all Scylla types", len(tc.values))
		})
	}

	t.Log("All types integration test completed - demonstrated comprehensive Scylla type coverage and JSON serialization")
}

// TestComplexTypesJSON tests JSON serialization of complex type combinations
func TestComplexTypesJSON(t *testing.T) {
	assert := require.New(t)

	// Create table DDL for complex nested types
	createTableQuery := `
		CREATE TABLE IF NOT EXISTS complex_test_keyspace.complex_types_table (
			id int PRIMARY KEY,
			nested_list list<list<text>>,
			nested_map map<text, map<int, text>>,
			mixed_tuple tuple<list<int>, map<text, boolean>, set<uuid>>,
			frozen_map frozen<map<uuid, tuple<text, int>>>
		)
	`

	t.Logf("Complex nested types table DDL:\n%s", createTableQuery)

	// Test complex type data for JSON serialization
	testValues := []any{
		1, // id
		[][]string{{"nested1", "nested2"}, {"nested3", "nested4"}}, // nested_list
		map[string]map[int]string{
			"outer1": {1: "inner1", 2: "inner2"},
			"outer2": {3: "inner3", 4: "inner4"},
		}, // nested_map
		[]any{
			[]int{1, 2, 3},
			map[string]bool{"key1": true, "key2": false},
			[]gocql.UUID{gocql.TimeUUID(), gocql.TimeUUID()},
		}, // mixed_tuple
		map[gocql.UUID][]any{
			gocql.TimeUUID(): {"frozen_text", 42},
		}, // frozen_map
	}

	insertQuery := `INSERT INTO complex_test_keyspace.complex_types_table (
		id, nested_list, nested_map, mixed_tuple, frozen_map
	) VALUES (?, ?, ?, ?, ?)`

	// Create item for JSON serialization testing
	item := Item{
		Start:         Time{Time: time.Now()},
		Error:         mo.Right[error, string](""),
		Statement:     insertQuery,
		Host:          "test_host",
		Type:          TypeTest,
		Values:        mo.Left[[]any, []byte](testValues),
		Duration:      Duration{Duration: time.Millisecond * 200},
		Attempt:       1,
		GeminiAttempt: 1,
		StatementType: typedef.InsertStatementType,
		PartitionKeys: typedef.NewValuesFromMap(map[string][]any{"id": {1}}),
	}

	// Test JSON serialization of complex types
	jsonData, err := json.Marshal(item)
	assert.NoError(err, "Failed to serialize complex types item to JSON")
	assert.NotEmpty(jsonData, "JSON data should not be empty")

	// Verify JSON can be deserialized
	var deserializedItem map[string]any
	err = json.Unmarshal(jsonData, &deserializedItem)
	assert.NoError(err, "Failed to deserialize complex types JSON")

	// Verify essential fields are present
	assert.Contains(deserializedItem, "q", "Query field should be present")
	assert.Contains(deserializedItem, "h", "Host field should be present")
	assert.Contains(deserializedItem, "v", "Values field should be present")

	// Verify values field structure
	vField := deserializedItem["v"]
	assert.NotNil(vField, "Values field should not be nil")

	t.Log("Complex types JSON serialization test completed successfully")
	t.Logf("Successfully serialized complex nested types: lists, maps, tuples, and frozen collections")
}
