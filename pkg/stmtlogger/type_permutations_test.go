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
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/samber/mo"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/typedef"
)

// TestAllTypePermutations tests all possible permutations of collection types with all basic Scylla types
// This covers lists, sets, maps, and tuples containing every basic type
func TestAllTypePermutations(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	// All basic Scylla types for permutation testing
	basicTypes := map[string]any{
		"ascii":     "ascii_value",
		"bigint":    int64(9223372036854775807),
		"blob":      []byte{0x48, 0x65, 0x6c, 0x6c, 0x6f},
		"boolean":   true,
		"date":      time.Date(2023, 12, 25, 0, 0, 0, 0, time.UTC),
		"decimal":   big.NewRat(31415, 10000),
		"double":    2.718281828459045,
		"duration":  gocql.Duration{Months: 1, Days: 15, Nanoseconds: 3 * time.Hour.Nanoseconds()},
		"float":     float32(3.14159),
		"inet":      net.ParseIP("192.168.1.1"),
		"int":       int32(42),
		"smallint":  int16(32767),
		"text":      "text_value",
		"time":      15*time.Hour + 30*time.Minute + 45*time.Second,
		"timestamp": time.Date(2023, 12, 25, 15, 30, 45, 0, time.UTC),
		"timeuuid":  gocql.UUIDFromTime(time.Now()),
		"tinyint":   int8(127),
		"uuid":      gocql.TimeUUID(),
		"varchar":   "varchar_value",
		"varint":    big.NewInt(123456789),
	}

	// Test Lists with all types
	t.Run("lists_with_all_types", func(t *testing.T) {
		t.Parallel()
		for typeName, typeValue := range basicTypes {
			t.Run(fmt.Sprintf("list_%s", typeName), func(t *testing.T) {
				t.Parallel()
				testListType(t, assert, typeName, typeValue)
			})
		}
	})

	// Test Sets with all types
	t.Run("sets_with_all_types", func(t *testing.T) {
		t.Parallel()
		for typeName, typeValue := range basicTypes {
			t.Run(fmt.Sprintf("set_%s", typeName), func(t *testing.T) {
				t.Parallel()
				testSetType(t, assert, typeName, typeValue)
			})
		}
	})

	// Test Maps with all key-value combinations (limited to valid key types)
	t.Run("maps_with_all_types", func(t *testing.T) {
		t.Parallel()

		// Valid key types for maps (excluding blob, duration, varint, decimal which can't be keys)
		validKeyTypes := map[string]any{
			"ascii":     "ascii_key",
			"bigint":    int64(123),
			"boolean":   true,
			"date":      time.Date(2023, 12, 25, 0, 0, 0, 0, time.UTC),
			"double":    2.718,
			"float":     float32(3.14),
			"inet":      net.ParseIP("192.168.1.1"),
			"int":       int32(42),
			"smallint":  int16(123),
			"text":      "text_key",
			"time":      15*time.Hour + 30*time.Minute,
			"timestamp": time.Date(2023, 12, 25, 15, 30, 45, 0, time.UTC),
			"timeuuid":  gocql.UUIDFromTime(time.Now()),
			"tinyint":   int8(42),
			"uuid":      gocql.TimeUUID(),
			"varchar":   "varchar_key",
		}

		for keyTypeName, keyValue := range validKeyTypes {
			for valueTypeName, valueValue := range basicTypes {
				t.Run(fmt.Sprintf("map_%s_%s", keyTypeName, valueTypeName), func(t *testing.T) {
					t.Parallel()
					testMapType(t, assert, keyTypeName, keyValue, valueTypeName, valueValue)
				})
			}
		}
	})

	// Test Tuples with various type combinations
	t.Run("tuples_with_all_combinations", func(t *testing.T) {
		t.Parallel()

		// Test 2-element tuples with all type pairs
		typeNames := make([]string, 0, len(basicTypes))
		typeValues := make([]any, 0, len(basicTypes))
		for name, value := range basicTypes {
			typeNames = append(typeNames, name)
			typeValues = append(typeValues, value)
		}

		// Test all pairs (limited sample to avoid excessive test combinations)
		for i := 0; i < len(typeNames) && i < 5; i++ {
			for j := 0; j < len(typeNames) && j < 5; j++ {
				t.Run(fmt.Sprintf("tuple_%s_%s", typeNames[i], typeNames[j]), func(t *testing.T) {
					t.Parallel()
					testTupleType(t, assert, []string{typeNames[i], typeNames[j]}, []any{typeValues[i], typeValues[j]})
				})
			}
		}

		// Test 3-element tuple
		if len(typeNames) >= 3 {
			t.Run("tuple_3_elements", func(t *testing.T) {
				t.Parallel()
				testTupleType(t, assert,
					[]string{typeNames[0], typeNames[1], typeNames[2]},
					[]any{typeValues[0], typeValues[1], typeValues[2]})
			})
		}

		// Test 4-element tuple
		if len(typeNames) >= 4 {
			t.Run("tuple_4_elements", func(t *testing.T) {
				t.Parallel()
				testTupleType(t, assert,
					[]string{typeNames[0], typeNames[1], typeNames[2], typeNames[3]},
					[]any{typeValues[0], typeValues[1], typeValues[2], typeValues[3]})
			})
		}
	})
}

func testListType(t *testing.T, assert *require.Assertions, typeName string, typeValue any) {
	t.Helper()
	// Create list with multiple values of the same type
	listValue := []any{typeValue, typeValue, typeValue}

	query := fmt.Sprintf("INSERT INTO test_keyspace.list_table_%s (id, list_col) VALUES (?, ?)", typeName)
	values := []any{1, listValue}

	item := Item{
		Start:         Time{Time: time.Now()},
		Error:         mo.Right[error, string](""),
		Statement:     query,
		Host:          "test_host",
		Type:          TypeTest,
		Values:        mo.Left[[]any, []byte](values),
		Duration:      Duration{Duration: time.Millisecond * 50},
		Attempt:       1,
		GeminiAttempt: 1,
		StatementType: typedef.InsertStatementType,
		PartitionKeys: typedef.NewValuesFromMap(map[string][]any{"id": {1}}),
	}

	// Test JSON serialization
	jsonData, err := json.Marshal(item)
	assert.NoErrorf(err, "Failed to serialize list<%s> to JSON", typeName)
	assert.NotEmptyf(jsonData, "JSON data should not be empty for list<%s>", typeName)

	// Test JSON deserialization
	var deserializedItem map[string]any
	err = json.Unmarshal(jsonData, &deserializedItem)
	assert.NoErrorf(err, "Failed to deserialize JSON for list<%s>", typeName)

	// Verify essential fields
	assert.Containsf(deserializedItem, "v", "Values field should be present for list<%s>", typeName)

	t.Logf("Successfully tested list<%s> serialization", typeName)
}

func testSetType(t *testing.T, assert *require.Assertions, typeName string, typeValue any) {
	t.Helper()
	// Create set with multiple values of the same type (use different values to avoid duplicates)
	var setValue []any
	switch v := typeValue.(type) {
	case string:
		setValue = []any{v + "_1", v + "_2", v + "_3"}
	case int32:
		setValue = []any{v, v + 1, v + 2}
	case int64:
		setValue = []any{v, v + 1, v + 2}
	case int16:
		setValue = []any{v, v + 1, v + 2}
	case int8:
		setValue = []any{v, v + 1, v + 2}
	case float32:
		setValue = []any{v, v + 0.1, v + 0.2}
	case float64:
		setValue = []any{v, v + 0.1, v + 0.2}
	case bool:
		setValue = []any{true, false} // Only two boolean values possible
	default:
		setValue = []any{typeValue} // For complex types, use single value
	}

	query := fmt.Sprintf("INSERT INTO test_keyspace.set_table_%s (id, set_col) VALUES (?, ?)", typeName)
	values := []any{1, setValue}

	item := Item{
		Start:         Time{Time: time.Now()},
		Error:         mo.Right[error, string](""),
		Statement:     query,
		Host:          "test_host",
		Type:          TypeTest,
		Values:        mo.Left[[]any, []byte](values),
		Duration:      Duration{Duration: time.Millisecond * 50},
		Attempt:       1,
		GeminiAttempt: 1,
		StatementType: typedef.InsertStatementType,
		PartitionKeys: typedef.NewValuesFromMap(map[string][]any{"id": {1}}),
	}

	// Test JSON serialization
	jsonData, err := json.Marshal(item)
	assert.NoErrorf(err, "Failed to serialize set<%s> to JSON", typeName)
	assert.NotEmptyf(jsonData, "JSON data should not be empty for set<%s>", typeName)

	// Test JSON deserialization
	var deserializedItem map[string]any
	err = json.Unmarshal(jsonData, &deserializedItem)
	assert.NoErrorf(err, "Failed to deserialize JSON for set<%s>", typeName)

	// Verify essential fields
	assert.Containsf(deserializedItem, "v", "Values field should be present for set<%s>", typeName)

	t.Logf("Successfully tested set<%s> serialization", typeName)
}

func testMapType(t *testing.T, assert *require.Assertions, keyTypeName string, keyValue any, valueTypeName string, valueValue any) {
	t.Helper()
	// Create map with the key-value pair - handle non-hashable types for JSON serialization
	actualKey := keyValue

	// Convert non-hashable types to string representations for map keys
	switch v := keyValue.(type) {
	case net.IP:
		actualKey = v.String()
	case *big.Int:
		actualKey = v.String()
	case *big.Rat:
		actualKey = v.String()
	case gocql.Duration:
		actualKey = fmt.Sprintf("{%d %d %d}", v.Months, v.Days, v.Nanoseconds)
	case time.Duration:
		actualKey = v.String()
	case time.Time:
		actualKey = v.Format(time.RFC3339Nano)
	case gocql.UUID:
		actualKey = v.String()
	}

	mapValue := map[any]any{actualKey: valueValue}

	query := fmt.Sprintf("INSERT INTO test_keyspace.map_table_%s_%s (id, map_col) VALUES (?, ?)", keyTypeName, valueTypeName)
	values := []any{1, mapValue}

	item := Item{
		Start:         Time{Time: time.Now()},
		Error:         mo.Right[error, string](""),
		Statement:     query,
		Host:          "test_host",
		Type:          TypeTest,
		Values:        mo.Left[[]any, []byte](values),
		Duration:      Duration{Duration: time.Millisecond * 50},
		Attempt:       1,
		GeminiAttempt: 1,
		StatementType: typedef.InsertStatementType,
		PartitionKeys: typedef.NewValuesFromMap(map[string][]any{"id": {1}}),
	}

	// Test JSON serialization
	jsonData, err := json.Marshal(item)
	assert.NoErrorf(err, "Failed to serialize map<%s, %s> to JSON", keyTypeName, valueTypeName)
	assert.NotEmptyf(jsonData, "JSON data should not be empty for map<%s, %s>", keyTypeName, valueTypeName)

	// Test JSON deserialization
	var deserializedItem map[string]any
	err = json.Unmarshal(jsonData, &deserializedItem)
	assert.NoErrorf(err, "Failed to deserialize JSON for map<%s, %s>", keyTypeName, valueTypeName)

	// Verify essential fields
	assert.Containsf(deserializedItem, "v", "Values field should be present for map<%s, %s>", keyTypeName, valueTypeName)

	t.Logf("Successfully tested map<%s, %s> serialization", keyTypeName, valueTypeName)
}

func testTupleType(t *testing.T, assert *require.Assertions, typeNames []string, typeValues []any) {
	t.Helper()
	tupleValue := typeValues

	query := fmt.Sprintf("INSERT INTO test_keyspace.tuple_table_%d (id, tuple_col) VALUES (?, ?)", len(typeNames))
	values := []any{1, tupleValue}

	item := Item{
		Start:         Time{Time: time.Now()},
		Error:         mo.Right[error, string](""),
		Statement:     query,
		Host:          "test_host",
		Type:          TypeTest,
		Values:        mo.Left[[]any, []byte](values),
		Duration:      Duration{Duration: time.Millisecond * 50},
		Attempt:       1,
		GeminiAttempt: 1,
		StatementType: typedef.InsertStatementType,
		PartitionKeys: typedef.NewValuesFromMap(map[string][]any{"id": {1}}),
	}

	// Test JSON serialization
	jsonData, err := json.Marshal(item)
	tupleDesc := fmt.Sprintf("tuple<%v>", typeNames)
	assert.NoErrorf(err, "Failed to serialize %s to JSON", tupleDesc)
	assert.NotEmptyf(jsonData, "JSON data should not be empty for %s", tupleDesc)

	// Test JSON deserialization
	var deserializedItem map[string]any
	err = json.Unmarshal(jsonData, &deserializedItem)
	assert.NoErrorf(err, "Failed to deserialize JSON for %s", tupleDesc)

	// Verify essential fields
	assert.Containsf(deserializedItem, "v", "Values field should be present for %s", tupleDesc)

	t.Logf("Successfully tested %s serialization", tupleDesc)
}
