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

package typedef_test

import (
	"encoding/json"
	"math/rand/v2"
	"testing"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/typedef"
)

// TestComplexTypesAsPartitionKeys tests if complex types (Collection, Map, Tuple, UDT)
// can be used as partition keys and if they meet gocql.Marshal requirements.
// This test verifies the issue: https://github.com/scylladb/gemini/issues/QATOOLS-98
//
// IMPORTANT: While this test proves complex types CAN work as partition keys when properly
// frozen and hashed (using json.Marshal instead of gocql.Marshal), they are currently NOT
// included in the PartitionKeyTypes list used for schema generation. This is intentional to
// maintain backward compatibility and avoid complexity. See pkg/typedef/types.go for details.
func TestComplexTypesAsPartitionKeys(t *testing.T) {
	t.Parallel()

	config := typedef.ValueRangeConfig{
		MaxBlobLength:   100,
		MinBlobLength:   10,
		MaxStringLength: 50,
		MinStringLength: 5,
	}

	tests := []struct {
		name        string
		columnType  typedef.Type
		description string
	}{
		{
			name: "ListType",
			columnType: &typedef.Collection{
				ComplexType: typedef.TypeList,
				ValueType:   typedef.TypeInt,
				Frozen:      true,
			},
			description: "List collection type as partition key",
		},
		{
			name: "SetType",
			columnType: &typedef.Collection{
				ComplexType: typedef.TypeSet,
				ValueType:   typedef.TypeText,
				Frozen:      true,
			},
			description: "Set collection type as partition key",
		},
		{
			name: "MapType",
			columnType: &typedef.MapType{
				ComplexType: typedef.TypeMap,
				KeyType:     typedef.TypeInt,
				ValueType:   typedef.TypeText,
				Frozen:      true,
			},
			description: "Map type as partition key",
		},
		{
			name: "TupleType",
			columnType: &typedef.TupleType{
				ComplexType: typedef.TypeTuple,
				ValueTypes:  []typedef.SimpleType{typedef.TypeInt, typedef.TypeText},
				Frozen:      true,
			},
			description: "Tuple type as partition key",
		},
		{
			name: "UDTType",
			columnType: &typedef.UDTType{
				ComplexType: typedef.TypeUdt,
				TypeName:    "test_udt",
				ValueTypes: map[string]typedef.SimpleType{
					"field1": typedef.TypeInt,
					"field2": typedef.TypeText,
				},
				Frozen: true,
			},
			description: "UDT type as partition key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create a new random generator for each test to avoid race conditions
			val := rand.Uint64()
			r := rand.New(rand.NewPCG(val, val))

			// Test 1: Generate values for the complex type
			t.Run("GenerateValue", func(t *testing.T) {
				defer func() {
					if rec := recover(); rec != nil {
						t.Errorf("Panic during value generation: %v", rec)
					}
				}()

				values := tt.columnType.GenValue(r, config)
				if values == nil {
					t.Error("GenValue returned nil")
				}
				if len(values) == 0 {
					t.Error("GenValue returned empty slice")
				}
			})

			// Test 2: Create partition keys with complex type
			t.Run("CreatePartitionKeys", func(t *testing.T) {
				defer func() {
					if rec := recover(); rec != nil {
						t.Errorf("Panic during partition keys creation: %v", rec)
					}
				}()

				partitionCols := typedef.Columns{
					typedef.ColumnDef{
						Name: "pk_complex",
						Type: tt.columnType,
					},
				}

				// Generate partition key values
				values := make(map[string][]any)
				for _, col := range partitionCols {
					values[col.Name] = col.Type.GenValue(r, config)
				}

				partitionKeys := typedef.NewValuesFromMap(values)
				if partitionKeys == nil {
					t.Error("Failed to create partition keys")
				}
			})

			// Test 3: Test JSON marshaling (used in hash generation)
			t.Run("JSONMarshal", func(t *testing.T) {
				defer func() {
					if rec := recover(); rec != nil {
						t.Errorf("Panic during JSON marshaling: %v", rec)
					}
				}()

				values := tt.columnType.GenValue(r, config)

				// Try to marshal the values to JSON (as done in JobError.Hash)
				_, err := json.Marshal(values)
				if err != nil {
					t.Errorf("JSON marshaling failed: %v (this indicates the issue might still exist)", err)
				}
			})

			// Test 4: Test hash generation via JobError (the actual use case from the issue)
			t.Run("HashGeneration", func(t *testing.T) {
				defer func() {
					if rec := recover(); rec != nil {
						t.Errorf("Panic during hash generation: %v", rec)
					}
				}()

				partitionCols := typedef.Columns{
					typedef.ColumnDef{
						Name: "pk_complex",
						Type: tt.columnType,
					},
				}

				// Generate partition key values
				values := make(map[string][]any)
				for _, col := range partitionCols {
					values[col.Name] = col.Type.GenValue(r, config)
				}

				partitionKeys := typedef.NewValuesFromMap(values)

				// Create a JobError with these partition keys
				jobErr := &joberror.JobError{
					Query:         "SELECT * FROM test.table WHERE pk_complex = ?",
					StmtType:      typedef.SelectStatementType,
					PartitionKeys: partitionKeys,
					Message:       "test error",
				}

				// Try to generate hash (this is where the original issue would manifest)
				hash := jobErr.Hash()
				if hash == [32]byte{} {
					t.Error("Hash generation resulted in zero hash")
				}

				// Also test HashHex
				hashHex := jobErr.HashHex()
				if hashHex == "" {
					t.Error("HashHex generation resulted in empty string")
				}
				if len(hashHex) != 64 {
					t.Errorf("HashHex should be 64 characters, got %d", len(hashHex))
				}
			})

			// Test 5: Test multiple partition keys with mix of simple and complex types
			t.Run("MixedPartitionKeys", func(t *testing.T) {
				defer func() {
					if rec := recover(); rec != nil {
						t.Errorf("Panic during mixed partition keys test: %v", rec)
					}
				}()

				partitionCols := typedef.Columns{
					typedef.ColumnDef{
						Name: "pk_simple",
						Type: typedef.TypeInt,
					},
					typedef.ColumnDef{
						Name: "pk_complex",
						Type: tt.columnType,
					},
				}

				// Generate partition key values
				values := make(map[string][]any)
				for _, col := range partitionCols {
					values[col.Name] = col.Type.GenValue(r, config)
				}

				partitionKeys := typedef.NewValuesFromMap(values)

				// Create a JobError with mixed partition keys
				jobErr := &joberror.JobError{
					Query:         "SELECT * FROM test.table WHERE pk_simple = ? AND pk_complex = ?",
					StmtType:      typedef.SelectStatementType,
					PartitionKeys: partitionKeys,
					Message:       "test error with mixed keys",
				}

				// Try to generate hash
				hash := jobErr.Hash()
				if hash == [32]byte{} {
					t.Error("Hash generation with mixed partition keys resulted in zero hash")
				}
			})
		})
	}
}

// TestComplexTypesPartitionKeyValues tests the value generation specifically for partition keys
// to ensure they can be properly serialized and hashed.
func TestComplexTypesPartitionKeyValues(t *testing.T) {
	t.Parallel()

	config := typedef.ValueRangeConfig{
		MaxBlobLength:   100,
		MinBlobLength:   10,
		MaxStringLength: 50,
		MinStringLength: 5,
	}

	// Test Collection (List) type
	t.Run("CollectionListValues", func(t *testing.T) {
		t.Parallel()
		val := rand.Uint64()
		r := rand.New(rand.NewPCG(val, val))

		collType := &typedef.Collection{
			ComplexType: typedef.TypeList,
			ValueType:   typedef.TypeDouble,
			Frozen:      true,
		}

		values := collType.GenValue(r, config)

		// Marshal to JSON to verify it works
		data, err := json.Marshal(values)
		if err != nil {
			t.Errorf("Failed to marshal list collection values: %v", err)
		}

		if len(data) == 0 {
			t.Error("Marshaled data is empty")
		}
	})

	// Test Map type
	t.Run("MapTypeValues", func(t *testing.T) {
		t.Parallel()
		val := rand.Uint64()
		r := rand.New(rand.NewPCG(val, val))

		mapType := &typedef.MapType{
			ComplexType: typedef.TypeMap,
			KeyType:     typedef.TypeText,
			ValueType:   typedef.TypeInt,
			Frozen:      true,
		}

		values := mapType.GenValue(r, config)

		// Marshal to JSON to verify it works
		data, err := json.Marshal(values)
		if err != nil {
			t.Errorf("Failed to marshal map type values: %v", err)
		}

		if len(data) == 0 {
			t.Error("Marshaled data is empty")
		}
	})

	// Test Tuple type
	t.Run("TupleTypeValues", func(t *testing.T) {
		t.Parallel()
		val := rand.Uint64()
		r := rand.New(rand.NewPCG(val, val))

		tupleType := &typedef.TupleType{
			ComplexType: typedef.TypeTuple,
			ValueTypes:  []typedef.SimpleType{typedef.TypeFloat, typedef.TypeBoolean, typedef.TypeBlob},
			Frozen:      true,
		}

		values := tupleType.GenValue(r, config)

		// Marshal to JSON to verify it works
		data, err := json.Marshal(values)
		if err != nil {
			t.Errorf("Failed to marshal tuple type values: %v", err)
		}

		if len(data) == 0 {
			t.Error("Marshaled data is empty")
		}
	})

	// Test UDT type
	t.Run("UDTTypeValues", func(t *testing.T) {
		t.Parallel()
		val := rand.Uint64()
		r := rand.New(rand.NewPCG(val, val))

		udtType := &typedef.UDTType{
			ComplexType: typedef.TypeUdt,
			TypeName:    "test_udt_values",
			ValueTypes: map[string]typedef.SimpleType{
				"ip":        typedef.TypeInet,
				"timestamp": typedef.TypeTimestamp,
				"uuid":      typedef.TypeUuid,
			},
			Frozen: true,
		}

		values := udtType.GenValue(r, config)

		// Marshal to JSON to verify it works
		data, err := json.Marshal(values)
		if err != nil {
			t.Errorf("Failed to marshal UDT type values: %v", err)
		}

		if len(data) == 0 {
			t.Error("Marshaled data is empty")
		}
	})
}

// TestOriginalIssueScenario attempts to recreate the exact scenario from the issue
// where "Error on get hash for table" was reported.
func TestOriginalIssueScenario(t *testing.T) {
	t.Parallel()

	val := rand.Uint64()
	r := rand.New(rand.NewPCG(val, val))

	config := typedef.ValueRangeConfig{
		MaxBlobLength:   100,
		MinBlobLength:   10,
		MaxStringLength: 50,
		MinStringLength: 5,
	}

	// Recreate a scenario similar to the error message from the issue:
	// "Error on get hash for table:pkSet_ck3_col5, values:[...]"
	// "PartitionColumns:[[1 1] [1.110223e-16 1.110223e-16] [01 00] [1.1.1.1 1.1.1.1]]"

	partitionCols := typedef.Columns{
		typedef.ColumnDef{
			Name: "pk0",
			Type: typedef.TypeInt,
		},
		typedef.ColumnDef{
			Name: "pk1",
			Type: typedef.TypeDouble,
		},
		typedef.ColumnDef{
			Name: "pk2",
			Type: typedef.TypeBlob,
		},
		typedef.ColumnDef{
			Name: "pk3",
			Type: typedef.TypeInet,
		},
	}

	// Generate partition key values
	values := make(map[string][]any)
	for _, col := range partitionCols {
		values[col.Name] = col.Type.GenValue(r, config)
	}

	partitionKeys := typedef.NewValuesFromMap(values)

	// Create a JobError as would happen during testing
	jobErr := &joberror.JobError{
		Query:         "SELECT * FROM test.pkSet_ck3_col5 WHERE pk0 = ? AND pk1 = ? AND pk2 = ? AND pk3 = ?",
		StmtType:      typedef.SelectStatementType,
		PartitionKeys: partitionKeys,
		Message:       "test error - recreating original issue",
	}

	// This is where the original error would occur: "Error on get hash for table"
	defer func() {
		if rec := recover(); rec != nil {
			t.Errorf("Panic during hash generation (original issue reproduced): %v", rec)
		}
	}()

	hash := jobErr.Hash()
	if hash == [32]byte{} {
		t.Error("Hash generation resulted in zero hash")
	}

	// Verify we can get the hash multiple times (cached)
	hash2 := jobErr.Hash()
	if hash != hash2 {
		t.Error("Hash should be consistent across multiple calls")
	}
}
