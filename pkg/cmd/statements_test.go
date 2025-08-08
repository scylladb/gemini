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

package main

import (
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestParseStatementRatiosJSON_PartialConfigurations(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name               string
		json               string
		expectError        bool
		validateInsertSum  bool
		validateDeleteSum  bool
		validateSelectSum  bool
		expectedInsertJSON float64 // -1 means don't check
		expectedInsertReg  float64 // -1 means don't check
	}{
		{
			name:               "Empty insert_subtypes should preserve defaults",
			json:               `{"mutation": {"insert": 0.8, "update": 0.15, "delete": 0.05, "insert_subtypes": {}}}`,
			validateInsertSum:  true,
			validateDeleteSum:  true,
			validateSelectSum:  true,
			expectedInsertJSON: 0.1,
			expectedInsertReg:  0.9,
		},
		{
			name:               "Only regular_insert specified - should auto-calculate json_insert",
			json:               `{"mutation": {"insert": 0.8, "update": 0.15, "delete": 0.05, "insert_subtypes": {"regular_insert": 0.7}}}`,
			validateInsertSum:  true,
			validateDeleteSum:  true,
			validateSelectSum:  true,
			expectedInsertJSON: 0.3, // 1.0 - 0.7
			expectedInsertReg:  0.7,
		},
		{
			name:               "Only json_insert specified - should auto-calculate regular_insert",
			json:               `{"mutation": {"insert": 0.8, "update": 0.15, "delete": 0.05, "insert_subtypes": {"json_insert": 0.3}}}`,
			validateInsertSum:  true,
			validateDeleteSum:  true,
			validateSelectSum:  true,
			expectedInsertJSON: 0.3,
			expectedInsertReg:  0.7, // 1.0 - 0.3
		},
		{
			name:               "Empty delete_subtypes should preserve defaults",
			json:               `{"mutation": {"insert": 0.8, "update": 0.15, "delete": 0.05, "delete_subtypes": {}}}`,
			validateInsertSum:  true,
			validateDeleteSum:  true,
			validateSelectSum:  true,
			expectedInsertJSON: -1, // Don't check
			expectedInsertReg:  -1, // Don't check
		},
		{
			name:               "Empty select_subtypes should preserve defaults",
			json:               `{"validation": {"select_subtypes": {}}}`,
			validateInsertSum:  true,
			validateDeleteSum:  true,
			validateSelectSum:  true,
			expectedInsertJSON: -1, // Don't check
			expectedInsertReg:  -1, // Don't check
		},
		{
			name:               "Partial delete_subtypes should distribute remaining",
			json:               `{"mutation": {"delete_subtypes": {"whole_partition": 0.5, "single_row": 0.3}}}`,
			validateInsertSum:  true,
			validateDeleteSum:  true,
			validateSelectSum:  true,
			expectedInsertJSON: -1, // Don't check
			expectedInsertReg:  -1, // Don't check
		},
		{
			name:               "Partial select_subtypes should distribute remaining",
			json:               `{"validation": {"select_subtypes": {"single_partition": 0.8}}}`,
			validateInsertSum:  true,
			validateDeleteSum:  true,
			validateSelectSum:  true,
			expectedInsertJSON: -1, // Don't check
			expectedInsertReg:  -1, // Don't check
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ratios, err := parseStatementRatiosJSON(tc.json)

			if tc.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			const tolerance = 0.001

			// Validate insert subtypes sum
			if tc.validateInsertSum {
				insertSum := ratios.MutationRatios.InsertSubtypeRatios.RegularInsertRatio +
					ratios.MutationRatios.InsertSubtypeRatios.JSONInsertRatio
				if math.Abs(insertSum-1.0) > tolerance {
					t.Errorf("Insert subtypes sum: got %.3f, expected 1.0", insertSum)
				}
			}

			// Validate delete subtypes sum
			if tc.validateDeleteSum {
				deleteSum := ratios.MutationRatios.DeleteSubtypeRatios.WholePartitionRatio +
					ratios.MutationRatios.DeleteSubtypeRatios.SingleRowRatio +
					ratios.MutationRatios.DeleteSubtypeRatios.SingleColumnRatio +
					ratios.MutationRatios.DeleteSubtypeRatios.MultiplePartitionsRatio
				if math.Abs(deleteSum-1.0) > tolerance {
					t.Errorf("Delete subtypes sum: got %.3f, expected 1.0", deleteSum)
				}
			}

			// Validate select subtypes sum
			if tc.validateSelectSum {
				selectSum := ratios.ValidationRatios.SelectSubtypeRatios.SinglePartitionRatio +
					ratios.ValidationRatios.SelectSubtypeRatios.MultiplePartitionRatio +
					ratios.ValidationRatios.SelectSubtypeRatios.ClusteringRangeRatio +
					ratios.ValidationRatios.SelectSubtypeRatios.MultiplePartitionClusteringRangeRatio +
					ratios.ValidationRatios.SelectSubtypeRatios.SingleIndexRatio
				if math.Abs(selectSum-1.0) > tolerance {
					t.Errorf("Select subtypes sum: got %.3f, expected 1.0", selectSum)
				}
			}

			// Check specific expected values
			if tc.expectedInsertJSON >= 0 {
				if math.Abs(ratios.MutationRatios.InsertSubtypeRatios.JSONInsertRatio-tc.expectedInsertJSON) > tolerance {
					t.Errorf("Insert JSON ratio: got %.3f, expected %.3f",
						ratios.MutationRatios.InsertSubtypeRatios.JSONInsertRatio, tc.expectedInsertJSON)
				}
			}

			if tc.expectedInsertReg >= 0 {
				if math.Abs(ratios.MutationRatios.InsertSubtypeRatios.RegularInsertRatio-tc.expectedInsertReg) > tolerance {
					t.Errorf("Insert regular ratio: got %.3f, expected %.3f",
						ratios.MutationRatios.InsertSubtypeRatios.RegularInsertRatio, tc.expectedInsertReg)
				}
			}
		})
	}
}

func TestParseStatementRatiosJSON_File(t *testing.T) {
	t.Parallel()

	// Create temporary directory for test files
	tempDir := t.TempDir()

	testCases := []struct {
		name              string
		fileContent       string
		expectError       bool
		validateInsertSum bool
		validateDeleteSum bool
		validateSelectSum bool
	}{
		{
			name: "Valid complete configuration file",
			fileContent: `{
				"mutation": {
					"insert": 0.5,
					"update": 0.3,
					"delete": 0.2,
					"insert_subtypes": {
						"regular_insert": 0.8,
						"json_insert": 0.2
					},
					"delete_subtypes": {
						"whole_partition": 0.25,
						"single_row": 0.25,
						"single_column": 0.25,
						"multiple_partitions": 0.25
					}
				},
				"validation": {
					"read": 1.0,
					"select_subtypes": {
						"single_partition": 0.2,
						"multiple_partition": 0.2,
						"clustering_range": 0.2,
						"multiple_partition_clustering_range": 0.2,
						"single_index": 0.2
					}
				}
			}`,
			validateInsertSum: true,
			validateDeleteSum: true,
			validateSelectSum: true,
		},
		{
			name: "Partial configuration file",
			fileContent: `{
				"mutation": {
					"insert": 0.8,
					"update": 0.15,
					"delete": 0.05,
					"insert_subtypes": {
						"regular_insert": 0.7
					}
				}
			}`,
			validateInsertSum: true,
			validateDeleteSum: true,
			validateSelectSum: true,
		},
		{
			name:              "Empty file should use defaults",
			fileContent:       `{}`,
			validateInsertSum: true,
			validateDeleteSum: true,
			validateSelectSum: true,
		},
		{
			name:        "Invalid JSON file",
			fileContent: `{"mutation": {"insert": 0.5, "update":}`, // malformed JSON
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Create temporary file
			filename := filepath.Join(tempDir, tc.name+".json")
			err := os.WriteFile(filename, []byte(tc.fileContent), 0o644)
			if err != nil {
				t.Fatalf("Failed to create test file: %v", err)
			}

			// Test parsing the file
			ratios, err := parseStatementRatiosJSON(filename)

			if tc.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			tolerance := 1e-9

			// Validate insert subtypes sum
			if tc.validateInsertSum {
				insertSum := ratios.MutationRatios.InsertSubtypeRatios.RegularInsertRatio +
					ratios.MutationRatios.InsertSubtypeRatios.JSONInsertRatio
				if math.Abs(insertSum-1.0) > tolerance {
					t.Errorf("Insert subtypes sum: got %.3f, expected 1.0", insertSum)
				}
			}

			// Validate delete subtypes sum
			if tc.validateDeleteSum {
				deleteSum := ratios.MutationRatios.DeleteSubtypeRatios.WholePartitionRatio +
					ratios.MutationRatios.DeleteSubtypeRatios.SingleRowRatio +
					ratios.MutationRatios.DeleteSubtypeRatios.SingleColumnRatio +
					ratios.MutationRatios.DeleteSubtypeRatios.MultiplePartitionsRatio
				if math.Abs(deleteSum-1.0) > tolerance {
					t.Errorf("Delete subtypes sum: got %.3f, expected 1.0", deleteSum)
				}
			}

			// Validate select subtypes sum
			if tc.validateSelectSum {
				selectSum := ratios.ValidationRatios.SelectSubtypeRatios.SinglePartitionRatio +
					ratios.ValidationRatios.SelectSubtypeRatios.MultiplePartitionRatio +
					ratios.ValidationRatios.SelectSubtypeRatios.ClusteringRangeRatio +
					ratios.ValidationRatios.SelectSubtypeRatios.MultiplePartitionClusteringRangeRatio +
					ratios.ValidationRatios.SelectSubtypeRatios.SingleIndexRatio
				if math.Abs(selectSum-1.0) > tolerance {
					t.Errorf("Select subtypes sum: got %.3f, expected 1.0", selectSum)
				}
			}

			// For partial config test, verify the auto-calculated value
			if tc.name == "Partial configuration file" {
				expectedJSONInsert := 0.3 // 1.0 - 0.7
				if math.Abs(ratios.MutationRatios.InsertSubtypeRatios.JSONInsertRatio-expectedJSONInsert) > tolerance {
					t.Errorf("JSON insert ratio: got %.3f, expected %.3f",
						ratios.MutationRatios.InsertSubtypeRatios.JSONInsertRatio, expectedJSONInsert)
				}
			}
		})
	}
}

func TestParseStatementRatiosJSON_FileNotFound(t *testing.T) {
	t.Parallel()

	_, err := parseStatementRatiosJSON("/nonexistent/path/to/file.json")
	if err == nil {
		t.Errorf("Expected error for non-existent file, but got none")
	}
}
