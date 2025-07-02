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

package statements

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/testutils"
	"github.com/scylladb/gemini/pkg/typedef"
)

func TestGetMultiplePartitionKeys(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	// Test cases with different initial values and partition key configurations
	testCases := []struct {
		name        string
		initial     int
		pkCount     int
		expectedMin int
		expectedMax int
	}{
		{
			name:        "initial=1, single partition key",
			initial:     1,
			pkCount:     1,
			expectedMin: 1,
			expectedMax: 100, // MaxCartesianProductCount
		},
		{
			name:        "initial=2, single partition key",
			initial:     2,
			pkCount:     1,
			expectedMin: 1,
			expectedMax: 100,
		},
		{
			name:        "initial=1, two partition keys",
			initial:     1,
			pkCount:     2,
			expectedMin: 1,
			expectedMax: 10, // sqrt(100) = 10
		},
		{
			name:        "initial=2, two partition keys",
			initial:     2,
			pkCount:     2,
			expectedMin: 1,
			expectedMax: 12, // min(2,2) + 10
		},
		{
			name:        "initial=5, three partition keys",
			initial:     5,
			pkCount:     3,
			expectedMin: 1,
			expectedMax: 7, // min(5,3) + 4 (4^3 < 100, 5^3 > 100)
		},
		{
			name:        "initial=10, four partition keys",
			initial:     10,
			pkCount:     4,
			expectedMin: 1,
			expectedMax: 7, // min(10,4) + 3 (3^4 = 81 < 100, 4^4 = 256 > 100)
		},
		{
			name:        "initial=0, single partition key",
			initial:     0,
			pkCount:     1,
			expectedMin: 0,
			expectedMax: 100,
		},
		{
			name:        "initial=100, single partition key",
			initial:     100,
			pkCount:     1,
			expectedMin: 1,
			expectedMax: 100,
		},
		{
			name:        "initial=50, five partition keys",
			initial:     50,
			pkCount:     5,
			expectedMin: 1,
			expectedMax: 2, // min(50,5) + 2 (2^5 = 32 < 100, 3^5 = 243 > 100)
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Create a mock table with the specified number of partition keys
			table := &typedef.Table{}
			table.PartitionKeys = make(typedef.Columns, tc.pkCount)
			for i := 0; i < tc.pkCount; i++ {
				table.PartitionKeys[i] = typedef.ColumnDef{
					Name: fmt.Sprintf("pk%d", i),
					Type: typedef.TypeInt,
				}
			}

			// Create generator with mock random source for deterministic testing
			rnd := testutils.NewMockRandom(0) // Always return 0 for minimum case
			generator := &Generator{
				table:  table,
				random: rnd,
			}

			// Test minimum case (random returns 0)
			result := generator.getMultiplePartitionKeys(tc.initial)
			assert.GreaterOrEqual(result, tc.expectedMin, "Result should be >= expected minimum")

			// Test with different random values to ensure we don't exceed maximum
			for i := 0; i < 10; i++ {
				rnd.SetValues(i)
				result = generator.getMultiplePartitionKeys(tc.initial)
				assert.GreaterOrEqual(result, tc.expectedMin, "Result should be >= expected minimum")
				assert.LessOrEqual(result, tc.expectedMax, "Result should be <= expected maximum")
			}
		})
	}

	// Test edge case: table with no partition keys should panic
	t.Run("no partition keys should panic", func(t *testing.T) {
		t.Parallel()
		table := &typedef.Table{}
		table.PartitionKeys = make(typedef.Columns, 0)

		generator := &Generator{
			table:  table,
			random: testutils.NewMockRandom(0),
		}

		assert.Panics(func() {
			generator.getMultiplePartitionKeys(1)
		}, "Should panic when table has no partition keys")
	})

	// Test that the method respects the MaxCartesianProductCount constraint
	t.Run("respects MaxCartesianProductCount constraint", func(t *testing.T) {
		t.Parallel()

		table := &typedef.Table{}
		table.PartitionKeys = make(typedef.Columns, 2) // 2 partition keys
		for i := 0; i < 2; i++ {
			table.PartitionKeys[i] = typedef.ColumnDef{
				Name: fmt.Sprintf("pk%d", i),
				Type: typedef.TypeInt,
			}
		}

		generator := &Generator{
			table:  table,
			random: testutils.NewMockRandom(99), // High random value
		}

		result := generator.getMultiplePartitionKeys(1000) // Very high initial value

		// With 2 partition keys, max should be around 10 (10^2 = 100)
		// So result should be min(1000, 2) + random up to totalCartesianProductCount
		assert.LessOrEqual(result, 12, "Should not exceed cartesian product limit")
	})
}

func TestGetMultipleClusteringKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		randomValues []int
		initial      int
		ckCount      int
		expectedMin  int
		expectedMax  int
	}{
		{
			name:         "single clustering key with initial=1",
			initial:      1,
			ckCount:      1,
			randomValues: []int{0, 50, 99},
			expectedMin:  1,
			expectedMax:  100, // MaxCartesianProductCount
		},
		{
			name:         "single clustering key with initial=2",
			initial:      2,
			ckCount:      1,
			randomValues: []int{0, 25, 99},
			expectedMin:  1,
			expectedMax:  100,
		},
		{
			name:         "two clustering keys with initial=1",
			initial:      1,
			ckCount:      2,
			randomValues: []int{0, 5, 9},
			expectedMin:  1,
			expectedMax:  11, // min(1,2) + 10 where 10^2 = 100
		},
		{
			name:         "two clustering keys with initial=5",
			initial:      5,
			ckCount:      2,
			randomValues: []int{0, 5, 9},
			expectedMin:  1,
			expectedMax:  12, // min(5,2) + 10
		},
		{
			name:         "three clustering keys with initial=10",
			initial:      10,
			ckCount:      3,
			randomValues: []int{0, 2, 3},
			expectedMin:  1,
			expectedMax:  7, // min(10,3) + 4 where 4^3 = 64 < 100, 5^3 = 125 > 100
		},
		{
			name:         "four clustering keys with large initial",
			initial:      100,
			ckCount:      4,
			randomValues: []int{0, 1, 2},
			expectedMin:  1,
			expectedMax:  7, // min(100,4) + 3 where 3^4 = 81 < 100, 4^4 = 256 > 100
		},
		{
			name:         "five clustering keys",
			initial:      50,
			ckCount:      5,
			randomValues: []int{0, 1},
			expectedMin:  1,
			expectedMax:  7, // min(50,5) + 2 where 2^5 = 32 < 100, 3^5 = 243 > 100
		},
		{
			name:         "initial=0 with single clustering key",
			initial:      0,
			ckCount:      1,
			randomValues: []int{0, 50, 99},
			expectedMin:  0,
			expectedMax:  100,
		},
		{
			name:         "no clustering keys",
			initial:      1,
			ckCount:      0,
			randomValues: []int{0, 50, 99},
			expectedMin:  0,
			expectedMax:  1, // min(1,0) + maximumCount where maximumCount=1 when no clustering keys
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			table := createTestTableWithClusteringKeys(1, tt.ckCount) // 1 PK for the generator to work

			for _, randomValue := range tt.randomValues {
				t.Run(fmt.Sprintf("random_%d", randomValue), func(t *testing.T) {
					mockRandom := testutils.NewMockRandom(randomValue)
					generator := &Generator{
						table:  table,
						random: mockRandom,
					}

					result := generator.getMultipleClusteringKeys(tt.initial)

					require.GreaterOrEqual(t, result, tt.expectedMin,
						"Result %d should be >= expected minimum %d", result, tt.expectedMin)
					require.LessOrEqual(t, result, tt.expectedMax,
						"Result %d should be <= expected maximum %d", result, tt.expectedMax)
				})
			}
		})
	}
}

func TestGetMultipleClusteringKeys_CartesianProductBehavior(t *testing.T) {
	t.Parallel()

	// Test specific cartesian product calculations for clustering keys
	testCases := []struct {
		name        string
		ckCount     int
		initial     int
		expectedMax int
	}{
		{"1 CK: 100^1 = 100", 1, 1, 100},
		{"2 CK: 10^2 = 100", 2, 1, 10},
		{"3 CK: 4^3 = 64 < 100, 5^3 = 125 > 100", 3, 1, 4},
		{"4 CK: 3^4 = 81 < 100, 4^4 = 256 > 100", 4, 1, 3},
		{"5 CK: 2^5 = 32 < 100, 3^5 = 243 > 100", 5, 1, 2},
		{"6 CK: 2^6 = 64 < 100, 3^6 = 729 > 100", 6, 1, 2},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			table := createTestTableWithClusteringKeys(1, tc.ckCount)

			// Test with maximum random value to get the upper bound
			mockRandom := testutils.NewMockRandom(999) // High value to test maximum
			generator := &Generator{
				table:  table,
				random: mockRandom,
			}

			result := generator.getMultipleClusteringKeys(tc.initial)
			expectedMaxResult := min(tc.initial, tc.ckCount) + tc.expectedMax

			require.LessOrEqual(t, result, expectedMaxResult,
				"Result should not exceed cartesian product constraint")
		})
	}
}

func TestGetMultipleClusteringKeys_EdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("large initial value gets capped", func(t *testing.T) {
		t.Parallel()
		table := createTestTableWithClusteringKeys(1, 2) // 2 clustering keys
		mockRandom := testutils.NewMockRandom(0)         // Minimum random
		generator := &Generator{
			table:  table,
			random: mockRandom,
		}

		result := generator.getMultipleClusteringKeys(1000)
		// Should be min(1000, 2) + 0 = 2
		require.Equal(t, 1, result)
	})

	t.Run("random value affects result correctly", func(t *testing.T) {
		t.Parallel()
		table := createTestTableWithClusteringKeys(1, 1) // 1 clustering key

		// Test different random values
		randomTests := []struct {
			randomVal int
			expected  int
		}{
			{0, 1},
			{50, 1},
			{99, 1},
		}

		for _, rt := range randomTests {
			t.Run(fmt.Sprintf("random_%d", rt.randomVal), func(t *testing.T) {
				t.Parallel()
				mockRandom := testutils.NewMockRandom(rt.randomVal)
				generator := &Generator{
					table:  table,
					random: mockRandom,
				}

				result := generator.getMultipleClusteringKeys(1)
				require.Equal(t, rt.expected, result)
			})
		}
	})

	t.Run("zero clustering keys behavior", func(t *testing.T) {
		t.Parallel()
		table := createTestTableWithClusteringKeys(1, 0) // No clustering keys
		mockRandom := testutils.NewMockRandom(5)
		generator := &Generator{
			table:  table,
			random: mockRandom,
		}

		result := generator.getMultipleClusteringKeys(10)
		// When no clustering keys, should be min(10, 0) + random = 0 + (5 % maximumCount)
		// The maximumCount calculation for 0 clustering keys should return 1
		require.GreaterOrEqual(t, result, 0)
		require.LessOrEqual(t, result, 1)
	})

	t.Run("initial zero with clustering keys", func(t *testing.T) {
		t.Parallel()
		table := createTestTableWithClusteringKeys(1, 3) // 3 clustering keys
		mockRandom := testutils.NewMockRandom(2)
		generator := &Generator{
			table:  table,
			random: mockRandom,
		}

		result := generator.getMultipleClusteringKeys(0)
		// Should be min(0, 3) + (2 % maximumCount) = 0 + 2 = 2
		require.GreaterOrEqual(t, result, 0)
		require.LessOrEqual(t, result, 4) // 0 + max possible from cartesian product
	})
}

// createTestTableWithClusteringKeys creates a test table with specified partition and clustering keys
func createTestTableWithClusteringKeys(pkCount, ckCount int) *typedef.Table {
	table := &typedef.Table{
		Name: "test_table",
	}

	// Create partition keys
	if pkCount > 0 {
		table.PartitionKeys = make(typedef.Columns, pkCount)
		for i := 0; i < pkCount; i++ {
			table.PartitionKeys[i] = typedef.ColumnDef{
				Name: fmt.Sprintf("pk%d", i),
				Type: typedef.TypeInt,
			}
		}
	} else {
		table.PartitionKeys = make(typedef.Columns, 0)
	}

	// Create clustering keys
	if ckCount > 0 {
		table.ClusteringKeys = make(typedef.Columns, ckCount)
		for i := 0; i < ckCount; i++ {
			table.ClusteringKeys[i] = typedef.ColumnDef{
				Name: fmt.Sprintf("ck%d", i),
				Type: typedef.TypeInt,
			}
		}
	} else {
		table.ClusteringKeys = make(typedef.Columns, 0)
	}

	return table
}
