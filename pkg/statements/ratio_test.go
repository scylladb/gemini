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
	"errors"
	"math"
	"math/rand/v2"
	"testing"
)

func TestDefaultStatementRatios(t *testing.T) {
	t.Parallel()

	ratios := DefaultStatementRatios()

	// Test that mutation ratios sum to 1.0
	mutationSum := ratios.MutationRatios.InsertRatio + ratios.MutationRatios.UpdateRatio + ratios.MutationRatios.DeleteRatio
	if math.Abs(mutationSum-1.0) > 0.001 {
		t.Errorf("Mutation ratios sum to %.3f, expected 1.0", mutationSum)
	}

	// Test that insert subtype ratios sum to 1.0
	insertSum := ratios.MutationRatios.InsertSubtypeRatios.RegularInsertRatio + ratios.MutationRatios.InsertSubtypeRatios.JSONInsertRatio
	if math.Abs(insertSum-1.0) > 0.001 {
		t.Errorf("Insert subtype ratios sum to %.3f, expected 1.0", insertSum)
	}

	// Test that delete subtype ratios sum to 1.0
	deleteSum := ratios.MutationRatios.DeleteSubtypeRatios.WholePartitionRatio +
		ratios.MutationRatios.DeleteSubtypeRatios.SingleRowRatio +
		ratios.MutationRatios.DeleteSubtypeRatios.ClusteringSubsetRatio +
		ratios.MutationRatios.DeleteSubtypeRatios.MultiplePartitionsRatio
	if math.Abs(deleteSum-1.0) > 0.001 {
		t.Errorf("Delete subtype ratios sum to %.3f, expected 1.0", deleteSum)
	}

	// Test that select subtype ratios sum to 1.0
	selectSum := ratios.ValidationRatios.SelectSubtypeRatios.SinglePartitionRatio +
		ratios.ValidationRatios.SelectSubtypeRatios.MultiplePartitionRatio +
		ratios.ValidationRatios.SelectSubtypeRatios.ClusteringRangeRatio +
		ratios.ValidationRatios.SelectSubtypeRatios.MultiplePartitionClusteringRangeRatio +
		ratios.ValidationRatios.SelectSubtypeRatios.SingleIndexRatio
	if math.Abs(selectSum-1.0) > 0.001 {
		t.Errorf("Select subtype ratios sum to %.3f, expected 1.0", selectSum)
	}
}

func TestNewStatementRatioController(t *testing.T) {
	t.Parallel()

	ratios := DefaultStatementRatios()
	random := rand.New(rand.NewChaCha8([32]byte{}))

	controller, err := NewRatioController(ratios, random)
	if err != nil {
		t.Fatalf("Failed to create statement ratio controller: %v", err)
	}

	if controller == nil {
		t.Fatal("Controller should not be nil")
	}
}

func TestStatementRatioControllerValidation(t *testing.T) {
	t.Parallel()

	random := rand.New(rand.NewChaCha8([32]byte{}))

	tests := []struct {
		name        string
		ratios      Ratios
		expectError bool
	}{
		{
			name:        "Valid default ratios",
			ratios:      DefaultStatementRatios(),
			expectError: false,
		},
		{
			name: "Invalid main ratios sum",
			ratios: Ratios{
				MutationRatios: MutationRatios{
					InsertRatio: 0.5,
					UpdateRatio: 0.3,
					DeleteRatio: 0.3, // Sum = 1.1, should cause error
					InsertSubtypeRatios: InsertRatios{
						RegularInsertRatio: 0.8,
						JSONInsertRatio:    0.2,
					},
					DeleteSubtypeRatios: TargetedRatios{
						WholePartitionRatio:     0.25,
						SingleRowRatio:          0.25,
						ClusteringSubsetRatio:   0.25,
						MultiplePartitionsRatio: 0.25,
					},
				},
				ValidationRatios: ValidationRatios{
					SelectSubtypeRatios: SelectRatios{
						SinglePartitionRatio:                  0.2,
						MultiplePartitionRatio:                0.2,
						ClusteringRangeRatio:                  0.2,
						MultiplePartitionClusteringRangeRatio: 0.2,
						SingleIndexRatio:                      0.2,
					},
				},
			},
			expectError: true,
		},
		{
			name: "Invalid insert subtype ratios sum",
			ratios: Ratios{
				MutationRatios: MutationRatios{
					InsertRatio: 0.5,
					UpdateRatio: 0.3,
					DeleteRatio: 0.2,
					InsertSubtypeRatios: InsertRatios{
						RegularInsertRatio: 0.9,
						JSONInsertRatio:    0.2, // Sum = 1.1
					},
					DeleteSubtypeRatios: TargetedRatios{
						WholePartitionRatio:     0.25,
						SingleRowRatio:          0.25,
						ClusteringSubsetRatio:   0.25,
						MultiplePartitionsRatio: 0.25,
					},
				},
				ValidationRatios: ValidationRatios{
					SelectSubtypeRatios: SelectRatios{
						SinglePartitionRatio:                  0.2,
						MultiplePartitionRatio:                0.2,
						ClusteringRangeRatio:                  0.2,
						MultiplePartitionClusteringRangeRatio: 0.2,
						SingleIndexRatio:                      0.2,
					},
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewRatioController(tt.ratios, random)
			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestStatementTypeDistribution(t *testing.T) {
	t.Parallel()

	ratios := Ratios{
		MutationRatios: MutationRatios{
			InsertRatio: 0.5,
			UpdateRatio: 0.3,
			DeleteRatio: 0.2,
			InsertSubtypeRatios: InsertRatios{
				RegularInsertRatio: 0.8,
				JSONInsertRatio:    0.2,
			},
			DeleteSubtypeRatios: TargetedRatios{
				WholePartitionRatio:     0.25,
				SingleRowRatio:          0.25,
				ClusteringSubsetRatio:   0.25,
				MultiplePartitionsRatio: 0.25,
			},
		},
		ValidationRatios: ValidationRatios{
			SelectSubtypeRatios: SelectRatios{
				SinglePartitionRatio:                  0.2,
				MultiplePartitionRatio:                0.2,
				ClusteringRangeRatio:                  0.2,
				MultiplePartitionClusteringRangeRatio: 0.2,
				SingleIndexRatio:                      0.2,
			},
		},
	}

	random := rand.New(rand.NewChaCha8([32]byte{}))
	controller, err := NewRatioController(ratios, random)
	if err != nil {
		t.Fatalf("Failed to create controller: %v", err)
	}

	// Test distribution over many samples
	samples := 10000
	counts := make(map[StatementType]int)

	for range samples {
		stmtType, sErr := controller.GetMutationStatementType()
		if sErr != nil {
			t.Fatalf("GetMutationStatementType: %v", sErr)
		}
		counts[stmtType]++
	}

	// Check that distribution is approximately correct (within 5% tolerance)
	tolerance := 0.05
	expectedCounts := map[StatementType]int{
		StatementTypeInsert: int(ratios.MutationRatios.InsertRatio * float64(samples)),
		StatementTypeUpdate: int(ratios.MutationRatios.UpdateRatio * float64(samples)),
		StatementTypeDelete: int(ratios.MutationRatios.DeleteRatio * float64(samples)),
	}

	for stmtType, expectedCount := range expectedCounts {
		actualCount := counts[stmtType]
		diff := math.Abs(float64(actualCount-expectedCount)) / float64(expectedCount)
		if diff > tolerance {
			t.Errorf("Statement type %s: expected ~%d, got %d (%.1f%% difference)",
				stmtType, expectedCount, actualCount, diff*100)
		}
	}
}

// TestGetMutationStatementTypeFiltering verifies that filtering a mutation type
// redistributes its probability mass PROPORTIONALLY across the surviving types
// instead of dumping it onto insert. This is the warmup / no-delete path
// (generateDelete == false filters StatementTypeDelete).
func TestGetMutationStatementTypeFiltering(t *testing.T) {
	t.Parallel()

	newController := func(t *testing.T, insert, update, del float64) *RatioController {
		t.Helper()
		ratios := DefaultStatementRatios()
		ratios.MutationRatios.InsertRatio = insert
		ratios.MutationRatios.UpdateRatio = update
		ratios.MutationRatios.DeleteRatio = del
		c, err := NewRatioController(ratios, rand.New(rand.NewChaCha8([32]byte{})))
		if err != nil {
			t.Fatalf("NewRatioController: %v", err)
		}
		return c
	}

	const (
		samples   = 100000
		tolerance = 0.03 // relative
	)

	sample := func(t *testing.T, c *RatioController, filter ...StatementType) map[StatementType]int {
		t.Helper()
		counts := make(map[StatementType]int)
		for range samples {
			ty, err := c.GetMutationStatementType(filter...)
			if err != nil {
				t.Fatalf("GetMutationStatementType: %v", err)
			}
			counts[ty]++
		}
		return counts
	}

	assertFraction := func(t *testing.T, counts map[StatementType]int, ty StatementType, want float64) {
		t.Helper()
		got := float64(counts[ty]) / float64(samples)
		if math.Abs(got-want) > tolerance {
			t.Errorf("%s fraction: got %.3f, want ~%.3f", ty, got, want)
		}
	}

	t.Run("filtering deletes gives updates their proportional share", func(t *testing.T) {
		t.Parallel()
		// Insert:Update:Delete = 0.2:0.3:0.5. With deletes filtered the freed 0.5
		// must split proportionally: Insert 0.2/0.5=0.4, Update 0.3/0.5=0.6.
		// The old fallback-to-insert behavior produced Insert≈0.7, Update≈0.3.
		counts := sample(t, newController(t, 0.2, 0.3, 0.5), StatementTypeDelete)
		if counts[StatementTypeDelete] != 0 {
			t.Errorf("expected zero deletes when filtered, got %d", counts[StatementTypeDelete])
		}
		assertFraction(t, counts, StatementTypeInsert, 0.4)
		assertFraction(t, counts, StatementTypeUpdate, 0.6)
	})

	t.Run("filtering deletes never fabricates inserts when InsertRatio is zero", func(t *testing.T) {
		t.Parallel()
		// Insert=0, Update=0.5, Delete=0.5. Filtering deletes must yield ONLY
		// updates. The old code returned insert (via fallback) ~50% of the time.
		counts := sample(t, newController(t, 0.0, 0.5, 0.5), StatementTypeDelete)
		if counts[StatementTypeInsert] != 0 {
			t.Errorf("expected zero inserts when InsertRatio==0, got %d", counts[StatementTypeInsert])
		}
		if counts[StatementTypeDelete] != 0 {
			t.Errorf("expected zero deletes when filtered, got %d", counts[StatementTypeDelete])
		}
		assertFraction(t, counts, StatementTypeUpdate, 1.0)
	})

	t.Run("no filter preserves configured ratios", func(t *testing.T) {
		t.Parallel()
		counts := sample(t, newController(t, 0.2, 0.3, 0.5))
		assertFraction(t, counts, StatementTypeInsert, 0.2)
		assertFraction(t, counts, StatementTypeUpdate, 0.3)
		assertFraction(t, counts, StatementTypeDelete, 0.5)
	})

	t.Run("filtering every type reports no candidates instead of returning one", func(t *testing.T) {
		t.Parallel()
		// Returning any type here would hand back a statement the caller
		// explicitly excluded — the exact failure the filter exists to prevent.
		c := newController(t, 0.2, 0.3, 0.5)
		_, err := c.GetMutationStatementType(StatementTypeInsert, StatementTypeUpdate, StatementTypeDelete)
		if !errors.Is(err, ErrNoMutationCandidates) {
			t.Errorf("all-filtered: got err %v, want ErrNoMutationCandidates", err)
		}
	})

	t.Run("filtering leaves only zero-weight types reports no candidates", func(t *testing.T) {
		t.Parallel()
		// Insert=0, Update=0.5, Delete=0.5. Filtering update+delete leaves only
		// insert, which carries no probability mass — there is nothing to pick.
		c := newController(t, 0.0, 0.5, 0.5)
		_, err := c.GetMutationStatementType(StatementTypeUpdate, StatementTypeDelete)
		if !errors.Is(err, ErrNoMutationCandidates) {
			t.Errorf("zero-weight survivor: got err %v, want ErrNoMutationCandidates", err)
		}
	})

	t.Run("delete-only ratios during warmup report no candidates", func(t *testing.T) {
		t.Parallel()
		// The production repro: Insert=0, Update=0, Delete=1 passes validation
		// (the three sum to 1.0; there is no per-type floor), and the only caller
		// filters DELETE whenever generateDelete is false. Every surviving type
		// then has zero weight. Returning insert here would generate INSERTs on a
		// table configured to produce none.
		c := newController(t, 0.0, 0.0, 1.0)
		got, err := c.GetMutationStatementType(StatementTypeDelete)
		if !errors.Is(err, ErrNoMutationCandidates) {
			t.Errorf("delete-only + filtered delete: got (%s, %v), want ErrNoMutationCandidates", got, err)
		}
	})
}

func TestInsertSubtypeDistribution(t *testing.T) {
	t.Parallel()

	ratios := DefaultStatementRatios()
	random := rand.New(rand.NewChaCha8([32]byte{}))
	controller, err := NewRatioController(ratios, random)
	if err != nil {
		t.Fatalf("Failed to create controller: %v", err)
	}

	samples := 10000
	counts := make(map[int]int)

	for range samples {
		subtype := controller.GetInsertSubtype()
		counts[subtype]++
	}

	// Check distribution
	tolerance := 0.05
	expectedRegular := int(ratios.MutationRatios.InsertSubtypeRatios.RegularInsertRatio * float64(samples))
	expectedJSON := int(ratios.MutationRatios.InsertSubtypeRatios.JSONInsertRatio * float64(samples))

	regularDiff := math.Abs(float64(counts[InsertStatements]-expectedRegular)) / float64(expectedRegular)
	jsonDiff := math.Abs(float64(counts[InsertJSONStatement]-expectedJSON)) / float64(expectedJSON)

	if regularDiff > tolerance {
		t.Errorf("Regular insert: expected ~%d, got %d (%.1f%% difference)",
			expectedRegular, counts[InsertStatements], regularDiff*100)
	}

	if jsonDiff > tolerance {
		t.Errorf("JSON insert: expected ~%d, got %d (%.1f%% difference)",
			expectedJSON, counts[InsertJSONStatement], jsonDiff*100)
	}
}

func TestDeleteSubtypeDistribution(t *testing.T) {
	t.Parallel()

	ratios := DefaultStatementRatios()
	random := rand.New(rand.NewChaCha8([32]byte{}))
	controller, err := NewRatioController(ratios, random)
	if err != nil {
		t.Fatalf("Failed to create controller: %v", err)
	}

	samples := 10000
	counts := make(map[int]int)

	for range samples {
		subtype := controller.GetTargetedSubtype()
		counts[subtype]++
	}

	// Check distribution
	tolerance := 0.05
	deleteRatios := ratios.MutationRatios.DeleteSubtypeRatios
	expectedCounts := map[int]int{
		TargetedWholePartition:     int(deleteRatios.WholePartitionRatio * float64(samples)),
		TargetedSingleRow:          int(deleteRatios.SingleRowRatio * float64(samples)),
		TargetedClusteringSubset:   int(deleteRatios.ClusteringSubsetRatio * float64(samples)),
		TargetedMultiplePartitions: int(deleteRatios.MultiplePartitionsRatio * float64(samples)),
	}

	for deleteType, expectedCount := range expectedCounts {
		actualCount := counts[deleteType]
		diff := math.Abs(float64(actualCount-expectedCount)) / float64(expectedCount)
		if diff > tolerance {
			t.Errorf("Delete type %d: expected ~%d, got %d (%.1f%% difference)",
				deleteType, expectedCount, actualCount, diff*100)
		}
	}
}

func TestUpdateRatios(t *testing.T) {
	t.Parallel()

	ratios := DefaultStatementRatios()
	random := rand.New(rand.NewChaCha8([32]byte{}))
	controller, err := NewRatioController(ratios, random)
	if err != nil {
		t.Fatalf("Failed to create controller: %v", err)
	}

	// Update with new ratios
	newRatios := Ratios{
		MutationRatios: MutationRatios{
			InsertRatio: 0.6,
			UpdateRatio: 0.2,
			DeleteRatio: 0.2,
			InsertSubtypeRatios: InsertRatios{
				RegularInsertRatio: 0.9,
				JSONInsertRatio:    0.1,
			},
			DeleteSubtypeRatios: TargetedRatios{
				WholePartitionRatio:     0.4,
				SingleRowRatio:          0.3,
				ClusteringSubsetRatio:   0.2,
				MultiplePartitionsRatio: 0.1,
			},
		},
		ValidationRatios: ValidationRatios{
			SelectSubtypeRatios: SelectRatios{
				SinglePartitionRatio:                  0.5,
				MultiplePartitionRatio:                0.2,
				ClusteringRangeRatio:                  0.1,
				MultiplePartitionClusteringRangeRatio: 0.1,
				SingleIndexRatio:                      0.1,
			},
		},
	}

	err = controller.UpdateRatios(newRatios)
	if err != nil {
		t.Fatalf("Failed to update ratios: %v", err)
	}
}

func TestStatementTypeString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		expected string
		stmtType StatementType
	}{
		{stmtType: StatementTypeInsert, expected: "insert"},
		{stmtType: StatementTypeUpdate, expected: "update"},
		{stmtType: StatementTypeDelete, expected: "delete"},
		{stmtType: StatementTypeSelect, expected: "select"},
		{stmtType: StatementType(999), expected: "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			t.Parallel()
			if got := tt.stmtType.String(); got != tt.expected {
				t.Errorf("StatementType.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestGetStatementInfo(t *testing.T) {
	ratios := DefaultStatementRatios()
	info := ratios.GetStatementInfo()

	// Check that main ratios are present
	mainRatios, ok := info["main"].(map[string]float64)
	if !ok {
		t.Fatal("main_ratios not found or wrong type")
	}

	if mainRatios["insert"] != ratios.MutationRatios.InsertRatio {
		t.Errorf("Insert ratio in info: got %.3f, expected %.3f",
			mainRatios["insert"], ratios.MutationRatios.InsertRatio)
	}

	// Check that insert subtypes are present
	insertSubtypes, ok := info["insert_subtypes"].(map[string]float64)
	if !ok {
		t.Fatal("insert_subtypes not found or wrong type")
	}

	if insertSubtypes["regular"] != ratios.MutationRatios.InsertSubtypeRatios.RegularInsertRatio {
		t.Errorf("Regular insert ratio in info: got %.3f, expected %.3f",
			insertSubtypes["regular"], ratios.MutationRatios.InsertSubtypeRatios.RegularInsertRatio)
	}
}
