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
	"fmt"
	"math"
	"slices"

	"github.com/scylladb/gemini/pkg/utils"
)

// StatementType represents the main categories of statements
type StatementType int

const (
	StatementTypeInsert StatementType = iota
	StatementTypeUpdate
	StatementTypeDelete
	StatementTypeSelect
	StatementTypeCount
)

func (s StatementType) String() string {
	switch s {
	case StatementTypeInsert:
		return "insert"
	case StatementTypeUpdate:
		return "update"
	case StatementTypeDelete:
		return "delete"
	case StatementTypeSelect:
		return "select"
	default:
		return "unknown"
	}
}

// Ratios defines the distribution ratios for different statement types
// Mutations and validation are separate systems that each sum to 1.0
type Ratios struct {
	// Mutation ratios (Insert, Update, Delete) - sum should be 1.0
	MutationRatios MutationRatios `json:"mutation"`

	// Validation ratios (Select statements) - sum should be 1.0
	ValidationRatios ValidationRatios `json:"validation"`
}

// MutationRatios defines the distribution ratios for mutation operations
type MutationRatios struct {
	InsertRatio float64 `json:"insert"`
	UpdateRatio float64 `json:"update"`
	DeleteRatio float64 `json:"delete"`

	// Insert subtype ratios (within insert statements)
	InsertSubtypeRatios InsertRatios `json:"insert_subtypes"`

	// Targeted subtype ratios (within delete statements). The JSON key remains
	// "delete_subtypes" for backward compatibility with existing config files.
	DeleteSubtypeRatios TargetedRatios `json:"delete_subtypes"`
}

// ValidationRatios defines the distribution ratios for validation operations
type ValidationRatios struct {
	// Select subtype ratios (within select statements)
	SelectSubtypeRatios SelectRatios `json:"select_subtypes"`
}

// InsertRatios defines ratios for different insert statement types
type InsertRatios struct {
	RegularInsertRatio float64 `json:"regular_insert"`
	JSONInsertRatio    float64 `json:"json_insert"`
}

// TargetedRatios defines ratios for the targeted mutation subtypes (whole
// partition, single row, clustering subset, multiple partitions). These govern
// DELETE subtype selection and, for single-row, also drive UPDATE targeting.
type TargetedRatios struct {
	WholePartitionRatio     float64 `json:"whole_partition"`
	SingleRowRatio          float64 `json:"single_row"`
	ClusteringSubsetRatio   float64 `json:"clustering_subset"`
	MultiplePartitionsRatio float64 `json:"multiple_partitions"`
}

// SelectRatios defines ratios for different select statement types
type SelectRatios struct {
	SinglePartitionRatio                  float64 `json:"single_partition"`
	MultiplePartitionRatio                float64 `json:"multiple_partition"`
	ClusteringRangeRatio                  float64 `json:"clustering_range"`
	MultiplePartitionClusteringRangeRatio float64 `json:"multiple_partition_clustering_range"`
	SingleIndexRatio                      float64 `json:"single_index"`
}

// DefaultStatementRatios returns a balanced default configuration
func DefaultStatementRatios() Ratios {
	return Ratios{
		MutationRatios: MutationRatios{
			InsertRatio: 0.70,
			UpdateRatio: 0.25,
			DeleteRatio: 0.05,
			InsertSubtypeRatios: InsertRatios{
				RegularInsertRatio: 0.9,
				JSONInsertRatio:    0.1,
			},
			DeleteSubtypeRatios: TargetedRatios{
				WholePartitionRatio:     0.3,
				SingleRowRatio:          0.3,
				ClusteringSubsetRatio:   0.3,
				MultiplePartitionsRatio: 0.1,
			},
		},
		ValidationRatios: ValidationRatios{
			SelectSubtypeRatios: SelectRatios{
				SinglePartitionRatio:                  0.6,
				MultiplePartitionRatio:                0.3,
				ClusteringRangeRatio:                  0.05,
				MultiplePartitionClusteringRangeRatio: 0.04,
				SingleIndexRatio:                      0.01,
			},
		},
	}
}

// RatioController manages the distribution of statement types based on configured ratios
type RatioController struct {
	random      utils.Random
	mutationCDF [MutationStatementsCount]float64
	insertCDF   [InsertStatementCount]float64
	updateCDF   [UpdateStatementCount]float64
	targetedCDF [TargetedStatementCount]float64
	selectCDF   [SelectStatementsCount]float64
}

// NewRatioController creates a new statement ratio controller
func NewRatioController(ratios Ratios, random utils.Random) (*RatioController, error) {
	controller := &RatioController{
		random: random,
	}

	if err := controller.validate(ratios); err != nil {
		return nil, fmt.Errorf("invalid ratios: %w", err)
	}

	controller.buildCDFs(ratios)
	return controller, nil
}

// validate checks if the ratios are valid (sum to 1.0 with some tolerance)
func (c *RatioController) validate(ratios Ratios) error {
	const tolerance = 0.001

	// Check mutation ratios
	mutationSum := ratios.MutationRatios.InsertRatio + ratios.MutationRatios.UpdateRatio + ratios.MutationRatios.DeleteRatio
	if math.Abs(mutationSum-1.0) > tolerance {
		return fmt.Errorf("mutation ratios sum to %.3f, expected 1.0", mutationSum)
	}

	// Check insert subtype ratios
	insertSum := ratios.MutationRatios.InsertSubtypeRatios.RegularInsertRatio + ratios.MutationRatios.InsertSubtypeRatios.JSONInsertRatio
	if math.Abs(insertSum-1.0) > tolerance {
		return fmt.Errorf("insert subtype ratios sum to %.3f, expected 1.0", insertSum)
	}

	if ratios.MutationRatios.DeleteRatio > 0.001 {
		// Check delete subtype ratios
		deleteSum := ratios.MutationRatios.DeleteSubtypeRatios.WholePartitionRatio +
			ratios.MutationRatios.DeleteSubtypeRatios.SingleRowRatio +
			ratios.MutationRatios.DeleteSubtypeRatios.ClusteringSubsetRatio +
			ratios.MutationRatios.DeleteSubtypeRatios.MultiplePartitionsRatio
		if math.Abs(deleteSum-1.0) > tolerance {
			return fmt.Errorf("delete subtype ratios sum to %.3f, expected 1.0", deleteSum)
		}
	}

	// Check select subtype ratios
	selectSum := ratios.ValidationRatios.SelectSubtypeRatios.SinglePartitionRatio +
		ratios.ValidationRatios.SelectSubtypeRatios.MultiplePartitionRatio +
		ratios.ValidationRatios.SelectSubtypeRatios.ClusteringRangeRatio +
		ratios.ValidationRatios.SelectSubtypeRatios.MultiplePartitionClusteringRangeRatio +
		ratios.ValidationRatios.SelectSubtypeRatios.SingleIndexRatio
	if math.Abs(selectSum-1.0) > tolerance {
		return fmt.Errorf("select subtype ratios sum to %.3f, expected 1.0", selectSum)
	}

	return nil
}

// buildCDFs builds cumulative distribution functions for efficient random selection
func (c *RatioController) buildCDFs(ratios Ratios) {
	// Mutation ratios CDF
	c.mutationCDF = [MutationStatementsCount]float64{
		ratios.MutationRatios.InsertRatio,
		ratios.MutationRatios.InsertRatio + ratios.MutationRatios.UpdateRatio,
		ratios.MutationRatios.InsertRatio + ratios.MutationRatios.UpdateRatio + ratios.MutationRatios.DeleteRatio,
	}

	// Insert subtypes CDF
	c.insertCDF = [InsertStatementCount]float64{
		ratios.MutationRatios.InsertSubtypeRatios.RegularInsertRatio,
		1.0, // Regular + JSON
	}

	// Update subtypes CDF (currently only one type)
	c.updateCDF = [UpdateStatementCount]float64{1.0}

	// Targeted (delete/update) subtypes CDF
	c.targetedCDF = [TargetedStatementCount]float64{
		ratios.MutationRatios.DeleteSubtypeRatios.WholePartitionRatio,
		ratios.MutationRatios.DeleteSubtypeRatios.WholePartitionRatio +
			ratios.MutationRatios.DeleteSubtypeRatios.SingleRowRatio,
		ratios.MutationRatios.DeleteSubtypeRatios.WholePartitionRatio +
			ratios.MutationRatios.DeleteSubtypeRatios.SingleRowRatio +
			ratios.MutationRatios.DeleteSubtypeRatios.ClusteringSubsetRatio,
		1.0, // All delete types
	}

	// Select subtypes CDF
	selectRatios := ratios.ValidationRatios.SelectSubtypeRatios
	c.selectCDF = [SelectStatementsCount]float64{
		selectRatios.SinglePartitionRatio,
		selectRatios.SinglePartitionRatio + selectRatios.MultiplePartitionRatio,
		selectRatios.SinglePartitionRatio + selectRatios.MultiplePartitionRatio +
			selectRatios.ClusteringRangeRatio,
		selectRatios.SinglePartitionRatio + selectRatios.MultiplePartitionRatio +
			selectRatios.ClusteringRangeRatio + selectRatios.MultiplePartitionClusteringRangeRatio,
		1.0, // All select types
	}
}

// ErrNoMutationCandidates is returned by GetMutationStatementType when the
// requested filter leaves no mutation type with non-zero probability mass —
// e.g. every type was filtered, or the only surviving types have a ratio of
// zero. There is no correct statement to generate in that case, so the caller
// must handle it rather than receive an arbitrary (and possibly explicitly
// excluded) type.
var ErrNoMutationCandidates = errors.New(
	"no mutation statement type available: every type with a non-zero ratio was filtered out",
)

// GetMutationStatementType picks a mutation statement type (insert/update/delete)
// weighted by the configured ratios. Types passed in filter are excluded and
// their probability mass is redistributed PROPORTIONALLY across the remaining
// types rather than leaking into a hardcoded fallback.
//
// A filtered type is never returned. When no candidate survives the filter the
// method reports ErrNoMutationCandidates instead of guessing: returning a type
// the caller explicitly excluded would fabricate exactly the disallowed
// statement (e.g. a DELETE during warmup) that the filter exists to prevent.
//
// The previous implementation walked the cumulative CDF and, when the matching
// bucket was filtered, fell through to `return StatementTypeInsert`. That dumped
// the entire filtered mass onto insert: when deletes were filtered (warmup /
// no-delete modes) the configured insert:update proportion was silently skewed,
// updates never received any of the freed delete mass, and inserts were
// generated even when InsertRatio was zero. Redistributing over the surviving
// weights fixes all three and generalizes to filtering any type.
func (c *RatioController) GetMutationStatementType(filter ...StatementType) (StatementType, error) {
	// Reconstruct per-type weights from the cumulative CDF and sum the mass of
	// the non-filtered types.
	var weights [MutationStatementsCount]float64
	var total, prev float64
	for i := range c.mutationCDF {
		weights[i] = c.mutationCDF[i] - prev
		prev = c.mutationCDF[i]
		if !slices.Contains(filter, StatementType(i)) {
			total += weights[i]
		}
	}

	if total <= 0 {
		return 0, ErrNoMutationCandidates
	}

	// Sample within the surviving mass, then walk the non-filtered types.
	// Float64() is in [0,1) so r is in [0,total); the strict `r < acc` guard
	// skips zero-weight types (e.g. InsertRatio == 0) instead of returning them.
	r := c.random.Float64() * total
	last := StatementTypeCount
	var acc float64
	for i := range weights {
		if slices.Contains(filter, StatementType(i)) || weights[i] <= 0 {
			continue
		}

		acc += weights[i]
		if r < acc {
			return StatementType(i), nil
		}

		last = StatementType(i)
	}

	// Unreachable unless floating-point accumulation leaves r >= acc on the
	// final bucket. Fall back to the last surviving candidate — never a
	// filtered or zero-weight one. total > 0 guarantees last was assigned.
	if last == StatementTypeCount {
		return 0, ErrNoMutationCandidates
	}

	return last, nil
}

// GetValidationStatementType returns a validation statement type (currently only Select)
func (c *RatioController) GetValidationStatementType() StatementType {
	return StatementTypeSelect
}

// GetInsertSubtype returns the insert subtype based on configured ratios
func (c *RatioController) GetInsertSubtype() int {
	r := c.random.Float64()

	for i, cdf := range c.insertCDF {
		if r <= cdf {
			return i // 0 = InsertStatements, 1 = InsertJSONStatement
		}
	}

	return InsertStatements
}

// GetTargetedSubtype returns the targeted mutation subtype (whole partition,
// single row, clustering subset, multiple partitions) based on configured
// ratios. Used by DELETE generation; UPDATE generation always targets a single
// row directly and does not call this.
func (c *RatioController) GetTargetedSubtype() int {
	r := c.random.Float64()

	for i, cdf := range c.targetedCDF {
		if r <= cdf {
			return i // TargetedWholePartition, TargetedSingleRow, TargetedClusteringSubset, TargetedMultiplePartitions
		}
	}

	return TargetedWholePartition
}

// GetSelectSubtype returns the select subtype based on configured ratios
func (c *RatioController) GetSelectSubtype() int {
	r := c.random.Float64()

	for i, cdf := range c.selectCDF {
		if r <= cdf {
			return i // SelectSinglePartitionQuery, SelectMultiplePartitionQuery, etc.
		}
	}

	return SelectSinglePartitionQuery
}

// UpdateRatios updates the ratios and rebuilds the CDFs
func (c *RatioController) UpdateRatios(ratios Ratios) error {
	controller := &RatioController{
		random: c.random,
	}

	if err := controller.validate(ratios); err != nil {
		return fmt.Errorf("invalid ratios: %w", err)
	}

	c.buildCDFs(ratios)
	return nil
}

// GetStatementInfo returns information about the current statement distribution
func (c Ratios) GetStatementInfo() map[string]any {
	selectRatios := c.ValidationRatios.SelectSubtypeRatios
	totalSelectRatio := selectRatios.SinglePartitionRatio +
		selectRatios.MultiplePartitionRatio +
		selectRatios.ClusteringRangeRatio +
		selectRatios.MultiplePartitionClusteringRangeRatio +
		selectRatios.SingleIndexRatio

	return map[string]any{
		"main": map[string]float64{
			"insert": c.MutationRatios.InsertRatio,
			"update": c.MutationRatios.UpdateRatio,
			"delete": c.MutationRatios.DeleteRatio,
			"select": totalSelectRatio,
		},
		"insert_subtypes": map[string]float64{
			"regular": c.MutationRatios.InsertSubtypeRatios.RegularInsertRatio,
			"json":    c.MutationRatios.InsertSubtypeRatios.JSONInsertRatio,
		},
		"delete_subtypes": map[string]float64{
			"whole_partition":     c.MutationRatios.DeleteSubtypeRatios.WholePartitionRatio,
			"single_row":          c.MutationRatios.DeleteSubtypeRatios.SingleRowRatio,
			"clustering_subset":   c.MutationRatios.DeleteSubtypeRatios.ClusteringSubsetRatio,
			"multiple_partitions": c.MutationRatios.DeleteSubtypeRatios.MultiplePartitionsRatio,
		},
		"select_subtypes": map[string]float64{
			"single_partition":                    selectRatios.SinglePartitionRatio,
			"multiple_partition":                  selectRatios.MultiplePartitionRatio,
			"clustering_range":                    selectRatios.ClusteringRangeRatio,
			"multiple_partition_clustering_range": selectRatios.MultiplePartitionClusteringRangeRatio,
			"single_index":                        selectRatios.SingleIndexRatio,
		},
	}
}

// rowTrackerCapacityScale is the linear scaling factor used to convert the
// effective tracked-row consume ratio into a row tracker capacity.
// Calibration: at the default config (deleteRatio=0.05, targeted subtypes=0.6),
// the delete contribution is 0.03 → capacity = 0.03 * 33333 ≈ 1000.
const (
	rowTrackerCapacityScale = 33333
	rowTrackerCapacityMin   = 100
	rowTrackerCapacityMax   = 100_000

	// updateConsumeWeight scales down UPDATE's contribution to the consume ratio.
	// A single-row UPDATE is an opportunistic consumer: it pops a tracked row
	// when available and otherwise falls back to a random-key upsert, so it does
	// not need a tracker sized to its full ratio. The light weight keeps a modest
	// pool — enough that updates still hit real rows, notably when deletes are
	// disabled — without oversizing the tracker or the validation sample rate.
	//
	// Note: with deletes disabled this puts the tracker's enable threshold at
	// UpdateRatio >= 0.01 (below that, effective < 0.001 and tracking is off, so
	// updates run purely as random-key upserts).
	updateConsumeWeight = 0.1
	// updateConsumeCap bounds the update contribution so a very high update ratio
	// cannot, on its own, drive the tracker capacity and sample rate up.
	updateConsumeCap = 0.03
)

// TargetedConsumeRatio returns the effective probability that a mutation
// consumes a row from the row tracker, summed over the two consumers:
//   - targeted deletes (single-row or cluster): deleteRatio * (singleRow + cluster),
//     full weight — a targeted delete with no tracked row produces nothing.
//   - single-row updates: weighted down (updateConsumeWeight, capped by
//     updateConsumeCap) because they fall back to a random-key upsert when the
//     tracker is empty.
//
// It drives both the row tracker capacity and the validation sample rate.
func (c Ratios) TargetedConsumeRatio() float64 {
	var ratio float64

	if dr := c.MutationRatios.DeleteRatio; dr >= 0.001 {
		targeted := c.MutationRatios.DeleteSubtypeRatios.SingleRowRatio +
			c.MutationRatios.DeleteSubtypeRatios.ClusteringSubsetRatio
		ratio += dr * targeted
	}

	ratio += min(c.MutationRatios.UpdateRatio*updateConsumeWeight, updateConsumeCap)

	return ratio
}

// ComputeRowTrackerCapacity returns the recommended row tracker capacity based
// on the configured mutation ratios. Returns 0 when no mutation consumes
// tracked rows, scaling up to a maximum of rowTrackerCapacityMax for heavy
// delete/update workloads.
func (c Ratios) ComputeRowTrackerCapacity() int {
	effective := c.TargetedConsumeRatio()
	if effective < 0.001 {
		return 0
	}

	capacity := int(effective * rowTrackerCapacityScale)

	return max(rowTrackerCapacityMin, min(rowTrackerCapacityMax, capacity))
}
