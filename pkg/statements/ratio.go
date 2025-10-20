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

	// Delete subtype ratios (within delete statements)
	DeleteSubtypeRatios DeleteRatios `json:"delete_subtypes"`
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

// DeleteRatios defines ratios for different delete statement types
type DeleteRatios struct {
	WholePartitionRatio     float64 `json:"whole_partition"`
	SingleRowRatio          float64 `json:"single_row"`
	SingleColumnRatio       float64 `json:"single_column"`
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
			InsertRatio: 0.75,
			UpdateRatio: 0.2,
			DeleteRatio: 0.05,
			InsertSubtypeRatios: InsertRatios{
				RegularInsertRatio: 0.9,
				JSONInsertRatio:    0.1,
			},
			DeleteSubtypeRatios: DeleteRatios{
				WholePartitionRatio:     0.4,
				SingleRowRatio:          0.3,
				SingleColumnRatio:       0.2,
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
	deleteCDF   [DeleteStatementCount]float64
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

	// Check delete subtype ratios
	deleteSum := ratios.MutationRatios.DeleteSubtypeRatios.WholePartitionRatio +
		ratios.MutationRatios.DeleteSubtypeRatios.SingleRowRatio +
		ratios.MutationRatios.DeleteSubtypeRatios.SingleColumnRatio +
		ratios.MutationRatios.DeleteSubtypeRatios.MultiplePartitionsRatio
	if math.Abs(deleteSum-1.0) > tolerance {
		return fmt.Errorf("delete subtype ratios sum to %.3f, expected 1.0", deleteSum)
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

	// Delete subtypes CDF
	c.deleteCDF = [DeleteStatementCount]float64{
		ratios.MutationRatios.DeleteSubtypeRatios.WholePartitionRatio,
		ratios.MutationRatios.DeleteSubtypeRatios.WholePartitionRatio +
			ratios.MutationRatios.DeleteSubtypeRatios.SingleRowRatio,
		ratios.MutationRatios.DeleteSubtypeRatios.WholePartitionRatio +
			ratios.MutationRatios.DeleteSubtypeRatios.SingleRowRatio +
			ratios.MutationRatios.DeleteSubtypeRatios.SingleColumnRatio,
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

// GetMutationStatementType returns a mutation statement type (Insert, Update, Delete) based on configured ratios
func (c *RatioController) GetMutationStatementType(filter ...StatementType) StatementType {
	r := c.random.Float64()

	for i, cdf := range c.mutationCDF {
		if r <= cdf && !slices.Contains(filter, StatementType(i)) {
			return StatementType(i)
		}
	}

	// Fallback (should not happen with valid CDFs)
	return StatementTypeInsert
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

// GetDeleteSubtype returns the delete subtype based on configured ratios
func (c *RatioController) GetDeleteSubtype() int {
	r := c.random.Float64()

	for i, cdf := range c.deleteCDF {
		if r <= cdf {
			return i // DeleteWholePartition, DeleteSingleRow, DeleteSingleColumn, DeleteMultiplePartitions
		}
	}

	return DeleteWholePartition
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
			"single_column":       c.MutationRatios.DeleteSubtypeRatios.SingleColumnRatio,
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
