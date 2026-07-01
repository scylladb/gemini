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

	"github.com/scylladb/gemini/pkg/partitions"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

// ErrNoTrackedRows is returned by targeted delete generators (deleteSingleRow,
// deleteClusteringSubset) when the row tracker is empty. Callers should treat this as a
// transient "nothing to do" signal and skip the operation rather than falling
// back to random data.
var ErrNoTrackedRows = errors.New("no tracked rows available")

const (
	SelectSinglePartitionQuery int = iota
	SelectMultiplePartitionQuery
	SelectClusteringRangeQuery
	SelectMultiplePartitionClusteringRangeQuery
	SelectSingleIndexQuery
	SelectStatementsCount
)

// Targeted mutation subtypes. These describe how a mutation (DELETE or
// single-row UPDATE) selects the rows it affects: a whole partition, a single
// row, a clustering prefix, or multiple partitions. The targeting machinery is
// shared between delete and update generation — hence the neutral "Targeted"
// naming rather than "Delete".
const (
	TargetedWholePartition = iota
	TargetedSingleRow
	TargetedClusteringSubset
	TargetedMultiplePartitions
	TargetedStatementCount
)

const (
	InsertStatements = iota
	InsertJSONStatement
	InsertStatementCount
)

const (
	UpdateStatement = iota
	UpdateStatementCount
)

const MutationStatementsCount = 3

type Generator struct {
	generator                      partitions.Interface
	random                         utils.Random
	table                          *typedef.Table
	valueRangeConfig               *typedef.ValueRangeConfig
	ratioController                *RatioController
	keyspace                       string
	keyspaceAndTable               string
	deleteWholePartitionQuery      string
	deleteSingleRowQuery           string
	insertQuery                    string
	insertQueryLWT                 string
	insertJSONQuery                string
	selectSinglePartitionQuery     string
	selectMultiplePartitionQueries []string
	selectColumns                  []string
	deleteClusteringSubsetQueries  []string
	updateVariants                 []updateVariant
	trackedMisses                  TrackedMissCounts
	useLWT                         bool
}

func New(
	schema string,
	valueGenerator partitions.Interface,
	table *typedef.Table,
	random utils.Random,
	valueRangeConfig *typedef.ValueRangeConfig,
	ratioController *RatioController,
	useLWT bool,
) *Generator {
	g := &Generator{
		keyspace:         schema,
		keyspaceAndTable: schema + "." + table.Name,
		table:            table,
		random:           random,
		valueRangeConfig: valueRangeConfig,
		useLWT:           useLWT,
		generator:        valueGenerator,
		ratioController:  ratioController,
		selectColumns:    table.SelectColumnNames(),
	}
	g.buildCachedDeleteQueries()
	g.buildCachedUpdateQueries()
	g.buildCachedInsertQueries()
	g.buildCachedSelectQueries()
	return g
}

// MarkInvalid marks the partition identified by keys as permanently invalid so
// that future Next() calls skip it and the validation phase does not SELECT it.
// Delegates directly to the underlying partitions.Interface.
func (g *Generator) MarkInvalid(keys *typedef.PartitionKeys) bool {
	return g.generator.MarkInvalid(keys)
}

// TrackRow stores a row observed during validation for later targeted
// mutation (single-row delete or single-row update).
// Delegates directly to the underlying partitions.Interface.
func (g *Generator) TrackRow(row partitions.TrackedRow) {
	g.generator.TrackRow(row)
}

// PopTrackedRow retrieves a previously tracked row for a targeted mutation.
// Delegates directly to the underlying partitions.Interface.
func (g *Generator) PopTrackedRow() (partitions.TrackedRow, bool) {
	return g.generator.PopTrackedRow()
}

// TrackedMissCounts holds the number of targeted-mutation fallbacks caused by a
// popped tracked row whose flat key-value slices were too short for the table
// schema, broken down by mutation kind. The statement layer only accumulates
// these (it stays free of any metrics dependency); the jobs layer drains them
// and translates them into the tracked_row_schema_mismatch_total metric.
type TrackedMissCounts struct {
	Update                 uint64
	DeleteSingleRow        uint64
	DeleteClusteringSubset uint64
}

// DrainTrackedMisses returns the tracked-row schema-mismatch counts accumulated
// since the previous call and resets them to zero.
//
// Not safe for concurrent use: each Generator is owned by a single worker
// goroutine, which is also the only caller of this method (and of the
// Update/Delete methods that increment the counters).
func (g *Generator) DrainTrackedMisses() TrackedMissCounts {
	c := g.trackedMisses
	g.trackedMisses = TrackedMissCounts{}
	return c
}

func (g *Generator) getMultiplePartitionKeys() int {
	l := g.table.PartitionKeys.Len()
	if l == 0 {
		panic("table has no partition keys")
	}

	maximum := TotalCartesianProductCount(float64(g.random.IntN(l)), float64(l))

	return max(1, maximum)
}

func (g *Generator) getMultipleClusteringKeys() int {
	l := g.table.ClusteringKeys.Len()
	if l == 0 {
		return 0
	}

	maximum := TotalCartesianProductCount(float64(g.random.IntN(l)), float64(l))
	return max(1, maximum)
}

func (g *Generator) getIndex(initial int) int {
	l := len(g.table.Indexes)
	return min(initial, l) + g.random.IntN(l)
}

const MaxCartesianProductCount = float64(100.0)

// TotalCartesianProductCount chooses the first number of partition keys that
// multiplied by the number of partition keys does not exceed MaxCartesianProductCount.
func TotalCartesianProductCount(initial, pkLen float64) int {
	for i := int(initial); i > 0; i-- {
		multiplier := math.Pow(float64(i), pkLen)
		if multiplier < MaxCartesianProductCount {
			return i
		}
	}

	return 1
}
