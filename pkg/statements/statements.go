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
	"math"

	"github.com/scylladb/gemini/pkg/partitions"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

const (
	SelectSinglePartitionQuery int = iota
	SelectMultiplePartitionQuery
	SelectClusteringRangeQuery
	SelectMultiplePartitionClusteringRangeQuery
	SelectSingleIndexQuery
	SelectStatementsCount
)

const (
	DeleteWholePartition = iota
	DeleteSingleRow
	DeleteSingleColumn
	DeleteMultiplePartitions
	DeleteStatementCount
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
	generator        partitions.Interface
	random           utils.Random
	table            *typedef.Table
	valueRangeConfig *typedef.ValueRangeConfig
	ratioController  *RatioController
	keyspace         string
	keyspaceAndTable string
	useLWT           bool
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
	return &Generator{
		keyspace:         schema,
		keyspaceAndTable: schema + "." + table.Name,
		table:            table,
		random:           random,
		valueRangeConfig: valueRangeConfig,
		useLWT:           useLWT,
		generator:        valueGenerator,
		ratioController:  ratioController,
	}
}

func (g *Generator) getMultiplePartitionKeys() int {
	l := g.table.PartitionKeys.Len()
	if l == 0 {
		panic("table has no partition keys")
	}

	maximum := TotalCartesianProductCount(float64(g.random.IntN(l)), float64(l))

	return max(1, maximum)
}

//nolint:unused
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
