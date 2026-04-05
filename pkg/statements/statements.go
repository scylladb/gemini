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

	"github.com/scylladb/gocqlx/v3/qb"

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
	selectColumns    []string
	// Cached query strings — same table schema always produces the same CQL.
	cachedInsertQuery              string
	cachedInsertJSONQuery          string
	cachedSelectQuery              string
	cachedDeleteQuery              string
	cachedMultiPartitionSelect     map[int]string // numQueryPKs → query
	cachedMultiPartitionDelete     map[int]string // numQueryPKs → query
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
	g.buildCachedQueries()
	return g
}

// buildCachedQueries pre-builds query strings that are identical for every
// invocation with the same table schema. This avoids repeated bytes.Buffer
// allocations and string building in the hot path.
func (g *Generator) buildCachedQueries() {
	// Cache INSERT query
	builder := qb.Insert(g.keyspaceAndTable)
	for _, pk := range g.table.PartitionKeys {
		builder.Columns(pk.Name)
	}
	for _, ck := range g.table.ClusteringKeys {
		builder.Columns(ck.Name)
	}
	for _, col := range g.table.Columns {
		switch colType := col.Type.(type) {
		case *typedef.TupleType:
			builder.TupleColumn(col.Name, len(colType.ValueTypes))
		default:
			builder.Columns(col.Name)
		}
	}
	g.cachedInsertQuery, _ = builder.ToCql()

	// Cache INSERT JSON query
	jsonBuilder := qb.Insert(g.keyspaceAndTable).Json()
	g.cachedInsertJSONQuery, _ = jsonBuilder.ToCql()

	// Cache single-partition SELECT query
	selectBuilder := qb.Select(g.keyspaceAndTable).Columns(g.selectColumns...)
	for _, pk := range g.table.PartitionKeys {
		selectBuilder = selectBuilder.Where(qb.Eq(pk.Name))
	}
	g.cachedSelectQuery, _ = selectBuilder.ToCql()

	// Cache single-partition DELETE query
	deleteBuilder := qb.Delete(g.keyspaceAndTable)
	for _, pk := range g.table.PartitionKeys {
		deleteBuilder = deleteBuilder.Where(qb.Eq(pk.Name))
	}
	g.cachedDeleteQuery, _ = deleteBuilder.ToCql()

	// Cache all multi-partition SELECT and DELETE variants.
	// numQueryPKs ranges from 1 to max where i^pkLen < MaxCartesianProductCount.
	pkLen := g.table.PartitionKeys.Len()
	maxPKs := int(MaxCartesianProductCount) // upper bound, usually ~100
	g.cachedMultiPartitionSelect = make(map[int]string, maxPKs)
	g.cachedMultiPartitionDelete = make(map[int]string, maxPKs)

	for n := 1; n <= maxPKs; n++ {
		if math.Pow(float64(n), float64(pkLen)) >= MaxCartesianProductCount {
			break
		}

		sb := qb.Select(g.keyspaceAndTable).Columns(g.selectColumns...)
		for _, pk := range g.table.PartitionKeys {
			sb.Where(qb.InTuple(pk.Name, n))
		}
		g.cachedMultiPartitionSelect[n], _ = sb.ToCql()

		db := qb.Delete(g.keyspaceAndTable)
		for _, pk := range g.table.PartitionKeys {
			db.Where(qb.InTuple(pk.Name, n))
		}
		g.cachedMultiPartitionDelete[n], _ = db.ToCql()
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
