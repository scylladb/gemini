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
	"context"
	"math"

	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

type ValueGenerator interface {
	Get(context.Context) typedef.PartitionKeys
	GetOld(context.Context) typedef.PartitionKeys
	GiveOlds(ctx context.Context, tokens ...typedef.PartitionKeys)
}

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
	DeleteStatements
)

const (
	InsertStatements = iota
	InsertJSONStatement
	UpdateStatement

	MutationStatements
)

type Generator struct {
	generator        ValueGenerator
	table            *typedef.Table
	random           utils.Random
	partitionConfig  *typedef.PartitionRangeConfig
	keyspace         string
	keyspaceAndTable string
	useLWT           bool
}

func New(
	schema string,
	valueGenerator ValueGenerator,
	table *typedef.Table,
	random utils.Random,
	partitionConfig *typedef.PartitionRangeConfig,
	useLWT bool,
) *Generator {
	return &Generator{
		keyspace:         schema,
		keyspaceAndTable: schema + "." + table.Name,
		table:            table,
		random:           random,
		partitionConfig:  partitionConfig,
		useLWT:           useLWT,
		generator:        valueGenerator,
	}
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
