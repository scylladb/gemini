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
	"math/rand/v2"

	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/typedef"
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
	DeleteStatements
)

const (
	InsertStatements = iota
	InsertJSONStatement
	UpdateStatement

	MutationStatements
)

type Generator struct {
	generator        generators.Interface
	table            *typedef.Table
	random           *rand.Rand
	partitionConfig  *typedef.PartitionRangeConfig
	keyspace         string
	keyspaceAndTable string
	useLWT           bool
}

func New(
	schema string,
	table *typedef.Table,
	generator generators.Interface,
	random *rand.Rand,
	partitionConfig *typedef.PartitionRangeConfig,
	useLWT bool,
) *Generator {
	return &Generator{
		keyspace:         schema,
		keyspaceAndTable: schema + "." + table.Name,
		table:            table,
		generator:        generator,
		random:           random,
		partitionConfig:  partitionConfig,
		useLWT:           useLWT,
	}
}
