// Copyright 2019 ScyllaDB
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

package generators

import (
	"fmt"
	"log"
	"math/rand/v2"
	"strings"

	"github.com/scylladb/gemini/pkg/builders"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

func GenSchema(sc typedef.SchemaConfig, seed rand.Source) *typedef.Schema {
	builder := builders.SchemaBuilder{}
	builder.Config(sc)
	keyspace := typedef.Keyspace{
		Name:              "ks1",
		Replication:       sc.ReplicationStrategy,
		OracleReplication: sc.OracleReplicationStrategy,
	}
	builder.Keyspace(keyspace)
	r := rand.New(seed)
	numTables := r.IntN(sc.GetMaxTables()) + 1
	for i := range numTables {
		table := genTable(sc, fmt.Sprintf("table%d", i+1), r)
		builder.Table(table)
	}
	return builder.Build()
}

func genTable(sc typedef.SchemaConfig, tableName string, r *rand.Rand) *typedef.Table {
	partitionKeys := make(
		typedef.Columns,
		utils.RandInt2(r, sc.GetMinPartitionKeys(), sc.GetMaxPartitionKeys()),
	)
	for i := range len(partitionKeys) {
		partitionKeys[i] = &typedef.ColumnDef{
			Name: GenColumnName("pk", i),
			Type: GenPartitionKeyColumnType(r),
		}
	}
	clusteringKeys := make(
		typedef.Columns,
		utils.RandInt2(r, sc.GetMinClusteringKeys(), sc.GetMaxClusteringKeys()),
	)
	for i := range len(clusteringKeys) {
		clusteringKeys[i] = &typedef.ColumnDef{
			Name: GenColumnName("ck", i),
			Type: GenPrimaryKeyColumnType(r),
		}
	}
	table := typedef.Table{
		Name:           tableName,
		PartitionKeys:  partitionKeys,
		ClusteringKeys: clusteringKeys,
		KnownIssues: map[string]bool{
			typedef.KnownIssuesJSONWithTuples: true,
		},
	}
	for _, option := range sc.TableOptions {
		table.TableOptions = append(table.TableOptions, option.ToCQL())
	}
	if sc.UseCounters {
		table.Columns = typedef.Columns{
			{
				Name: GenColumnName("col", 0),
				Type: &typedef.CounterType{
					Value: 0,
				},
			},
		}
		return &table
	}

	columns := utils.RandInt2(r, sc.GetMinColumns(), sc.GetMaxColumns())
	table.Columns = make(typedef.Columns, 0, columns)

	for i := range columns {
		table.Columns = append(table.Columns, &typedef.ColumnDef{
			Name: GenColumnName("col", i),
			Type: GenColumnType(columns, &sc, r),
		})
	}

	if sc.CQLFeature > typedef.CQL_FEATURE_NORMAL && sc.UseMaterializedViews && columns > 0 {
		table.Indexes = CreateIndexesForColumn(&table, utils.RandInt2(r, 1, columns))
	}

	if sc.CQLFeature > typedef.CQL_FEATURE_NORMAL && sc.UseMaterializedViews &&
		len(clusteringKeys) > 0 &&
		table.Columns.ValidColumnsForPrimaryKey().Len() != 0 {
		table.MaterializedViews = CreateMaterializedViews(table.Columns, table.Name, partitionKeys, clusteringKeys, r)
	}

	return &table
}

func GetCreateKeyspaces(s *typedef.Schema) (string, string) {
	return fmt.Sprintf(
			"CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = %s",
			s.Keyspace.Name,
			s.Keyspace.Replication.ToCQL(),
		),
		fmt.Sprintf(
			"CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = %s",
			s.Keyspace.Name,
			s.Keyspace.OracleReplication.ToCQL(),
		)
}

func GetCreateSchema(s *typedef.Schema) []string {
	stmts := make([]string, 0, len(s.Tables)*2)

	for _, t := range s.Tables {
		createTypes := GetCreateTypes(t, s.Keyspace)
		stmts = append(stmts, createTypes...)
		createTable := GetCreateTable(t, s.Keyspace)
		stmts = append(stmts, createTable)
		for _, idef := range t.Indexes {
			stmts = append(
				stmts,
				fmt.Sprintf(
					"CREATE INDEX IF NOT EXISTS %s ON %s.%s (%s)",
					idef.IndexName,
					s.Keyspace.Name,
					t.Name,
					idef.ColumnName,
				),
			)
		}
		for _, mv := range t.MaterializedViews {
			var (
				mvPartitionKeys      []string
				mvPrimaryKeysNotNull []string
			)
			for _, pk := range mv.PartitionKeys {
				mvPartitionKeys = append(mvPartitionKeys, pk.Name)
				mvPrimaryKeysNotNull = append(
					mvPrimaryKeysNotNull,
					fmt.Sprintf("%s IS NOT NULL", pk.Name),
				)
			}
			for _, ck := range mv.ClusteringKeys {
				mvPrimaryKeysNotNull = append(
					mvPrimaryKeysNotNull,
					fmt.Sprintf("%s IS NOT NULL", ck.Name),
				)
			}
			var createMaterializedView string
			if len(mv.PartitionKeys) == 1 {
				createMaterializedView = "CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s AS SELECT * FROM %s.%s WHERE %s PRIMARY KEY (%s"
			} else {
				createMaterializedView = "CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s AS SELECT * FROM %s.%s WHERE %s PRIMARY KEY ((%s)"
			}
			createMaterializedView += ",%s)"
			stmts = append(stmts, fmt.Sprintf(createMaterializedView,
				s.Keyspace.Name, mv.Name, s.Keyspace.Name, t.Name,
				strings.Join(mvPrimaryKeysNotNull, " AND "),
				strings.Join(mvPartitionKeys, ","), strings.Join(t.ClusteringKeys.Names(), ",")))
		}
	}
	return stmts
}

func GetDropKeyspace(s *typedef.Schema) []string {
	return []string{
		fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", s.Keyspace.Name),
	}
}

func CreateMaterializedViews(
	c typedef.Columns,
	tableName string,
	partitionKeys, clusteringKeys typedef.Columns,
	r *rand.Rand,
) []typedef.MaterializedView {
	validColumns := c.ValidColumnsForPrimaryKey()
	var mvs []typedef.MaterializedView
	numMvs := 1
	for i := range numMvs {
		col := validColumns.Random(r)
		if col == nil {
			log.Printf("unable to generate valid columns for materialized view")
			continue
		}

		mv := typedef.MaterializedView{
			Name:           fmt.Sprintf("%s_mv_%d", tableName, i),
			PartitionKeys:  append(typedef.Columns{col}, partitionKeys...),
			ClusteringKeys: clusteringKeys,
			NonPrimaryKey:  col,
		}
		mvs = append(mvs, mv)
	}
	return mvs
}
