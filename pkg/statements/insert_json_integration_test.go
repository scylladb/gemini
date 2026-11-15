// Copyright 2026 ScyllaDB
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/partitions"
	"github.com/scylladb/gemini/pkg/testutils"
	"github.com/scylladb/gemini/pkg/typedef"
)

// TestInsertJSONAcceptedByScylla runs generated INSERT JSON statements against
// a real node, for every simple type that is not a tuple and for maps keyed by
// types other than text. JSON INSERT was disabled on every generated table, so
// no run ever checked that Scylla accepts what the generator writes.
func TestInsertJSONAcceptedByScylla(t *testing.T) {
	t.Parallel()

	containers := testutils.TestContainers(t)
	keyspace := typedef.Keyspace{
		Name:        testutils.GenerateUniqueKeyspaceName(t),
		Replication: nil,
	}

	columns := make(typedef.Columns, 0, 27)
	for i, st := range []typedef.SimpleType{
		typedef.TypeASCII, typedef.TypeBigint, typedef.TypeBlob, typedef.TypeBoolean, typedef.TypeDate,
		typedef.TypeDecimal, typedef.TypeDouble, typedef.TypeDuration, typedef.TypeFloat, typedef.TypeInet,
		typedef.TypeInt, typedef.TypeSmallint, typedef.TypeText, typedef.TypeTime, typedef.TypeTimestamp,
		typedef.TypeTimeuuid, typedef.TypeTinyint, typedef.TypeUUID, typedef.TypeVarchar, typedef.TypeVarint,
	} {
		columns = append(columns, typedef.ColumnDef{Name: GenColumnName("col", i), Type: st})
	}

	for i, key := range []typedef.SimpleType{
		typedef.TypeBoolean, typedef.TypeInt, typedef.TypeUUID, typedef.TypeTimestamp, typedef.TypeText,
	} {
		columns = append(columns, typedef.ColumnDef{
			Name: GenColumnName("map", i),
			Type: &typedef.MapType{ComplexType: typedef.TypeMap, KeyType: key, ValueType: typedef.TypeDouble},
		})
	}

	columns = append(columns,
		typedef.ColumnDef{Name: "list0", Type: &typedef.Collection{ComplexType: typedef.TypeList, ValueType: typedef.TypeText}},
		typedef.ColumnDef{Name: "set0", Type: &typedef.Collection{ComplexType: typedef.TypeSet, ValueType: typedef.TypeFloat}},
	)

	table := &typedef.Table{
		Name:           "json_insert",
		PartitionKeys:  typedef.Columns{{Name: "pk0", Type: typedef.TypeInt}},
		ClusteringKeys: typedef.Columns{{Name: "ck0", Type: typedef.TypeTimestamp}},
		Columns:        columns,
		KnownIssues:    map[string]bool{},
	}
	require.False(t, table.HasTupleColumns())

	require.NoError(t, containers.Test.Query(
		"CREATE KEYSPACE "+keyspace.Name+
			" WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}").Exec())
	t.Cleanup(func() { _ = containers.Test.Query("DROP KEYSPACE IF EXISTS " + keyspace.Name).Exec() })
	require.NoError(t, containers.Test.Query(GetCreateTable(table, keyspace)).Exec())

	const n = 64
	src, next := distributions.New(distributions.Uniform, n, 1, 0, 0)
	parts := partitions.New(t.Context(), rand.New(src), next, table, typedef.PartitionRangeConfig{}, n, 0)
	t.Cleanup(parts.Close)

	rng := rand.New(rand.NewChaCha8([32]byte{7}))
	rc, err := NewRatioController(DefaultStatementRatios(), rng)
	require.NoError(t, err)

	vc := &typedef.ValueRangeConfig{MaxBlobLength: 32, MinBlobLength: 1, MaxStringLength: 32, MinStringLength: 1}
	g := New(keyspace.Name, parts, table, rng, vc, rc, false)

	for range 200 {
		stmt, genErr := g.InsertJSON(t.Context())
		require.NoError(t, genErr)

		require.NoError(t, containers.Test.Query(stmt.Query, stmt.Values...).Exec(),
			"Scylla must accept the generated JSON: %v", stmt.Values)

		for i := range stmt.PartitionKeys {
			if stmt.PartitionKeys[i].Release != nil {
				stmt.PartitionKeys[i].Release()
			}
		}
	}
}
