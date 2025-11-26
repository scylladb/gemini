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

//go:build testing

package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/samber/mo"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/testutils"
	"github.com/scylladb/gemini/pkg/typedef"
)

func Test_DuplicateValuesWithCompare(t *testing.T) {
	t.Parallel()

	assert := require.New(t)
	scyllaContainer := testutils.TestContainers(t)
	keyspace := testutils.GenerateUniqueKeyspaceName(t)

	assert.NoError(scyllaContainer.Test.Query(
		fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", keyspace),
	).Exec())
	assert.NoError(scyllaContainer.Test.Query(
		fmt.Sprintf("CREATE TABLE %s.table_1 (id timeuuid PRIMARY KEY, value list<text>)", keyspace),
	).Exec())
	assert.NoError(scyllaContainer.Oracle.Query(
		fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", keyspace),
	).Exec())
	assert.NoError(scyllaContainer.Oracle.Query(
		fmt.Sprintf("CREATE TABLE %s.table_1 (id timeuuid PRIMARY KEY, value list<text>)", keyspace),
	).Exec())

	schema := &typedef.Schema{
		Keyspace: typedef.Keyspace{Name: keyspace},
		Config: typedef.SchemaConfig{
			ReplicationStrategy:              replication.NewSimpleStrategy(),
			OracleReplicationStrategy:        replication.NewSimpleStrategy(),
			AsyncObjectStabilizationAttempts: 10,
			AsyncObjectStabilizationDelay:    10 * time.Millisecond,
		},
		Tables: []*typedef.Table{{
			Name: "table_1",
			Columns: typedef.Columns{
				{Name: "id", Type: typedef.TypeUuid},
				{Name: "values", Type: &typedef.Collection{
					ComplexType: typedef.TypeList,
					ValueType:   typedef.TypeText,
					Frozen:      false,
				}},
			},
		}},
	}

	store := &delegatingStore{
		oracleStore:        newCQLStoreWithSession(scyllaContainer.OracleCluster, schema, zap.NewNop(), "", "oracle"),
		testStore:          newCQLStoreWithSession(scyllaContainer.TestCluster, schema, zap.NewNop(), "", "test"),
		logger:             zap.NewNop(),
		mutationRetrySleep: 100 * time.Millisecond,
		mutationRetries:    10,
	}

	assert.NoError(store.oracleStore.Init())
	assert.NoError(store.testStore.Init())

	uuid := gocql.TimeUUID()

	insert := typedef.SimpleStmt(
		fmt.Sprintf("INSERT INTO %s.table_1(id, value) VALUES(%s, ['%s', '%s'])", keyspace, uuid, "test1", "test2"),
		typedef.InsertStatementType,
	)

	insert2 := typedef.SimpleStmt(
		fmt.Sprintf("INSERT INTO %s.table_1(id, value) VALUES(%s, ['%s', '%s', '%s'])", keyspace, uuid, "test1", "test2", "test3"),
		typedef.InsertStatementType,
	)
	timestamp := mo.Some(time.Now())

	// Try to make it insert a double
	for range 1000 {
		go func() {
			_ = store.testStore.mutate(t.Context(), insert2, timestamp)
		}()
	}

	check := typedef.SimpleStmt(
		fmt.Sprintf("SELECT * FROM %s.table_1 WHERE id = %s", keyspace, uuid),
		typedef.InsertStatementType,
	)
	assert.NoError(store.Mutate(t.Context(), insert))
	count, err := store.Check(t.Context(), schema.Tables[0], check, 1)

	assert.NoError(err)
	assert.Equal(1, count, "Expected only one row to be inserted with duplicate values")
}
