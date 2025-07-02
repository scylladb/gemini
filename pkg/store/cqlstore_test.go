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
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
	"github.com/scylladb/gemini/pkg/workpool"
)

func Test_DuplicateValuesWithCompare(t *testing.T) {
	t.Parallel()

	assert := require.New(t)
	testSession, oracleSession := utils.TestContainers(t)

	assert.NoError(testSession.Query(
		"CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
	).Exec())
	assert.NoError(testSession.Query(
		"CREATE TABLE ks1.table_1 (id timeuuid PRIMARY KEY, value list<text>)",
	).Exec())
	assert.NoError(oracleSession.Query(
		"CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
	).Exec())
	assert.NoError(oracleSession.Query(
		"CREATE TABLE ks1.table_1 (id timeuuid PRIMARY KEY, value list<text>)",
	).Exec())

	schema := &typedef.Schema{
		Keyspace: typedef.Keyspace{Name: "ks1"},
		Config: typedef.SchemaConfig{
			ReplicationStrategy:              replication.NewSimpleStrategy(),
			OracleReplicationStrategy:        replication.NewSimpleStrategy(),
			AsyncObjectStabilizationAttempts: 10,
			UseMaterializedViews:             false,
			AsyncObjectStabilizationDelay:    10 * time.Millisecond,
		},
		Tables: []*typedef.Table{{
			Name: "table_1",
			Columns: typedef.Columns{
				{Name: "id", Type: typedef.TypeUuid},
				{Name: "id", Type: &typedef.BagType{
					ComplexType: typedef.TypeList,
					ValueType:   typedef.TypeText,
					Frozen:      false,
				}},
			},
		}},
	}

	store := &delegatingStore{
		workers:     workpool.New(2),
		oracleStore: newCQLStoreWithSession(oracleSession, schema, zap.NewNop(), "oracle", 1, 10*time.Millisecond, false),
		testStore:   newCQLStoreWithSession(testSession, schema, zap.NewNop(), "test", 5, 1*time.Millisecond, false),
		logger:      zap.NewNop(),
	}

	uuid := gocql.TimeUUID()

	insert := typedef.SimpleStmt(
		fmt.Sprintf("INSERT INTO ks1.table_1(id, value) VALUES(%s, ['%s', '%s'])", uuid, "test1", "test2"),
		typedef.InsertStatementType,
	)

	insert2 := typedef.SimpleStmt(
		fmt.Sprintf("INSERT INTO ks1.table_1(id, value) VALUES(%s, ['%s', '%s', '%s'])", uuid, "test1", "test2", "test3"),
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
		fmt.Sprintf("SELECT * FROM ks1.table_1 WHERE id = %s", uuid),
		typedef.InsertStatementType,
	)
	assert.NoError(store.Mutate(t.Context(), insert))
	assert.NoError(store.Check(t.Context(), schema.Tables[0], check, 1))
}
