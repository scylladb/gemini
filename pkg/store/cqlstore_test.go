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
		workers: newWorkers(2),
		oracleStore: &cqlStore{
			session:                 oracleSession,
			schema:                  schema,
			logger:                  zap.NewNop(),
			system:                  "oracle",
			maxRetriesMutate:        1,
			maxRetriesMutateSleep:   10 * time.Millisecond,
			useServerSideTimestamps: false,
		},
		testStore: &cqlStore{
			session:                 testSession,
			schema:                  schema,
			logger:                  zap.NewNop(),
			system:                  "test",
			maxRetriesMutate:        5,
			maxRetriesMutateSleep:   1 * time.Millisecond,
			useServerSideTimestamps: false,
		},
		logger: zap.NewNop(),
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
	assert.NoError(store.Check(t.Context(), schema.Tables[0], check, true))
}
