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

package stmtlogger

import (
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/pkg/errors"
	"github.com/samber/mo"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
	"github.com/scylladb/gemini/pkg/workpool"
)

func TestBuildQueriesCreation(t *testing.T) {
	t.Parallel()

	createKeyspace, createTable := buildCreateTableQuery([]typedef.ColumnDef{
		{Name: "col1", Type: typedef.TypeInt},
		{Name: "col2", Type: typedef.TypeAscii},
	}, replication.NewNetworkTopologyStrategy())

	snaps.MatchSnapshot(t, createKeyspace, "createKeyspace")
	snaps.MatchSnapshot(t, createTable, "createTable")
}

func TestBuildQueriesExecution(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	session := utils.SingleScylla(t)

	createKeyspace, createTable := buildCreateTableQuery([]typedef.ColumnDef{
		{Name: "col1", Type: typedef.TypeInt},
		{Name: "col2", Type: typedef.TypeAscii},
	}, replication.NewNetworkTopologyStrategy())

	assert.NoError(session.Query(createKeyspace).Exec())
	assert.NoError(session.Query(createTable).Exec())
}

func successStatement(ty Type) Item {
	start := time.Now().Add(-(5 * time.Second))
	statement := "INSERT INTO test_table (col1, col2) VALUES (?, ?)"
	item := Item{
		Start:         Time{Time: start},
		Error:         mo.Right[error, string](""),
		Statement:     statement,
		Host:          "test_host",
		Type:          ty,
		Values:        mo.Left[[]any, []byte]([]any{1, "test"}),
		Duration:      Duration{Duration: time.Second},
		Attempt:       1,
		GeminiAttempt: 1,
		StatementType: typedef.InsertStatementType,
		PartitionKeys: typedef.NewValuesFromMap(map[string][]any{"col1": {1}, "col2": {"test_1"}}),
	}

	return item
}

func errorStatement(ty Type) (Item, joberror.JobError) {
	start := time.Now().Add(-(10 * time.Second))
	ers := errors.New("test error")
	statement := "INSERT INTO test_table (col1, col2) VALUES (?, ?)"
	values := []any{2, "test_2"}

	item := Item{
		Start:         Time{Time: start},
		Error:         mo.Left[error, string](ers),
		Statement:     statement,
		Host:          "test_host",
		Type:          ty,
		Values:        mo.Left[[]any, []byte](values),
		Duration:      Duration{Duration: time.Second},
		Attempt:       1,
		GeminiAttempt: 1,
		StatementType: typedef.InsertStatementType,
		PartitionKeys: typedef.NewValuesFromMap(map[string][]any{"col1": {2}, "col2": {"test_2"}}),
	}

	err := joberror.JobError{
		Timestamp:     start,
		Err:           ers,
		Message:       "Mutation Validation failed",
		Query:         statement,
		StmtType:      typedef.SelectStatementType,
		PartitionKeys: typedef.NewValuesFromMap(map[string][]any{"col1": {2}, "col2": {"test_2"}}),
	}

	return item, err
}

func TestScyllaLogger(t *testing.T) {
	t.Parallel()

	item := CompressionTests[0]

	t.Run("Compression_"+item.Compression.String(), func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		oracleFile := dir + "/oracle_statements.json"
		testFile := dir + "/test_statements.json"

		assert := require.New(t)
		session := utils.SingleScylla(t)

		jobList := joberror.NewErrorList(1)
		pool := workpool.New(50)
		t.Cleanup(func() {
			_ = pool.Close()
		})
		chMetrics := metrics.NewChannelMetrics("test", "test")
		partitionKeys := []typedef.ColumnDef{
			{Name: "col1", Type: typedef.TypeInt},
			{Name: "col2", Type: typedef.TypeText},
		}

		ch := make(chan Item, 10)
		zapLogger := utils.Must(zap.NewDevelopment())

		logger, err := NewScyllaLoggerWithSession(
			typedef.PartitionKeys{Values: typedef.NewValuesFromMap(map[string][]any{"col1": {5}, "col2": {"test_ddl"}})},
			session,
			partitionKeys,
			replication.NewNetworkTopologyStrategy(),
			ch,
			oracleFile, testFile, item.Compression, jobList,
			pool, zapLogger, chMetrics,
		)
		assert.NoError(err)
		assert.NotNil(logger)

		itemTest, testJobErr := errorStatement(TypeTest)
		itemOracle, _ := errorStatement(TypeOracle)

		ch <- ddlStatement(TypeTest)
		ch <- ddlStatement(TypeOracle)
		ch <- successStatement(TypeTest)
		ch <- itemTest
		ch <- successStatement(TypeOracle)
		ch <- itemOracle

		jobList.AddError(testJobErr)
		time.Sleep(2 * time.Second)
		close(ch)
		assert.NoError(logger.Close())

		var count int
		assert.NoError(session.Query("SELECT COUNT(*) FROM logs.statements").Scan(&count))
		assert.Equal(6, count)

		oracleData := item.ReadData(t, utils.Must(os.Open(oracleFile)))
		testData := item.ReadData(t, utils.Must(os.Open(testFile)))

		oracleStatements := strings.SplitSeq(strings.TrimRight(oracleData, "\n"), "\n")
		testStatements := strings.SplitSeq(strings.TrimRight(testData, "\n"), "\n")

		sortedOracle := slices.SortedStableFunc(oracleStatements, strings.Compare)
		sortedTest := slices.SortedStableFunc(testStatements, strings.Compare)

		assert.Equal(sortedOracle, sortedTest)
		assert.Len(sortedOracle, 2)
		assert.Len(sortedTest, 2)
	})
}

func ddlStatement(ty Type) Item {
	return Item{
		Start:         Time{Time: time.Now().Add(-(15 * time.Second))},
		Error:         mo.Right[error, string](""),
		Statement:     "CREATE TABLE IF NOT EXISTS test_table (col1 int, col2 text, PRIMARY KEY (col1))",
		Host:          "test_host",
		Type:          ty,
		Values:        mo.Left[[]any, []byte](nil),
		Duration:      Duration{Duration: time.Second},
		Attempt:       1,
		GeminiAttempt: 1,
		StatementType: typedef.CreateSchemaStatementType,
	}
}
