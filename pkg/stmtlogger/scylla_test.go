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
	"encoding/json"
	"fmt"
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
	"github.com/scylladb/gemini/pkg/testutils"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/workpool"
)

func TestBuildQueriesCreation(t *testing.T) {
	t.Parallel()

	createKeyspace, createTable := buildCreateTableQuery("ks1_logs", "table1_statements", []typedef.ColumnDef{
		{Name: "col1", Type: typedef.TypeInt},
		{Name: "col2", Type: typedef.TypeAscii},
	}, replication.NewNetworkTopologyStrategy())

	snaps.MatchSnapshot(t, createKeyspace, "createKeyspace")
	snaps.MatchSnapshot(t, createTable, "createTable")
}

func TestBuildQueriesExecution(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	scyllaContainer := testutils.SingleScylla(t)

	createKeyspace, createTable := buildCreateTableQuery("ks1_logs", "table1_statements", []typedef.ColumnDef{
		{Name: "col1", Type: typedef.TypeInt},
		{Name: "col2", Type: typedef.TypeAscii},
	}, replication.NewNetworkTopologyStrategy())

	assert.NoError(scyllaContainer.Test.Query(createKeyspace).Exec())
	assert.NoError(scyllaContainer.Test.Query(createTable).Exec())
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
	dir := t.TempDir()
	oracleFile := dir + "/oracle_statements.json"
	testFile := dir + "/test_statements.json"
	item := CompressionTests[0]

	assert := require.New(t)
	scyllaContainer := testutils.SingleScylla(t)

	jobList := joberror.NewErrorList(1)
	pool := workpool.New(1)
	t.Cleanup(func() {
		_ = pool.Close()
	})
	chMetrics := metrics.NewChannelMetrics("test", "test")
	partitionKeys := []typedef.ColumnDef{
		{Name: "col1", Type: typedef.TypeInt},
		{Name: "col2", Type: typedef.TypeText},
	}

	ch := make(chan Item, 10)
	zapLogger := testutils.Must(zap.NewDevelopment())

	logger, err := NewScyllaLoggerWithSession(
		"ks1",
		"table1",
		typedef.PartitionKeys{Values: typedef.NewValuesFromMap(map[string][]any{"col1": {5}, "col2": {"test_ddl"}})},
		scyllaContainer.Test,
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
	assert.NoError(scyllaContainer.Test.Query(
		fmt.Sprintf("SELECT COUNT(*) FROM %s.%s",
			GetScyllaStatementLogsKeyspace("ks1"),
			GetScyllaStatementLogsTable("table1")),
	).Scan(&count))
	assert.Equal(6, count)

	oracleData := item.ReadData(t, testutils.Must(os.Open(oracleFile)))
	testData := item.ReadData(t, testutils.Must(os.Open(testFile)))

	oracleStatements := strings.SplitSeq(strings.TrimRight(oracleData, "\n"), "\n")
	testStatements := strings.SplitSeq(strings.TrimRight(testData, "\n"), "\n")

	sortedOracle := slices.SortedStableFunc(oracleStatements, strings.Compare)
	sortedTest := slices.SortedStableFunc(testStatements, strings.Compare)

	assert.Len(sortedOracle, 2)
	assert.Len(sortedTest, 2)

	expectItem := func(data string, i Item) {
		m := make(map[string]any, 10)
		assert.NoError(json.Unmarshal([]byte(data), &m))

		// Validate error field
		if i.Error.IsLeft() && i.Error.MustLeft() != nil {
			assert.Equal(i.Error.MustLeft().Error(), m["error"])
		} else if i.Error.IsRight() {
			assert.Equal(i.Error.MustRight(), m["error"])
		} else {
			assert.Nil(m["error"])
		}

		assert.NotEmpty(m["ts"].(string))
		assert.Equal(i.Statement, m["statement"])
		assert.Equal(i.Host, m["host"])
		assert.Equal(i.Duration.Duration.String(), m["dur"])
		assert.Equal(float64(i.Attempt), m["attempt"])

		if i.Values.IsLeft() {
			values := i.Values.MustLeft()
			if values != nil {
				jsonValuesStr := m["values"].(string)
				// Unescape the JSON string and parse it
				var parsedValues []any
				assert.NoError(json.Unmarshal([]byte(jsonValuesStr), &parsedValues), "Failed to unmarshal JSON values")
				assert.Equal(len(values), len(parsedValues))
				for idx, val := range values {
					switch v := val.(type) {
					case int:
						assert.Equal(float64(v), parsedValues[idx])
					case string:
						assert.Equal(v, parsedValues[idx])
					default:
						assert.Equal(val, parsedValues[idx])
					}
				}
			} else {
				assert.Nil(m["values"])
			}
		} else {
			assert.Equal(string(i.Values.MustRight()), m["values"])
		}
	}

	expectItem(sortedOracle[0], itemOracle)
	expectItem(sortedTest[0], itemTest)
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
