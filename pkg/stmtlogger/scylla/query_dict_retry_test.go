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

package scylla

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/stmtlogger"
)

// TestQueriesRowRetriedAfterFailedBatch: the first batch that carries the
// queries row of a statement can time out, and a timed-out batch has no
// row-by-row fallback. The row must then go into a later batch, or the
// queries table never holds the text of that statement.
func TestQueriesRowRetriedAfterFailedBatch(t *testing.T) {
	t.Parallel()

	pks := committerTestPartitionKeys()
	cql := makeTestCQL(pks)
	cql.insertStmt = "INSERT INTO ks_logs.tbl(pk0,pk1," + strings.Join(logColumns, ",") + ") VALUES (?,?,?,?,?,?,?)"
	cql.queriesInsert = "INSERT INTO ks_logs.queries (id, statement) VALUES (?, ?)"

	var (
		mu      sync.Mutex
		batches [][]string
	)

	executed := make(chan struct{}, 4)

	lg := &Logger{
		logger:     zap.NewNop(),
		statements: map[string]*cqlStatements{committerTestTable: cql},
		queries:    &queryDict{},
		logs: fakeLogSession{batch: func(_ context.Context, batch *gocql.Batch) error {
			mu.Lock()
			stmts := make([]string, 0, len(batch.Entries))
			for _, e := range batch.Entries {
				stmts = append(stmts, e.Stmt)
			}
			batches = append(batches, stmts)
			first := len(batches) == 1
			mu.Unlock()

			executed <- struct{}{}

			if first {
				return context.DeadlineExceeded
			}

			return nil
		}},
	}

	lg.pairs = make(chan logPair, 1)
	lg.wg.Add(1)
	lg.curWorkers.Store(1)

	go lg.insertWorker()

	// Two executions of one statement, each in a batch of its own.
	for i := range 2 {
		it := committerItem(i)
		it.Statement = "UPDATE ks.tbl SET c=? WHERE pk0=? AND pk1=?"
		it.Type = stmtlogger.TypeTest
		lg.pairs <- pairOf(it)
		<-executed
	}

	close(lg.pairs)
	lg.wg.Wait()

	require.Len(t, batches, 2)
	assert.Contains(t, batches[0], cql.queriesInsert, "the first batch carries the queries row")
	assert.Contains(t, batches[1], cql.queriesInsert, "the row was lost with the first batch, so the second must carry it")

	_, ok := lg.queries.written.Load(queryID("UPDATE ks.tbl SET c=? WHERE pk0=? AND pk1=?"))
	assert.True(t, ok, "the id counts as written once a batch with its row succeeded")
}
