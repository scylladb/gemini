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
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
)

func startPairWorker(ch chan stmtlogger.Item) *Logger {
	s := &Logger{logger: zap.NewNop(), channel: ch, pairs: make(chan logPair, 16)}
	s.wg.Add(1)
	go s.pairWorker()

	return s
}

func pairItem(origin *typedef.Stmt, ty stmtlogger.Type, host string) stmtlogger.Item {
	return stmtlogger.Item{
		Origin:        origin,
		Type:          ty,
		Host:          host,
		Start:         stmtlogger.Time{Time: time.Unix(100, 0)},
		StatementType: typedef.InsertStatementType,
	}
}

func TestPairWorker_JoinsBothClustersIntoOneRow(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ch := make(chan stmtlogger.Item, 4)
		s := startPairWorker(ch)

		stmt := &typedef.Stmt{}
		ch <- pairItem(stmt, stmtlogger.TypeTest, "test-host")
		ch <- pairItem(stmt, stmtlogger.TypeOracle, "oracle-host")

		pair := <-s.pairs
		require.True(t, pair.complete())
		require.Equal(t, "test-host", pair.test.Host)
		require.Equal(t, "oracle-host", pair.oracle.Host)

		close(ch)
		s.wg.Wait()
		_, open := <-s.pairs
		require.False(t, open, "nothing else is written")
	})
}

func TestPairWorker_WritesALoneItemAfterTheWindow(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ch := make(chan stmtlogger.Item, 4)
		s := startPairWorker(ch)

		ch <- pairItem(&typedef.Stmt{}, stmtlogger.TypeOracle, "oracle-host")

		synctest.Wait()
		require.Empty(t, s.pairs, "the item waits for its partner")

		synctest.Sleep(pairWindow + pairWindow/4)

		pair := <-s.pairs
		require.True(t, pair.hasOracle)
		require.False(t, pair.hasTest)

		close(ch)
		s.wg.Wait()
	})
}

func TestPairWorker_SplitsARepeatedCluster(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ch := make(chan stmtlogger.Item, 4)
		s := startPairWorker(ch)

		stmt := &typedef.Stmt{}
		ch <- pairItem(stmt, stmtlogger.TypeTest, "first")
		ch <- pairItem(stmt, stmtlogger.TypeTest, "second")
		ch <- pairItem(stmt, stmtlogger.TypeOracle, "oracle-host")

		first := <-s.pairs
		require.False(t, first.complete())
		require.Equal(t, "first", first.test.Host)

		second := <-s.pairs
		require.True(t, second.complete())
		require.Equal(t, "second", second.test.Host)

		close(ch)
		s.wg.Wait()
	})
}

func TestPairWorker_FlushesPendingItemsOnClose(t *testing.T) {
	t.Parallel()

	ch := make(chan stmtlogger.Item, 4)
	s := startPairWorker(ch)

	ch <- pairItem(&typedef.Stmt{}, stmtlogger.TypeTest, "test-host")
	close(ch)
	s.wg.Wait()

	pair, open := <-s.pairs
	require.True(t, open, "a pending item is written at shutdown")
	require.True(t, pair.hasTest)
}

func TestPairWorker_DropsSchemaAndSelect(t *testing.T) {
	t.Parallel()

	ch := make(chan stmtlogger.Item, 4)
	s := startPairWorker(ch)

	ch <- stmtlogger.Item{StatementType: typedef.CreateTableStatementType}
	ch <- stmtlogger.Item{StatementType: typedef.SelectStatementType, Type: stmtlogger.TypeTest}
	close(ch)
	s.wg.Wait()

	_, open := <-s.pairs
	require.False(t, open)
}
