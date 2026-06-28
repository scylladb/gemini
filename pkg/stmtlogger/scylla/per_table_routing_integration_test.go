//go:build testing

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

//nolint:govet
package scylla

import (
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/testutils"
	"github.com/scylladb/gemini/pkg/typedef"
)

// TestLogger_MultiTableLogging_NoDrops_Integration is the fast stand-in for the
// manual "--max-tables=3 with statement logging" check. It drives the real
// committer against a real ScyllaDB using a 3-table schema whose tables have
// DIFFERENT partition-key arities (1, 2, 3) — the exact shape that made the old
// shared single-table logger drop every non-first table's statements (counting
// them in StatementLoggerMalformedTotal).
//
// It asserts that:
//   - every table's statements land in that table's OWN per-table _logs table
//     (proving per-table routing, not just table[0]), and
//   - StatementLoggerMalformedTotal records zero new drops.
//
// It does NOT use t.Parallel: it asserts a delta on the process-global
// malformed counter, which must not be perturbed by sibling tests mid-run.
func TestLogger_MultiTableLogging_NoDrops_Integration(t *testing.T) {
	containers := testutils.TestContainers(t)
	logger := zap.NewNop()

	// Short fixed name: GetScyllaStatementLogsKeyspace appends "_logs" and Scylla
	// caps keyspace names at 48 chars, so a name derived from the (long) test
	// name would overflow. New drops the _logs keyspace at startup, so reuse
	// across runs is safe; no other test uses this name.
	keyspaceName := "ks_mt_routing"

	tables := []*typedef.Table{
		{Name: "tbl0", PartitionKeys: typedef.Columns{
			{Name: "pk0", Type: typedef.TypeText},
		}},
		{Name: "tbl1", PartitionKeys: typedef.Columns{
			{Name: "pk0", Type: typedef.TypeText},
			{Name: "pk1", Type: typedef.TypeInt},
		}},
		{Name: "tbl2", PartitionKeys: typedef.Columns{
			{Name: "pk0", Type: typedef.TypeText},
			{Name: "pk1", Type: typedef.TypeInt},
			{Name: "pk2", Type: typedef.TypeBigint},
		}},
	}

	schema := &typedef.Schema{
		Keyspace: typedef.Keyspace{Name: keyspaceName},
		Tables:   tables,
	}

	const itemsPerTable = 5

	itemCh := make(chan stmtlogger.Item, len(tables)*itemsPerTable)
	errorCh := make(chan *joberror.JobError)

	scyllaLogger, err := New(
		schema,
		func() (*gocql.Session, error) { return containers.Oracle, nil },
		func() (*gocql.Session, error) { return containers.Test, nil },
		containers.TestHosts,
		containers.TestPort(),
		containers.DockerMode,
		"", "",
		replication.NewSimpleStrategy(),
		itemCh,
		"", "", // no statement files: exercise the committer / _logs tables only
		errorCh,
		logger,
	)
	require.NoError(t, err)
	require.NotNil(t, scyllaLogger)

	logsKS := GetScyllaStatementLogsKeyspace(keyspaceName)
	querySession, err := newSession(containers.TestHosts, containers.TestPort(), containers.DockerMode, "", "", logger)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = querySession.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", logsKS)).Exec()
		querySession.Close()
	})

	// Build partition-key values that match each table's arity exactly.
	pkValuesFor := func(tbl *typedef.Table, idx int) map[string][]any {
		m := make(map[string][]any, len(tbl.PartitionKeys))
		for j, pk := range tbl.PartitionKeys {
			switch pk.Type {
			case typedef.TypeText:
				m[pk.Name] = []any{fmt.Sprintf("%s_k%d", tbl.Name, idx)}
			case typedef.TypeInt:
				m[pk.Name] = []any{idx*10 + j}
			case typedef.TypeBigint:
				m[pk.Name] = []any{int64(idx*100 + j)}
			default:
				t.Fatalf("unexpected pk type %v", pk.Type)
			}
		}
		return m
	}

	before := testutil.ToFloat64(metrics.StatementLoggerMalformedTotal)

	for _, tbl := range tables {
		for i := range itemsPerTable {
			itemCh <- stmtlogger.Item{
				Start:         stmtlogger.Time{Time: time.Unix(0, int64(i+1))}, // distinct ts → distinct rows
				PartitionKeys: typedef.PartitionKeys{Values: typedef.NewValuesFromMap(pkValuesFor(tbl, i))},
				Error:         mo.Left[error, string](nil),
				Statement:     fmt.Sprintf("INSERT INTO %s.%s (...) VALUES (...)", keyspaceName, tbl.Name),
				Table:         keyspaceName + "." + tbl.Name,
				Host:          "127.0.0.1",
				Type:          stmtlogger.TypeTest,
				Values:        mo.Left[[]any, []byte]([]any{"v", i}),
				Duration:      stmtlogger.Duration{Duration: time.Millisecond},
				Attempt:       1,
				GeminiAttempt: 1,
				StatementType: typedef.InsertStatementType,
			}
		}
	}

	// Close the item channel and wait for the committer to drain every queued
	// item; the row counts below are then deterministic (no sleeps).
	close(itemCh)
	require.NoError(t, scyllaLogger.Close())

	// Each table's statements must be present in its OWN per-table _logs table.
	for _, tbl := range tables {
		logsTable := GetScyllaStatementLogsTable(tbl.Name)
		var count int
		err = querySession.Query(
			fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", logsKS, logsTable),
		).Scan(&count)
		require.NoError(t, err, "querying %s.%s", logsKS, logsTable)
		assert.Equal(t, itemsPerTable, count,
			"table %q (partition-key arity %d) must have all its statements logged",
			tbl.Name, tbl.PartitionKeys.LenValues())
	}

	after := testutil.ToFloat64(metrics.StatementLoggerMalformedTotal)
	assert.Equal(t, before, after,
		"no statement-log items should be dropped for any table (StatementLoggerMalformedTotal must stay flat)")
}
