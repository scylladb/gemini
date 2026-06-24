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

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
)

// TestLogger_PerTableRouting_NoDrops reproduces the multi-table statement-log
// drop bug: when the logger is shared across a schema with tables of differing
// partition-key arities, items from any table other than Tables[0] used to be
// bound against Tables[0]'s columns and dropped (counted in
// StatementLoggerMalformedTotal). With per-table routing every table's items
// bind against their own columns, so nothing is dropped.
//
// Not parallel: it asserts exact deltas on the process-global malformed counter,
// which only the committer/bind path mutates.
func TestLogger_PerTableRouting_NoDrops(t *testing.T) {
	// Table A stands in for schema.Tables[0] (two partition keys); table B is a
	// second table with a single partition key — exactly the shape that the
	// old single-table committer dropped.
	pksA := typedef.Columns{
		{Name: "pk0", Type: typedef.TypeText},
		{Name: "pk1", Type: typedef.TypeInt},
	}
	pksB := typedef.Columns{
		{Name: "pk0", Type: typedef.TypeText},
	}

	cqlA := makeTestCQL(pksA)
	cqlA.insertStmt = "INSERT INTO ks_logs.a_statements(...) VALUES (...)"
	cqlB := makeTestCQL(pksB)
	cqlB.insertStmt = "INSERT INTO ks_logs.b_statements(...) VALUES (...)"

	lg := &Logger{
		logger: zaptest.NewLogger(t),
		statements: map[string]*cqlStatements{
			"ks.a": cqlA,
			"ks.b": cqlB,
		},
	}

	itemA := makeItem(pksA, map[string][]any{"pk0": {"ka"}, "pk1": {1}}, stmtlogger.TypeTest, 0)
	itemA.Table = "ks.a"
	itemA.Statement = "INSERT INTO ks.a (pk0,pk1) VALUES (?,?)"

	itemB := makeItem(pksB, map[string][]any{"pk0": {"kb"}}, stmtlogger.TypeOracle, 1)
	itemB.Table = "ks.b"
	itemB.Statement = "INSERT INTO ks.b (pk0) VALUES (?)"

	before := testutil.ToFloat64(metrics.StatementLoggerMalformedTotal)

	// Both items route to their own table's statements and bind cleanly.
	gotA, _, okA := lg.bind(itemA, make([]any, 0, lg.maxArgsCap()))
	require.True(t, okA, "table A item must bind")
	require.Same(t, cqlA, gotA, "table A item must route to table A statements")

	gotB, _, okB := lg.bind(itemB, make([]any, 0, lg.maxArgsCap()))
	require.True(t, okB, "table B item must bind (regression: it used to be dropped)")
	require.Same(t, cqlB, gotB, "table B item must route to table B statements")

	assert.Equal(t, before, testutil.ToFloat64(metrics.StatementLoggerMalformedTotal),
		"no statement-log items should be dropped for any table")

	// Demonstrate the bug per-table routing avoids: binding table B's item
	// against table A's (two-PK) columns mis-counts the arity and is rejected,
	// which is exactly the drop that happened when one fixed table was shared.
	_, okMisbind := cqlA.fillArgs(make([]any, 0, cqlA.argsCap()), itemB)
	assert.False(t, okMisbind,
		"binding B against A's columns must fail — this is the drop routing fixes")

	// An item whose table is not in the schema is still dropped and counted, so
	// genuinely unroutable items remain visible via the metric.
	itemUnknown := makeItem(pksB, map[string][]any{"pk0": {"x"}}, stmtlogger.TypeTest, 2)
	itemUnknown.Table = "ks.missing"
	_, _, okUnknown := lg.bind(itemUnknown, make([]any, 0, lg.maxArgsCap()))
	assert.False(t, okUnknown, "unknown-table item must be dropped")
	assert.Equal(t, before+1, testutil.ToFloat64(metrics.StatementLoggerMalformedTotal),
		"only the unknown-table item should be counted as malformed")
}
