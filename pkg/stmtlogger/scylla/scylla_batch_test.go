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

package scylla

import (
	"context"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
)

// helper to build minimal cqlStatements with hooks for testing
func makeTestCQL(partitionKeys typedef.Columns) *cqlStatements {
	return &cqlStatements{
		insertStmt:    "INSERT INTO ks.tbl(...) VALUES (...)",
		partitionKeys: partitionKeys,
		newBatch: func(_ context.Context) *gocql.Batch {
			b := &gocql.Batch{Type: gocql.UnloggedBatch}
			return b
		},
		// execBatch and execQuery will be overridden per test when needed
	}
}

func makeItem(_ typedef.Columns, pkVals map[string][]any, ty stmtlogger.Type, idx int) stmtlogger.Item {
	v := typedef.NewValuesFromMap(pkVals)
	return stmtlogger.Item{
		Start:         stmtlogger.Time{Time: time.Unix(0, int64(idx))},
		PartitionKeys: v,
		Error:         mo.Right[error, string](""),
		Statement:     "INSERT ...",
		Host:          "127.0.0.1",
		Type:          ty,
		Values:        mo.Left[[]any, []byte]([]any{"a", idx}),
		Duration:      stmtlogger.Duration{Duration: time.Millisecond},
		Attempt:       1,
		GeminiAttempt: 1,
		StatementType: typedef.InsertStatementType,
	}
}

func TestCQLStatements_FillArgsAndArgsCap(t *testing.T) {
	pks := typedef.Columns{{Name: "pk0", Type: typedef.TypeText}, {Name: "pk1", Type: typedef.TypeInt}}
	c := makeTestCQL(pks)

	// argsCap should be pk.LenValues + additional columns
	expectedCap := pks.LenValues() + len(additionalColumnsArr)
	assert.Equal(t, expectedCap, c.argsCap())

	// fillArgs should append in proper order and preserve capacity
	dst := make([]any, 0, c.argsCap())
	item := makeItem(pks, map[string][]any{"pk0": {"k"}, "pk1": {1}}, stmtlogger.TypeTest, 0)
	out := c.fillArgs(dst, item)
	require.Equal(t, c.argsCap(), cap(out))
	// first come partition keys in order
	require.GreaterOrEqual(t, len(out), 2)
	assert.Equal(t, "k", out[0])
	assert.Equal(t, 1, out[1])
}
