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

package status_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/typedef"
)

func newTestStatus(limit int) *status.GlobalStatus {
	return status.NewGlobalStatus(limit)
}

func newMinimalSchema() *typedef.Schema {
	return &typedef.Schema{
		Keyspace: typedef.Keyspace{Name: "test_ks"},
		Tables:   []*typedef.Table{{Name: "test_table"}},
	}
}

// TestNewGlobalStatus verifies construction and zero values.
func TestNewGlobalStatus(t *testing.T) {
	t.Parallel()
	gs := newTestStatus(10)
	require.NotNil(t, gs)
	assert.Equal(t, uint64(0), gs.WriteOps.Load())
	assert.Equal(t, uint64(0), gs.ReadOps.Load())
	assert.Equal(t, uint64(0), gs.WriteErrors.Load())
	assert.Equal(t, uint64(0), gs.ReadErrors.Load())
	assert.Equal(t, uint64(0), gs.ValidatedRows.Load())
	assert.False(t, gs.HasErrors())
	assert.False(t, gs.HasReachedErrorCount())
}

// TestWriteOp increments write ops counter.
func TestWriteOp(t *testing.T) {
	t.Parallel()
	gs := newTestStatus(10)
	gs.WriteOp()
	gs.WriteOp()
	assert.Equal(t, uint64(2), gs.WriteOps.Load())
}

// TestReadOp increments read ops counter.
func TestReadOp(t *testing.T) {
	t.Parallel()
	gs := newTestStatus(10)
	gs.ReadOp()
	assert.Equal(t, uint64(1), gs.ReadOps.Load())
}

// TestAddValidatedRows accumulates validated row counts.
func TestAddValidatedRows(t *testing.T) {
	t.Parallel()
	gs := newTestStatus(10)
	gs.AddValidatedRows(5)
	gs.AddValidatedRows(3)
	assert.Equal(t, uint64(8), gs.ValidatedRows.Load())
}

// TestAddWriteError records write errors and increments counter.
func TestAddWriteError(t *testing.T) {
	t.Parallel()
	gs := newTestStatus(10)
	gs.AddWriteError(joberror.JobError{
		Timestamp: time.Now(),
		Message:   "write failed",
	})
	assert.Equal(t, uint64(1), gs.WriteErrors.Load())
	assert.True(t, gs.HasErrors())
}

// TestAddReadError records read errors and increments counter.
func TestAddReadError(t *testing.T) {
	t.Parallel()
	gs := newTestStatus(10)
	gs.AddReadError(joberror.JobError{
		Timestamp: time.Now(),
		Message:   "read failed",
	})
	assert.Equal(t, uint64(1), gs.ReadErrors.Load())
	assert.True(t, gs.HasErrors())
}

// TestHasErrors returns false when no errors, true when either write or read errors exist.
func TestHasErrors(t *testing.T) {
	t.Parallel()
	gs := newTestStatus(10)
	assert.False(t, gs.HasErrors())

	gs.WriteOp()
	assert.False(t, gs.HasErrors())

	gs.AddWriteError(joberror.JobError{Timestamp: time.Now(), Message: "e"})
	assert.True(t, gs.HasErrors())
}

// TestHasReachedErrorCount returns true when error count >= limit.
func TestHasReachedErrorCount(t *testing.T) {
	t.Parallel()
	gs := newTestStatus(2)
	assert.False(t, gs.HasReachedErrorCount())

	gs.AddWriteError(joberror.JobError{Timestamp: time.Now(), Message: "e1"})
	assert.False(t, gs.HasReachedErrorCount())

	gs.AddReadError(joberror.JobError{Timestamp: time.Now(), Message: "e2"})
	assert.True(t, gs.HasReachedErrorCount())
}

// TestString produces a properly formatted summary string.
func TestString(t *testing.T) {
	t.Parallel()
	gs := newTestStatus(10)
	gs.WriteOp()
	gs.ReadOp()

	s := gs.String()
	assert.Contains(t, s, "write ops")
	assert.Contains(t, s, "read ops")
	assert.Contains(t, s, "write errors")
	assert.Contains(t, s, "read errors")
}

// TestUint64MarshalJSON verifies atomic counter marshals to a plain number.
func TestUint64MarshalJSON(t *testing.T) {
	t.Parallel()
	gs := newTestStatus(10)
	gs.WriteOp()
	gs.WriteOp()

	data, err := json.Marshal(&gs.WriteOps)
	require.NoError(t, err)
	assert.Equal(t, "2", string(data))
}

// TestSetStatementFiles stores the statement file paths without error.
func TestSetStatementFiles(t *testing.T) {
	t.Parallel()
	gs := newTestStatus(10)
	// No-panic and no-error is the contract — the fields are unexported.
	gs.SetStatementFiles("test.cql", "oracle.cql")
}

// TestPrintResultAsJSON produces valid JSON containing expected fields.
func TestPrintResultAsJSON(t *testing.T) {
	t.Parallel()
	gs := newTestStatus(10)
	gs.WriteOp()
	gs.ReadOp()

	schema := newMinimalSchema()
	var buf bytes.Buffer
	err := gs.PrintResultAsJSON(&buf, schema, "v1.0.0", map[string]any{"insert": 0.5})
	require.NoError(t, err)

	out := buf.String()
	assert.True(t, json.Valid([]byte(out)), "output must be valid JSON")
	assert.Contains(t, out, "gemini_version")
	assert.Contains(t, out, "v1.0.0")
	assert.Contains(t, out, "result")
}

// TestPrintResultAsJSONWithSummary includes corruption summary when provided.
func TestPrintResultAsJSONWithSummary(t *testing.T) {
	t.Parallel()
	gs := newTestStatus(10)

	schema := newMinimalSchema()
	summary := []joberror.CorruptionEntry{{ErrorKind: "mismatch"}}
	var buf bytes.Buffer
	err := gs.PrintResultAsJSONWithSummary(&buf, schema, "v2.0.0", nil, summary)
	require.NoError(t, err)

	out := buf.String()
	assert.True(t, json.Valid([]byte(out)))
	assert.Contains(t, out, "corruption_summary")
}

// TestPrintResultAsJSONWithSummary_NoSummary omits corruption_summary when nil.
func TestPrintResultAsJSONWithSummary_NoSummary(t *testing.T) {
	t.Parallel()
	gs := newTestStatus(10)

	schema := newMinimalSchema()
	var buf bytes.Buffer
	err := gs.PrintResultAsJSONWithSummary(&buf, schema, "v2.0.0", nil, nil)
	require.NoError(t, err)

	out := buf.String()
	assert.True(t, json.Valid([]byte(out)))
	assert.NotContains(t, out, "corruption_summary")
}

// TestPrintResult writes JSON to the provided writer.
func TestPrintResult(t *testing.T) {
	t.Parallel()
	gs := newTestStatus(10)
	gs.WriteOp()

	schema := newMinimalSchema()
	var buf bytes.Buffer
	gs.PrintResult(&buf, schema, "v1.2.3", map[string]any{})

	out := buf.String()
	// It must produce some output
	assert.True(t, len(strings.TrimSpace(out)) > 0)
}
