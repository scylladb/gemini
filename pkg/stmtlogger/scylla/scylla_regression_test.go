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
//
//nolint:govet
package scylla

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
)

// TestStatementSink_JSONLRegression pins the file format: one valid JSON object
// per line, with the job error's own query, message and partition keys, and the
// streamed rows verbatim. Rows are written straight from the CQL iterator now,
// so a malformed separator would corrupt the file for every later reader.
//
//nolint:gocyclo
func TestStatementSink_JSONLRegression(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	oraclePath := filepath.Join(dir, "oracle_statements.jsonl")
	testPath := filepath.Join(dir, "test_statements.jsonl")

	jobErr := &joberror.JobError{
		Timestamp: time.Now(),
		Query:     "SELECT * FROM ks.tbl WHERE pk0 = ? AND pk1 = ?",
		Message:   "synthetic error for test",
		StmtType:  typedef.SelectStatementType,
		PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
			"pk0": {"abc"},
			"pk1": {int32(7)},
		}),
	}

	s := &Logger{
		logger: zaptest.NewLogger(t),
		fetchHook: func(_ context.Context, _ stmtlogger.Type, item *joberror.JobError, w io.Writer) (int64, error) {
			e := newLineEncoder(w)
			e.head(reorganizePartitionKeys(item.PartitionKeys.ToMap(), item), item.Timestamp, "", item.Query, item.Message)
			e.array("mutationFragments")
			e.row(true, json.RawMessage(`{"fragment":1}`))
			e.endArray(false)
			e.array("statements")
			e.row(true, json.RawMessage(`{"statement":1}`))
			e.endArray(true)
			e.end(false)

			return e.Close()
		},
	}

	if err := s.openSinks(oraclePath, testPath); err != nil {
		t.Fatalf("failed to open sinks: %v", err)
	}

	s.writeErrorStatements(t.Context(), stmtlogger.TypeOracle, jobErr)
	s.writeErrorStatements(t.Context(), stmtlogger.TypeTest, jobErr)
	s.closeSinks()

	validateJSONL := func(path string) {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("failed to open statements file %s: %v", path, err)
		}
		defer f.Close()

		var lines []string

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			if line := strings.TrimSpace(scanner.Text()); line != "" {
				lines = append(lines, line)
			}
		}

		if len(lines) != 1 {
			t.Fatalf("expected exactly 1 JSONL line in %s, got %d", path, len(lines))
		}

		var out Line
		if err := json.Unmarshal([]byte(lines[0]), &out); err != nil {
			t.Fatalf("invalid JSON in %s: %v\nline: %s", path, err, lines[0])
		}

		if out.Query != jobErr.Query {
			t.Fatalf("unexpected query in %s: got %q want %q", path, out.Query, jobErr.Query)
		}
		if out.Message != jobErr.Message {
			t.Fatalf("unexpected message in %s: got %q want %q", path, out.Message, jobErr.Message)
		}
		if len(out.PartitionKeys) != 1 {
			t.Fatalf("expected 1 partition, got %d in %s", len(out.PartitionKeys), path)
		}

		got := out.PartitionKeys[0]
		if got.PartitionKeys["pk0"] != "abc" {
			t.Fatalf("unexpected pk0 in %s: %#v", path, got.PartitionKeys["pk0"])
		}

		pk1Val, ok := got.PartitionKeys["pk1"].(float64)
		if !ok || int(pk1Val) != 7 {
			t.Fatalf("unexpected pk1 in %s: %#v", path, got.PartitionKeys["pk1"])
		}

		if len(out.MutationFragments) != 1 || string(out.MutationFragments[0]) != `{"fragment":1}` {
			t.Fatalf("unexpected mutationFragments in %s: %v", path, out.MutationFragments)
		}
		if len(out.Statements) != 1 || string(out.Statements[0]) != `{"statement":1}` {
			t.Fatalf("unexpected statements in %s: %v", path, out.Statements)
		}
	}

	validateJSONL(oraclePath)
	validateJSONL(testPath)
}
