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
	"encoding/json"
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

//nolint:gocyclo
func TestStatementFlusher_JSONMarshalRegression(t *testing.T) {
	t.Parallel()

	// Prepare temp files
	dir := t.TempDir()
	oraclePath := filepath.Join(dir, "oracle_statements.jsonl")
	testPath := filepath.Join(dir, "test_statements.jsonl")

	// Minimal logger instance sufficient for statementFlusher
	s := &Logger{logger: zaptest.NewLogger(t)}

	// Channel for flusher
	ch := make(chan statementChData, 2)

	// Start the flusher
	go s.statementFlusher(ch, oraclePath, testPath)

	// Create a sample JobError and associated data
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

	// Data map uses [32]byte keys internally – this is the crux of the
	// regression: such maps must never be sent to JSON encoder via logs.
	data := cqlDataMap{
		jobErr.Hash(): {
			partitionKeys: map[string][]any{
				"pk0": {"abc"},
				"pk1": {int32(7)},
			},
			mutationFragments: []json.RawMessage{json.RawMessage(`{"fragment":1}`)},
			statements:        []json.RawMessage{json.RawMessage(`{"statement":1}`)},
		},
	}

	// Send both oracle and test entries
	ch <- statementChData{ty: stmtlogger.TypeOracle, Data: data, Error: jobErr}
	ch <- statementChData{ty: stmtlogger.TypeTest, Data: data, Error: jobErr}

	// Close the channel to let flusher finish
	close(ch)

	// Avoid a race where Wait could be called before wg.Add(1) in the goroutine.
	// Instead, poll for file appearance with a timeout.
	waitForFile := func(path string) {
		deadline := time.Now().Add(2 * time.Second)
		for {
			if _, err := os.Stat(path); err == nil {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for file %s to be created", path)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	waitForFile(oraclePath)
	waitForFile(testPath)

	// Validate that both files contain exactly one valid JSON object each
	validateJSONL := func(path string) {
		// Because the file may be created before the line is written, loop until
		// we observe a non-empty line or time out.
		var lines []string
		deadline := time.Now().Add(2 * time.Second)
		for {
			f, err := os.Open(path)
			if err != nil {
				t.Fatalf("failed to open statements file %s: %v", path, err)
			}

			scanner := bufio.NewScanner(f)
			lines = lines[:0]
			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				if line != "" {
					lines = append(lines, line)
				}
			}
			_ = f.Close()

			if len(lines) > 0 {
				break
			}
			if time.Now().After(deadline) {
				break
			}
			time.Sleep(10 * time.Millisecond)
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
		// Validate PKs copied through
		if got := out.PartitionKeys["pk0"]; len(got) != 1 || got[0] != "abc" {
			t.Fatalf("unexpected pk0 in %s: %#v", path, out.PartitionKeys["pk0"])
		}
		{
			got := out.PartitionKeys["pk1"]
			if len(got) != 1 {
				t.Fatalf("unexpected pk1 length in %s: %#v", path, got)
			}
			var ok bool
			switch v := got[0].(type) {
			case float64:
				ok = int(v) == 7
			case int:
				ok = v == 7
			case int32:
				ok = v == 7
			case int64:
				ok = v == 7
			case uint:
				ok = int(v) == 7
			case uint32:
				ok = int(v) == 7
			case uint64:
				ok = int(v) == 7
			default:
				ok = false
			}
			if !ok {
				t.Fatalf("unexpected pk1 in %s: %#v", path, out.PartitionKeys["pk1"])
			}
		}

		if len(out.MutationFragments) != 1 || string(out.MutationFragments[0]) != `{"fragment":1}` {
			t.Fatalf("unexpected mutationFragments in %s: %s", path, string(out.MutationFragments[0]))
		}
		if len(out.Statements) != 1 || string(out.Statements[0]) != `{"statement":1}` {
			t.Fatalf("unexpected statements in %s: %s", path, string(out.Statements[0]))
		}
	}

	validateJSONL(oraclePath)
	validateJSONL(testPath)
}
