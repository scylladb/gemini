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
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/samber/mo"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
)

// distinctItem builds an item whose statement AND values are unique to idx, so
// that any bleed between the statement column and the values column (a mispair)
// is detectable: a coherent bind always has statement[i] paired with values[i].
func distinctItem(idx int) stmtlogger.Item {
	return stmtlogger.Item{
		Start:         stmtlogger.Time{Time: time.Unix(0, int64(idx))},
		PartitionKeys: typedef.PartitionKeys{Values: typedef.NewValuesFromMap(map[string][]any{"pk0": {fmt.Sprintf("k%d", idx)}, "pk1": {idx}})},
		Error:         mo.Right[error, string](""),
		Statement:     fmt.Sprintf("UPDATE ks.tbl SET c%d=? WHERE pk0=? AND pk1=?", idx),
		Host:          "127.0.0.1",
		Type:          stmtlogger.TypeTest,
		Values:        mo.Left[[]any, []byte]([]any{idx, fmt.Sprintf("v%d", idx)}),
		Duration:      stmtlogger.Duration{Duration: time.Millisecond},
		Attempt:       1,
		GeminiAttempt: 1,
		StatementType: typedef.InsertStatementType,
	}
}

// checkCoherent verifies the bound row's partition-key, statement and values
// columns all belong to the SAME item (idx) — the invariant that makes _logs
// triage trustworthy. Layout (see cqlStatements.fillArgs):
// [pk values...][ts][seq][Type][Statement][Values][Host]...
//
// It reports a failure as an error rather than failing the test directly, so it
// is safe to call from a worker goroutine: testify's require calls t.FailNow,
// which Go permits only on the goroutine running the test function.
func checkCoherent(out []any, pkLen, idx int) error {
	want := distinctItem(idx)

	wantPK := fmt.Sprintf("k%d", idx)
	if gotPK, ok := out[0].(string); !ok || gotPK != wantPK {
		return fmt.Errorf("idx %d: partition key bled from another item: got %v, want %q", idx, out[0], wantPK)
	}

	gotStmt, ok := out[pkLen+3].(string)
	if !ok {
		return fmt.Errorf("idx %d: statement column has type %T, want string", idx, out[pkLen+3])
	}

	if gotStmt != want.Statement {
		return fmt.Errorf("idx %d: statement column bled from another item: got %q, want %q", idx, gotStmt, want.Statement)
	}

	gotValues, ok := out[pkLen+4].([]string)
	if !ok {
		return fmt.Errorf("idx %d: values column has type %T, want []string", idx, out[pkLen+4])
	}

	if wantValues := prepareValuesOptimized(want.Values); !slices.Equal(gotValues, wantValues) {
		return fmt.Errorf("idx %d: values column bled from another item: got %v, want %v", idx, gotValues, wantValues)
	}

	return nil
}

// TestFillArgs_BufferReuseKeepsPairing mirrors the committer's held[idx] reuse:
// one dst buffer is rebound across a long stream of distinct items. Each bound
// row must carry its OWN statement and values — no residue from the prior item.
func TestFillArgs_BufferReuseKeepsPairing(t *testing.T) {
	t.Parallel()
	pks := typedef.Columns{{Name: "pk0", Type: typedef.TypeText}, {Name: "pk1", Type: typedef.TypeInt}}
	c := makeTestCQL(pks)
	pkLen := pks.LenValues()

	dst := make([]any, 0, c.argsCap())
	for i := range 5000 {
		var ok bool
		dst, ok = c.fillArgs(dst, distinctItem(i))
		require.True(t, ok)
		require.NoError(t, checkCoherent(dst, pkLen, i))
	}
}

// TestFillArgs_ConcurrentBindingIsCoherent runs many goroutines binding through
// a SHARED *cqlStatements (as every committer worker does), each with its own
// dst buffer (as held[idx] is per-worker). Run with -race: catches any shared
// mutable state introduced into fillArgs/cqlStatements that would let one
// goroutine's statement pair with another's values.
func TestFillArgs_ConcurrentBindingIsCoherent(t *testing.T) {
	t.Parallel()
	pks := typedef.Columns{{Name: "pk0", Type: typedef.TypeText}, {Name: "pk1", Type: typedef.TypeInt}}
	c := makeTestCQL(pks)
	pkLen := pks.LenValues()

	const workers = 16

	// Workers must not assert: require/t.FailNow are only legal on the goroutine
	// running the test function. Each worker reports its first incoherent bind
	// back through a buffered channel and the parent does the asserting.
	failures := make(chan error, workers)
	start := make(chan struct{})

	var wg sync.WaitGroup

	wg.Add(workers)

	for w := range workers {
		go func(base int) {
			defer wg.Done()

			dst := make([]any, 0, c.argsCap())
			<-start // release all workers together to maximise overlap

			for i := range 2000 {
				idx := base*1_000_000 + i

				var ok bool

				dst, ok = c.fillArgs(dst, distinctItem(idx))
				if !ok {
					failures <- fmt.Errorf("idx %d: fillArgs reported failure", idx)
					return
				}

				if err := checkCoherent(dst, pkLen, idx); err != nil {
					failures <- err
					return
				}
			}
		}(w)
	}

	close(start)
	wg.Wait()
	close(failures)

	for err := range failures {
		t.Error(err)
	}
}
