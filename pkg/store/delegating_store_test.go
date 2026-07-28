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

package store

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/typedef"
)

// fakeStore implements storeLoader for testing delegatingStore logic without DB.
type fakeStore struct {
	loadErr   error
	nameStr   string
	mutateSeq []error
	loadRows  Rows
	loadSeq   []struct {
		err  error
		rows Rows
	}
	mutateCalls atomic.Int64
	loadCalls   atomic.Int64
}

func (f *fakeStore) load(_ context.Context, _ *typedef.Stmt) (Rows, error) {
	idx := int(f.loadCalls.Add(1)) - 1
	if len(f.loadSeq) > 0 {
		if idx < len(f.loadSeq) {
			return f.loadSeq[idx].rows, f.loadSeq[idx].err
		}
		last := f.loadSeq[len(f.loadSeq)-1]
		return last.rows, last.err
	}
	return f.loadRows, f.loadErr
}

func (f *fakeStore) loadIter(_ context.Context, _ *typedef.Stmt) RowIterator {
	rows := f.loadRows
	err := f.loadErr
	return func(yield func(Row, error) bool) {
		for _, r := range rows {
			if !yield(r, nil) {
				return
			}
		}
		if err != nil {
			_ = yield(Row{}, err)
		}
	}
}

func (f *fakeStore) mutate(_ context.Context, _ *typedef.Stmt, _ mo.Option[time.Time]) error {
	idx := int(f.mutateCalls.Add(1)) - 1
	if len(f.mutateSeq) == 0 {
		return nil
	}
	if idx < len(f.mutateSeq) {
		return f.mutateSeq[idx]
	}
	// If calls exceed sequence, return last element repeatedly
	return f.mutateSeq[len(f.mutateSeq)-1]
}
func (f *fakeStore) Init() error  { return nil }
func (f *fakeStore) Close() error { return nil }
func (f *fakeStore) name() string { return f.nameStr }

func TestDelegatingStore_Create_CallsMutateOnBothStores(t *testing.T) {
	t.Parallel()
	test := &fakeStore{nameStr: "test"}
	oracle := &fakeStore{nameStr: "oracle"}

	ds := &delegatingStore{
		testStore:            test,
		oracleStore:          oracle,
		serverSideTimestamps: false, // exercise timestamp branch
	}

	stmt := typedef.SimpleStmt("INSERT INTO ks.tab (x) VALUES (1)", typedef.InsertStatementType)
	err := ds.Create(t.Context(), stmt, stmt)
	require.NoError(t, err)

	assert.GreaterOrEqual(t, test.mutateCalls.Load(), int64(1))
	assert.GreaterOrEqual(t, oracle.mutateCalls.Load(), int64(1))
}

func TestDelegatingStore_Mutate_SucceedsAfterRetry(t *testing.T) {
	t.Parallel()
	retryErr := errors.New("transient")
	test := &fakeStore{nameStr: "test", mutateSeq: []error{retryErr, nil}}
	ds := &delegatingStore{
		inflight:             new(sync.WaitGroup),
		testStore:            test,
		mutationRetries:      2,
		mutationRetrySleep:   1 * time.Millisecond,
		minimumDelay:         1 * time.Millisecond,
		serverSideTimestamps: true,
	}

	stmt := typedef.SimpleStmt("UPDATE ks.t SET v=1 WHERE k=1", typedef.UpdateStatementType)
	err := ds.Mutate(t.Context(), stmt)
	require.NoError(t, err)
	assert.Equal(t, int64(2), test.mutateCalls.Load())
}

func TestDelegatingStore_Mutate_BothFail_ReturnsError(t *testing.T) {
	t.Parallel()
	e1 := errors.New("test-fail")
	e2 := errors.New("oracle-fail")
	test := &fakeStore{nameStr: "test", mutateSeq: []error{e1, e1}}
	oracle := &fakeStore{nameStr: "oracle", mutateSeq: []error{e2, e2}}

	ds := &delegatingStore{
		inflight:             new(sync.WaitGroup),
		testStore:            test,
		oracleStore:          oracle,
		mutationRetries:      1, // two attempts total
		mutationRetrySleep:   1 * time.Millisecond,
		minimumDelay:         1 * time.Millisecond,
		serverSideTimestamps: true,
	}
	stmt := typedef.SimpleStmt("DELETE FROM ks.t WHERE k=1", typedef.DeleteSingleRowType)
	err := ds.Mutate(t.Context(), stmt)
	require.Error(t, err)
}

// TestDelegatingStore_Mutate_PartialCompensationFlagsInvalidation pins the one
// case the per-store success flags cannot express.
//
// When BOTH original writes time out, TestStoreSuccess and OracleStoreSuccess
// are equal (both false) — yet each server may independently have committed,
// because a timeout says nothing about whether the write applied. Compensation
// exists to collapse that ambiguity by deleting the partition on both sides. If
// it only half-succeeds (test erased, oracle not), the clusters are genuinely
// different while the flags still look symmetric.
//
// Without CompensationFailed, mutation.run's `OracleStoreSuccess !=
// TestStoreSuccess` check is false, MarkInvalid is never called, and the
// partition stays in validation coverage — so gemini reports a divergence it
// created itself. That is the exact false-positive class this change set exists
// to eliminate.
func TestDelegatingStore_Mutate_PartialCompensationFlagsInvalidation(t *testing.T) {
	t.Parallel()

	timeout := context.DeadlineExceeded
	// Two attempts each (mutationRetries=1), then the compensating DELETE:
	// it succeeds on test and fails on oracle, leaving the partition erased on
	// one cluster and possibly still present on the other.
	test := &fakeStore{nameStr: "test", mutateSeq: []error{timeout, timeout, nil}}
	oracle := &fakeStore{
		nameStr:   "oracle",
		mutateSeq: []error{timeout, timeout, errors.New("compensating delete failed")},
	}

	ds := &delegatingStore{
		inflight:            new(sync.WaitGroup),
		testStore:           test,
		oracleStore:         oracle,
		mutationRetries:     1,
		mutationRetrySleep:  1 * time.Millisecond,
		minimumDelay:        1 * time.Millisecond,
		partitionKeyColumns: typedef.Columns{{Name: "pk", Type: typedef.TypeInt}},
		keyspaceAndTable:    "ks.t",
	}

	stmt := &typedef.Stmt{
		PartitionKeys: []typedef.PartitionKeys{
			{Values: typedef.NewValuesFromMap(map[string][]any{"pk": {1}})},
		},
		Query: "INSERT INTO ks.t(pk) VALUES (?)",
	}

	err := ds.Mutate(t.Context(), stmt)
	require.Error(t, err, "a half-successful compensation must not be reported as success")

	var mutErr *MutationError
	require.ErrorAs(t, err, &mutErr)

	// The precondition that makes this bug invisible: the flags agree.
	require.Equal(t, mutErr.TestStoreSuccess, mutErr.OracleStoreSuccess,
		"precondition: both original writes timed out, so the flags look symmetric")

	assert.True(t, mutErr.CompensationFailed,
		"partial compensation must be signalled explicitly; the success flags cannot express it")
}

func TestDelegatingStore_Mutate_AsymmetricCommit_IncrementsMetric(t *testing.T) {
	t.Parallel()
	// Unique store names so this test owns its own label vector element and
	// does not race the global counter against other parallel tests.
	const testName, oracleName = "test-asym-uncomp", "oracle-asym-uncomp"

	counter := metrics.MutationAsymmetricAcksTotal.WithLabelValues("uncompensated", testName)
	before := testutil.ToFloat64(counter)

	// Test commits on the first attempt; oracle fails every attempt with a
	// non-timeout error, so compensation does not apply and the divergence is
	// surfaced as an uncompensated asymmetric commit on the test cluster.
	test := &fakeStore{nameStr: testName, mutateSeq: []error{nil, nil}}
	oracle := &fakeStore{nameStr: oracleName, mutateSeq: []error{errors.New("oracle-fail"), errors.New("oracle-fail")}}

	ds := &delegatingStore{
		inflight:             new(sync.WaitGroup),
		testStore:            test,
		oracleStore:          oracle,
		mutationRetries:      1, // two attempts total
		mutationRetrySleep:   1 * time.Millisecond,
		minimumDelay:         1 * time.Millisecond,
		serverSideTimestamps: true,
	}

	stmt := typedef.SimpleStmt("UPDATE ks.t SET v=1 WHERE k=1", typedef.UpdateStatementType)
	err := ds.Mutate(t.Context(), stmt)
	require.Error(t, err)

	assert.Equal(t, before+1, testutil.ToFloat64(counter),
		"asymmetric commit (test committed, oracle failed) must increment the divergence counter")
}

func TestDelegatingStore_Mutate_ContextCanceledDuringBackoff_ReturnsNil(t *testing.T) {
	t.Parallel()
	retryErr := errors.New("temporary")
	test := &fakeStore{nameStr: "test", mutateSeq: []error{retryErr, retryErr}}

	ds := &delegatingStore{
		inflight:        new(sync.WaitGroup),
		testStore:       test,
		mutationRetries: 2,
		// Use longer delays to ensure we cancel during the backoff sleep path
		mutationRetrySleep:   200 * time.Millisecond,
		minimumDelay:         200 * time.Millisecond,
		serverSideTimestamps: true,
	}
	ctx, cancel := context.WithCancel(t.Context())
	// Force first attempt, then cancel before delay finishes
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	stmt := typedef.SimpleStmt("INSERT INTO ks.t (k) VALUES (1)", typedef.InsertStatementType)
	err := ds.Mutate(ctx, stmt)
	// On context canceled during delay, code returns nil
	assert.NoError(t, err)
}

func TestDelegatingStore_Check_OracleNil_ReturnsCount(t *testing.T) {
	t.Parallel()
	test := &fakeStore{nameStr: "test", loadRows: Rows{
		NewRow([]string{"pk0"}, []any{1}),
		NewRow([]string{"pk0"}, []any{2}),
		NewRow([]string{"pk0"}, []any{3}),
	}}
	ds := &delegatingStore{testStore: test, validationRetries: 1}
	table := &typedef.Table{Name: "t"}
	stmt := typedef.SimpleStmt("SELECT * FROM ks.t WHERE pk0=?", typedef.SelectStatementType)

	n, _, err := ds.Check(t.Context(), table, stmt, 1)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
}

func TestDelegatingStore_Check_WithOracle_DiffAndMatch(t *testing.T) {
	t.Parallel()
	table := &typedef.Table{
		Name:          "t",
		PartitionKeys: []typedef.ColumnDef{{Name: "pk0", Type: typedef.TypeInt}},
	}
	// First a mismatch
	test1 := &fakeStore{nameStr: "test", loadRows: Rows{NewRow([]string{"pk0", "v"}, []any{1, "a"})}}
	oracle1 := &fakeStore{nameStr: "oracle", loadRows: Rows{NewRow([]string{"pk0", "v"}, []any{1, "b"})}}
	ds1 := &delegatingStore{testStore: test1, oracleStore: oracle1, validationRetries: 1, minimumDelay: 1 * time.Millisecond, inflight: new(sync.WaitGroup)}
	errStmt := typedef.SimpleStmt("SELECT * FROM ks.t WHERE pk0=1", typedef.SelectStatementType)
	n, _, err := ds1.Check(t.Context(), table, errStmt, 1)
	require.Error(t, err)
	assert.Equal(t, 0, n)

	// Then a match
	test2 := &fakeStore{nameStr: "test", loadRows: Rows{NewRow([]string{"pk0", "v"}, []any{1, "a"})}}
	oracle2 := &fakeStore{nameStr: "oracle", loadRows: Rows{NewRow([]string{"pk0", "v"}, []any{1, "a"})}}
	ds2 := &delegatingStore{testStore: test2, oracleStore: oracle2, validationRetries: 1, inflight: new(sync.WaitGroup)}
	okStmt := typedef.SimpleStmt("SELECT * FROM ks.t WHERE pk0=1", typedef.SelectStatementType)
	n2, _, err2 := ds2.Check(t.Context(), table, okStmt, 1)
	require.NoError(t, err2)
	assert.Equal(t, 1, n2)
}
