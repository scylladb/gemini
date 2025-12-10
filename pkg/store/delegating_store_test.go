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
	"sync/atomic"
	"testing"
	"time"

	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func TestDelegatingStore_Mutate_ContextCanceledDuringBackoff_ReturnsNil(t *testing.T) {
	t.Parallel()
	retryErr := errors.New("temporary")
	test := &fakeStore{nameStr: "test", mutateSeq: []error{retryErr, retryErr}}

	ds := &delegatingStore{
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

	n, err := ds.Check(t.Context(), table, stmt, 1)
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
	ds1 := &delegatingStore{testStore: test1, oracleStore: oracle1, validationRetries: 1, minimumDelay: 1 * time.Millisecond}
	errStmt := typedef.SimpleStmt("SELECT * FROM ks.t WHERE pk0=1", typedef.SelectStatementType)
	n, err := ds1.Check(t.Context(), table, errStmt, 1)
	require.Error(t, err)
	assert.Equal(t, 0, n)

	// Then a match
	test2 := &fakeStore{nameStr: "test", loadRows: Rows{NewRow([]string{"pk0", "v"}, []any{1, "a"})}}
	oracle2 := &fakeStore{nameStr: "oracle", loadRows: Rows{NewRow([]string{"pk0", "v"}, []any{1, "a"})}}
	ds2 := &delegatingStore{testStore: test2, oracleStore: oracle2, validationRetries: 1}
	okStmt := typedef.SimpleStmt("SELECT * FROM ks.t WHERE pk0=1", typedef.SelectStatementType)
	n2, err2 := ds2.Check(t.Context(), table, okStmt, 1)
	require.NoError(t, err2)
	assert.Equal(t, 1, n2)
}
