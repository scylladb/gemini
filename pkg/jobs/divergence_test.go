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

package jobs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"uuid"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/partitions"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
)

// knownInvalidGenerator answers MarkInvalid like partitions.Partitions: false
// for a partition that is already invalid, true for the first mark of any
// other partition.
type knownInvalidGenerator struct {
	trackingGenerator

	invalid map[uuid.UUID]bool
	marked  []uuid.UUID
	mu      sync.Mutex
}

func newKnownInvalidGenerator(known ...uuid.UUID) *knownInvalidGenerator {
	g := &knownInvalidGenerator{invalid: make(map[uuid.UUID]bool)}
	for _, id := range known {
		g.invalid[id] = true
	}

	return g
}

func (g *knownInvalidGenerator) MarkInvalid(keys *typedef.PartitionKeys) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.invalid[keys.ID] {
		return false
	}
	g.invalid[keys.ID] = true
	g.marked = append(g.marked, keys.ID)

	return true
}

func divergenceTable() *typedef.Table {
	return &typedef.Table{
		Name:           "t",
		PartitionKeys:  typedef.Columns{{Name: "pk1", Type: typedef.TypeInt}},
		ClusteringKeys: typedef.Columns{{Name: "ck1", Type: typedef.TypeBigint}},
	}
}

// diffOn returns a divergence whose differing rows belong to the partitions
// with the given pk1 values.
func diffOn(stmt *typedef.Stmt, pkVals ...int32) error {
	ve := store.NewValidationError("validation", stmt)
	cr := &store.ComparisonResults{}
	for _, pk := range pkVals {
		row := makeRowForPartition(pk)
		cr.TestOnlyRows = append(cr.TestOnlyRows, row)
		cr.TestRows = append(cr.TestRows, row)
	}
	ve.ComparisonResults = cr
	ve.Finalize(errors.New("rows differ"))

	return ve
}

func newDivergenceValidation(gen *knownInvalidGenerator, st store.Store) *Validation {
	return &Validation{
		table:        divergenceTable(),
		keyspaceName: "ks",
		generator:    gen,
		store:        st,
		status:       status.NewGlobalStatus(100),
		stopFlag:     stop.NewFlag("divergence"),
		maxAttempts:  3,
	}
}

// TestReportDivergence_BlamesOnlyDifferingPartitions pins that a
// multi-partition SELECT blames only the partitions whose rows differ. A
// partition that is already invalid diverged because of gemini, for example
// after an asymmetric write. Its difference must not be reported against a
// healthy partition of the same statement.
func TestReportDivergence_BlamesOnlyDifferingPartitions(t *testing.T) {
	t.Parallel()

	t.Run("difference only on a known invalid partition is dropped", func(t *testing.T) {
		t.Parallel()

		stmt := makeMultiPartitionStmt(1, 2)
		known, healthy := stmt.PartitionKeys[0].ID, stmt.PartitionKeys[1].ID
		gen := newKnownInvalidGenerator(known)
		v := newDivergenceValidation(gen, &mockStore{})

		err := v.handleCheckFailure(attemptHistory{divergence: diffOn(stmt, 1)}, stmt)

		require.NoError(t, err, "a difference gemini caused is not a SUT bug")
		require.NotContains(t, gen.marked, healthy, "the healthy partition must not be marked invalid")
	})

	t.Run("difference on a healthy partition is reported", func(t *testing.T) {
		t.Parallel()

		stmt := makeMultiPartitionStmt(1, 2)
		known, healthy := stmt.PartitionKeys[0].ID, stmt.PartitionKeys[1].ID
		gen := newKnownInvalidGenerator(known)
		v := newDivergenceValidation(gen, &mockStore{})

		err := v.handleCheckFailure(attemptHistory{divergence: diffOn(stmt, 2)}, stmt)

		jobErr, ok := errors.AsType[*joberror.JobError](err)
		require.True(t, ok, "a new divergence is reported")
		require.Equal(t, []uuid.UUID{healthy}, jobErr.PartitionIDs)
		require.Equal(t, []uuid.UUID{healthy}, gen.marked)
	})

	t.Run("report keeps only the rows of new partitions", func(t *testing.T) {
		t.Parallel()

		stmt := makeMultiPartitionStmt(1, 2)
		known, healthy := stmt.PartitionKeys[0].ID, stmt.PartitionKeys[1].ID
		gen := newKnownInvalidGenerator(known)
		v := newDivergenceValidation(gen, &mockStore{})

		err := v.handleCheckFailure(attemptHistory{divergence: diffOn(stmt, 1, 2)}, stmt)

		jobErr, ok := errors.AsType[*joberror.JobError](err)
		require.True(t, ok)
		require.Equal(t, []uuid.UUID{healthy}, jobErr.PartitionIDs)
		require.NotNil(t, jobErr.Results)
		require.Len(t, jobErr.Results.TestOnlyRows, 1, "the rows of the known partition are left out")
		require.Contains(t, string(jobErr.Results.TestOnlyRows[0]), `"pk1":2`)
	})
}

// sequenceStore returns the errors of seq from CheckOnce and Check in order,
// and then the last error again.
type sequenceStore struct {
	mockStore

	seq   []error
	calls int
}

func (s *sequenceStore) next() error {
	err := s.seq[min(s.calls, len(s.seq)-1)]
	s.calls++
	return err
}

func (s *sequenceStore) CheckOnce(context.Context, *typedef.Table, *typedef.Stmt, int) (int, store.Rows, error) {
	return 0, nil, s.next()
}

func (s *sequenceStore) Check(context.Context, *typedef.Table, *typedef.Stmt, int) (int, store.Rows, error) {
	return 0, nil, s.next()
}

func loadErr(stmt *typedef.Stmt, testErr, oracleErr error) error {
	ve := store.NewValidationError("validation", stmt)
	ve.TestErr = testErr
	ve.OracleErr = oracleErr
	final := testErr
	if final == nil {
		final = oracleErr
	}
	ve.Finalize(final)

	return ve
}

// runAllAttempts runs every attempt of stmt through the retry path and
// returns the read errors that the run recorded.
func runAllAttempts(t *testing.T, v *Validation, stmt *typedef.Stmt) []joberror.JobError {
	t.Helper()

	rq := newRetryQueue(v.maxAttempts, 0, 0)
	t.Cleanup(func() { rq.Drain(nil) })
	metric := prometheus.NewCounter(prometheus.CounterOpts{Name: "t"})

	result := v.checkOnce(t.Context(), stmt, 0, attemptHistory{}, metric)
	for result.needsRetry {
		require.NoError(t, v.handleRunResult(result, rq))
		if rq.Len() == 0 {
			break
		}
		retry := rq.Take(0)
		result = v.checkOnce(t.Context(), retry.stmt, retry.attempt, retry.history, metric)
	}

	return v.status.Errors.Errors()
}

// TestValidation_DivergenceSurvivesLaterReadErrors pins that every attempt
// counts, not only the last one. A divergence on the first attempt and
// timeouts on the later ones is a divergence. A run of read failures is a
// read error, not a data mismatch. A read failure of the oracle alone is not
// charged to the system under test.
func TestValidation_DivergenceSurvivesLaterReadErrors(t *testing.T) {
	t.Parallel()

	stmt := makeStmt(7)

	for _, tc := range []struct {
		seq         []error
		name        string
		wantMessage string
		wantErrors  int
		wantResults bool
	}{
		{
			name:        "divergence then timeouts",
			seq:         []error{diffOn(stmt, 7), loadErr(stmt, context.DeadlineExceeded, nil)},
			wantErrors:  1,
			wantMessage: "Validation failed",
			wantResults: true,
		},
		{
			name:        "test cluster read errors only",
			seq:         []error{loadErr(stmt, context.DeadlineExceeded, nil)},
			wantErrors:  1,
			wantMessage: "no data mismatch was seen",
		},
		{
			name: "oracle read errors only",
			seq:  []error{loadErr(stmt, nil, errors.New("oracle: Unavailable"))},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := makeStmt(7)
			s.PartitionKeys[0].ID = stmt.PartitionKeys[0].ID
			v := newDivergenceValidation(newKnownInvalidGenerator(), &sequenceStore{seq: tc.seq})

			errs := runAllAttempts(t, v, s)

			require.Len(t, errs, tc.wantErrors)
			if tc.wantErrors == 0 {
				return
			}
			require.Contains(t, errs[0].Message, tc.wantMessage)
			require.Equal(t, tc.wantResults, errs[0].Results != nil)
		})
	}
}

// TestValidateDeletedPartition_ClassifiesTheCheckResult pins how the
// re-validation of a deleted partition charges the result of store.Check.
func TestValidateDeletedPartition_ClassifiesTheCheckResult(t *testing.T) {
	t.Parallel()

	keys := makeStmt(3).PartitionKeys[0]
	stmt := &typedef.Stmt{PartitionKeys: []typedef.PartitionKeys{keys}}

	for _, tc := range []struct {
		err         error
		name        string
		wantMessage string
		wantJobErr  bool
	}{
		{name: "divergence", err: diffOn(stmt, 3), wantJobErr: true, wantMessage: "difference found"},
		{name: "test read error", err: loadErr(stmt, context.DeadlineExceeded, nil), wantJobErr: true, wantMessage: "no data mismatch was seen"},
		{name: "oracle read error", err: loadErr(stmt, nil, errors.New("oracle: Unavailable"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			v := newDivergenceValidation(newKnownInvalidGenerator(), &sequenceStore{seq: []error{tc.err}})
			v.selectColumns = v.table.SelectColumnNames()

			err := v.validateDeletedPartition(t.Context(), keys, 0)

			if !tc.wantJobErr {
				require.NoError(t, err)
				return
			}
			jobErr, ok := errors.AsType[*joberror.JobError](err)
			require.True(t, ok)
			require.Contains(t, jobErr.Message, tc.wantMessage)
		})
	}
}

// claimingGenerator answers ClaimDivergedKey like partitions.Partitions: true
// for the first claim of a fingerprint.
type claimingGenerator struct {
	knownInvalidGenerator

	claimed map[string]bool
}

func (g *claimingGenerator) ClaimDivergedKey(fp string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.claimed[fp] {
		return false
	}
	g.claimed[fp] = true

	return true
}

func pkFingerprint(t *testing.T, pk int32) string {
	t.Helper()

	fp, ok := partitions.KeyFingerprint(divergenceTable().PartitionKeys, makeRowForPartition(pk).Get)
	require.True(t, ok)

	return fp
}

// TestIndexDivergence_ReportsOnlyNewPartitions: an index query names no
// partition, so its differing rows are attributed to partitions by their key
// fingerprint. A partition that gemini already knows differs is left out, and
// each new partition is reported once.
func TestIndexDivergence_ReportsOnlyNewPartitions(t *testing.T) {
	t.Parallel()

	indexStmt := func() *typedef.Stmt {
		return &typedef.Stmt{
			QueryType: typedef.SelectByIndexStatementType,
			Query:     "SELECT * FROM ks.t WHERE col0=? ALLOW FILTERING",
		}
	}

	gen := &claimingGenerator{claimed: map[string]bool{pkFingerprint(t, 1): true}}
	v := &Validation{
		table:        divergenceTable(),
		keyspaceName: "ks",
		generator:    gen,
		store:        &mockStore{},
		status:       status.NewGlobalStatus(100),
		stopFlag:     stop.NewFlag("index-divergence"),
		maxAttempts:  3,
	}

	stmt := indexStmt()
	err := v.handleCheckFailure(attemptHistory{divergence: diffOn(stmt, 1)}, stmt)
	require.NoError(t, err, "a partition that gemini made diverge is not a SUT bug")

	stmt = indexStmt()
	err = v.handleCheckFailure(attemptHistory{divergence: diffOn(stmt, 1, 2)}, stmt)
	jobErr, ok := errors.AsType[*joberror.JobError](err)
	require.True(t, ok, "a new partition is reported")
	require.NotNil(t, jobErr.Results)
	require.Len(t, jobErr.Results.TestOnlyRows, 1, "the rows of the known partition are left out")
	require.Contains(t, string(jobErr.Results.TestOnlyRows[0]), `"pk1":2`)
	require.Len(t, jobErr.Results.TestRows, 1)

	stmt = indexStmt()
	err = v.handleCheckFailure(attemptHistory{divergence: diffOn(stmt, 2)}, stmt)
	require.NoError(t, err, "a partition is reported once")
}
