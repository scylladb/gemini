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
	"math/rand/v2"
	"sync"
	"testing"
	"uuid"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/partitions"
	"github.com/scylladb/gemini/pkg/statements"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
)

// invalidationRecorder is a partitions.Interface stub that records MarkInvalid
// calls and hands out usable partition keys, so a real statements.Generator can
// build a mutation statement on top of it.
type invalidationRecorder struct {
	invalid []uuid.UUID
	mu      sync.Mutex
}

func (g *invalidationRecorder) MarkInvalid(keys *typedef.PartitionKeys) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.invalid = append(g.invalid, keys.ID)

	return true
}

func (g *invalidationRecorder) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()

	return len(g.invalid)
}

func (g *invalidationRecorder) keys() typedef.PartitionKeys {
	return typedef.PartitionKeys{
		ID:     uuid.New(),
		Values: typedef.NewValuesFromMap(map[string][]any{"pk1": {int32(1)}}),
	}
}

func (g *invalidationRecorder) Next() typedef.PartitionKeys            { return g.keys() }
func (g *invalidationRecorder) NextForWrite() typedef.PartitionKeys    { return g.keys() }
func (g *invalidationRecorder) Get(_ uint64) typedef.PartitionKeys     { return g.keys() }
func (g *invalidationRecorder) Extend() typedef.PartitionKeys          { return g.keys() }
func (g *invalidationRecorder) ReplaceNext() typedef.PartitionKeys     { return g.keys() }
func (g *invalidationRecorder) Replace(_ uint64) typedef.PartitionKeys { return g.keys() }

func (g *invalidationRecorder) Stats() partitions.Stats                    { return partitions.Stats{} }
func (g *invalidationRecorder) ReplaceWithoutOld(_ uint64)                 {}
func (g *invalidationRecorder) ReplaceNextWithoutOld()                     {}
func (g *invalidationRecorder) Deleted() <-chan typedef.PartitionKeys      { return nil }
func (g *invalidationRecorder) ValidationSuccess(_ *typedef.PartitionKeys) {}
func (g *invalidationRecorder) ValidationFailure(_ *typedef.PartitionKeys) {}
func (g *invalidationRecorder) ValidationStats(_ *typedef.PartitionKeys) (uint64, uint64, uint64, []uint64, uint64) {
	return 0, 0, 0, nil, 0
}
func (g *invalidationRecorder) IsInvalid(_ uint64) bool          { return false }
func (g *invalidationRecorder) InvalidCount() uint64             { return 0 }
func (g *invalidationRecorder) TrackRow(_ partitions.TrackedRow) {}
func (g *invalidationRecorder) RowTrackerFillRatio() float64     { return 0 }
func (g *invalidationRecorder) PeekTrackedRow() (partitions.TrackedRow, bool) {
	return partitions.TrackedRow{}, false
}

func (g *invalidationRecorder) BorrowByID(id uuid.UUID) (typedef.PartitionKeys, bool) {
	return typedef.PartitionKeys{ID: id}, true
}

func (g *invalidationRecorder) PopTrackedRow() (partitions.TrackedRow, bool) {
	return partitions.TrackedRow{}, false
}
func (g *invalidationRecorder) TrackedRowCount() uint64 { return 0 }
func (g *invalidationRecorder) Len() uint64             { return 10 }
func (g *invalidationRecorder) Close()                  {}

var _ partitions.Interface = (*invalidationRecorder)(nil)

// mutationErrStore returns a fixed error from Mutate.
type mutationErrStore struct {
	mockStore

	err error
}

func (m *mutationErrStore) Mutate(context.Context, *typedef.Stmt) error { return m.err }

// TestMutationRun_ChargesOnlyTheTestCluster pins two decisions of
// Mutation.run. They are independent of each other:
//
//   - Invalidation. A write that can leave the clusters with different data
//     marks its partitions invalid, whatever error ended it.
//   - Error accounting. Only an error of the test cluster is charged to the
//     system under test. An oracle failure is a fault of the reference
//     cluster, and a test-cluster timeout is an infrastructure signal.
//
// FinalError holds both errors joined. A decision that errors.Is takes from
// FinalError fails the "test hard error, oracle timeout" case.
func TestMutationRun_ChargesOnlyTheTestCluster(t *testing.T) {
	t.Parallel()

	hard := errors.New("Cannot achieve consistency level QUORUM")
	oracleHard := errors.New("oracle: Unavailable")

	for _, tc := range []struct {
		testErr         error
		oracleErr       error
		name            string
		wantInvalidated bool
		wantJobError    bool
	}{
		{name: "test ok, oracle hard error", oracleErr: oracleHard, wantInvalidated: true},
		{name: "test ok, oracle timeout", oracleErr: context.DeadlineExceeded, wantInvalidated: true},
		{name: "test hard error, oracle ok", testErr: hard, wantInvalidated: true, wantJobError: true},
		{
			name:            "test hard error, oracle timeout",
			testErr:         hard,
			oracleErr:       context.DeadlineExceeded,
			wantInvalidated: true,
			wantJobError:    true,
		},
		{name: "test timeout, oracle hard error", testErr: context.DeadlineExceeded, oracleErr: oracleHard, wantInvalidated: true},
		{name: "both hard errors", testErr: hard, oracleErr: oracleHard, wantJobError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gen := &invalidationRecorder{}

			table := &typedef.Table{
				Name:          "t",
				PartitionKeys: typedef.Columns{{Name: "pk1", Type: typedef.TypeInt}},
				Columns:       typedef.Columns{{Name: "c1", Type: typedef.TypeInt}},
			}

			controller, err := statements.NewRatioController(
				statements.DefaultStatementRatios(),
				rand.New(rand.NewChaCha8([32]byte{})),
			)
			require.NoError(t, err)

			vrc := typedef.ValueRangeConfig{}

			mutErr := &store.MutationError{
				TestErr:            tc.testErr,
				OracleErr:          tc.oracleErr,
				TestStoreSuccess:   tc.testErr == nil,
				OracleStoreSuccess: tc.oracleErr == nil,
			}
			mutErr.Finalize(errors.Join(tc.testErr, tc.oracleErr))

			m := &Mutation{
				table:     table,
				schema:    &typedef.Schema{Keyspace: typedef.Keyspace{Name: "ks"}},
				logger:    zap.NewNop(),
				status:    status.NewGlobalStatus(10),
				generator: gen,
				store:     &mutationErrStore{err: mutErr},
				statement: statements.New(
					"ks", gen, table,
					rand.New(rand.NewChaCha8([32]byte{})),
					&vrc, controller, false,
				),
			}

			runErr := m.run(t.Context())

			assert.Equal(t, tc.wantInvalidated, gen.count() > 0,
				"a write that can leave the clusters different must invalidate its partitions")

			jobErr, isJobErr := errors.AsType[*joberror.JobError](runErr)
			assert.Equal(t, tc.wantJobError, isJobErr,
				"only an error of the test cluster is charged to the system under test")
			if isJobErr {
				assert.ErrorIs(t, jobErr.Err, tc.testErr)
			}
		})
	}
}

// TestMutationRun_UnknownOutcomeAndCancelInvalidate pins two more reasons to
// distrust a partition: a write whose outcome is unknown and cannot be undone
// (a counter increment that timed out), and a write the shutdown interrupted
// after one cluster took it. Neither is charged to the error budget.
func TestMutationRun_UnknownOutcomeAndCancelInvalidate(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		build      func() *store.MutationError
		wantReturn error
		name       string
	}{
		{
			name: "counter increment with unknown outcome",
			build: func() *store.MutationError {
				mutErr := &store.MutationError{OutcomeUnknown: true}
				mutErr.Finalize(context.DeadlineExceeded)
				return mutErr
			},
		},
		{
			name: "asymmetric write interrupted by shutdown",
			build: func() *store.MutationError {
				mutErr := &store.MutationError{TestStoreSuccess: true}
				mutErr.Finalize(context.Canceled)
				return mutErr
			},
			wantReturn: context.Canceled,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gen := &invalidationRecorder{}
			table := &typedef.Table{
				Name:          "t",
				PartitionKeys: typedef.Columns{{Name: "pk1", Type: typedef.TypeInt}},
				Columns:       typedef.Columns{{Name: "c1", Type: typedef.TypeInt}},
			}

			controller, err := statements.NewRatioController(
				statements.DefaultStatementRatios(),
				rand.New(rand.NewChaCha8([32]byte{})),
			)
			require.NoError(t, err)

			vrc := typedef.ValueRangeConfig{}
			m := &Mutation{
				table:     table,
				schema:    &typedef.Schema{Keyspace: typedef.Keyspace{Name: "ks"}},
				logger:    zap.NewNop(),
				status:    status.NewGlobalStatus(10),
				generator: gen,
				store:     &mutationErrStore{err: tc.build()},
				statement: statements.New(
					"ks", gen, table,
					rand.New(rand.NewChaCha8([32]byte{})),
					&vrc, controller, false,
				),
			}

			runErr := m.run(t.Context())

			assert.Positive(t, gen.count(), "the partition must be invalidated")

			var jobErr *joberror.JobError
			assert.NotErrorAs(t, runErr, &jobErr, "neither case is a data error")

			if tc.wantReturn != nil {
				assert.ErrorIs(t, runErr, tc.wantReturn)
			}
		})
	}
}
