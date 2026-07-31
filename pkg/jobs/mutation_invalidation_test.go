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

	"github.com/google/uuid"
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
func (g *invalidationRecorder) ValidationStats(_ uuid.UUID) (uint64, uint64, uint64, []uint64, uint64) {
	return 0, 0, 0, nil, 0
}
func (g *invalidationRecorder) IsInvalid(_ uint64) bool          { return false }
func (g *invalidationRecorder) InvalidCount() uint64             { return 0 }
func (g *invalidationRecorder) TrackRow(_ partitions.TrackedRow) {}
func (g *invalidationRecorder) RowTrackerFillRatio() float64     { return 0 }
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

// TestMutationRun_AsymmetricWriteInvalidatesRegardlessOfErrorKind pins that the
// partition-invalidation decision does not depend on HOW the write failed.
//
// The regression: the invalidation used to live inside an
// `errors.Is(err, context.DeadlineExceeded)` branch. An asymmetric write that
// failed for any other reason — test store commits, oracle returns Unavailable,
// say — skipped it entirely, was converted straight to a JobError, and left the
// partition in validation coverage. The two clusters genuinely differ at that
// point, so validation later reports a divergence gemini itself produced.
//
// A timeout is not special here: what makes the partition untrustworthy is the
// asymmetry, not the error kind.
func TestMutationRun_AsymmetricWriteInvalidatesRegardlessOfErrorKind(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		err  error
		name string
		// run() reports a data error by returning a *joberror.JobError, which Do()
		// then charges against the error budget. Timeouts are deliberately exempt
		// (infrastructure signal: a slow CI runner would otherwise exhaust the
		// budget) and yield a nil error instead. Invalidation and error accounting
		// are independent decisions, and hoisting the former must not disturb the
		// latter.
		wantJobError bool
	}{
		{
			name:         "non-timeout error",
			err:          errors.New("Cannot achieve consistency level QUORUM"),
			wantJobError: true,
		},
		{
			name:         "timeout error",
			err:          context.DeadlineExceeded,
			wantJobError: false,
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

			// Asymmetric: the test store committed, the oracle did not.
			mutErr := &store.MutationError{
				TestStoreSuccess:   true,
				OracleStoreSuccess: false,
			}
			mutErr.Finalize(tc.err)

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

			assert.Positive(t, gen.count(),
				"an asymmetric write must invalidate its partitions no matter which error ended it")

			var jobErr *joberror.JobError
			assert.Equal(t, tc.wantJobError, errors.As(runErr, &jobErr),
				"hoisting the invalidation must not change which errors are charged to the budget")
		})
	}
}
