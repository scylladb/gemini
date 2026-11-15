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
	"math/rand/v2"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/statements"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
)

// idRecordingStore records the partition IDs of every mutation and every
// validation.
type idRecordingStore struct {
	mockStore

	written   map[uuid.UUID]struct{}
	validated map[uuid.UUID]struct{}
	mu        sync.Mutex
}

func newIDRecordingStore() *idRecordingStore {
	return &idRecordingStore{
		written:   make(map[uuid.UUID]struct{}),
		validated: make(map[uuid.UUID]struct{}),
	}
}

func (s *idRecordingStore) record(into map[uuid.UUID]struct{}, stmt *typedef.Stmt) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range stmt.PartitionKeys {
		into[stmt.PartitionKeys[i].ID] = struct{}{}
	}
}

func (s *idRecordingStore) Mutate(_ context.Context, stmt *typedef.Stmt) error {
	s.record(s.written, stmt)
	// Keep the worker from spinning a core for the whole test.
	time.Sleep(time.Millisecond)
	return nil
}

func (s *idRecordingStore) CheckOnce(_ context.Context, _ *typedef.Table, stmt *typedef.Stmt, _ int) (int, store.Rows, error) {
	s.record(s.validated, stmt)
	time.Sleep(time.Millisecond)
	return 0, nil, nil
}

func poolTestSchema() *typedef.Schema {
	return &typedef.Schema{
		Keyspace: typedef.Keyspace{Name: "ks"},
		Tables: []*typedef.Table{{
			Name:          "t",
			PartitionKeys: typedef.Columns{{Name: "pk1", Type: typedef.TypeInt}},
			Columns:       typedef.Columns{{Name: "c1", Type: typedef.TypeInt}},
		}},
	}
}

func newPoolTestJobs(t *testing.T, st store.Store, schema *typedef.Schema) (*Jobs, distributions.DistributionFunc) {
	t.Helper()

	src, idxFunc := distributions.New(distributions.Uniform, 8, 1, 0, 0)
	ratios := statements.DefaultStatementRatios()
	controller, err := statements.NewRatioController(ratios, rand.New(rand.NewChaCha8([32]byte{})))
	require.NoError(t, err)

	return New(1, 1, schema, st, status.NewGlobalStatus(100), controller, zap.NewNop(), src, ratios, nil,
		distributions.NewFactory(distributions.Uniform, 8, 0, 0)), idxFunc
}

// TestInvalidCap_IsHalfThePoolNotTheErrorBudget pins that the pool has an
// invalid-partition cap of its own. Write-side invalidations are not errors of
// the system under test, so the error budget must not limit them.
func TestInvalidCap_IsHalfThePoolNotTheErrorBudget(t *testing.T) {
	t.Parallel()

	require.Equal(t, uint64(500), invalidCap(1000))
	require.Equal(t, uint64(1), invalidCap(1), "zero means no cap, so the floor keeps it above zero")
	require.Equal(t, uint64(1), invalidCap(0))

	schema := poolTestSchema()
	j, idxFunc := newPoolTestJobs(t, newIDRecordingStore(), schema)
	pools := j.NewPools(t.Context(), idxFunc, 10, schema.Config.GetPartitionRangeConfig())
	t.Cleanup(pools.Close)

	pool := pools[0]
	for i := range pool.Len() {
		keys := pool.Get(i)
		pool.MarkInvalid(&keys)
		keys.Release()
	}

	require.Equal(t, uint64(5), pool.InvalidCount(), "the cap is half the pool")
}

// TestRun_ReusesPoolsAcrossPhases pins that the main phase validates the
// partitions that the warmup wrote. A Run that builds a pool of its own from
// fresh seeds validates none of them by key.
func TestRun_ReusesPoolsAcrossPhases(t *testing.T) {
	t.Parallel()

	schema := poolTestSchema()
	st := newIDRecordingStore()
	j, idxFunc := newPoolTestJobs(t, st, schema)

	pools := j.NewPools(t.Context(), idxFunc, 8, schema.Config.GetPartitionRangeConfig())
	t.Cleanup(pools.Close)

	require.NoError(t, j.Run(t.Context(), 200*time.Millisecond, pools, stop.NewFlag("warmup"), WarmupMode))
	require.NoError(t, j.Run(t.Context(), 200*time.Millisecond, pools, stop.NewFlag("read"), ReadMode))

	st.mu.Lock()
	defer st.mu.Unlock()
	require.NotEmpty(t, st.written)
	require.NotEmpty(t, st.validated)
	for id := range st.validated {
		require.Contains(t, st.written, id, "the main phase validates a partition that the warmup wrote")
	}
}

// TestRun_RejectsPoolsOfAnotherSchema pins that Run checks that the pools
// match the tables of its schema.
func TestRun_RejectsPoolsOfAnotherSchema(t *testing.T) {
	t.Parallel()

	j, _ := newPoolTestJobs(t, newIDRecordingStore(), poolTestSchema())
	require.Error(t, j.Run(t.Context(), time.Millisecond, nil, stop.NewFlag("x"), ReadMode))
}
