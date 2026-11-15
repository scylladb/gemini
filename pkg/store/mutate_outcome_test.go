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

package store

import (
	"context"
	"encoding/json/v2"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/typedef"
)

// writeStore fails every write with writeErr and accepts every compensating
// partition DELETE. It counts both.
type writeStore struct {
	fakeStore

	writeErr error
	writes   atomic.Int64
	deletes  atomic.Int64
}

func (w *writeStore) mutate(_ context.Context, stmt *typedef.Stmt, _ mo.Option[time.Time]) error {
	if stmt.QueryType == typedef.DeleteWholePartitionType {
		w.deletes.Add(1)
		return nil
	}

	w.writes.Add(1)

	return w.writeErr
}

func outcomeStmt(nonIdempotent bool) *typedef.Stmt {
	return &typedef.Stmt{
		PartitionKeys: []typedef.PartitionKeys{{Values: typedef.NewValuesFromMap(map[string][]any{"pk": {1}})}},
		Query:         "UPDATE ks.t SET c=c+1 WHERE pk=?",
		QueryType:     typedef.UpdateStatementType,
		NonIdempotent: nonIdempotent,
	}
}

func outcomeDS(test, oracle storeLoader) *delegatingStore {
	return &delegatingStore{
		inflight:            new(sync.WaitGroup),
		testStore:           test,
		oracleStore:         oracle,
		logger:              zap.NewNop(),
		partitionKeyColumns: typedef.Columns{{Name: "pk", Type: typedef.TypeInt}},
		keyspaceAndTable:    "ks.t",
		mutationRetries:     3,
		minimumDelay:        time.Millisecond,
		mutationRetrySleep:  time.Millisecond,
	}
}

// TestMutateDoesNotResendNonIdempotentWrite: a counter increment that a
// cluster may already have applied must not be sent to it again, or the
// cluster adds twice. Its partition must be invalidated instead, because a
// DELETE cannot undo a counter.
func TestMutateDoesNotResendNonIdempotentWrite(t *testing.T) {
	t.Parallel()

	test := &writeStore{writeErr: gocql.ErrTimeoutNoResponse}
	oracle := &writeStore{}

	err := outcomeDS(test, oracle).Mutate(t.Context(), outcomeStmt(true))

	var mutErr *MutationError
	require.ErrorAs(t, err, &mutErr)
	assert.Equal(t, int64(1), test.writes.Load(), "the timed-out counter write must not be sent again")
	assert.Equal(t, int64(1), oracle.writes.Load())
	assert.True(t, mutErr.OutcomeUnknown, "the partition must be marked for invalidation")
	assert.Zero(t, test.deletes.Load()+oracle.deletes.Load(), "a counter cannot be compensated with a DELETE")
}

// TestMutateRetriesIdempotentWrite: a plain write is safe to send again,
// because client timestamps make a second apply a no-op.
func TestMutateRetriesIdempotentWrite(t *testing.T) {
	t.Parallel()

	test := &writeStore{writeErr: gocql.ErrTimeoutNoResponse}
	oracle := &writeStore{}

	_ = outcomeDS(test, oracle).Mutate(t.Context(), outcomeStmt(false))

	assert.Equal(t, int64(4), test.writes.Load(), "an idempotent write is retried up to the limit")
}

// TestMutateCompensatesMixedFailure: when one cluster fails hard and the other
// times out, the timed-out cluster may have applied the write. The partition
// must be made equal on both clusters, and the hard error of the test cluster
// is still charged to the system under test.
func TestMutateCompensatesMixedFailure(t *testing.T) {
	t.Parallel()

	test := &writeStore{writeErr: errors.New("syntax error")}
	oracle := &writeStore{writeErr: context.DeadlineExceeded}

	err := outcomeDS(test, oracle).Mutate(t.Context(), outcomeStmt(false))

	assert.Equal(t, int64(1), test.deletes.Load(), "the compensating DELETE must run on the test cluster")
	assert.Equal(t, int64(1), oracle.deletes.Load(), "the compensating DELETE must run on the oracle")

	mutErr, ok := errors.AsType[*MutationError](err)
	require.True(t, ok, "the hard error of the test cluster must still be returned")
	assert.True(t, mutErr.Compensated, "the partition is equal again and stays valid")
	assert.ErrorIs(t, err, test.writeErr)
}

// TestMutateCompensatedTimeoutsReturnNil: when every failure may have applied
// the write, a successful compensation leaves nothing to charge.
func TestMutateCompensatedTimeoutsReturnNil(t *testing.T) {
	t.Parallel()

	test := &writeStore{writeErr: context.DeadlineExceeded}
	oracle := &writeStore{}

	err := outcomeDS(test, oracle).Mutate(t.Context(), outcomeStmt(false))

	require.NoError(t, err, "a successful compensation leaves the partition equal and valid")
	assert.Equal(t, int64(1), test.deletes.Load())
}

// TestMutateNoCompensationForDefiniteFailure: an error the server returns before
// it runs the write means the write was not applied, so nothing is compensated.
func TestMutateNoCompensationForDefiniteFailure(t *testing.T) {
	t.Parallel()

	test := &writeStore{writeErr: &gocql.RequestErrUnavailable{}}
	oracle := &writeStore{}

	err := outcomeDS(test, oracle).Mutate(t.Context(), outcomeStmt(false))

	var mutErr *MutationError
	require.ErrorAs(t, err, &mutErr)
	assert.Zero(t, test.deletes.Load()+oracle.deletes.Load())
	assert.False(t, mutErr.TestStoreSuccess)
	assert.True(t, mutErr.OracleStoreSuccess, "the asymmetric flags invalidate the partition")
}

// TestMutationErrorJSONKeepsFlags: the report must show whether a write left
// the clusters asymmetric.
func TestMutationErrorJSONKeepsFlags(t *testing.T) {
	t.Parallel()

	mutErr := NewStoreMutationError(outcomeStmt(true))
	mutErr.SetStoreSuccess(TypeOracle, true)
	mutErr.CompensationFailed = true
	mutErr.OutcomeUnknown = true
	mutErr.OracleErr = errors.New("oracle timeout")
	mutErr.Finalize(errors.New("boom"))

	data, err := json.Marshal(mutErr)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, true, decoded["oracle_store_success"])
	assert.Equal(t, false, decoded["test_store_success"])
	assert.Equal(t, true, decoded["compensation_failed"])
	assert.Equal(t, true, decoded["outcome_unknown"])
	assert.Equal(t, "boom", decoded["final_error"])
	assert.Equal(t, "oracle timeout", decoded["oracle_error"])
}
