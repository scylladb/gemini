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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/typedef"
)

type loadStep = struct {
	err  error
	rows Rows
}

func checkStore(test, oracle *fakeStore) *delegatingStore {
	return &delegatingStore{
		testStore:            test,
		oracleStore:          oracle,
		validationRetries:    3,
		validationRetrySleep: time.Millisecond,
		inflight:             new(sync.WaitGroup),
	}
}

// TestCheck_DecidesFromAllAttempts pins that the last attempt alone does not
// decide the result of Check. A divergence that an earlier attempt saw
// survives a later timeout. A run of read failures is not a divergence, and a
// read failure of the oracle alone is told apart from one of the test cluster.
func TestCheck_DecidesFromAllAttempts(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name:          "t",
		PartitionKeys: typedef.Columns{{Name: "pk0", Type: typedef.TypeInt}},
	}
	stmt := typedef.SimpleStmt("SELECT * FROM ks.t WHERE pk0=1", typedef.SelectStatementType)
	rowA := Rows{NewRow([]string{"pk0", "v"}, []any{1, "a"})}
	rowB := Rows{NewRow([]string{"pk0", "v"}, []any{1, "b"})}

	t.Run("divergence then timeouts", func(t *testing.T) {
		t.Parallel()

		test := &fakeStore{nameStr: "test", loadSeq: []loadStep{
			{rows: rowA},
			{err: context.DeadlineExceeded},
		}}
		oracle := &fakeStore{nameStr: "oracle", loadRows: rowB}

		_, _, err := checkStore(test, oracle).Check(t.Context(), table, stmt, 0)

		require.True(t, IsDivergence(err), "a divergence of an earlier attempt must be reported")
		require.NotErrorIs(t, err, context.DeadlineExceeded)
		ve, ok := errors.AsType[*ValidationError](err)
		require.True(t, ok)
		require.Equal(t, uint64(3), ve.TotalAttempts.Load())
	})

	t.Run("test cluster read errors only", func(t *testing.T) {
		t.Parallel()

		test := &fakeStore{nameStr: "test", loadErr: context.DeadlineExceeded}
		oracle := &fakeStore{nameStr: "oracle", loadRows: rowA}

		_, _, err := checkStore(test, oracle).Check(t.Context(), table, stmt, 0)

		require.False(t, IsDivergence(err))
		require.False(t, IsOracleLoadError(err))
		ve, ok := errors.AsType[*ValidationError](err)
		require.True(t, ok)
		require.ErrorIs(t, ve.TestErr, context.DeadlineExceeded)
	})

	t.Run("oracle read errors only", func(t *testing.T) {
		t.Parallel()

		test := &fakeStore{nameStr: "test", loadRows: rowA}
		oracle := &fakeStore{nameStr: "oracle", loadErr: errors.New("oracle: Unavailable")}

		_, _, err := checkStore(test, oracle).Check(t.Context(), table, stmt, 0)

		require.False(t, IsDivergence(err))
		require.True(t, IsOracleLoadError(err), "a failure of the oracle alone must be told apart")
	})

	t.Run("test read error beats a later oracle read error", func(t *testing.T) {
		t.Parallel()

		test := &fakeStore{nameStr: "test", loadSeq: []loadStep{
			{err: context.DeadlineExceeded},
			{rows: rowA},
		}}
		oracle := &fakeStore{nameStr: "oracle", loadErr: errors.New("oracle: Unavailable")}

		_, _, err := checkStore(test, oracle).Check(t.Context(), table, stmt, 0)

		require.False(t, IsOracleLoadError(err), "the test cluster failed on an attempt")
	})
}

// TestMutate_KeepsTheErrorOfEachCluster pins that a failed mutation carries
// the error of each cluster, and that FinalError comes from the test cluster.
// With a joined error, a timeout of the oracle hides a hard error of the test
// cluster from errors.Is(err, context.DeadlineExceeded).
func TestMutate_KeepsTheErrorOfEachCluster(t *testing.T) {
	t.Parallel()

	hard := errors.New("test: InvalidRequest")
	oracleHard := errors.New("oracle: Unavailable")
	stmt := typedef.SimpleStmt("INSERT INTO ks.t (pk0) VALUES (1)", typedef.InsertStatementType)

	for _, tc := range []struct {
		testErr, oracleErr error
		name               string
	}{
		{name: "test hard error, oracle timeout", testErr: hard, oracleErr: context.DeadlineExceeded},
		{name: "test ok, oracle hard error", oracleErr: oracleHard},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			test := &fakeStore{nameStr: "test", mutateSeq: []error{tc.testErr}}
			oracle := &fakeStore{nameStr: "oracle", mutateSeq: []error{tc.oracleErr}}
			ds := &delegatingStore{
				testStore:            test,
				oracleStore:          oracle,
				inflight:             new(sync.WaitGroup),
				serverSideTimestamps: true,
			}

			err := ds.Mutate(t.Context(), stmt)

			me, ok := errors.AsType[*MutationError](err)
			require.True(t, ok)
			require.Equal(t, tc.testErr, me.TestErr)
			require.Equal(t, tc.oracleErr, me.OracleErr)
			require.Equal(t, tc.testErr == nil, me.TestStoreSuccess)
			if tc.testErr != nil {
				require.NotErrorIs(t, err, context.DeadlineExceeded,
					"an oracle timeout must not hide the hard error of the test cluster")
				require.ErrorIs(t, err, tc.testErr)
			}
			require.Contains(t, err.Error(), tc.oracleErr.Error(), "the message names the oracle error")
		})
	}
}
