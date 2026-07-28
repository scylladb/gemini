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
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/statements"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/typedef"
)

// noCandidateRatios is the one production configuration that can starve a
// mutation worker: DeleteRatio 1.0 passes validation (the three only have to
// sum to 1.0 -- there is no per-type floor), and a warmup / no-delete worker
// filters DELETE, leaving every surviving type at zero weight.
func noCandidateRatios() statements.Ratios {
	r := statements.DefaultStatementRatios()
	r.MutationRatios.InsertRatio = 0.0
	r.MutationRatios.UpdateRatio = 0.0
	r.MutationRatios.DeleteRatio = 1.0

	return r
}

// TestMutationDo_NoCandidates_StopsInsteadOfSpinning pins the terminal handling
// of statements.ErrNoMutationCandidates in Mutation.Do.
//
// The regression this guards against is subtle: Do's loop `continue`s on any
// error it does not specifically recognise. ErrNoMutationCandidates is
// permanent -- the ratios and the worker's filter are both fixed for the life
// of the run, so no retry can ever succeed -- which means falling through would
// spin the worker at full speed forever, burning a core and emitting nothing.
// Do must instead stop the run and surface the misconfiguration.
func TestMutationDo_NoCandidates_StopsInsteadOfSpinning(t *testing.T) {
	t.Parallel()

	controller, err := statements.NewRatioController(
		noCandidateRatios(),
		rand.New(rand.NewChaCha8([32]byte{})),
	)
	require.NoError(t, err)

	table := &typedef.Table{Name: "no_candidates"}
	vrc := typedef.ValueRangeConfig{}

	m := &Mutation{
		table:  table,
		logger: zap.NewNop(),
		// delete == false is the warmup / no-delete worker: it filters
		// StatementTypeDelete, which is the only type carrying any weight here.
		delete:   false,
		stopFlag: stop.NewFlag(t.Name()),
		// Populated so that a regression which drops the terminal branch falls
		// through into the real hot-spin loop and trips the timeout below with a
		// useful message, rather than crashing on a nil status and burying the
		// actual diagnosis under a stack trace.
		status: status.NewGlobalStatus(10),
		statement: statements.New(
			"ks",
			nil, // never reached: the ratio check fails before any partition access
			table,
			rand.New(rand.NewChaCha8([32]byte{})),
			&vrc,
			controller,
			false,
		),
	}

	// Guard against the hot-spin regression itself: if Do ever goes back to
	// continue-ing on this error it will not return, and the test must fail with
	// a clear diagnosis rather than hanging until the whole package times out.
	done := make(chan error, 1)
	go func() { done <- m.Do(t.Context()) }()

	select {
	case doErr := <-done:
		require.ErrorIs(t, doErr, statements.ErrNoMutationCandidates,
			"Do must surface the misconfiguration rather than swallowing it")
	case <-time.After(10 * time.Second):
		t.Fatal("Mutation.Do did not return: it is spinning on ErrNoMutationCandidates instead of stopping")
	}

	assert.True(t, m.stopFlag.IsHardOrSoft(),
		"Do must set the stop flag so sibling workers wind down too")
}
