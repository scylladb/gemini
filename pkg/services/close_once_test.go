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

package services

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
)

// closeCountingStore counts Close calls. The other methods are never called.
type closeCountingStore struct {
	store.Store

	err    error
	closes int
}

func (c *closeCountingStore) Close() error {
	c.closes++
	return c.err
}

// TestWorkloadCloseRunsOnce: run closes the workload before it prints the
// summary, so the statement files are complete, and a deferred Close covers the
// early returns. The second call must not close the store again.
func TestWorkloadCloseRunsOnce(t *testing.T) {
	t.Parallel()

	st := &closeCountingStore{err: errors.New("close failed")}
	w := &Workload{
		logger:   zap.NewNop(),
		status:   status.NewGlobalStatus(10),
		stopFlag: stop.NewFlag(t.Name()),
		store:    st,
	}

	first := w.Close()
	second := w.Close()

	assert.Equal(t, 1, st.closes, "the store must close once")
	require.ErrorIs(t, first, st.err)
	require.ErrorIs(t, second, st.err, "every call returns the error of the first")
}
