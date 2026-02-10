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

package stmtlogger

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/typedef"
)

// simpleCloser is a trivial io.Closer implementation returning nil
// Used to satisfy Logger's requirement for a non-nil closer so it enqueues items.
type simpleCloser struct{}

func (simpleCloser) Close() error { return nil }

func TestLogger_LogStmt_SelectWithValidation_Enqueues(t *testing.T) {
	t.Parallel()

	ch := make(chan Item, 1)
	l, err := NewLogger(WithChannel(ch), WithLogger(simpleCloser{}, nil))
	require.NoError(t, err)

	item := Item{
		StatementType:  typedef.SelectStatementType,
		Statement:      "SELECT * FROM ks.t WHERE pk = ?",
		Duration:       Duration{Duration: time.Millisecond},
		FirstSuccessNS: 1,
	}

	require.NoError(t, l.LogStmt(item))

	select {
	case got := <-ch:
		require.Equal(t, item.StatementType, got.StatementType)
		require.Equal(t, item.FirstSuccessNS, got.FirstSuccessNS)
	default:
		t.Fatal("expected item to be enqueued for select with validation map")
	}

	require.NoError(t, l.Close())
}
