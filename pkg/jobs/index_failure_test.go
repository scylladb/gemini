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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/typedef"
)

// TestHandleCheckFailureReportsIndexQuery: an index query names no partition,
// so no partition can be marked invalid. Its failure must still be reported,
// or a SUT bug on an indexed read is never seen.
func TestHandleCheckFailureReportsIndexQuery(t *testing.T) {
	t.Parallel()

	gen := &invalidationRecorder{}
	v := &Validation{
		table:     &typedef.Table{Name: "t"},
		generator: gen,
	}

	stmt := &typedef.Stmt{
		Query:     "SELECT * FROM ks.t WHERE c0=? ALLOW FILTERING",
		Values:    []any{1},
		QueryType: typedef.SelectByIndexStatementType,
	}

	err := v.handleCheckFailure(errors.New("rows differ"), stmt)

	var jobErr *joberror.JobError
	require.ErrorAs(t, err, &jobErr, "the divergence must be reported")
	assert.Equal(t, typedef.SelectByIndexStatementType, jobErr.StmtType)
	assert.Zero(t, gen.count(), "there is no partition to mark")
}
