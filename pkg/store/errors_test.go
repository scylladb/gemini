// Copyright 2025 ScyllaDB
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
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/typedef"
)

func TestValidationError_Error_FinalOnly(t *testing.T) {
	stmt := &typedef.Stmt{Query: "SELECT * FROM ks.tbl WHERE pk0=?"}

	ve := NewValidationError("Validation failed", stmt)
	ve.StartTime = time.Now().Add(-500 * time.Millisecond)

	// Add multiple attempts
	ve.AddAttempt(0, TypeTest, errors.New("temporary network failure"), 5*time.Millisecond)
	ve.AddAttempt(1, TypeOracle, errors.New("rows differ: - col1: a + col1: b"), 7*time.Millisecond)

	lastErr := errors.New("final difference")
	ve.Finalize(lastErr)

	msg := ve.Error()

	assert.Equal(t, "final difference", msg)
	assert.Equal(t, 2, ve.TotalAttempts)
	assert.ErrorIs(t, ve, lastErr)
}

func TestErrorRowDifference_Error_CountsOnly(t *testing.T) {
	t.Parallel()
	e := ErrorRowDifference{TestRows: 5, OracleRows: 3}
	msg := e.Error()
	assert.Contains(t, msg, "row count differ (test store rows 5, oracle store rows 3")
}

func TestErrorRowDifference_Error_WithMissingLists(t *testing.T) {
	t.Parallel()
	e := ErrorRowDifference{
		MissingInTest:   []string{"r1"},
		MissingInOracle: []string{"r2"},
		TestRows:        1,
		OracleRows:      1,
	}
	msg := e.Error()
	assert.Contains(t, msg, "missing_in_test=[r1]")
	assert.Contains(t, msg, "missing_in_oracle=[r2]")
}

func TestErrorRowDifference_Error_WithDiff(t *testing.T) {
	t.Parallel()
	diff := "pk: pk0=a\n- col1: 1\n+ col1: 2\n"
	e := ErrorRowDifference{Diff: diff}
	assert.Equal(t, diff, e.Error())
}

func TestValidationError_Utilities(t *testing.T) {
	t.Parallel()

	stmt := &typedef.Stmt{Query: "SELECT x"}

	ve := NewValidationError("validation", stmt)
	e1 := errors.New("e1")

	// Add attempts on both stores
	ve.AddAttempt(0, TypeTest, e1, 10*time.Millisecond)

	// Finalize and Error/Unwrap/Is
	final := errors.New("final")
	ve.Finalize(final)
	msg := ve.Error()
	assert.Equal(t, final.Error(), msg)
	assert.ErrorIs(t, ve, final) // Unwrap
	assert.False(t, errors.Is(ve, e1))
}

func TestValidationError_MarshalJSON_SanitizesAttemptsAndTable(t *testing.T) {
	t.Parallel()

	stmt := &typedef.Stmt{Query: "SELECT * FROM ks.tbl WHERE pk0=?"}

	ve := NewValidationError("Validation failed", stmt)
	ve.AddAttempt(0, TypeTest, errors.New("temporary network failure"), 5*time.Millisecond)
	ve.Finalize(errors.New("final difference"))

	data, err := json.Marshal(ve)
	require.NoError(t, err)
	jsonStr := string(data)

	assert.Contains(t, jsonStr, `"final_error":"final difference"`)
	assert.Contains(t, jsonStr, `"statement"`)
}

func TestMutationError_Utilities(t *testing.T) {
	t.Parallel()

	stmt := &typedef.Stmt{Query: "UPDATE t SET v=1 WHERE k=1"}
	me := NewStoreMutationError(stmt, nil)

	// Set store success flags and verify Error report suffix
	me.SetStoreSuccess(TypeTest, true)
	me.SetStoreSuccess(TypeOracle, false)
	me.Finalize(errors.New("x"))

	msg := me.Error()
	assert.Contains(t, msg, "Store status: test=true, oracle=false")

	// Is/Unwrap
	assert.ErrorIs(t, me, me.FinalError)
}
