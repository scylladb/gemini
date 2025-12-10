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
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/typedef"
)

func TestValidationError_Error_OnlyLastAttempt(t *testing.T) {
	stmt := &typedef.Stmt{Query: "SELECT * FROM ks.tbl WHERE pk0=?"}
	table := &typedef.Table{Name: "tbl"}

	ve := NewValidationError("Validation failed", stmt, table)
	ve.StartTime = time.Now().Add(-500 * time.Millisecond)

	// Add multiple attempts
	ve.AddAttempt(0, TypeTest, errors.New("temporary network failure"), 5*time.Millisecond)
	ve.AddAttempt(1, TypeOracle, errors.New("rows differ: - col1: a + col1: b"), 7*time.Millisecond)

	lastErr := errors.New("final difference")
	ve.Finalize(lastErr)

	msg := ve.Error()

	// Basic summary
	if !regexp.MustCompile(`Validation failed failed after 2 attempts \(took`).MatchString(msg) {
		t.Fatalf("expected summary with attempts count, got:\n%s", msg)
	}
	if !regexp.MustCompile(`on table tbl`).MatchString(msg) {
		t.Fatalf("expected table name in error, got:\n%s", msg)
	}
	if !regexp.MustCompile(`with query: SELECT \* FROM ks.tbl WHERE pk0=\?`).MatchString(msg) {
		t.Fatalf("expected query in error, got:\n%s", msg)
	}

	// Only last attempt should be present
	if !regexp.MustCompile(`Last attempt:\n\s+attempt 1 \[oracle \(took`).MatchString(msg) {
		t.Fatalf("expected only last attempt details, got:\n%s", msg)
	}
	if regexp.MustCompile(`attempt 0 \[test`).MatchString(msg) {
		t.Fatalf("should not list earlier attempts, got:\n%s", msg)
	}

	// Final error included
	if !regexp.MustCompile(`Final error: final difference`).MatchString(msg) {
		t.Fatalf("expected final error message, got:\n%s", msg)
	}
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
	assert.Contains(t, msg, "test is missing rows: [r1]")
	assert.Contains(t, msg, "oracle is missing rows: [r2]")
}

func TestValidationError_Utilities(t *testing.T) {
	t.Parallel()

	stmt := &typedef.Stmt{Query: "SELECT x"}
	table := &typedef.Table{Name: "t"}

	ve := NewValidationError("validation", stmt, table)
	e1 := errors.New("e1")
	e2 := errors.New("e2")

	// Add attempts on both stores
	ve.AddAttempt(0, TypeTest, e1, 10*time.Millisecond)
	ve.AddAttempt(1, TypeOracle, e2, 5*time.Millisecond)

	// GetAttemptsByStore
	ts := ve.GetAttemptsByStore(TypeTest)
	os := ve.GetAttemptsByStore(TypeOracle)
	require.Len(t, ts, 1)
	require.Len(t, os, 1)
	assert.Equal(t, e1, ts[0].Error)
	assert.Equal(t, e2, os[0].Error)

	// GetLastAttempt
	la := ve.GetLastAttempt()
	require.NotNil(t, la)
	assert.Equal(t, 1, la.Attempt)
	assert.Equal(t, TypeOracle, la.Store)

	// GetLastAttemptByStore
	laTest := ve.GetLastAttemptByStore(TypeTest)
	require.NotNil(t, laTest)
	assert.Equal(t, 0, laTest.Attempt)
	laOracle := ve.GetLastAttemptByStore(TypeOracle)
	require.NotNil(t, laOracle)
	assert.Equal(t, 1, laOracle.Attempt)

	// HasStore
	assert.True(t, ve.HasStore(TypeTest))
	assert.True(t, ve.HasStore(TypeOracle))

	// Finalize and Error/Unwrap/Is
	final := errors.New("final")
	ve.Finalize(final)
	msg := ve.Error()
	assert.Contains(t, msg, "failed after 2 attempts")
	assert.ErrorIs(t, ve, final) // Unwrap
	assert.ErrorIs(t, ve, e1)    // Is matches attempt error
	assert.ErrorIs(t, ve, e2)    // Is matches attempt error
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
	e1 := errors.New("attempt-1")
	e2 := errors.New("attempt-2")
	me.AddAttempt(0, TypeTest, e1, 1*time.Millisecond)
	me.AddAttempt(1, TypeOracle, e2, 1*time.Millisecond)
	assert.ErrorIs(t, me, e1)
	assert.ErrorIs(t, me, e2)
	assert.ErrorIs(t, me, me.FinalError)
}
