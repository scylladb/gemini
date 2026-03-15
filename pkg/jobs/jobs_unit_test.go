// Copyright 2025 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jobs

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
)

// ---------------------------------------------------------------------------
// collectPartitionIDs
// ---------------------------------------------------------------------------

func TestCollectPartitionIDs_Empty(t *testing.T) {
	t.Parallel()
	ids := collectPartitionIDs(nil)
	assert.Nil(t, ids)
}

func TestCollectPartitionIDs_AllNil(t *testing.T) {
	t.Parallel()
	pks := []typedef.PartitionKeys{
		{ID: uuid.Nil},
		{ID: uuid.Nil},
	}
	ids := collectPartitionIDs(pks)
	// uuid.Nil entries are skipped
	assert.Empty(t, ids)
}

func TestCollectPartitionIDs_Mixed(t *testing.T) {
	t.Parallel()
	id1 := uuid.New()
	id2 := uuid.New()
	pks := []typedef.PartitionKeys{
		{ID: uuid.Nil},
		{ID: id1},
		{ID: uuid.Nil},
		{ID: id2},
	}
	ids := collectPartitionIDs(pks)
	require.Len(t, ids, 2)
	assert.Contains(t, ids, id1)
	assert.Contains(t, ids, id2)
}

func TestCollectPartitionIDs_AllValid(t *testing.T) {
	t.Parallel()
	id1, id2, id3 := uuid.New(), uuid.New(), uuid.New()
	pks := []typedef.PartitionKeys{{ID: id1}, {ID: id2}, {ID: id3}}
	ids := collectPartitionIDs(pks)
	require.Len(t, ids, 3)
}

// ---------------------------------------------------------------------------
// Jobs.parseMode
// ---------------------------------------------------------------------------

func makeJobs() *Jobs {
	return &Jobs{}
}

func TestParseMode_Write(t *testing.T) {
	t.Parallel()
	j := makeJobs()
	modes := j.parseMode(WriteMode)
	assert.Equal(t, []string{WriteMode}, modes)
}

func TestParseMode_Read(t *testing.T) {
	t.Parallel()
	j := makeJobs()
	modes := j.parseMode(ReadMode)
	assert.Equal(t, []string{ReadMode}, modes)
}

func TestParseMode_Warmup(t *testing.T) {
	t.Parallel()
	j := makeJobs()
	modes := j.parseMode(WarmupMode)
	assert.Equal(t, []string{WarmupMode}, modes)
}

func TestParseMode_Mixed(t *testing.T) {
	t.Parallel()
	j := makeJobs()
	modes := j.parseMode(MixedMode)
	require.Len(t, modes, 2)
	assert.Contains(t, modes, WriteMode)
	assert.Contains(t, modes, ReadMode)
}

func TestParseMode_Unknown(t *testing.T) {
	t.Parallel()
	j := makeJobs()
	modes := j.parseMode("unknown")
	assert.Empty(t, modes)
}

// ---------------------------------------------------------------------------
// Mutation.Name / Validation.Name
// ---------------------------------------------------------------------------

func TestMutationName(t *testing.T) {
	t.Parallel()
	m := &Mutation{table: &typedef.Table{Name: "my_table"}}
	assert.Equal(t, "mutation_my_table", m.Name())
}

func TestValidationName(t *testing.T) {
	t.Parallel()
	v := &Validation{table: &typedef.Table{Name: "my_table"}}
	assert.Equal(t, "validation_my_table", v.Name())
}

// ---------------------------------------------------------------------------
// marshalRows
// ---------------------------------------------------------------------------

func TestMarshalRows_Nil(t *testing.T) {
	t.Parallel()
	result := marshalRows(nil)
	assert.Nil(t, result)
}

func TestMarshalRows_Empty(t *testing.T) {
	t.Parallel()
	result := marshalRows(store.Rows{})
	assert.Nil(t, result)
}

func TestMarshalRows_Valid(t *testing.T) {
	t.Parallel()
	rows := store.Rows{
		store.NewRow([]string{"col1", "col2"}, []any{1, "hello"}),
		store.NewRow([]string{"col1", "col2"}, []any{2, "world"}),
	}
	result := marshalRows(rows)
	require.Len(t, result, 2)
	// Each element must be valid JSON
	for _, raw := range result {
		assert.True(t, json.Valid(raw))
	}
}

// ---------------------------------------------------------------------------
// marshalDifferentRows
// ---------------------------------------------------------------------------

func TestMarshalDifferentRows_Nil(t *testing.T) {
	t.Parallel()
	result := marshalDifferentRows(nil)
	assert.Nil(t, result)
}

func TestMarshalDifferentRows_Empty(t *testing.T) {
	t.Parallel()
	result := marshalDifferentRows([]store.RowDifference{})
	assert.Nil(t, result)
}

func TestMarshalDifferentRows_Valid(t *testing.T) {
	t.Parallel()
	diffs := []store.RowDifference{
		{
			TestRow:   store.NewRow([]string{"pk"}, []any{1}),
			OracleRow: store.NewRow([]string{"pk"}, []any{2}),
			Diff:      "pk: 1 != 2",
		},
	}
	result := marshalDifferentRows(diffs)
	require.Len(t, result, 1)
	assert.Equal(t, "pk: 1 != 2", result[0].Diff)
	assert.True(t, json.Valid(result[0].TestRow))
	assert.True(t, json.Valid(result[0].OracleRow))
}

// ---------------------------------------------------------------------------
// extractComparisonResults
// ---------------------------------------------------------------------------

func TestExtractComparisonResults_NonValidationError(t *testing.T) {
	t.Parallel()
	cr := extractComparisonResults(ErrNoStatement)
	assert.Nil(t, cr)
}

func TestExtractComparisonResults_ValidationErrorNilResults(t *testing.T) {
	t.Parallel()
	ve := &store.ValidationError{}
	cr := extractComparisonResults(ve)
	assert.Nil(t, cr)
}

func TestExtractComparisonResults_WithResults(t *testing.T) {
	t.Parallel()
	ve := &store.ValidationError{
		ComparisonResults: &store.ComparisonResults{
			TestRows:   store.Rows{store.NewRow([]string{"a"}, []any{1})},
			OracleRows: store.Rows{store.NewRow([]string{"a"}, []any{2})},
		},
	}
	cr := extractComparisonResults(ve)
	require.NotNil(t, cr)
	assert.Len(t, cr.TestRows, 1)
	assert.Len(t, cr.OracleRows, 1)
}

// ---------------------------------------------------------------------------
// Validation.collectLastValidations
// ---------------------------------------------------------------------------

func TestCollectLastValidations_NilGenerator(t *testing.T) {
	t.Parallel()
	v := &Validation{generator: nil}
	stmt := &typedef.Stmt{PartitionKeys: []typedef.PartitionKeys{{ID: uuid.New()}}}
	result := v.collectLastValidations(stmt)
	assert.Nil(t, result)
}

func TestCollectLastValidations_EmptyPartitionKeys(t *testing.T) {
	t.Parallel()
	v := &Validation{}
	stmt := &typedef.Stmt{PartitionKeys: []typedef.PartitionKeys{}}
	result := v.collectLastValidations(stmt)
	assert.Nil(t, result)
}

// ---------------------------------------------------------------------------
// ErrMutationJobStopped / ErrValidationJobStopped are sentinel errors
// ---------------------------------------------------------------------------

func TestSentinelErrors(t *testing.T) {
	t.Parallel()
	assert.EqualError(t, ErrMutationJobStopped, "mutation job stopped due to errors")
	assert.EqualError(t, ErrValidationJobStopped, "validation job stopped due to errors")
}

// ---------------------------------------------------------------------------
// Validation.buildJobError
// ---------------------------------------------------------------------------

func TestBuildJobError(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name: "t",
		PartitionKeys: typedef.Columns{
			{Name: "pk", Type: typedef.TypeInt},
		},
	}
	v := &Validation{
		table:        table,
		keyspaceName: "ks",
	}

	stmt := &typedef.Stmt{
		QueryType:     typedef.SelectStatementType,
		Query:         "SELECT * FROM ks.t WHERE pk=?",
		PartitionKeys: []typedef.PartitionKeys{{ID: uuid.New()}},
		Values:        []any{1},
	}

	jobErr := v.buildJobError(ErrNoStatement, stmt, "test message")
	require.NotNil(t, jobErr)
	assert.Equal(t, "test message", jobErr.Message)
	assert.Equal(t, typedef.SelectStatementType, jobErr.StmtType)
	// JobError wraps the original error in the Err field
	assert.ErrorIs(t, jobErr.Err, ErrNoStatement)
}

// ---------------------------------------------------------------------------
// joberror.JobError implements the error interface
// ---------------------------------------------------------------------------

func TestJobError_Error(t *testing.T) {
	t.Parallel()
	je := &joberror.JobError{
		Message:  "something went wrong",
		StmtType: typedef.SelectStatementType,
	}
	assert.Contains(t, je.Error(), "something went wrong")
}
