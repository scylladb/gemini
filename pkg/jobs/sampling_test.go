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
	"math/rand/v2"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/partitions"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
)

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

// trackingGenerator is a minimal partitions.Interface stub that:
//   - Records every TrackRow call.
//   - Returns a configurable FillRatio value.
//   - Satisfies the full interface with no-op stubs for everything else.
type trackingGenerator struct {
	tracked   []partitions.TrackedRow
	fillRatio float64
	noTrack   bool // when true, TrackRow discards (used by benchmarks to isolate caller allocs)
}

func (g *trackingGenerator) TrackRow(row partitions.TrackedRow) {
	if g.noTrack {
		return
	}
	g.tracked = append(g.tracked, row)
}

func (g *trackingGenerator) RowTrackerFillRatio() float64 { return g.fillRatio }

// -- no-op stubs for the rest of partitions.Interface --

func (g *trackingGenerator) Stats() partitions.Stats                    { return partitions.Stats{} }
func (g *trackingGenerator) Get(_ uint64) typedef.PartitionKeys         { return typedef.PartitionKeys{} }
func (g *trackingGenerator) Next() typedef.PartitionKeys                { return typedef.PartitionKeys{} }
func (g *trackingGenerator) Extend() typedef.PartitionKeys              { return typedef.PartitionKeys{} }
func (g *trackingGenerator) ReplaceNext() typedef.PartitionKeys         { return typedef.PartitionKeys{} }
func (g *trackingGenerator) Replace(_ uint64) typedef.PartitionKeys     { return typedef.PartitionKeys{} }
func (g *trackingGenerator) ReplaceWithoutOld(_ uint64)                 {}
func (g *trackingGenerator) ReplaceNextWithoutOld()                     {}
func (g *trackingGenerator) Deleted() <-chan typedef.PartitionKeys      { return nil }
func (g *trackingGenerator) ValidationSuccess(_ *typedef.PartitionKeys) {}
func (g *trackingGenerator) ValidationFailure(_ *typedef.PartitionKeys) {}
func (g *trackingGenerator) ValidationStats(_ uuid.UUID) (uint64, uint64, uint64, []uint64, uint64) {
	return 0, 0, 0, nil, 0
}
func (g *trackingGenerator) MarkInvalid(_ *typedef.PartitionKeys) bool { return false }
func (g *trackingGenerator) IsInvalid(_ uint64) bool                   { return false }
func (g *trackingGenerator) InvalidCount() uint64                      { return 0 }
func (g *trackingGenerator) PopTrackedRow() (partitions.TrackedRow, bool) {
	return partitions.TrackedRow{}, false
}
func (g *trackingGenerator) TrackedRowCount() uint64 { return 0 }
func (g *trackingGenerator) Len() uint64             { return 10 }
func (g *trackingGenerator) Close()                  {}

// compile-time interface check
var _ partitions.Interface = (*trackingGenerator)(nil)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newSampleValidation builds a Validation with a no-op store and the supplied
// generator; a single PK column and one CK column so the sampling gate is reachable.
func newSampleValidation(gen partitions.Interface, maxSamples int) *Validation {
	table := &typedef.Table{
		Name: "t",
		PartitionKeys: typedef.Columns{
			{Name: "pk1", Type: typedef.TypeInt},
		},
		ClusteringKeys: typedef.Columns{
			{Name: "ck1", Type: typedef.TypeBigint},
		},
	}
	return &Validation{
		table:            table,
		keyspaceName:     "ks",
		store:            &mockStore{},
		generator:        gen,
		random:           rand.New(rand.NewChaCha8([32]byte{1})),
		sampleRate:       0.5,
		maxSamplesPerRun: maxSamples,
		selectColumns:    table.SelectColumnNames(),
	}
}

// makeStmt builds a minimal *typedef.Stmt with one partition key.
func makeStmt(pkVal int32) *typedef.Stmt {
	return &typedef.Stmt{
		QueryType: typedef.SelectStatementType,
		PartitionKeys: []typedef.PartitionKeys{
			{
				ID:     uuid.New(),
				Values: typedef.NewValuesFromMap(map[string][]any{"pk1": {pkVal}}),
			},
		},
	}
}

func makeMultiPartitionStmt(pkVals ...int32) *typedef.Stmt {
	pks := make([]typedef.PartitionKeys, 0, len(pkVals))
	for _, v := range pkVals {
		pks = append(pks, typedef.PartitionKeys{
			ID:     uuid.New(),
			Values: typedef.NewValuesFromMap(map[string][]any{"pk1": {v}}),
		})
	}
	return &typedef.Stmt{
		QueryType:     typedef.SelectMultiPartitionType,
		PartitionKeys: pks,
	}
}

// makeRows builds store.Rows with n rows each having a ck1 value.
func makeRows(n int) store.Rows {
	rows := make(store.Rows, n)
	for i := range n {
		rows[i] = store.NewRow([]string{"pk1", "ck1"}, []any{int32(i), int64(i * 10)})
	}
	return rows
}

func makeRowForPartition(pkVal int32) store.Row {
	return store.NewRow([]string{"pk1", "ck1"}, []any{pkVal, int64(pkVal) * 7})
}

// ---------------------------------------------------------------------------
// sampleRowsForTracker — fill-zone branch tests
// ---------------------------------------------------------------------------

func TestSampleRowsForTracker_LeanQueue_ForceAll(t *testing.T) {
	t.Parallel()

	// Fill < FillZoneAlwaysPush (0.30) → forceAll=true → between 1 and numRows
	// rows are pushed (random draw), but all are eligible.
	const numRows = 8
	gen := &trackingGenerator{fillRatio: 0.10}
	v := newSampleValidation(gen, 2 /*maxSamplesPerRun deliberately small*/)

	stmt := makeStmt(1)
	rows := makeRows(numRows)
	v.sampleRowsForTracker(stmt, rows, true)

	// forceAll bypasses maxSamplesPerRun; at least 1 row must be pushed.
	assert.NotEmpty(t, gen.tracked, "lean queue must push at least one row")
	assert.LessOrEqual(t, len(gen.tracked), numRows, "must not push more than available rows")
}

func TestSampleRowsForTracker_FillingQueue_RespectsMaxSamples(t *testing.T) {
	t.Parallel()

	// Fill ≥ FillZoneSampled (0.70) zone → forceAll=false → capped at maxSamplesPerRun.
	const numRows = 10
	const maxSamples = 3
	gen := &trackingGenerator{fillRatio: 0.80}
	v := newSampleValidation(gen, maxSamples)

	stmt := makeStmt(1)
	rows := makeRows(numRows)
	v.sampleRowsForTracker(stmt, rows, false)

	assert.LessOrEqual(t, len(gen.tracked), maxSamples, "filling queue must cap pushes at maxSamplesPerRun")
}

func TestSampleRowsForTracker_EmptyRows_NoPush(t *testing.T) {
	t.Parallel()

	gen := &trackingGenerator{fillRatio: 0.10}
	v := newSampleValidation(gen, 10)

	stmt := makeStmt(1)
	v.sampleRowsForTracker(stmt, store.Rows{}, true)

	assert.Empty(t, gen.tracked, "empty row set must result in zero pushes")
}

func TestSampleRowsForTracker_NoPartitionKeys_NoPush(t *testing.T) {
	t.Parallel()

	gen := &trackingGenerator{fillRatio: 0.10}
	v := newSampleValidation(gen, 10)

	// Stmt with no partition keys
	stmt := &typedef.Stmt{QueryType: typedef.SelectStatementType}
	v.sampleRowsForTracker(stmt, makeRows(5), true)

	assert.Empty(t, gen.tracked, "missing partition keys must result in zero pushes")
}

func TestSampleRowsForTracker_RowMissingCKValue_Skipped(t *testing.T) {
	t.Parallel()

	gen := &trackingGenerator{fillRatio: 0.10}
	// One row has ck1 = nil (missing), one is valid.
	rows := store.Rows{
		store.NewRow([]string{"pk1"}, []any{int32(1)}),                   // missing ck1
		store.NewRow([]string{"pk1", "ck1"}, []any{int32(2), int64(20)}), // valid
	}
	v := newSampleValidation(gen, 10)

	stmt := makeStmt(1)
	v.sampleRowsForTracker(stmt, rows, true)

	require.Len(t, gen.tracked, 1, "only the valid row must be pushed")
	assert.Equal(t, []any{int64(20)}, gen.tracked[0].ClusteringValues)
}

func TestSampleRowsForTracker_PKValuesInTrackedRow(t *testing.T) {
	t.Parallel()

	gen := &trackingGenerator{fillRatio: 0.10}
	v := newSampleValidation(gen, 10)

	pkVal := int32(42)
	stmt := makeStmt(pkVal)
	v.sampleRowsForTracker(stmt, makeRows(1), true)

	require.Len(t, gen.tracked, 1)
	assert.Equal(t, []any{pkVal}, gen.tracked[0].PartitionValues, "PartitionValues must contain raw PK bind values")
}

func TestSampleRowsForTracker_RandomCountBetween1AndN(t *testing.T) {
	t.Parallel()

	// Run many times to verify the random count is always in [1, numRows].
	const numRows = 6
	const iterations = 200

	for range iterations {
		gen := &trackingGenerator{fillRatio: 0.10}
		v := newSampleValidation(gen, numRows+1 /*cap well above numRows*/)
		stmt := makeStmt(1)
		v.sampleRowsForTracker(stmt, makeRows(numRows), true)

		assert.GreaterOrEqual(t, len(gen.tracked), 1, "must push at least 1 row")
		assert.LessOrEqual(t, len(gen.tracked), numRows, "must not push more rows than available")
	}
}

func TestSampleRowsForTracker_MultiPartition_AttributedToCorrectPartition(t *testing.T) {
	t.Parallel()

	gen := &trackingGenerator{fillRatio: 0.10} // lean → force-push path
	v := newSampleValidation(gen, 100)

	stmt := makeMultiPartitionStmt(10, 20, 30)

	idByPK := make(map[int32]uuid.UUID, len(stmt.PartitionKeys))
	for i := range stmt.PartitionKeys {
		pkVal := stmt.PartitionKeys[i].Values.Get("pk1")[0].(int32)
		idByPK[pkVal] = stmt.PartitionKeys[i].ID
	}

	rows := store.Rows{
		makeRowForPartition(10),
		makeRowForPartition(20),
		makeRowForPartition(30),
	}
	v.sampleRowsForTracker(stmt, rows, true)

	require.NotEmpty(t, gen.tracked, "multi-partition rows must be sampled")
	for _, tr := range gen.tracked {
		require.Len(t, tr.PartitionValues, 1)
		pkVal := tr.PartitionValues[0].(int32)
		wantID, ok := idByPK[pkVal]
		require.True(t, ok, "tracked PartitionValues must match an IN-clause partition")
		assert.Equal(t, wantID, tr.PartitionID, "row must carry the UUID of the partition whose PK it matches")
		// ck1 was set to pkVal*7 — confirm the row's CK travels with its own PK.
		require.Len(t, tr.ClusteringValues, 1)
		assert.Equal(t, int64(pkVal)*7, tr.ClusteringValues[0], "clustering value must belong to the matched row")
	}
}

func TestSampleRowsForTracker_MultiPartition_UnmatchedRowSkipped(t *testing.T) {
	t.Parallel()

	gen := &trackingGenerator{fillRatio: 0.10}
	v := newSampleValidation(gen, 100)

	stmt := makeMultiPartitionStmt(10, 20)
	// pk1=99 is not in the IN clause.
	rows := store.Rows{makeRowForPartition(99)}
	v.sampleRowsForTracker(stmt, rows, true)

	assert.Empty(t, gen.tracked, "a row matching no IN-clause partition must not be tracked")
}

// ---------------------------------------------------------------------------
// sampleRowsForTracker directly covers all fill-zone paths; the run() gating
// is an integration concern tested end-to-end. No additional run() tests here.
// ---------------------------------------------------------------------------
