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

package statements

import (
	"math/rand/v2"
	"testing"

	"github.com/google/uuid"

	"github.com/scylladb/gemini/pkg/partitions"
	"github.com/scylladb/gemini/pkg/typedef"
)

// constPopPartitions returns a fixed tracked row from PopTrackedRow without
// draining anything, so delete benchmarks measure statement construction in
// isolation from the tracker's Push/Pop bookkeeping.
type constPopPartitions struct {
	mockPartitions
	row partitions.TrackedRow
}

func (m *constPopPartitions) PopTrackedRow() (partitions.TrackedRow, bool) {
	return m.row, true
}

func (m *constPopPartitions) RowTrackerFillRatio() float64     { return 0.5 }
func (m *constPopPartitions) TrackedRowCount() uint64          { return 1 }
func (m *constPopPartitions) TrackRow(_ partitions.TrackedRow) {}

func newDeleteBenchGen(b *testing.B, table *typedef.Table, row partitions.TrackedRow) *Generator {
	b.Helper()
	mp := &constPopPartitions{
		mockPartitions: *newMockPartitions(10),
		row:            row,
	}
	ratios := DefaultStatementRatios()
	rng := rand.New(rand.NewChaCha8([32]byte{7}))
	rc, err := NewRatioController(ratios, rng)
	if err != nil {
		b.Fatal(err)
	}
	vc := &typedef.ValueRangeConfig{MaxBlobLength: 32, MinBlobLength: 1, MaxStringLength: 32, MinStringLength: 1}
	return New("ks", mp, table, rng, vc, rc, false)
}

func BenchmarkDeleteSingleRow(b *testing.B) {
	table := &typedef.Table{
		Name:           "t",
		PartitionKeys:  typedef.Columns{{Name: "pk1", Type: typedef.TypeInt}},
		ClusteringKeys: typedef.Columns{{Name: "ck1", Type: typedef.TypeBigint}, {Name: "ck2", Type: typedef.TypeText}},
	}
	row := partitions.TrackedRow{
		PartitionID:      uuid.New(),
		PartitionValues:  []any{int32(99)},
		ClusteringValues: []any{int64(123), "hello"},
	}
	gen := newDeleteBenchGen(b, table, row)
	ctx := b.Context()

	b.ReportAllocs()
	for b.Loop() {
		_, _ = gen.deleteSingleRow(ctx)
	}
}

func BenchmarkDeleteClusteringSubset(b *testing.B) {
	table := &typedef.Table{
		Name:          "t",
		PartitionKeys: typedef.Columns{{Name: "pk1", Type: typedef.TypeInt}},
		ClusteringKeys: typedef.Columns{
			{Name: "ck1", Type: typedef.TypeBigint},
			{Name: "ck2", Type: typedef.TypeText},
			{Name: "ck3", Type: typedef.TypeInt},
		},
	}
	row := partitions.TrackedRow{
		PartitionID:      uuid.New(),
		PartitionValues:  []any{int32(77)},
		ClusteringValues: []any{int64(10), "world", int32(5)},
	}
	gen := newDeleteBenchGen(b, table, row)
	ctx := b.Context()

	b.ReportAllocs()
	for b.Loop() {
		_, _ = gen.deleteClusteringSubset(ctx)
	}
}
