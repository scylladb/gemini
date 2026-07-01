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

package statements

import (
	"testing"

	"github.com/google/uuid"

	"github.com/scylladb/gemini/pkg/partitions"
	"github.com/scylladb/gemini/pkg/typedef"
)

// BenchmarkUpdateFromTracked measures a single-row UPDATE that consumes a
// tracked row (the dominant path in mixed mode). Calls the public Update so it
// compiles against both the pre- and post-refactor internals for benchstat.
func BenchmarkUpdateFromTracked(b *testing.B) {
	table := &typedef.Table{
		Name:           "t",
		PartitionKeys:  typedef.Columns{{Name: "pk1", Type: typedef.TypeInt}},
		ClusteringKeys: typedef.Columns{{Name: "ck1", Type: typedef.TypeBigint}, {Name: "ck2", Type: typedef.TypeText}},
		Columns: typedef.Columns{
			{Name: "col1", Type: typedef.TypeText},
			{Name: "col2", Type: typedef.TypeInt},
			{Name: "col3", Type: typedef.TypeBigint},
		},
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
		_, _ = gen.Update(ctx)
	}
}
