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

package partitions

import (
	"math/rand/v2"
	"testing"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/typedef"
)

// BenchmarkPartitionsGetParallel isolates the partition read hot path
// (Get -> values) under heavy concurrency, with no Replace/Stats noise, so it
// reflects the cost of building a PartitionKeys from an immutable slot.
func BenchmarkPartitionsGetParallel(b *testing.B) {
	const n = 10_000
	src, fn := distributions.New(distributions.Uniform, n, 1, 0, 0)
	table := createTestTable()
	parts := New(b.Context(), rand.New(src), fn, table, typedef.PartitionRangeConfig{}, n, 0)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewPCG(1, 2))
		for pb.Next() {
			pk := parts.Get(uint64(r.IntN(n)))
			if pk.Release != nil {
				pk.Release()
			}
		}
	})
}
