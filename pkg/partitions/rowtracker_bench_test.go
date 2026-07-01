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

package partitions

import (
	"strconv"
	"testing"

	"github.com/google/uuid"
)

func BenchmarkRowTracker_PushPop(b *testing.B) {
	rt := NewRowTracker(1024)
	id := uuid.New()
	row := TrackedRow{PartitionID: id, PartitionValues: []any{int32(1)}, ClusteringValues: []any{int64(2)}}

	b.ReportAllocs()
	for b.Loop() {
		rt.Push(row)
		rt.Pop()
	}
}

func BenchmarkRowTracker_Invalidate(b *testing.B) {
	for _, cfg := range []struct {
		capacity uint64
		live     int
	}{
		{capacity: 1024, live: 64},
		{capacity: 100_000, live: 64},
		{capacity: 100_000, live: 10_000},
	} {
		name := "cap=" + strconv.FormatUint(cfg.capacity, 10) + "/live=" + strconv.Itoa(cfg.live)
		b.Run(name, func(b *testing.B) {
			rt := NewRowTracker(cfg.capacity)
			liveID := uuid.New()
			for range cfg.live {
				rt.Push(TrackedRow{PartitionID: liveID, PartitionValues: []any{int32(1)}})
			}
			absent := uuid.New()

			b.ReportAllocs()
			for b.Loop() {
				rt.Invalidate(absent)
			}
		})
	}
}
