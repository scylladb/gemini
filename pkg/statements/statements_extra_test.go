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
	"sync/atomic"
	"testing"

	"github.com/google/uuid"

	"github.com/scylladb/gemini/pkg/partitions"
	"github.com/scylladb/gemini/pkg/typedef"
)

// mockPartitions is a thread-safe stub that satisfies partitions.Interface.
//
// Thread-safety is achieved by:
//   - Using sync/atomic operations for all integer counters.
//   - Protecting the deleted channel with a buffered channel (no shared mutable slice).
//   - Never storing a *rand.Rand (which is not goroutine-safe in math/rand v1).
//
// Do NOT add any field of type *rand.Rand or math/rand.Rand to this struct.
type mockPartitions struct {
	deleted      chan typedef.PartitionKeys
	count        atomic.Uint64
	invalidCount atomic.Uint64
}

func newMockPartitions(count uint64) *mockPartitions {
	mp := &mockPartitions{
		deleted: make(chan typedef.PartitionKeys, 16),
	}
	mp.count.Store(count)
	return mp
}

// compile-time check that mockPartitions implements partitions.Interface.
var _ partitions.Interface = (*mockPartitions)(nil)

func (m *mockPartitions) Stats() partitions.Stats {
	return partitions.Stats{
		CurrentPartitionCount:  m.count.Load(),
		InvalidPartitionsCount: m.invalidCount.Load(),
	}
}

func (m *mockPartitions) Get(_ uint64) typedef.PartitionKeys {
	return typedef.PartitionKeys{ID: uuid.New()}
}

func (m *mockPartitions) Next() typedef.PartitionKeys {
	return typedef.PartitionKeys{ID: uuid.New()}
}

func (m *mockPartitions) Extend() typedef.PartitionKeys {
	m.count.Add(1)
	return typedef.PartitionKeys{ID: uuid.New()}
}

func (m *mockPartitions) ReplaceNext() typedef.PartitionKeys {
	return typedef.PartitionKeys{ID: uuid.New()}
}

func (m *mockPartitions) Replace(_ uint64) typedef.PartitionKeys {
	return typedef.PartitionKeys{ID: uuid.New()}
}

func (m *mockPartitions) ReplaceWithoutOld(_ uint64) {}

func (m *mockPartitions) ReplaceNextWithoutOld() {}

func (m *mockPartitions) Deleted() <-chan typedef.PartitionKeys {
	return m.deleted
}

func (m *mockPartitions) ValidationSuccess(_ *typedef.PartitionKeys) {}

func (m *mockPartitions) ValidationFailure(_ *typedef.PartitionKeys) {}

func (m *mockPartitions) ValidationStats(_ uuid.UUID) (first, last, failure uint64, recent []uint64, successCount uint64) {
	return 0, 0, 0, nil, 0
}

func (m *mockPartitions) MarkInvalid(_ *typedef.PartitionKeys) bool {
	m.invalidCount.Add(1)
	return true
}

func (m *mockPartitions) IsInvalid(_ uint64) bool {
	return false
}

func (m *mockPartitions) InvalidCount() uint64 {
	return m.invalidCount.Load()
}

func (m *mockPartitions) Len() uint64 {
	return m.count.Load()
}

func (m *mockPartitions) Close() {
	close(m.deleted)
}

// ---- tests ----

func TestMockPartitions_ThreadSafe(t *testing.T) {
	t.Parallel()

	mp := newMockPartitions(0)
	t.Cleanup(mp.Close)

	const goroutines = 50
	done := make(chan struct{})

	for range goroutines {
		go func() {
			mp.Extend()
			mp.Next()
			mp.Get(0)
			mp.ReplaceNext()
			mp.Replace(0)
			mp.ReplaceWithoutOld(0)
			mp.ReplaceNextWithoutOld()
			mp.Stats()
			mp.Len()
			mp.MarkInvalid(&typedef.PartitionKeys{ID: uuid.New()})
			mp.InvalidCount()
			mp.IsInvalid(0)
			done <- struct{}{}
		}()
	}

	for range goroutines {
		<-done
	}

	if got := mp.Len(); got != goroutines {
		t.Errorf("Len() = %d after %d Extend calls, want %d", got, goroutines, goroutines)
	}
	if got := mp.InvalidCount(); got != goroutines {
		t.Errorf("InvalidCount() = %d after %d MarkInvalid calls, want %d", got, goroutines, goroutines)
	}
}

func TestMockPartitions_Len(t *testing.T) {
	t.Parallel()

	mp := newMockPartitions(10)
	t.Cleanup(mp.Close)

	if got := mp.Len(); got != 10 {
		t.Errorf("Len() = %d, want 10", got)
	}
}

func TestMockPartitions_Stats(t *testing.T) {
	t.Parallel()

	mp := newMockPartitions(5)
	t.Cleanup(mp.Close)

	s := mp.Stats()
	if s.CurrentPartitionCount != 5 {
		t.Errorf("Stats().CurrentPartitionCount = %d, want 5", s.CurrentPartitionCount)
	}
}

func TestMockPartitions_MarkInvalid(t *testing.T) {
	t.Parallel()

	mp := newMockPartitions(1)
	t.Cleanup(mp.Close)

	pk := &typedef.PartitionKeys{ID: uuid.New()}
	if !mp.MarkInvalid(pk) {
		t.Error("MarkInvalid() = false, want true")
	}
	if got := mp.InvalidCount(); got != 1 {
		t.Errorf("InvalidCount() = %d, want 1", got)
	}
}

func TestMockPartitions_DeletedChannel(t *testing.T) {
	t.Parallel()

	mp := newMockPartitions(1)
	// Closing is handled below — do not use t.Cleanup here.

	ch := mp.Deleted()
	if ch == nil {
		t.Fatal("Deleted() returned nil channel")
	}

	// Channel must be empty initially (nothing sent yet).
	select {
	case <-ch:
		t.Error("Deleted channel unexpectedly had data before any delete")
	default:
	}

	mp.Close()
}

func TestTotalCartesianProductCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		initial float64
		pkLen   float64
		wantMin int
	}{
		{initial: 10, pkLen: 1, wantMin: 1},
		{initial: 0, pkLen: 3, wantMin: 1},
		{initial: 5, pkLen: 2, wantMin: 1},
	}

	for _, tc := range tests {
		got := TotalCartesianProductCount(tc.initial, tc.pkLen)
		if got < tc.wantMin {
			t.Errorf("TotalCartesianProductCount(%v, %v) = %d, want >= %d",
				tc.initial, tc.pkLen, got, tc.wantMin)
		}
	}
}
