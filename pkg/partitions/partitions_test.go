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
	"math/rand/v2"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/typedef"
)

func createTestTable() *typedef.Table {
	return &typedef.Table{
		Name: "test_table",
		PartitionKeys: typedef.Columns{
			{
				Name: "pk1",
				Type: typedef.TypeInt,
			},
			{
				Name: "pk2",
				Type: typedef.TypeText,
			},
		},
	}
}

func createTestPartitions(t *testing.T, count uint64) *Partitions {
	t.Helper()
	src, fn := distributions.New(distributions.Uniform, count, 1, 0, 0)
	table := createTestTable()
	config := typedef.PartitionRangeConfig{
		MaxBlobLength:   100,
		MinBlobLength:   10,
		MaxStringLength: 50,
		MinStringLength: 5,
		DeleteBuckets:   nil, // No delete buckets - tests that need them should use createTestPartitionsWithDeletes
	}
	parts := New(t.Context(), rand.New(src), fn, table, config, count, 0)
	t.Cleanup(parts.Close)
	return parts
}

func assertPartitionKeysPopulated(t *testing.T, keys typedef.PartitionKeys) {
	t.Helper()
	assert.NotNil(t, keys.Values)
	assert.Positive(t, keys.Values.Len())
}

func assertPartitionKeysEqual(t *testing.T, left, right typedef.PartitionKeys) {
	t.Helper()
	require.NotNil(t, left.Values)
	require.NotNil(t, right.Values)

	leftKeys := left.Values.Keys()
	rightKeys := right.Values.Keys()
	assert.Equal(t, leftKeys, rightKeys)

	for _, key := range leftKeys {
		assert.Equal(t, left.Values.Get(key), right.Values.Get(key))
	}
}

// TestPartitionsGet verifies Get operation
func TestPartitionsGet(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 100)

	for i := range uint64(100) {
		values := parts.Get(i)
		require.NotNil(t, values)
		assertPartitionKeysPopulated(t, values)
	}
}

// TestPartitionsNext verifies Next operation using distribution function
func TestPartitionsNext(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 1000)

	for range 100 {
		values := parts.Next()
		require.NotNil(t, values)
		assertPartitionKeysPopulated(t, values)
	}
}

// TestPartitionsExtend verifies Extend operation
func TestPartitionsExtend(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 10)
	initialLen := parts.Len()

	for range 5 {
		values := parts.Extend()
		require.NotNil(t, values)
		assertPartitionKeysPopulated(t, values)
	}

	assert.Equal(t, initialLen+5, parts.Len())

	stats := parts.Stats()
	assert.Equal(t, uint64(5), stats.PartitionsCreated)
}

// TestPartitionsReplace verifies Replace operation
func TestPartitionsReplace(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 100)

	idx := uint64(42)
	oldValues := parts.Get(idx)
	require.NotNil(t, oldValues)

	replacedValues := parts.Replace(idx)
	require.NotNil(t, replacedValues)

	newValues := parts.Get(idx)
	require.NotNil(t, newValues)

	// Verify that values changed
	assertPartitionKeysEqual(t, oldValues, replacedValues)

	stats := parts.Stats()
	assert.Equal(t, uint64(1), stats.PartitionsDeleted)
}

// TestPartitionsReplaceNext verifies ReplaceNext operation
func TestPartitionsReplaceNext(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 100)

	for range 10 {
		values := parts.ReplaceNext()
		require.NotNil(t, values)
		assertPartitionKeysPopulated(t, values)
	}

	stats := parts.Stats()
	assert.Equal(t, uint64(10), stats.PartitionsDeleted)
}

// TestPartitionsReplaceWithoutOld verifies ReplaceWithoutOld operation
func TestPartitionsReplaceWithoutOld(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 100)

	idx := uint64(50)
	oldValues := parts.Get(idx)
	require.NotNil(t, oldValues)

	parts.ReplaceWithoutOld(idx)

	newValues := parts.Get(idx)
	require.NotNil(t, newValues)

	stats := parts.Stats()
	assert.Equal(t, uint64(1), stats.PartitionsDeleted)
}

// TestPartitionsReplaceNextWithoutOld verifies ReplaceNextWithoutOld operation
func TestPartitionsReplaceNextWithoutOld(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 100)

	for range 10 {
		parts.ReplaceNextWithoutOld()
	}

	stats := parts.Stats()
	assert.Equal(t, uint64(10), stats.PartitionsDeleted)
}

// TestPartitionsStats verifies Stats collection
func TestPartitionsStats(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 100)

	// Initial stats
	stats := parts.Stats()
	assert.Equal(t, uint64(100), stats.CurrentPartitionCount)
	assert.Equal(t, uint64(0), stats.PartitionsDeleted)
	assert.Equal(t, uint64(0), stats.PartitionsCreated)

	// Perform operations
	parts.Extend()
	parts.Extend()
	parts.Replace(0)
	parts.ReplaceWithoutOld(1)

	// Check updated stats
	stats = parts.Stats()
	assert.Equal(t, uint64(102), stats.CurrentPartitionCount)
	assert.Equal(t, uint64(2), stats.PartitionsDeleted)
	assert.Equal(t, uint64(2), stats.PartitionsCreated)
}

// TestPartitionsConcurrentGet verifies concurrent Get operations
func TestPartitionsConcurrentGet(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 1000)
	concurrency := 10
	iterations := 1000

	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := range concurrency {
		go func(workerID int) {
			defer wg.Done()
			src := rand.NewPCG(uint64(workerID), uint64(workerID*2))
			r := rand.New(src)

			for range iterations {
				idx := uint64(r.IntN(1000))
				values := parts.Get(idx)
				assert.NotNil(t, values)
				assertPartitionKeysPopulated(t, values)
			}
		}(i)
	}

	wg.Wait()
}

// TestPartitionsConcurrentNext verifies concurrent Next operations
func TestPartitionsConcurrentNext(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 1000)
	concurrency := 10
	iterations := 1000

	var wg sync.WaitGroup
	wg.Add(concurrency)

	for range concurrency {
		go func() {
			defer wg.Done()

			for range iterations {
				values := parts.Next()
				assert.NotNil(t, values)
				assertPartitionKeysPopulated(t, values)
			}
		}()
	}

	wg.Wait()
}

// TestPartitionsConcurrentExtend verifies concurrent Extend operations
func TestPartitionsConcurrentExtend(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 100)
	concurrency := 10
	iterations := 100

	var wg sync.WaitGroup
	wg.Add(concurrency)

	for range concurrency {
		go func() {
			defer wg.Done()

			for range iterations {
				values := parts.Extend()
				assert.NotNil(t, values)
				assertPartitionKeysPopulated(t, values)
			}
		}()
	}

	wg.Wait()

	expectedLen := uint64(100 + concurrency*iterations)
	assert.Equal(t, expectedLen, parts.Len())

	stats := parts.Stats()
	assert.Equal(t, uint64(concurrency*iterations), stats.PartitionsCreated)
}

// TestPartitionsConcurrentReplace verifies concurrent Replace operations
func TestPartitionsConcurrentReplace(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 1000)
	concurrency := 10
	iterations := 100

	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := range concurrency {
		go func(workerID int) {
			defer wg.Done()
			src := rand.NewPCG(uint64(workerID), uint64(workerID*2))
			r := rand.New(src)

			for range iterations {
				idx := uint64(r.IntN(1000))
				values := parts.Replace(idx)
				assert.NotNil(t, values)
				assertPartitionKeysPopulated(t, values)
			}
		}(i)
	}

	wg.Wait()

	stats := parts.Stats()
	assert.Equal(t, uint64(concurrency*iterations), stats.PartitionsDeleted)
}

// TestPartitionsConcurrentMixed verifies mixed concurrent operations
func TestPartitionsConcurrentMixed(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 1000)
	concurrency := 10
	iterations := 100

	var wg sync.WaitGroup
	wg.Add(concurrency * 4) // 4 operation types

	// Concurrent Get operations
	for i := range concurrency {
		go func(workerID int) {
			defer wg.Done()
			src := rand.NewPCG(uint64(workerID), uint64(workerID*2))
			r := rand.New(src)

			for range iterations {
				idx := uint64(r.IntN(1000))
				values := parts.Get(idx)
				assert.NotNil(t, values)
			}
		}(i)
	}

	// Concurrent Next operations
	for range concurrency {
		go func() {
			defer wg.Done()

			for range iterations {
				values := parts.Next()
				assert.NotNil(t, values)
			}
		}()
	}

	// Concurrent Extend operations
	for range concurrency {
		go func() {
			defer wg.Done()

			for range iterations / 10 {
				values := parts.Extend()
				assert.NotNil(t, values)
			}
		}()
	}

	// Concurrent Replace operations
	for k := range concurrency {
		go func(workerID int) {
			defer wg.Done()
			src := rand.NewPCG(uint64(workerID+100), uint64(workerID*2+100))
			r := rand.New(src)

			for range iterations / 10 {
				idx := uint64(r.IntN(1000))
				parts.ReplaceWithoutOld(idx)
			}
		}(k)
	}

	wg.Wait()

	stats := parts.Stats()
	assert.Greater(t, stats.CurrentPartitionCount, uint64(1000))
	assert.Positive(t, stats.PartitionsCreated)
	assert.Positive(t, stats.PartitionsDeleted)
}

// TestPartitionsMemoryUsage tests memory usage tracking
func TestPartitionsMemoryUsage(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 1000)

	stats := parts.Stats()
	assert.Equal(t, uint64(1000), stats.CurrentPartitionCount)

	// Note: Current implementation returns 0 for memory usage
	// This test verifies the Stats call works correctly
	assert.Equal(t, uint64(1000), stats.CurrentPartitionCount)
}

// TestPartitionsMetrics verifies metric tracking
func TestPartitionsMetrics(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 100)

	// Track creates
	for range 10 {
		parts.Extend()
	}

	// Track replaces
	for i := range 20 {
		parts.ReplaceWithoutOld(uint64(i % 100))
	}

	stats := parts.Stats()
	assert.Equal(t, uint64(110), stats.CurrentPartitionCount)
	assert.Equal(t, uint64(10), stats.PartitionsCreated)
	assert.Equal(t, uint64(20), stats.PartitionsDeleted)
}

// TestPartitionsAtomicCounters verifies atomic counter operations
func TestPartitionsAtomicCounters(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 100)
	concurrency := 100
	iterations := 100

	var wg sync.WaitGroup

	// Concurrent extends
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			for range iterations {
				parts.Extend()
			}
		}()
	}
	wg.Wait()

	// Concurrent replaces
	wg.Add(concurrency)
	for i := range concurrency {
		go func(workerID int) {
			defer wg.Done()
			src := rand.NewPCG(uint64(workerID), uint64(workerID*2))
			r := rand.New(src)

			for range iterations {
				idx := uint64(r.IntN(100))
				parts.ReplaceWithoutOld(idx)
			}
		}(i)
	}
	wg.Wait()

	stats := parts.Stats()
	expectedCount := uint64(100 + concurrency*iterations)
	assert.Equal(t, expectedCount, stats.CurrentPartitionCount)
	assert.Equal(t, uint64(concurrency*iterations), stats.PartitionsCreated)
	assert.Equal(t, uint64(concurrency*iterations), stats.PartitionsDeleted)
}

// TestPartitionsValuesCopy verifies valuesCopy returns a copy
func TestPartitionsValuesCopy(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 10)

	idx := uint64(5)
	values1 := parts.valuesCopy(idx)
	values2 := parts.valuesCopy(idx)

	require.NotNil(t, values1)
	require.NotNil(t, values2)

	// Values should be equal
	assertPartitionKeysEqual(t, values1, values2)
}

// TestNewPartitionKeys verifies NewPartitionKeys function
func TestNewPartitionKeys(t *testing.T) {
	t.Parallel()

	src := rand.NewPCG(1, 2)
	r := rand.New(src)
	table := createTestTable()
	config := typedef.PartitionRangeConfig{
		MaxBlobLength:   100,
		MinBlobLength:   10,
		MaxStringLength: 50,
		MinStringLength: 5,
	}

	pk := NewPartitionKeys(r, table, &config)
	require.NotNil(t, pk.Values)
	require.Positive(t, pk.Values.Len())
}

// TestNewPartitionValues_MixedWidthKeys puts a tuple key between two simple
// keys, so every column has a different start offset in the flat values.
func TestNewPartitionValues_MixedWidthKeys(t *testing.T) {
	t.Parallel()

	cols := typedef.Columns{
		{Name: "pk0", Type: typedef.TypeInt},
		{Name: "pk1", Type: &typedef.TupleType{
			ComplexType: typedef.TypeTuple,
			ValueTypes:  []typedef.SimpleType{typedef.TypeInt, typedef.TypeText},
		}},
		{Name: "pk2", Type: typedef.TypeDate},
	}
	date := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	values := []any{int32(1), int32(2), "two", date}

	out := typedef.NewPartitionValues(cols, values)

	require.Equal(t, []any{int32(1)}, out.Get("pk0"))
	require.Equal(t, []any{int32(2), "two"}, out.Get("pk1"))
	require.Equal(t, []any{date}, out.Get("pk2"))
}

// TestNewPartitionKeys_TupleKeyFirst generates values for a table whose first
// partition key is wider than the second key.
func TestNewPartitionKeys_TupleKeyFirst(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name: "tuple_first",
		PartitionKeys: typedef.Columns{
			{Name: "pk0", Type: &typedef.TupleType{
				ComplexType: typedef.TypeTuple,
				ValueTypes:  []typedef.SimpleType{typedef.TypeInt, typedef.TypeText},
			}},
			{Name: "pk1", Type: typedef.TypeDate},
		},
	}
	config := typedef.PartitionRangeConfig{MaxStringLength: 50, MinStringLength: 5}

	pk := NewPartitionKeys(rand.New(rand.NewPCG(1, 2)), table, &config)

	tuple := pk.Values.Get("pk0")
	require.Len(t, tuple, 2)
	require.IsType(t, int32(0), tuple[0])
	require.IsType(t, "", tuple[1])

	date := pk.Values.Get("pk1")
	require.Len(t, date, 1)
	require.IsType(t, time.Time{}, date[0])
}

// TestPartitionSize verifies partition size calculation
func TestPartitionSize(t *testing.T) {
	t.Parallel()

	p := Partition{seed: 10}

	// Current implementation returns 0
	size := p.size()
	assert.Equal(t, uint64(0), size)
}

// BenchmarkPartitionsConcurrentMixed benchmarks mixed concurrent operations
func BenchmarkPartitionsConcurrentMixed(b *testing.B) {
	src, fn := distributions.New(distributions.Uniform, 10_000, 1, 0, 0)
	table := createTestTable()
	config := typedef.PartitionRangeConfig{
		DeleteBuckets: []time.Duration{100 * time.Millisecond},
	}
	parts := New(b.Context(), rand.New(src), fn, table, config, 10_000, 0)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		src := rand.NewPCG(1, 2)
		r := rand.New(src)
		i := 0
		for pb.Next() {
			switch i % 4 {
			case 0:
				idx := uint64(r.IntN(10_000))
				_ = parts.Get(idx)
			case 1:
				_ = parts.Next()
			case 2:
				idx := uint64(r.IntN(10_000))
				parts.ReplaceWithoutOld(idx)
			case 3:
				_ = parts.Stats()
			}
			i++
		}
	})
}

func BenchmarkMemoryAllocation(b *testing.B) {
	sizes := []uint64{100, 1_000, 10_000, 100_000}

	for _, size := range sizes {
		b.Run(strconv.FormatUint(size, 10), func(b *testing.B) {
			src, fn := distributions.New(distributions.Uniform, size, 1, 0, 0)
			table := createTestTable()
			config := typedef.PartitionRangeConfig{
				DeleteBuckets: []time.Duration{100 * time.Millisecond},
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_ = New(b.Context(), rand.New(src), fn, table, config, size, 0)
			}
		})
	}
}

// TestReplaceWithoutOld_BorrowedKeysStayReportable borrows keys, replaces the
// slot through fill, and checks that a divergence on the borrowed keys is
// still reported. The UUID mapping must stay until the borrow is released.
func TestReplaceWithoutOld_BorrowedKeysStayReportable(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 10)

	keys := parts.Get(3)
	parts.ReplaceWithoutOld(3)

	require.True(t, parts.MarkInvalid(&keys), "a divergence on borrowed keys must be reported")

	keys.Release()

	parts.validationMu.RLock()
	_, mapped := parts.uuidToIdx[keys.ID]
	parts.validationMu.RUnlock()
	require.False(t, mapped, "the last release must remove the old UUID mapping")
}

// TestReplaceWithoutOld_UnborrowedPartitionCleanedAtOnce checks that fill
// removes the old UUID immediately when nothing holds a reference.
func TestReplaceWithoutOld_UnborrowedPartitionCleanedAtOnce(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 10)

	keys := parts.Get(4)
	keys.Release()
	parts.ReplaceWithoutOld(4)

	parts.validationMu.RLock()
	_, mapped := parts.uuidToIdx[keys.ID]
	parts.validationMu.RUnlock()
	require.False(t, mapped)
}

// TestNext_PrefersWrittenPartitions marks half of the pool written. With 16
// draws per pick, Next should land on an unwritten partition in about 0.5^16
// of the picks.
func TestNext_PrefersWrittenPartitions(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 100)
	written := make(map[uuid.UUID]bool, 50)
	for i := range uint64(50) {
		part := parts.partition(i * 2)
		part.written.Store(true)
		written[part.id] = true
	}

	const picks = 2000
	hits := 0
	for range picks {
		keys := parts.Next()
		if written[keys.ID] {
			hits++
		}
		keys.Release()
	}

	require.GreaterOrEqual(t, hits, picks*99/100, "Next must prefer written partitions")
}

// TestNext_TakesUnwrittenWhenNothingIsWritten checks that validation still
// reads an empty pool instead of blocking.
func TestNext_TakesUnwrittenWhenNothingIsWritten(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 10)
	keys := parts.Next()
	require.NotNil(t, keys.Values)
	keys.Release()
}

func TestNextForWrite_MarksThePartitionWritten(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 10)
	keys := parts.NextForWrite()
	t.Cleanup(keys.Release)

	parts.validationMu.RLock()
	idx := parts.uuidToIdx[keys.ID]
	parts.validationMu.RUnlock()

	require.True(t, parts.isWritten(idx))
}

func TestReplace_StartsTheNewPartitionUnwritten(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 10)
	parts.partition(5).written.Store(true)

	old := parts.Replace(5)
	t.Cleanup(old.Release)

	require.False(t, parts.isWritten(5), "a replacement partition holds no data yet")
}

// TestPublishInvalid_SumsAcrossPools marks partitions invalid in two pools and
// checks that partitions_invalid holds the sum. It is not parallel: the gauge
// is global, and Go runs parallel tests only after the sequential ones.
//
//nolint:paralleltest
func TestPublishInvalid_SumsAcrossPools(t *testing.T) {
	before := testutil.ToFloat64(metrics.PartitionsInvalid)

	first := createTestPartitions(t, 10)
	second := createTestPartitions(t, 10)

	for _, mark := range []struct {
		parts *Partitions
		idx   uint64
	}{{first, 1}, {first, 2}, {second, 3}, {second, 4}, {second, 5}} {
		keys := mark.parts.Get(mark.idx)
		require.True(t, mark.parts.MarkInvalid(&keys))
		keys.Release()
	}

	if got := testutil.ToFloat64(metrics.PartitionsInvalid); got != before+5 {
		t.Fatalf("partitions_invalid = %v, want %v: the gauge must hold both pools, not the last writer", got, before+5)
	}
}

// TestMarkInvalid_ReportsAReplacedPartitionOnce borrows keys, replaces the
// slot, and checks that only the first report of the old partition counts.
func TestMarkInvalid_ReportsAReplacedPartitionOnce(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 10)

	keys := parts.Get(2)
	old := parts.Replace(2)
	t.Cleanup(old.Release)

	require.True(t, parts.MarkInvalid(&keys), "the first report counts")
	require.False(t, parts.MarkInvalid(&keys), "a second report of the same partition does not")

	keys.Release()
	old.Release()

	_, tracked := parts.reportedRetired.Load(keys.ID)
	require.False(t, tracked, "the last release forgets the reported UUID")
}

// TestMarkInvalid_SkipsAReplacedInvalidPartition marks a partition invalid,
// replaces its slot, and checks that a held borrow does not report it again.
func TestMarkInvalid_SkipsAReplacedInvalidPartition(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 10)

	keys := parts.Get(3)
	require.True(t, parts.MarkInvalid(&keys), "the first report counts")

	old := parts.Replace(3)
	require.False(t, parts.MarkInvalid(&keys), "the replaced partition was already reported")

	keys.Release()
	old.Release()

	_, tracked := parts.reportedRetired.Load(keys.ID)
	require.False(t, tracked, "the last release forgets the reported UUID")
}

func TestBorrowByID_KeepsTheUUIDReportable(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 10)

	keys := parts.Get(6)
	id := keys.ID
	keys.Release()

	borrowed, ok := parts.BorrowByID(id)
	require.True(t, ok)

	parts.ReplaceWithoutOld(6)
	require.True(t, parts.MarkInvalid(&borrowed), "the borrow keeps the UUID mapped")
	borrowed.Release()

	_, ok = parts.BorrowByID(id)
	require.False(t, ok, "a replaced partition cannot be borrowed")
}

// TestBorrow_RegeneratesTheSameValues checks the seed contract: every borrow
// of a partition regenerates its key values, gets the same ones, and gets its
// own copy.
func TestBorrow_RegeneratesTheSameValues(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 4)

	first := parts.Get(1)
	second := parts.Get(1)
	t.Cleanup(first.Release)
	t.Cleanup(second.Release)

	require.Equal(t, first.Values.Get("pk1"), second.Values.Get("pk1"))
	require.Equal(t, first.Values.Get("pk2"), second.Values.Get("pk2"))

	first.Values.Get("pk1")[0] = int32(-1)
	require.NotEqual(t, int32(-1), second.Values.Get("pk1")[0], "borrows must not share a backing slice")

	old := parts.Replace(1)
	t.Cleanup(old.Release)
	replaced := parts.Get(1)
	t.Cleanup(replaced.Release)
	// pk2 is text, and this package does not fill the random string buffer, so
	// every text key is empty. The int key tells the two partitions apart.
	require.NotEqual(t, second.Values.Get("pk1"), replaced.Values.Get("pk1"), "a replacement gets new values")
}
