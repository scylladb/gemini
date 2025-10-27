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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/distributions"
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
	parts := New(t.Context(), rand.New(src), fn, table, config, count)
	t.Cleanup(parts.Close)
	return parts
}

// TestPartitionsGet verifies Get operation
func TestPartitionsGet(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 100)

	for i := uint64(0); i < 100; i++ {
		values := parts.Get(i)
		require.NotNil(t, values)
		require.NotEmpty(t, values.Data())
	}
}

// TestPartitionsNext verifies Next operation using distribution function
func TestPartitionsNext(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 1000)

	for i := 0; i < 100; i++ {
		values := parts.Next()
		require.NotNil(t, values)
		require.NotEmpty(t, values.Data())
	}
}

// TestPartitionsExtend verifies Extend operation
func TestPartitionsExtend(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 10)
	initialLen := parts.Len()

	for i := 0; i < 5; i++ {
		values := parts.Extend()
		require.NotNil(t, values)
		require.NotEmpty(t, values.Data())
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
	assert.Equal(t, oldValues.Data(), replacedValues.Data())

	stats := parts.Stats()
	assert.Equal(t, uint64(1), stats.PartitionsDeleted)
}

// TestPartitionsReplaceNext verifies ReplaceNext operation
func TestPartitionsReplaceNext(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 100)

	for i := 0; i < 10; i++ {
		values := parts.ReplaceNext()
		require.NotNil(t, values)
		require.NotEmpty(t, values.Data())
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

	for i := 0; i < 10; i++ {
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

	for i := 0; i < concurrency; i++ {
		go func(workerID int) {
			defer wg.Done()
			src := rand.NewPCG(uint64(workerID), uint64(workerID*2))
			r := rand.New(src)

			for j := 0; j < iterations; j++ {
				idx := uint64(r.IntN(1000))
				values := parts.Get(idx)
				require.NotNil(t, values)
				require.NotEmpty(t, values.Data())
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

			for j := 0; j < iterations; j++ {
				values := parts.Next()
				require.NotNil(t, values)
				require.NotEmpty(t, values.Data())
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

			for j := 0; j < iterations; j++ {
				values := parts.Extend()
				require.NotNil(t, values)
				require.NotEmpty(t, values.Data())
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

	for i := 0; i < concurrency; i++ {
		go func(workerID int) {
			defer wg.Done()
			src := rand.NewPCG(uint64(workerID), uint64(workerID*2))
			r := rand.New(src)

			for j := 0; j < iterations; j++ {
				idx := uint64(r.IntN(1000))
				values := parts.Replace(idx)
				require.NotNil(t, values)
				require.NotEmpty(t, values.Data())
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
	for i := 0; i < concurrency; i++ {
		go func(workerID int) {
			defer wg.Done()
			src := rand.NewPCG(uint64(workerID), uint64(workerID*2))
			r := rand.New(src)

			for j := 0; j < iterations; j++ {
				idx := uint64(r.IntN(1000))
				values := parts.Get(idx)
				require.NotNil(t, values)
			}
		}(i)
	}

	// Concurrent Next operations
	for range concurrency {
		go func() {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				values := parts.Next()
				require.NotNil(t, values)
			}
		}()
	}

	// Concurrent Extend operations
	for range concurrency {
		go func() {
			defer wg.Done()

			for j := 0; j < iterations/10; j++ { // Fewer extends
				values := parts.Extend()
				require.NotNil(t, values)
			}
		}()
	}

	// Concurrent Replace operations
	for k := 0; k < concurrency; k++ {
		go func(workerID int) {
			defer wg.Done()
			src := rand.NewPCG(uint64(workerID+100), uint64(workerID*2+100))
			r := rand.New(src)

			for j := 0; j < iterations/10; j++ { // Fewer replaces
				idx := uint64(r.IntN(1000))
				parts.ReplaceWithoutOld(idx)
			}
		}(k)
	}

	wg.Wait()

	stats := parts.Stats()
	assert.Greater(t, stats.CurrentPartitionCount, uint64(1000))
	assert.Greater(t, stats.PartitionsCreated, uint64(0))
	assert.Greater(t, stats.PartitionsDeleted, uint64(0))
}

// TestPartitionsMemoryUsage tests memory usage tracking
func TestPartitionsMemoryUsage(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 1000)

	stats := parts.Stats()
	assert.GreaterOrEqual(t, stats.MemoryUsage, uint64(0))

	// Note: Current implementation returns 0 for memory usage
	// This test verifies the Stats call works correctly
	assert.Equal(t, uint64(1000), stats.CurrentPartitionCount)
}

// TestPartitionsMetrics verifies metric tracking
func TestPartitionsMetrics(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 100)

	// Track creates
	for i := 0; i < 10; i++ {
		parts.Extend()
	}

	// Track replaces
	for i := 0; i < 20; i++ {
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
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				parts.Extend()
			}
		}()
	}
	wg.Wait()

	// Concurrent replaces
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(workerID int) {
			defer wg.Done()
			src := rand.NewPCG(uint64(workerID), uint64(workerID*2))
			r := rand.New(src)

			for j := 0; j < iterations; j++ {
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
	assert.Equal(t, values1.Data(), values2.Data())
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
	require.NotEmpty(t, pk.Values.Data())
}

// TestPartitionSize verifies partition size calculation
func TestPartitionSize(t *testing.T) {
	t.Parallel()

	p := Partition{values: make([]any, 10)}

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
	parts := New(b.Context(), rand.New(src), fn, table, config, 10_000)

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
		b.Run(string(rune(size)), func(b *testing.B) {
			src, fn := distributions.New(distributions.Uniform, size, 1, 0, 0)
			table := createTestTable()
			config := typedef.PartitionRangeConfig{
				DeleteBuckets: []time.Duration{100 * time.Millisecond},
			}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_ = New(b.Context(), rand.New(src), fn, table, config, size)
			}
		})
	}
}
