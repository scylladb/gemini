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
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/random"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

type (
	Interface interface {
		Stats() Stats
		Get(idx uint64) *typedef.Values
		Next() *typedef.Values

		Extend() *typedef.Values
		ReplaceNext() *typedef.Values
		Replace(idx uint64) *typedef.Values
		ReplaceWithoutOld(idx uint64)
		ReplaceNextWithoutOld()

		Deleted() <-chan *typedef.Values

		Len() uint64
		Close()
	}

	Partition struct {
		values []any
		mu     sync.RWMutex
	}

	partitions struct {
		parts              []*Partition
		partitionValuesLen uint64
		count              atomic.Uint64
		created            atomic.Uint64
		mu                 sync.RWMutex
	}

	Partitions struct {
		table   *typedef.Table
		idxFunc distributions.DistributionFunc
		deleted *deletedPartitions
		r       random.GoRoutineSafeRandom
		config  typedef.PartitionRangeConfig
		parts   partitions
	}

	Stats struct {
		CurrentPartitionCount  uint64
		PartitionsCreated      uint64
		PartitionsDeleted      uint64
		DeletedPartitionsCount uint64
		MemoryUsage            uint64
	}
)

func New(ctx context.Context, r rand.Source, idxFunc distributions.DistributionFunc, table *typedef.Table, config typedef.PartitionRangeConfig, count uint64) *Partitions {
	partitionValuesLen := table.PartitionKeys.LenValues()

	// Pre-allocate slice of partition pointers
	parts := make([]*Partition, count)
	for i := range count {
		parts[i] = &Partition{}
	}

	p := &Partitions{
		parts: partitions{
			partitionValuesLen: uint64(partitionValuesLen),
			parts:              parts,
		},
		deleted: newDeleted(ctx, config.DeleteBuckets, false),
		r:       *random.NewGoRoutineSafeRandom(r),
		table:   table,
		config:  config,
		idxFunc: idxFunc,
	}

	// Only start deleted-partitions processing when time buckets are configured.
	// When no buckets are provided, the feature is fully disabled (no goroutines,
	// nil channel), so regular validation proceeds unaffected.
	if len(config.DeleteBuckets) > 0 {
		go p.deleted.start(100 * time.Millisecond)
	}

	p.parts.count.Store(count)

	for i := range count {
		p.fill(i)
	}

	return p
}

func (p *Partition) size() uint64 {
	return 0
}

func (p *Partitions) partition(idx uint64) *Partition {
	p.parts.mu.RLock()
	part := p.parts.parts[idx]
	p.parts.mu.RUnlock()
	return part
}

func (p *Partitions) valuesNoLock(part *Partition) *typedef.Values {
	out := make(map[string][]any, p.parts.partitionValuesLen)

	for i, col := range p.table.PartitionKeys {
		// Copy the slice to avoid data races when the underlying array is modified
		out[col.Name] = append(out[col.Name], part.values[i*col.Type.LenValue():(i+1)*col.Type.LenValue()]...)
	}

	return typedef.NewValuesFromMap(out)
}

func (p *Partitions) values(idx uint64) *typedef.Values {
	part := p.partition(idx)
	part.mu.RLock()
	// Copy the slice reference while holding the lock
	values := part.values
	part.mu.RUnlock()

	// Build the result outside the lock to minimize contention
	out := make(map[string][]any, p.parts.partitionValuesLen)
	for i, col := range p.table.PartitionKeys {
		out[col.Name] = append(out[col.Name], values[i*col.Type.LenValue():(i+1)*col.Type.LenValue()]...)
	}

	return typedef.NewValuesFromMap(out)
}

func (p *Partitions) valuesCopy(idx uint64) *typedef.Values {
	part := p.partition(idx)
	part.mu.RLock()
	values := part.values
	part.mu.RUnlock()

	// Build result outside lock
	m := make(map[string][]any, p.parts.partitionValuesLen)
	for i, col := range p.table.PartitionKeys {
		m[col.Name] = append(m[col.Name], values[i*col.Type.LenValue():(i+1)*col.Type.LenValue()]...)
	}

	return typedef.NewValuesFromMap(m)
}

func (p *Partitions) fill(idx uint64) {
	values := generateValue(&p.r, p.table, &p.config)

	if uint64(len(values)) != p.parts.partitionValuesLen {
		panic(fmt.Sprintf(
			"invalid partition values generated: length %d, expected %d",
			len(values),
			p.parts.partitionValuesLen,
		))
	}

	part := p.partition(idx)
	part.mu.Lock()
	part.values = values
	part.mu.Unlock()
}

func (p *Partitions) Stats() Stats {
	count := p.parts.count.Load()
	mem := uint64(0)
	for i := range count {
		mem += p.partition(i).size()
	}

	if p.deleted != nil {
		return Stats{
			CurrentPartitionCount:  count,
			PartitionsCreated:      p.parts.created.Load(),
			PartitionsDeleted:      p.deleted.deleted.Load(),
			DeletedPartitionsCount: uint64(p.deleted.Len()),
			MemoryUsage:            mem,
		}
	}

	return Stats{
		CurrentPartitionCount: count,
		PartitionsCreated:     p.parts.created.Load(),
		MemoryUsage:           mem,
	}
}

func (p *Partitions) Get(idx uint64) *typedef.Values {
	return p.values(idx)
}

func (p *Partitions) Next() *typedef.Values {
	return p.Get(p.idxFunc())
}

func (p *Partitions) Extend() *typedef.Values {
	// Generate values outside the lock to minimize lock hold time
	values := generateValue(&p.r, p.table, &p.config)
	if uint64(len(values)) != p.parts.partitionValuesLen {
		panic(fmt.Sprintf(
			"invalid partition values generated: length %d, expected %d",
			len(values),
			p.parts.partitionValuesLen,
		))
	}

	// Hold lock while extending slice AND initializing the new partition
	// to prevent other goroutines from accessing it during reallocation
	p.parts.mu.Lock()
	newIdx := uint64(len(p.parts.parts))
	p.parts.parts = append(p.parts.parts, &Partition{values: values})
	p.parts.mu.Unlock()

	p.parts.count.Add(1)
	p.parts.created.Add(1)

	return p.valuesCopy(newIdx)
}

func (p *Partitions) Replace(idx uint64) *typedef.Values {
	values := generateValue(&p.r, p.table, &p.config)
	if uint64(len(values)) != p.parts.partitionValuesLen {
		panic(fmt.Sprintf(
			"invalid partition values generated: length %d, expected %d",
			len(values),
			p.parts.partitionValuesLen,
		))
	}

	part := p.partition(idx)
	part.mu.Lock()
	oldValues := p.valuesNoLock(part)
	part.values = values
	part.mu.Unlock()

	if p.deleted != nil {
		p.deleted.Delete(oldValues)
	}
	return oldValues
}

func (p *Partitions) Deleted() <-chan *typedef.Values {
	if p.deleted == nil {
		return nil
	}

	return p.deleted.ch
}

func (p *Partitions) ReplaceNext() *typedef.Values {
	return p.Replace(p.idxFunc())
}

func (p *Partitions) ReplaceWithoutOld(idx uint64) {
	p.fill(idx)
	if p.deleted != nil {
		p.deleted.deleted.Add(1)
	}
}

func (p *Partitions) ReplaceNextWithoutOld() {
	p.fill(p.idxFunc())
	if p.deleted != nil {
		p.deleted.deleted.Add(1)
	}
}

func (p *Partitions) Len() uint64 {
	return p.parts.count.Load()
}

func (p *Partitions) Close() {
	if p.deleted != nil {
		p.deleted.Close()
	}
}

func generateValue(r utils.Random, table *typedef.Table, config typedef.RangeConfig) []any {
	values := make([]any, 0, table.PartitionKeys.LenValues())

	for _, pk := range table.PartitionKeys {
		values = pk.Type.GenValueOut(values, r, config)
	}

	return values
}

func NewPartitionKeys(r utils.Random, table *typedef.Table, config typedef.RangeConfig) typedef.PartitionKeys {
	values := generateValue(r, table, config)

	m := make(map[string][]any, table.PartitionKeys.LenValues())

	for i, col := range table.PartitionKeys {
		m[col.Name] = append(m[col.Name], values[i*col.Type.LenValue():(i+1)*col.Type.LenValue()]...)
	}

	return typedef.PartitionKeys{
		Values: typedef.NewValuesFromMap(m),
	}
}
