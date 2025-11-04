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
	}

	partitions struct {
		parts              []any
		partitionValuesLen uint64
		count              atomic.Uint64
		created            atomic.Uint64
		mu                 sync.RWMutex
	}

	Partitions struct {
		table   *typedef.Table
		idxFunc distributions.DistributionFunc
		r       random.GoRoutineSafeRandom
		config  typedef.PartitionRangeConfig
		parts   partitions
		deleted deletedPartitions
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

	p := &Partitions{
		parts: partitions{
			partitionValuesLen: uint64(partitionValuesLen),
			parts:              make([]any, count*uint64(partitionValuesLen)),
		},
		deleted: *newDeleted(ctx, config.DeleteBuckets, false),
		r:       *random.NewGoRoutineSafeRandom(r),
		table:   table,
		config:  config,
		idxFunc: idxFunc,
	}

	go p.deleted.start(100 * time.Millisecond)

	p.parts.count.Store(count)

	for i := range count {
		p.fill(i)
	}

	return p
}

func (p Partition) size() uint64 {
	return 0
}

func (p *Partitions) partition(idx uint64) Partition {
	return Partition{
		values: p.parts.parts[idx*p.parts.partitionValuesLen : (idx*p.parts.partitionValuesLen)+p.parts.partitionValuesLen],
	}
}

func (p *Partitions) values(idx uint64) *typedef.Values {
	m := make(map[string][]any, p.parts.partitionValuesLen)

	p.parts.mu.RLock()
	values := p.partition(idx).values
	for i, col := range p.table.PartitionKeys {
		// Copy the slice to avoid data races when the underlying array is modified
		m[col.Name] = append(m[col.Name], values[i*col.Type.LenValue():(i+1)*col.Type.LenValue()]...)
	}
	p.parts.mu.RUnlock()

	return typedef.NewValuesFromMap(m)
}

func (p *Partitions) valuesCopy(idx uint64) *typedef.Values {
	m := make(map[string][]any, p.parts.partitionValuesLen)

	p.parts.mu.RLock()
	values := p.partition(idx).values
	for i, col := range p.table.PartitionKeys {
		m[col.Name] = append(m[col.Name], values[i*col.Type.LenValue():(i+1)*col.Type.LenValue()]...)
	}
	p.parts.mu.RUnlock()

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

	p.parts.mu.Lock()
	copy(p.partition(idx).values, values)
	p.parts.mu.Unlock()
}

func (p *Partitions) Stats() Stats {
	p.parts.mu.RLock()
	defer p.parts.mu.RUnlock()

	mem := uint64(0)
	for i := range p.parts.count.Load() {
		mem += p.partition(i).size()
	}

	return Stats{
		CurrentPartitionCount:  p.parts.count.Load(),
		PartitionsCreated:      p.parts.created.Load(),
		PartitionsDeleted:      p.deleted.deleted.Load(),
		DeletedPartitionsCount: uint64(p.deleted.Len()),
		MemoryUsage:            mem,
	}
}

func (p *Partitions) Get(idx uint64) *typedef.Values {
	return p.values(idx)
}

func (p *Partitions) Next() *typedef.Values {
	return p.Get(p.idxFunc())
}

func (p *Partitions) Extend() *typedef.Values {
	p.parts.mu.Lock()
	p.parts.parts = append(p.parts.parts, make([]any, p.parts.partitionValuesLen)...)
	p.parts.mu.Unlock()

	value := p.parts.count.Add(1)
	p.fill(value - 1)

	p.parts.created.Add(1)

	return p.values(value - 1)
}

func (p *Partitions) Replace(idx uint64) *typedef.Values {
	oldValues := p.valuesCopy(idx)
	p.fill(idx)

	p.deleted.Delete(oldValues)
	return oldValues
}

func (p *Partitions) Deleted() <-chan *typedef.Values {
	return p.deleted.ch
}

func (p *Partitions) ReplaceNext() *typedef.Values {
	return p.Replace(p.idxFunc())
}

func (p *Partitions) ReplaceWithoutOld(idx uint64) {
	p.fill(idx)
	p.deleted.deleted.Add(1)
}

func (p *Partitions) ReplaceNextWithoutOld() {
	p.fill(p.idxFunc())
	p.deleted.deleted.Add(1)
}

func (p *Partitions) Len() uint64 {
	return p.parts.count.Load()
}

func (p *Partitions) Close() {
	p.deleted.Close()
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
