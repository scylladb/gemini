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

	"github.com/google/uuid"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/random"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

type (
	Interface interface {
		Stats() Stats
		Get(idx uint64) typedef.PartitionKeys
		Next() typedef.PartitionKeys

		Extend() typedef.PartitionKeys
		ReplaceNext() typedef.PartitionKeys
		Replace(idx uint64) typedef.PartitionKeys
		ReplaceWithoutOld(idx uint64)
		ReplaceNextWithoutOld()

		Deleted() <-chan typedef.PartitionKeys

		// Validation tracking
		ValidationSuccess(values *typedef.PartitionKeys)
		ValidationFailure(values *typedef.PartitionKeys)
		ValidationStats(id uuid.UUID) (first, last, failure uint64, recent []uint64)

		Len() uint64
		Close()
	}

	Partition struct {
		values   []any
		id       uuid.UUID
		refCount atomic.Int32
		mu       sync.RWMutex
	}

	partitions struct {
		parts              []*Partition
		partitionValuesLen uint64
		count              atomic.Uint64
		created            atomic.Uint64
		mu                 sync.RWMutex
	}

	Partitions struct {
		table         *typedef.Table
		idxFunc       distributions.DistributionFunc
		deleted       *deletedPartitions
		validationMap map[uuid.UUID]*typedef.ValidationData
		r             random.GoRoutineSafeRandom
		config        typedef.PartitionRangeConfig
		parts         partitions
		validationMu  sync.RWMutex
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
		deleted:       newDeleted(ctx, config.DeleteBuckets, false),
		r:             *random.NewGoRoutineSafeRandom(r),
		table:         table,
		config:        config,
		idxFunc:       idxFunc,
		validationMap: make(map[uuid.UUID]*typedef.ValidationData),
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

func (p *Partitions) valuesNoLock(part *Partition) typedef.PartitionKeys {
	out := make(map[string][]any, p.parts.partitionValuesLen)

	for i, col := range p.table.PartitionKeys {
		// Copy the slice to avoid data races when the underlying array is modified
		out[col.Name] = append(out[col.Name], part.values[i*col.Type.LenValue():(i+1)*col.Type.LenValue()]...)
	}

	part.refCount.Add(1)
	return typedef.PartitionKeys{
		Values: typedef.NewValuesFromMap(out),
		ID:     part.id,
		Release: func() {
			if part.refCount.Add(-1) == 0 {
				p.deleteValidation(part.id)
			}
		},
	}
}

func (p *Partitions) deleteValidation(id uuid.UUID) {
	p.validationMu.Lock()
	delete(p.validationMap, id)
	p.validationMu.Unlock()
}

func (p *Partitions) values(idx uint64) typedef.PartitionKeys {
	part := p.partition(idx)
	part.mu.RLock()
	// Copy the slice reference while holding the lock
	values := part.values
	id := part.id
	part.mu.RUnlock()

	// Build the result outside the lock to minimize contention
	out := make(map[string][]any, p.parts.partitionValuesLen)
	for i, col := range p.table.PartitionKeys {
		out[col.Name] = append(out[col.Name], values[i*col.Type.LenValue():(i+1)*col.Type.LenValue()]...)
	}

	part.refCount.Add(1)
	var once sync.Once
	return typedef.PartitionKeys{
		Values: typedef.NewValuesFromMap(out),
		ID:     id,
		Release: func() {
			once.Do(func() {
				if part.refCount.Add(-1) == 0 {
					p.deleteValidation(id)
				}
			})
		},
	}
}

func (p *Partitions) valuesCopy(idx uint64) typedef.PartitionKeys {
	part := p.partition(idx)
	part.mu.RLock()
	values := part.values
	id := part.id
	part.mu.RUnlock()

	// Build result outside lock
	m := make(map[string][]any, p.parts.partitionValuesLen)
	for i, col := range p.table.PartitionKeys {
		m[col.Name] = append(m[col.Name], values[i*col.Type.LenValue():(i+1)*col.Type.LenValue()]...)
	}

	part.refCount.Add(1)
	var once sync.Once
	return typedef.PartitionKeys{
		Values: typedef.NewValuesFromMap(m),
		ID:     id,
		Release: func() {
			once.Do(func() {
				if part.refCount.Add(-1) == 0 {
					p.deleteValidation(id)
				}
			})
		},
	}
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

	id, _ := uuid.NewV7()
	p.validationMu.Lock()
	if _, ok := p.validationMap[id]; !ok {
		p.validationMap[id] = &typedef.ValidationData{}
	}
	p.validationMu.Unlock()

	p.parts.mu.Lock()
	p.parts.parts[idx] = &Partition{
		values: values,
		id:     id,
	}
	p.parts.mu.Unlock()
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

func (p *Partitions) Get(idx uint64) typedef.PartitionKeys {
	return p.values(idx)
}

func (p *Partitions) Next() typedef.PartitionKeys {
	return p.Get(p.idxFunc())
}

func (p *Partitions) Extend() typedef.PartitionKeys {
	// Generate values outside the lock to minimize lock hold time
	values := generateValue(&p.r, p.table, &p.config)
	if uint64(len(values)) != p.parts.partitionValuesLen {
		panic(fmt.Sprintf(
			"invalid partition values generated: length %d, expected %d",
			len(values),
			p.parts.partitionValuesLen,
		))
	}

	id, _ := uuid.NewV7()
	p.validationMu.Lock()
	if _, ok := p.validationMap[id]; !ok {
		p.validationMap[id] = &typedef.ValidationData{}
	}
	p.validationMu.Unlock()

	// Hold lock while extending slice AND initializing the new partition
	// to prevent other goroutines from accessing it during reallocation
	p.parts.mu.Lock()
	newIdx := uint64(len(p.parts.parts))
	part := &Partition{values: values, id: id}
	p.parts.parts = append(p.parts.parts, part)
	p.parts.mu.Unlock()

	p.parts.count.Add(1)
	p.parts.created.Add(1)

	return p.valuesCopy(newIdx)
}

func (p *Partitions) Replace(idx uint64) typedef.PartitionKeys {
	values := generateValue(&p.r, p.table, &p.config)
	if uint64(len(values)) != p.parts.partitionValuesLen {
		panic(fmt.Sprintf(
			"invalid partition values generated: length %d, expected %d",
			len(values),
			p.parts.partitionValuesLen,
		))
	}

	id, _ := uuid.NewV7()
	p.validationMu.Lock()
	if _, ok := p.validationMap[id]; !ok {
		p.validationMap[id] = &typedef.ValidationData{}
	}
	p.validationMu.Unlock()

	p.parts.mu.Lock()
	oldPart := p.parts.parts[idx]
	p.parts.parts[idx] = &Partition{values: values, id: id}
	p.parts.mu.Unlock()

	oldKeys := p.valuesNoLock(oldPart)

	if p.deleted != nil {
		p.deleted.Delete(oldKeys)
	}
	return oldKeys
}

func (p *Partitions) Deleted() <-chan typedef.PartitionKeys {
	if p.deleted == nil {
		return nil
	}

	return p.deleted.ch
}

func (p *Partitions) ReplaceNext() typedef.PartitionKeys {
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

func (p *Partitions) ValidationSuccess(keys *typedef.PartitionKeys) {
	if keys == nil {
		return
	}
	now := uint64(time.Now().UTC().UnixNano())
	p.validationMu.RLock()
	data, ok := p.validationMap[keys.ID]
	p.validationMu.RUnlock()
	if !ok {
		return
	}
	data.FirstSuccessNS.CompareAndSwap(0, now)
	data.LastSuccessNS.Store(now)
	pos := data.RecentIdx.Add(1) - 1
	data.Recent[pos%uint64(len(data.Recent))].Store(now)
}

func (p *Partitions) ValidationFailure(keys *typedef.PartitionKeys) {
	if keys == nil {
		return
	}
	now := uint64(time.Now().UTC().UnixNano())
	p.validationMu.RLock()
	data, ok := p.validationMap[keys.ID]
	p.validationMu.RUnlock()
	if !ok {
		return
	}
	data.LastFailureNS.Store(now)
}

func (p *Partitions) ValidationStats(id uuid.UUID) (first, last, failure uint64, recent []uint64) {
	p.validationMu.RLock()
	data, ok := p.validationMap[id]
	p.validationMu.RUnlock()
	if !ok {
		return 0, 0, 0, nil
	}
	first = data.FirstSuccessNS.Load()
	last = data.LastSuccessNS.Load()
	failure = data.LastFailureNS.Load()
	recent = make([]uint64, 0, len(data.Recent))
	for i := range data.Recent {
		val := data.Recent[i].Load()
		if val != 0 {
			recent = append(recent, val)
		}
	}
	return first, last, failure, recent
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
