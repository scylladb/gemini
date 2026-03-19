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
		ValidationStats(id uuid.UUID) (first, last, failure uint64, recent []uint64, successCount uint64)

		// Invalid partition tracking.
		// MarkInvalid marks the partition identified by keys as permanently invalid.
		// It is a non-blocking, idempotent operation — only the first caller for a
		// given partition index succeeds (returns true); subsequent calls return false.
		// Returns false when the partition was already invalid or when the maximum
		// number of invalid partitions has been reached.
		MarkInvalid(keys *typedef.PartitionKeys) bool
		// IsInvalid reports whether the partition at idx is invalid.
		IsInvalid(idx uint64) bool
		// InvalidCount returns the current number of permanently invalid partitions.
		InvalidCount() uint64

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
		uuidToIdx     map[uuid.UUID]uint64
		invalidByIdx  sync.Map
		r             random.GoRoutineSafeRandom
		config        typedef.PartitionRangeConfig
		parts         partitions
		invalidCount  atomic.Uint64
		maxInvalid    uint64
		validationMu  sync.RWMutex
	}

	Stats struct {
		CurrentPartitionCount  uint64
		PartitionsCreated      uint64
		PartitionsDeleted      uint64
		DeletedPartitionsCount uint64
		InvalidPartitionsCount uint64
		MemoryUsage            uint64
	}
)

func New(
	ctx context.Context,
	r rand.Source,
	idxFunc distributions.DistributionFunc,
	table *typedef.Table,
	config typedef.PartitionRangeConfig,
	count, maxInvalid uint64,
) *Partitions {
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
		uuidToIdx:     make(map[uuid.UUID]uint64),
		maxInvalid:    maxInvalid,
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
	// Do NOT remove from uuidToIdx here. The UUID→idx mapping must remain alive
	// for the entire lifetime of the partition at that slot so that MarkInvalid
	// can still find it after all outstanding references have been released.
	// uuidToIdx is cleaned up in fill() / Replace() when a new partition occupies
	// the slot and the old UUID is genuinely retired.
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

	// Capture the old partition's UUID before replacing the slot so we can
	// retire it from uuidToIdx.  During initial population the old *Partition
	// is the zero-value struct whose id is uuid.Nil, which is never inserted
	// into uuidToIdx, so we guard with a nil check.
	p.parts.mu.RLock()
	oldPart := p.parts.parts[idx]
	p.parts.mu.RUnlock()

	p.validationMu.Lock()
	if _, ok := p.validationMap[id]; !ok {
		p.validationMap[id] = &typedef.ValidationData{}
	}
	p.uuidToIdx[id] = idx
	// Retire the old UUID from the slot mapping now that a new partition owns it.
	if oldPart != nil && oldPart.id != (uuid.UUID{}) {
		delete(p.uuidToIdx, oldPart.id)
	}
	p.validationMu.Unlock()

	p.parts.mu.Lock()
	p.parts.parts[idx] = &Partition{
		values: values,
		id:     id,
	}
	p.parts.mu.Unlock()

	// If this slot was previously marked invalid, clear it so the new partition
	// is visible to Next()/ReplaceNext() again.
	if _, wasInvalid := p.invalidByIdx.LoadAndDelete(idx); wasInvalid {
		p.invalidCount.Add(^uint64(0)) // atomic decrement
	}
}

func (p *Partitions) Stats() Stats {
	count := p.parts.count.Load()
	mem := uint64(0)
	for i := range count {
		mem += p.partition(i).size()
	}

	invalidCount := p.invalidCount.Load()

	if p.deleted != nil {
		return Stats{
			CurrentPartitionCount:  count,
			PartitionsCreated:      p.parts.created.Load(),
			PartitionsDeleted:      p.deleted.deleted.Load(),
			DeletedPartitionsCount: uint64(p.deleted.Len()),
			InvalidPartitionsCount: invalidCount,
			MemoryUsage:            mem,
		}
	}

	return Stats{
		CurrentPartitionCount:  count,
		PartitionsCreated:      p.parts.created.Load(),
		InvalidPartitionsCount: invalidCount,
		MemoryUsage:            mem,
	}
}

func (p *Partitions) Get(idx uint64) typedef.PartitionKeys {
	return p.values(idx)
}

// pickValidIdx returns the index of a valid (non-invalid) partition.
// Strategy (in order):
//  1. Fast-path: if no invalid partitions, call the distribution function directly.
//  2. Linear scan: when more than half the slots are invalid, scan sequentially
//     so we always find a valid slot deterministically.
//  3. Random retry: up to (total − invalid) attempts before a best-effort fallback.
func (p *Partitions) pickValidIdx() uint64 {
	total := p.parts.count.Load()
	invalid := p.invalidCount.Load()

	if invalid == 0 {
		return p.idxFunc()
	}

	if invalid*2 >= total {
		for i := range total {
			if _, bad := p.invalidByIdx.Load(i); !bad {
				return i
			}
		}
		// All slots invalid — best-effort fallback.
		return p.idxFunc()
	}

	maxRetries := total - invalid
	for range maxRetries {
		idx := p.idxFunc()
		if _, bad := p.invalidByIdx.Load(idx); !bad {
			return idx
		}
	}
	return p.idxFunc()
}

// Next returns the next partition according to the distribution function.
// If the selected partition has been marked invalid it is skipped and a new
// one is picked. The retry budget is the total number of partitions minus the
// number already marked invalid, so the loop always terminates.
func (p *Partitions) Next() typedef.PartitionKeys {
	return p.Get(p.pickValidIdx())
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

	// Hold lock while extending slice AND initializing the new partition
	// to prevent other goroutines from accessing it during reallocation
	p.parts.mu.Lock()
	newIdx := uint64(len(p.parts.parts))
	part := &Partition{values: values, id: id}
	p.parts.parts = append(p.parts.parts, part)
	p.parts.mu.Unlock()

	p.validationMu.Lock()
	if _, ok := p.validationMap[id]; !ok {
		p.validationMap[id] = &typedef.ValidationData{}
	}
	p.uuidToIdx[id] = newIdx
	p.validationMu.Unlock()

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
	p.uuidToIdx[id] = idx
	p.validationMu.Unlock()

	p.parts.mu.Lock()
	oldPart := p.parts.parts[idx]
	p.parts.parts[idx] = &Partition{values: values, id: id}
	p.parts.mu.Unlock()

	// Do NOT explicitly delete oldPart.id from uuidToIdx here.
	// Outstanding references to the old partition (e.g. a validation goroutine
	// mid-retry) still hold the old UUID. Removing it prematurely means
	// MarkInvalid() can no longer locate the partition by UUID and silently
	// drops the error. The old UUID will be cleaned up naturally when the last
	// caller invokes Release() and refCount drops to zero (via deleteValidation).

	// If the slot was previously marked invalid, clear it so the new partition
	// is visible to Next()/ReplaceNext() again.
	if _, wasInvalid := p.invalidByIdx.LoadAndDelete(idx); wasInvalid {
		p.invalidCount.Add(^uint64(0)) // atomic decrement
	}

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

// ReplaceNext picks a partition by distribution and replaces it, sending
// the old keys to the deleted queue. Invalid partitions are skipped.
func (p *Partitions) ReplaceNext() typedef.PartitionKeys {
	return p.Replace(p.pickValidIdx())
}

func (p *Partitions) ReplaceWithoutOld(idx uint64) {
	p.fill(idx)
	if p.deleted != nil {
		p.deleted.deleted.Add(1)
	}
}

func (p *Partitions) ReplaceNextWithoutOld() {
	p.fill(p.pickValidIdx())
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
	data.SuccessCount.Add(1)
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

func (p *Partitions) ValidationStats(id uuid.UUID) (first, last, failure uint64, recent []uint64, successCount uint64) {
	p.validationMu.RLock()
	data, ok := p.validationMap[id]
	p.validationMu.RUnlock()
	if !ok {
		return 0, 0, 0, nil, 0
	}
	first = data.FirstSuccessNS.Load()
	last = data.LastSuccessNS.Load()
	failure = data.LastFailureNS.Load()
	successCount = data.SuccessCount.Load()
	recent = make([]uint64, 0, len(data.Recent))
	for i := range data.Recent {
		val := data.Recent[i].Load()
		if val != 0 {
			recent = append(recent, val)
		}
	}
	return first, last, failure, recent, successCount
}

// MarkInvalid permanently marks the partition identified by keys as invalid.
//
// The operation is non-blocking and idempotent: the first goroutine to call it
// for a given partition index atomically claims the slot and returns true.
// Every subsequent call for the same slot returns false immediately.
//
// If maxInvalid is configured and the limit has already been reached the call
// also returns false and the partition is NOT marked (the caller should treat
// this situation as a hard stop trigger — too many bad partitions).
func (p *Partitions) MarkInvalid(keys *typedef.PartitionKeys) bool {
	if keys == nil {
		return false
	}

	// Look up the slot index for this UUID.
	p.validationMu.RLock()
	idx, ok := p.uuidToIdx[keys.ID]
	p.validationMu.RUnlock()
	if !ok {
		// UUID not tracked (partition slot was replaced) — nothing to mark.
		return false
	}

	// Reserve a slot in the counter first using a CAS loop. This avoids the
	// TOCTOU window where multiple goroutines each increment past maxInvalid
	// and then all roll back, leaving zero slots marked.
	for {
		cur := p.invalidCount.Load()
		if p.maxInvalid > 0 && cur >= p.maxInvalid {
			return false
		}
		if p.invalidCount.CompareAndSwap(cur, cur+1) {
			break
		}
	}

	// Counter slot reserved. Now atomically claim the map entry.
	// LoadOrStore returns (existing, true) if already present.
	_, alreadyInvalid := p.invalidByIdx.LoadOrStore(idx, struct{}{})
	if alreadyInvalid {
		// Another goroutine beat us to this slot — release our counter reservation.
		p.invalidCount.Add(^uint64(0)) // atomic decrement
		return false
	}

	return true
}

// IsInvalid reports whether the partition at the given index has been permanently
// marked invalid.
func (p *Partitions) IsInvalid(idx uint64) bool {
	_, bad := p.invalidByIdx.Load(idx)
	return bad
}

// InvalidCount returns the current number of permanently invalid partitions.
func (p *Partitions) InvalidCount() uint64 {
	return p.invalidCount.Load()
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
