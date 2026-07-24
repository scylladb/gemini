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

		// Row tracking for targeted single-row deletions.
		// TrackRow stores a row observed during validation for later deletion.
		TrackRow(row TrackedRow)
		// PopTrackedRow retrieves a previously tracked row for deletion.
		// Returns false if no tracked rows are available.
		PopTrackedRow() (TrackedRow, bool)
		// TrackedRowCount returns the number of currently tracked rows.
		TrackedRowCount() uint64
		// RowTrackerFillRatio returns the fill level of the row tracker as a
		// value in [0, 1]. Used by the validation job to select the appropriate
		// sampling zone (always-push / sampled / skip).
		RowTrackerFillRatio() float64

		Len() uint64
		Close()
	}

	// Partition holds one partition slot's data. The values/id fields are
	// immutable after construction: fill/Replace/Extend install a brand-new
	// *Partition into the slot under partitions.mu rather than mutating an
	// existing one. That pointer swap (and the synchronization on partitions.mu
	// when readers load the pointer) is the only ordering needed, so reads of
	// values/id require no per-partition lock. refCount stays atomic because it
	// is mutated in place across the partition's lifetime.
	//
	// retired is set (once, via fill/Replace) when this *Partition is evicted
	// from its slot. Its validation/uuid mappings are removed only when the last
	// outstanding reference is released AND retired is set, so a live partition
	// whose refCount merely cycles to zero between borrows never loses its
	// validation data, while a replaced partition is reliably cleaned up exactly
	// once (regardless of which borrow happens to release last).
	Partition struct {
		values   []any
		id       uuid.UUID
		refCount atomic.Int32
		retired  atomic.Bool
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
		rowTracker    *RowTracker
		validationMap map[uuid.UUID]*typedef.ValidationData
		uuidToIdx     map[uuid.UUID]uint64
		invalidByIdx  sync.Map
		r             random.GoRoutineSafeRandom
		parts         partitions
		config        typedef.PartitionRangeConfig
		invalidCount  atomic.Uint64
		maxInvalid    uint64
		validationMu  sync.RWMutex
		// invalidMu serializes mutations of the invalid bookkeeping
		// (invalidByIdx + invalidCount) between MarkInvalid and the clear paths
		// in fill/Replace. Reads (pickValidIdx/IsInvalid/Stats) stay lock-free on
		// the atomic count and sync.Map; only the rare write paths take this lock.
		invalidMu sync.Mutex
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
	partitionValuesLen := table.PartitionKeysLenValues()

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
		deleted:       newDeleted(ctx, config.DeleteBuckets, config.MaxDeletedHeapSize),
		rowTracker:    NewRowTracker(uint64(config.RowTrackerCapacity)),
		r:             *random.NewGoRoutineSafeRandom(r),
		table:         table,
		config:        config,
		idxFunc:       idxFunc,
		validationMap: make(map[uuid.UUID]*typedef.ValidationData),
		uuidToIdx:     make(map[uuid.UUID]uint64),
		maxInvalid:    maxInvalid,
	}

	// NOTE: deleted-partitions background goroutine is started internally by
	// newDeleted() when timeBuckets is non-empty. Do NOT start it again here.

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

// newKeys takes one reference on part and returns partition keys whose Release
// drops that reference exactly once (guarded by a per-borrow sync.Once). The
// UUID's validation/idx mappings are removed only when the final reference is
// released AND the partition has been retired (evicted from its slot). A live
// partition whose refCount transiently reaches zero between borrows keeps its
// validation data; a retired partition is cleaned up exactly once, by whichever
// borrow releases last.
func (p *Partitions) newKeys(part *Partition) typedef.PartitionKeys {
	part.refCount.Add(1)
	id := part.id
	var once sync.Once
	return typedef.PartitionKeys{
		Values: typedef.NewPartitionValues(p.table.PartitionKeys, part.values),
		ID:     id,
		Release: func() {
			once.Do(func() {
				if part.refCount.Add(-1) == 0 && part.retired.Load() {
					p.deleteValidation(id)
				}
			})
		},
	}
}

// deleteValidation removes both the validation metadata and the UUID→idx
// mapping for a partition UUID. It is called only when a retired partition's
// last reference is released, so no validator can still be holding this UUID.
// Without this cleanup the maps grow unbounded — every production DELETE would
// leak one entry forever (root cause of the 2026-04-30 partitions memory
// growth).
func (p *Partitions) deleteValidation(id uuid.UUID) {
	p.validationMu.Lock()
	delete(p.validationMap, id)
	delete(p.uuidToIdx, id)
	p.validationMu.Unlock()
}

func (p *Partitions) values(idx uint64) typedef.PartitionKeys {
	// values/id are immutable for the lifetime of this *Partition (see the
	// Partition doc comment), so they can be read without a per-partition lock
	// and the returned Values may share capped sub-slices of part.values.
	return p.newKeys(p.partition(idx))
}

func (p *Partitions) valuesCopy(idx uint64) typedef.PartitionKeys {
	return p.newKeys(p.partition(idx))
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
	// fill() hands out no Release closure for the old partition, so it cleans
	// both maps here directly (rather than deferring to a reference release).
	if oldPart != nil && oldPart.id != (uuid.UUID{}) {
		delete(p.uuidToIdx, oldPart.id)
		delete(p.validationMap, oldPart.id)
	}
	p.validationMu.Unlock()

	// Mark the evicted partition retired so any borrow still outstanding on it
	// (e.g. a concurrent ReplaceWithoutOld racing a Get) cleans up on release
	// instead of resurrecting stale mappings.
	if oldPart != nil {
		oldPart.retired.Store(true)
	}

	p.parts.mu.Lock()
	p.parts.parts[idx] = &Partition{
		values: values,
		id:     id,
	}
	p.parts.mu.Unlock()

	// If this slot was previously marked invalid, clear it so the new partition
	// is visible to Next()/ReplaceNext() again.
	p.clearInvalid(idx)
}

// clearInvalid removes idx from the invalid set (if present) and decrements the
// invalid counter, serialized against MarkInvalid via invalidMu.
func (p *Partitions) clearInvalid(idx uint64) {
	p.invalidMu.Lock()
	if _, wasInvalid := p.invalidByIdx.LoadAndDelete(idx); wasInvalid {
		p.invalidCount.Add(^uint64(0)) // atomic decrement
	}
	p.invalidMu.Unlock()
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

	// Mark the evicted partition retired. Its validationMap/uuidToIdx entries
	// are removed only when its last reference is released — the caller's
	// returned keys AND, when present, the deleted-partitions heap's reference.
	// The heap releases its reference through the validator's Release closure,
	// which runs only after re-validation completes, so MarkInvalid can still
	// locate the UUID while a divergence on the deleted partition is being
	// reported. Not marking it at all (the pre-refCount behavior) leaked one
	// entry per production DELETE forever — see deleteValidation.
	oldPart.retired.Store(true)

	// If the slot was previously marked invalid, clear it so the new partition
	// is visible to Next()/ReplaceNext() again.
	p.clearInvalid(idx)

	oldKeys := p.newKeys(oldPart)

	if p.deleted != nil {
		// Take a second, independent reference on behalf of the deleted-
		// partition heap. Each reference has its own Release/sync.Once, so the
		// caller's release and the heap's release both count down refCount and
		// the UUID is retired only once the last of the two fires.
		p.deleted.Delete(p.newKeys(oldPart))
	}

	// Purge any tracked rows that belong to the old partition so that
	// targeted-delete operations do not attempt to delete rows from a
	// partition that no longer exists. Release closures are called inside
	// Invalidate, outside any internal lock.
	if p.rowTracker != nil {
		p.rowTracker.Invalidate(oldPart.id)
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

// MarkInvalid reports whether the caller is the first to observe a divergence
// for the partition identified by keys, and — when that partition still occupies
// its slot — permanently marks the slot invalid so it is skipped by future
// selection.
//
// It returns true when the caller should report the divergence:
//   - the slot still holds this partition and this is the first mark for it
//     (the slot is added to the invalid set and counts toward maxInvalid), or
//   - the slot has already been replaced by a fresh, valid partition (e.g. a
//     deleted partition being re-validated). In that case the divergence is
//     still real and must be reported, but the reused slot is deliberately NOT
//     marked or counted — marking it would skip a valid partition and inflate
//     the invalid budget (the freshly-replaced-slot race).
//
// It returns false when there is nothing to report: the UUID is no longer
// tracked, the slot is already marked invalid, or the maxInvalid limit has been
// reached (the caller should treat the limit case as a hard-stop trigger).
func (p *Partitions) MarkInvalid(keys *typedef.PartitionKeys) bool {
	if keys == nil {
		return false
	}

	// Look up the slot index for this UUID.
	p.validationMu.RLock()
	idx, ok := p.uuidToIdx[keys.ID]
	p.validationMu.RUnlock()
	if !ok {
		// UUID no longer tracked (slot replaced and fully retired) — nothing to
		// report.
		return false
	}

	// Serialize the whole mark against the fill/Replace clear paths so the
	// counter update and the map claim happen atomically relative to a
	// concurrent replacement. Without this, the counter and the map are updated
	// in opposite orders by the two sides and a freshly-replaced valid slot can
	// end up marked invalid with an inflated count.
	p.invalidMu.Lock()
	defer p.invalidMu.Unlock()

	// The slot may have been replaced (via Replace/fill) since our uuidToIdx
	// lookup — or, for a deleted partition, was replaced by design. If it no
	// longer holds THIS UUID, the current occupant is a fresh, valid partition:
	// report the divergence (return true) but do NOT mark or count the reused
	// slot. This both keeps deleted-partition divergences reportable and avoids
	// flagging a valid replacement.
	if p.partition(idx).id != keys.ID {
		return true
	}

	if p.maxInvalid > 0 && p.invalidCount.Load() >= p.maxInvalid {
		return false
	}

	// Claim the map entry. LoadOrStore returns (existing, true) if already present.
	if _, alreadyInvalid := p.invalidByIdx.LoadOrStore(idx, struct{}{}); alreadyInvalid {
		return false
	}
	p.invalidCount.Add(1)

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

// TrackRow stores a row observed during validation for later deletion.
func (p *Partitions) TrackRow(row TrackedRow) {
	if p.rowTracker == nil {
		return
	}
	p.rowTracker.Push(row)
}

// PopTrackedRow retrieves a previously tracked row for deletion.
// Returns false if no tracked rows are available.
func (p *Partitions) PopTrackedRow() (TrackedRow, bool) {
	if p.rowTracker == nil {
		return TrackedRow{}, false
	}
	return p.rowTracker.Pop()
}

// TrackedRowCount returns the number of currently tracked rows.
func (p *Partitions) TrackedRowCount() uint64 {
	if p.rowTracker == nil {
		return 0
	}
	return p.rowTracker.Len()
}

// RowTrackerFillRatio returns the fill level of the row tracker in [0, 1].
// Returns 0 when row tracking is disabled.
func (p *Partitions) RowTrackerFillRatio() float64 {
	if p.rowTracker == nil {
		return 0
	}
	return p.rowTracker.FillRatio()
}

func generateValue(r utils.Random, table *typedef.Table, config typedef.RangeConfig) []any {
	values := make([]any, 0, table.PartitionKeysLenValues())

	for _, pk := range table.PartitionKeys {
		values = pk.Type.GenValueOut(values, r, config)
	}

	return values
}

func NewPartitionKeys(r utils.Random, table *typedef.Table, config typedef.RangeConfig) typedef.PartitionKeys {
	values := generateValue(r, table, config)

	return typedef.PartitionKeys{
		Values: typedef.NewPartitionValues(table.PartitionKeys, values),
	}
}
