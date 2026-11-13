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
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/random"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

type (
	Interface interface {
		Stats() Stats
		Get(idx uint64) typedef.PartitionKeys
		// Next returns a partition to validate. It prefers a partition that a
		// mutation has written, see NextForWrite.
		Next() typedef.PartitionKeys
		// NextForWrite returns a partition to mutate and marks it written.
		NextForWrite() typedef.PartitionKeys

		Extend() typedef.PartitionKeys
		ReplaceNext() typedef.PartitionKeys
		Replace(idx uint64) typedef.PartitionKeys
		ReplaceWithoutOld(idx uint64)
		ReplaceNextWithoutOld()

		Deleted() <-chan typedef.PartitionKeys

		// Validation tracking
		ValidationSuccess(values *typedef.PartitionKeys)
		ValidationFailure(values *typedef.PartitionKeys)
		ValidationStats(keys *typedef.PartitionKeys) (first, last, failure uint64, recent []uint64, successCount uint64)

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
		// PeekTrackedRow returns a tracked row and leaves it for the targeted
		// mutations. Returns false if no tracked rows are available.
		PeekTrackedRow() (TrackedRow, bool)
		// BorrowByID takes a reference on the partition with id, so its UUID
		// stays mapped until Release. Returns false when the partition was
		// already replaced.
		BorrowByID(id uuid.UUID) (typedef.PartitionKeys, bool)
		// TrackedRowCount returns the number of currently tracked rows.
		TrackedRowCount() uint64
		// RowTrackerFillRatio returns the fill level of the row tracker as a
		// value in [0, 1]. Used by the validation job to select the appropriate
		// sampling zone (always-push / sampled / skip).
		RowTrackerFillRatio() float64

		Len() uint64
		Close()
	}

	// Partition holds one partition slot's data. The seed/id fields are
	// immutable after construction: fill/Replace/Extend install a brand-new
	// *Partition into the slot under partitions.mu rather than mutating an
	// existing one. That pointer swap (and the synchronization on partitions.mu
	// when readers load the pointer) is the only ordering needed, so reads of
	// seed/id require no per-partition lock. refCount stays atomic because it
	// is mutated in place across the partition's lifetime.
	//
	// retired is set (once, via fill/Replace) when this *Partition is evicted
	// from its slot. Its uuid mapping is removed only when the last
	// outstanding reference is released AND retired is set, so a live partition
	// whose refCount merely cycles to zero between borrows stays mapped, while a
	// replaced partition is reliably cleaned up exactly once (regardless of which
	// borrow happens to release last).
	Partition struct {
		// validation holds the partition's validation stats. Borrowed keys
		// point to it, so it lives as long as the partition or its keys.
		validation typedef.ValidationSlot
		id         uuid.UUID
		// seed regenerates the partition's key values on every borrow, see
		// Partitions.valuesOf. Storing a seed instead of the values keeps a
		// partition that is never borrowed at a few dozen bytes.
		seed     uint64
		refCount atomic.Int32
		retired  atomic.Bool
		// written is set when a mutation takes this partition's keys. A
		// replacement partition starts unwritten.
		written atomic.Bool
	}

	partitions struct {
		parts              []*Partition
		partitionValuesLen uint64
		count              atomic.Uint64
		created            atomic.Uint64
		mu                 sync.RWMutex
	}

	Partitions struct {
		table        *typedef.Table
		idxFunc      distributions.DistributionFunc
		deleted      *deletedPartitions
		rowTracker   *RowTracker
		uuidToIdx    map[uuid.UUID]uint64
		invalidByIdx sync.Map
		// reportedRetired holds the UUIDs of replaced partitions that
		// MarkInvalid already reported, so a replaced partition costs the error
		// budget once. deleteValidation removes the entry.
		reportedRetired sync.Map
		r               random.GoRoutineSafeRandom
		parts           partitions
		config          typedef.PartitionRangeConfig
		invalidCount    atomic.Uint64
		// reportedInvalid is this pool's share of the partitions_invalid
		// gauge, see publishInvalid.
		reportedInvalid atomic.Int64
		maxInvalid      uint64
		validationMu    sync.RWMutex
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
		deleted:    newDeleted(ctx, config.DeleteBuckets, config.MaxDeletedHeapSize),
		rowTracker: NewRowTracker(uint64(config.RowTrackerCapacity)),
		r:          *random.NewGoRoutineSafeRandom(r),
		table:      table,
		config:     config,
		idxFunc:    idxFunc,
		uuidToIdx:  make(map[uuid.UUID]uint64),
		maxInvalid: maxInvalid,
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

// newPartition returns a partition with a fresh seed for its key values.
func (p *Partitions) newPartition(id uuid.UUID) *Partition {
	return &Partition{id: id, seed: p.r.Uint64()}
}

// valuesOf regenerates part's key values from its seed. Every value generator
// draws only from the RNG it gets, so the same seed always gives the same
// values. Each borrow gets its own slice, so a borrower cannot change the
// values another borrower sees.
func (p *Partitions) valuesOf(part *Partition) []any {
	values := generateValue(rand.New(rand.NewPCG(part.seed, partitionValueStream)), p.table, &p.config)
	if uint64(len(values)) != p.parts.partitionValuesLen {
		panic(fmt.Sprintf(
			"invalid partition values generated: length %d, expected %d",
			len(values),
			p.parts.partitionValuesLen,
		))
	}

	return values
}

// partitionValueStream is the second PCG word of the value generator. The seed
// of the partition is the first.
const partitionValueStream = 0x9e3779b97f4a7c15

func (p *Partitions) partition(idx uint64) *Partition {
	p.parts.mu.RLock()
	part := p.parts.parts[idx]
	p.parts.mu.RUnlock()
	return part
}

// newKeys takes one reference on part and returns partition keys whose Release
// drops that reference exactly once (guarded by a per-borrow sync.Once). The
// UUID's idx mapping is removed only when the final reference is released AND
// the partition has been retired (evicted from its slot). A live partition
// whose refCount transiently reaches zero between borrows stays mapped; a
// retired partition is cleaned up exactly once, by whichever borrow releases
// last.
func (p *Partitions) newKeys(part *Partition) typedef.PartitionKeys {
	part.refCount.Add(1)
	return p.keysFor(part)
}

// borrow takes one reference on the partition in slot idx while it holds the
// slot lock. A concurrent Replace then either sees the reference before it
// retires the partition, or installs its new partition before the borrow
// reads the slot. Without the lock, the last release of a retired partition
// could remove its UUID mapping between the slot read and the reference.
func (p *Partitions) borrow(idx uint64) typedef.PartitionKeys {
	p.parts.mu.RLock()
	part := p.parts.parts[idx]
	part.refCount.Add(1)
	p.parts.mu.RUnlock()

	return p.keysFor(part)
}

// keysFor returns partition keys for part whose Release drops one reference
// that the caller already took.
func (p *Partitions) keysFor(part *Partition) typedef.PartitionKeys {
	id := part.id
	var once sync.Once
	return typedef.PartitionKeys{
		Values:     typedef.NewPartitionValues(p.table.PartitionKeys, p.valuesOf(part)),
		ID:         id,
		Validation: &part.validation,
		Release: func() {
			once.Do(func() {
				if part.refCount.Add(-1) == 0 && part.retired.Load() {
					p.deleteValidation(id)
				}
			})
		},
	}
}

// deleteValidation removes the UUID→idx mapping for a partition UUID. It runs for a retired partition with no
// reference left: on its last release, or in fill when nothing borrowed it.
// No validator can still hold this UUID.
// Without this cleanup the maps grow without bound, because every DELETE
// retires one partition UUID.
func (p *Partitions) deleteValidation(id uuid.UUID) {
	p.validationMu.Lock()
	delete(p.uuidToIdx, id)
	p.validationMu.Unlock()
	p.reportedRetired.Delete(id)
}

func (p *Partitions) values(idx uint64) typedef.PartitionKeys {
	// seed/id are immutable for the lifetime of this *Partition (see the
	// Partition doc comment), so they can be read without a per-partition lock.
	return p.borrow(idx)
}

func (p *Partitions) valuesCopy(idx uint64) typedef.PartitionKeys {
	return p.borrow(idx)
}

func (p *Partitions) fill(idx uint64) {
	id, _ := uuid.NewV7()

	p.validationMu.Lock()
	p.uuidToIdx[id] = idx
	p.validationMu.Unlock()

	oldPart := p.installPartition(idx, p.newPartition(id))
	if oldPart == nil {
		return
	}

	// During initial population the old *Partition is the zero-value struct
	// with uuid.Nil, which is never in the maps.
	if oldPart.id == (uuid.UUID{}) {
		return
	}

	// A validator can still hold keys of the old partition. Its UUID mapping
	// must stay until that borrow is released, or a divergence the validator
	// finds is dropped because MarkInvalid cannot find the UUID. So fill marks
	// the partition retired and removes the UUID now only when no borrow is
	// left. Otherwise the last Release removes it. installPartition already
	// swapped the slot under parts.mu, so no new borrow of oldPart can start.
	// The retired store comes before the refCount load, and a Release does
	// its refCount decrement before its retired load, so at least one side
	// sees the other and the UUID is always removed.
	oldPart.retired.Store(true)
	if oldPart.refCount.Load() == 0 {
		p.deleteValidation(oldPart.id)
	}
}

// installPartition puts part in slot idx, clears the invalid mark of the slot,
// and returns the partition it replaced. It holds invalidMu across both steps,
// the lock MarkInvalid holds, so a mark on the new partition cannot land
// between the swap and the clear and get erased, and a mark on the old
// partition cannot land after the clear and stick to the new one.
func (p *Partitions) installPartition(idx uint64, part *Partition) *Partition {
	p.invalidMu.Lock()
	defer p.invalidMu.Unlock()

	p.parts.mu.Lock()
	old := p.parts.parts[idx]
	p.parts.parts[idx] = part
	p.parts.mu.Unlock()

	if _, wasInvalid := p.invalidByIdx.LoadAndDelete(idx); wasInvalid {
		p.invalidCount.Add(^uint64(0)) // atomic decrement
		p.publishInvalid()
	}

	return old
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

// writtenPickAttempts bounds how many distribution draws Next makes to find a
// written partition. If each draw lands on an unwritten partition with
// probability p, about p^16 of the reads still go to one. For the uniform
// distribution p is the unwritten fraction of the pool. These reads keep
// checking that neither cluster returns rows for a key gemini never wrote.
const writtenPickAttempts = 16

// Next returns a partition to validate, drawn from the distribution function
// and skipping invalid partitions. It prefers a partition that a mutation has
// written: a SELECT on an unwritten partition compares two empty results. It
// draws up to writtenPickAttempts times and takes the last draw when none of
// them is written.
func (p *Partitions) Next() typedef.PartitionKeys {
	idx := p.pickValidIdx()
	for range writtenPickAttempts - 1 {
		if p.isWritten(idx) {
			break
		}
		idx = p.pickValidIdx()
	}

	return p.Get(idx)
}

// NextForWrite returns the next partition to mutate according to the
// distribution function, skipping invalid partitions, and marks it written.
func (p *Partitions) NextForWrite() typedef.PartitionKeys {
	p.parts.mu.RLock()
	idx := p.pickValidIdx()
	part := p.parts.parts[idx]
	part.refCount.Add(1)
	p.parts.mu.RUnlock()

	part.written.Store(true)

	return p.keysFor(part)
}

func (p *Partitions) isWritten(idx uint64) bool {
	p.parts.mu.RLock()
	written := p.parts.parts[idx].written.Load()
	p.parts.mu.RUnlock()

	return written
}

func (p *Partitions) Extend() typedef.PartitionKeys {
	id, _ := uuid.NewV7()

	// Hold lock while extending slice AND initializing the new partition
	// to prevent other goroutines from accessing it during reallocation
	p.parts.mu.Lock()
	newIdx := uint64(len(p.parts.parts))
	part := p.newPartition(id)
	p.parts.parts = append(p.parts.parts, part)
	p.parts.mu.Unlock()

	p.validationMu.Lock()
	p.uuidToIdx[id] = newIdx
	p.validationMu.Unlock()

	p.parts.count.Add(1)
	p.parts.created.Add(1)
	metrics.PartitionsExtended.Inc()

	return p.valuesCopy(newIdx)
}

func (p *Partitions) Replace(idx uint64) typedef.PartitionKeys {
	id, _ := uuid.NewV7()
	p.validationMu.Lock()
	p.uuidToIdx[id] = idx
	p.validationMu.Unlock()

	oldPart := p.installPartition(idx, p.newPartition(id))

	// Take the caller's reference and, when deletes are tracked, a second
	// reference for the deleted-partitions heap BEFORE the partition is marked
	// retired. A borrower that releases the last older reference after the
	// retired flag is set would otherwise drop refCount to zero and remove the
	// UUID mapping before the heap holds its reference. MarkInvalid could then
	// not find the UUID, and a divergence on the deleted partition would go
	// unreported. Each reference has its own Release and sync.Once.
	oldKeys := p.newKeys(oldPart)

	var heapKeys typedef.PartitionKeys
	if p.deleted != nil {
		heapKeys = p.newKeys(oldPart)
	}

	// Mark the evicted partition retired. Its uuidToIdx entry
	// are removed only when its last reference is released: the caller's
	// returned keys and, when present, the heap's reference. The heap releases
	// its reference through the validator's Release closure, which runs only
	// after re-validation completes, so MarkInvalid can still locate the UUID
	// while a divergence on the deleted partition is reported.
	oldPart.retired.Store(true)

	if p.deleted != nil {
		p.deleted.Delete(heapKeys)
	}
	metrics.PartitionsReplaced.Inc()

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

// ValidationSuccess records a successful validation of the partition the keys
// were borrowed from. It reads the partition's slot through the keys, so it
// takes no lock and does no map lookup.
func (p *Partitions) ValidationSuccess(keys *typedef.PartitionKeys) {
	if keys == nil || keys.Validation == nil {
		return
	}
	keys.Validation.RecordSuccess(uint64(time.Now().UTC().UnixNano()))
}

// ValidationFailure records a failed validation of the partition the keys were
// borrowed from.
func (p *Partitions) ValidationFailure(keys *typedef.PartitionKeys) {
	if keys == nil || keys.Validation == nil {
		return
	}
	keys.Validation.RecordFailure(uint64(time.Now().UTC().UnixNano()))
}

// ValidationStats returns the recorded validation stats of the partition the
// keys were borrowed from.
func (p *Partitions) ValidationStats(keys *typedef.PartitionKeys) (first, last, failure uint64, recent []uint64, successCount uint64) {
	if keys == nil || keys.Validation == nil {
		return 0, 0, 0, nil, 0
	}
	return keys.Validation.Stats()
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
	// report the divergence (return true) the first time only, and do NOT mark
	// or count the reused slot. This keeps deleted-partition divergences
	// reportable, avoids flagging a valid replacement, and stops every later
	// validator and bucket emission from reporting the same divergence again.
	if p.partition(idx).id != keys.ID {
		_, reported := p.reportedRetired.LoadOrStore(keys.ID, struct{}{})
		return !reported
	}

	if p.maxInvalid > 0 && p.invalidCount.Load() >= p.maxInvalid {
		return false
	}

	// Claim the map entry. LoadOrStore returns (existing, true) if already present.
	if _, alreadyInvalid := p.invalidByIdx.LoadOrStore(idx, struct{}{}); alreadyInvalid {
		return false
	}
	p.invalidCount.Add(1)
	p.publishInvalid()

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

// PeekTrackedRow returns a tracked row and leaves it in the tracker.
func (p *Partitions) PeekTrackedRow() (TrackedRow, bool) {
	if p.rowTracker == nil {
		return TrackedRow{}, false
	}
	return p.rowTracker.Peek()
}

// BorrowByID takes a reference on the partition with id while it holds the
// slot lock, and only when the slot still holds that partition. The returned
// keys keep the UUID mapped until Release, so MarkInvalid can report a
// divergence on it. Returns false when the partition was already replaced.
func (p *Partitions) BorrowByID(id uuid.UUID) (typedef.PartitionKeys, bool) {
	p.validationMu.RLock()
	idx, ok := p.uuidToIdx[id]
	p.validationMu.RUnlock()
	if !ok {
		return typedef.PartitionKeys{}, false
	}

	p.parts.mu.RLock()
	part := p.parts.parts[idx]
	if part.id != id {
		p.parts.mu.RUnlock()
		return typedef.PartitionKeys{}, false
	}
	part.refCount.Add(1)
	p.parts.mu.RUnlock()

	return p.keysFor(part), true
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

// publishInvalid adds this pool's change in invalid partitions to the
// partitions_invalid gauge. Each table has its own pool, so a pool that set the
// gauge to its own count would hide the other tables. Swap makes concurrent
// calls safe: the gauge always equals the sum of what each pool last reported.
func (p *Partitions) publishInvalid() {
	current := int64(p.invalidCount.Load())
	metrics.PartitionsInvalid.Add(float64(current - p.reportedInvalid.Swap(current)))
}
