# Targeted Delete Operations

## Table of Contents
- [Overview](#overview)
- [Motivation](#motivation)
- [Architecture](#architecture)
- [Row Tracker](#row-tracker)
- [Delete Operation Types](#delete-operation-types)
- [Auto-Tuning](#auto-tuning)
- [Configuration](#configuration)
- [Data Flow](#data-flow)
- [Concurrency Model](#concurrency-model)
- [Fallback Behavior](#fallback-behavior)
- [Performance Characteristics](#performance-characteristics)

---

## Overview

Targeted delete operations extend Gemini's deletion testing to cover **single-row** and **clustering-subset** deletes (prefix-range deletes on the clustering key) using real data observed during validation. This ensures that tombstone testing covers rows of varying ages, not just partition-level deletes.

Prior to this feature, Gemini only supported whole-partition deletes (via `ReplaceNext()`). The targeted deletion system adds two new delete subtypes that operate on specific rows within a partition, exercising different code paths in ScyllaDB's storage engine and compaction logic.

---

## Motivation

ScyllaDB (and Cassandra) handle different delete granularities differently:

| Delete Type | Storage Impact | Compaction Behavior |
|-------------|---------------|---------------------|
| Whole partition | Partition-level tombstone | Efficient to compact |
| Clustering subset (CK prefix) | Range tombstone | More complex compaction |
| Single row | Cell-level tombstone | Most granular, can accumulate |

Testing only whole-partition deletes misses bugs in:
- Range tombstone handling
- Cell-level tombstone accumulation
- Read-path filtering of individual dead rows within live partitions
- Interactions between live data and scattered tombstones

Targeted deletions address this gap by deleting specific rows and row ranges that are **known to exist** (observed during validation SELECTs), ensuring the DELETE actually produces tombstones that interact with live data.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Validation Workers                           │
│                                                                     │
│  store.Check() → SELECT on both clusters, compare rows             │
│         │                                                           │
│         │ On success: testRows returned for free (no extra SELECT)  │
│         │ Sample random 1..N rows from testRows                     │
│         ▼                                                           │
│  ┌─────────────────────────────────────────────────────────┐        │
│  │              Row Tracker (FIFO Queue)                    │        │
│  │  ┌──────┬──────┬──────┬──────┬──────┬──────┬──────┐    │        │
│  │  │ Row0 │ Row1 │ Row2 │ Row3 │ ...  │ RowN │      │    │        │
│  │  └──────┴──────┴──────┴──────┴──────┴──────┴──────┘    │        │
│  │  Capacity: auto-sized from deletion ratios              │        │
│  │  Full → new pushes are silently dropped                 │        │
│  └─────────────────────────────────────────────────────────┘        │
│                           │                                         │
│                           │ Pop()                                   │
│                           ▼                                         │
│  ┌─────────────────────────────────────────────────────────┐        │
│  │              Mutation Workers (Delete)                   │        │
│  │                                                         │        │
│  │  deleteSingleRow:  DELETE WHERE pk=? AND ck1=? … ckN=? │        │
│  │  deleteClusteringSubset:  DELETE WHERE pk=? AND ck1=? … ckM=? │  │
│  │  (prefix of 1..N-1 clustering keys, M < N)             │        │
│  └─────────────────────────────────────────────────────────┘        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Row Tracker

The row tracker (`pkg/partitions/rowtracker.go`) is a bounded concurrent FIFO queue that stores rows observed during validation for later consumption by delete operations.

### Design

```go
type RowTracker struct {
    mu          sync.Mutex
    buf         []TrackedRow
    invalidBits []uint64       // one bit per slot, set by Invalidate()
    head        uint64         // next write position (monotonically increasing)
    count       atomic.Uint64  // current number of tracked rows (including invalidated)
    capacity    uint64
}

type TrackedRow struct {
    PartitionValues  []any     // flat CQL bind values for PK columns, in column order
    ClusteringValues []any     // flat CQL bind values for CK columns, in column order
    PartitionID      uuid.UUID // for invalidation when the partition is replaced
}
```

`TrackedRow` carries no `Release` closure. The partition's ref-count lifecycle is owned entirely by the statement that triggered validation; the tracked row is a lightweight snapshot used only to build a targeted-delete query. Carrying a stale closure would risk double-decrement bugs.

### Queue Semantics

- **Push**: Writes at `head % capacity`. If the queue is **full, the row is dropped silently**. This is intentional — there is no point overwriting older rows: if the queue is full, validation is already keeping up with deletions.
- **Pop**: Reads from the oldest position `(head - count) % capacity`. Skips invalidated slots (marked by `Invalidate`). Returns `(TrackedRow, false)` if empty.
- **Invalidate(id)**: O(capacity) scan under the lock; sets the invalid bit for every slot whose `PartitionID` matches. Called when a partition is replaced so that stale rows are never returned by `Pop`.
- **Len**: Lock-free approximate count via `atomic.Uint64` (includes invalidated-but-not-yet-popped slots).
- **Capacity 0**: Disables tracking entirely (Push is a no-op, Pop always returns false).

### Why a FIFO Queue (not a Ring Buffer)?

A ring buffer that overwrites old entries has two problems:

1. **Delete/populate race**: If deletes consume rows faster than validation populates the buffer, overwriting means we lose rows before they can be used.
2. **Wasted work**: Replacing entries when consumption is slow wastes CPU for no benefit — if the queue is full, we should just skip the push.

The FIFO queue with skip-on-full fixes both: the queue stays bounded in memory, and pushes are cheap no-ops when capacity is already reached.

### Invalidation Bitset

When a partition is replaced (`Partitions.Replace`), all tracked rows for that partition are immediately invalidated via `RowTracker.Invalidate(id)`. The bitset approach avoids any map allocation or GC pressure. Each `uint64` word covers 64 consecutive slots. `Pop` skips invalidated slots in a tight retry loop; the count is decremented as each slot is consumed (valid or not).

---

## Delete Operation Types

### Single-Row Delete (`DeleteSingleRow`)

Deletes exactly one row by specifying all partition keys AND all clustering keys.

```sql
DELETE FROM ks.table WHERE pk1 = ? AND pk2 = ? AND ck1 = ? AND ck2 = ? AND ck3 = ?
```

**Flow:**
1. Pop a tracked row from the row tracker
2. Use its `PartitionValues` for all PK equality conditions
3. Use its `ClusteringValues` for ALL CK equality conditions
4. If no tracked rows available, return `ErrNoTrackedRows` (caller backs off and retries)

If the table has no clustering keys, falls back to `deleteSinglePartition` (a whole-partition delete is equivalent).

### Clustering-Subset Delete (`DeleteClusteringSubset`)

Deletes all rows whose first `n` clustering key columns match the tracked row's values (a CQL prefix delete on the clustering key). This removes a contiguous subset of rows that share a clustering-key prefix, producing a range tombstone in ScyllaDB's storage engine.

```sql
-- n=1 of 3 CKs (deletes all rows sharing ck1):
DELETE FROM ks.table WHERE pk1 = ? AND ck1 = ?

-- n=2 of 3 CKs (deletes all rows sharing ck1 AND ck2):
DELETE FROM ks.table WHERE pk1 = ? AND ck1 = ? AND ck2 = ?
```

**Flow:**
1. If `len(ClusteringKeys) <= 1`, fall back to `deleteSingleRow` (a 1-CK prefix == single row)
2. Pop a tracked row from the row tracker
3. Pick `n = 1 + random.IntN(len(ClusteringKeys)-1)` — uniformly in `[1, N-1]`
4. Bind `ck1..ckN` with equality using the tracked row's `ClusteringValues`
5. If no tracked rows available, return `ErrNoTrackedRows` (caller backs off and retries)

Because `n < len(ClusteringKeys)`, at least one CK column is always left unbound — this is always a clustering-subset delete, never a single-row delete.

---

## Auto-Tuning

Both the row tracker capacity and the validation sample rate are automatically computed based on the configured deletion ratios. This eliminates the need for manual tuning in most cases.

### Targeted Delete Ratio

The "effective targeted delete ratio" is the probability that any given mutation will be a targeted delete consuming a row from the tracker:

```
targetedDeleteRatio = deleteRatio * (singleRowRatio + clusteringSubsetRatio)
```

At the default configuration:
- `deleteRatio = 0.05` (5% of mutations are deletes)
- `singleRowRatio = 0.30`, `clusteringSubsetRatio = 0.30` (60% of deletes are targeted)
- **Effective = 0.05 × 0.60 = 0.03** (3% of mutations consume tracked rows)

### Row Tracker Capacity Auto-Sizing

When `--row-tracker-capacity=N > 0` (default `100000`), capacity is computed as:

```
auto = clamp(100, 100000, floor(targetedDeleteRatio * 33333))
capacity = min(auto, N)   // N acts as an upper bound
```

With `--row-tracker-capacity=-1`, the cap is removed (fully auto, bounded only by the `100000` hard ceiling). With `--row-tracker-capacity=0`, tracking is disabled entirely.

| Delete Ratio | Targeted % | Effective | Auto-Capacity |
|:---:|:---:|:---:|:---:|
| 0% | any | 0.000 | 0 (disabled) |
| 2% | 60% | 0.012 | 400 |
| 5% (default) | 60% | 0.030 | 1000 |
| 10% | 60% | 0.060 | 2000 |
| 20% | 70% | 0.140 | 4667 |
| 50% | 80% | 0.400 | 13333 |
| 100% | 100% | 1.000 | 33333 |

### Validation Sample Rate Auto-Sizing

```
sampleRate       = min(0.50, targetedDeleteRatio * 3.5)
maxSamplesPerRun = clamp(1, 10, floor(30 * sampleRate))
```

| Effective Ratio | Sample Rate | Max Samples/Run |
|:---:|:---:|:---:|
| 0.000 | 0% (disabled) | - |
| 0.012 | 4.2% | 1 |
| 0.030 (default) | 10.5% | 3 |
| 0.060 | 21% | 6 |
| 0.140 | 49% | 10 |
| 0.400+ | 50% (cap) | 10 |

### Fill-Zone Sampling

The tracker's current fill level further controls whether sampling is attempted:

| Fill Level | Zone | Behaviour |
|:---:|:---:|:---|
| < 30% | Lean | Always sample, ignoring `maxSamplesPerRun` cap |
| 30–70% | Healthy | No sampling — queue is in a good state |
| 70–90% | Filling | Probabilistic gate: sample only if `rand < sampleRate` |
| ≥ 90% | Full | Skip sampling entirely — queue is nearly full |

### Design Rationale

The auto-tuning ensures:
- **Zero overhead when deletions are disabled**: No sampling, no tracker allocation
- **Balanced fill rate**: The tracker fills at a rate proportional to how fast rows are consumed
- **Self-adjusting**: Heavy delete workloads automatically sample more to keep the tracker full
- **Bounded**: Caps prevent excessive memory usage or CPU overhead

---

## Configuration

### CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--row-tracker-capacity` | `100000` | Row tracker capacity upper bound. `N > 0` = auto-size clamped to N. `-1` = fully auto (no cap). `0` = disable. |
| `--statement-ratios` | (defaults) | JSON controlling delete subtype distribution |

### Statement Ratios (Delete Subtypes)

```json
{
  "mutation": {
    "delete": 0.05,
    "delete_subtypes": {
      "whole_partition": 0.30,
      "single_row": 0.30,
      "clustering_subset": 0.30,
      "multiple_partitions": 0.10
    }
  }
}
```

### Example Configurations

**Heavy targeted deletion workload:**

```bash
gemini --statement-ratios='{
  "mutation": {
    "insert": 0.50,
    "update": 0.20,
    "delete": 0.30,
    "delete_subtypes": {
      "whole_partition": 0.10,
      "single_row": 0.45,
      "clustering_subset": 0.45,
      "multiple_partitions": 0.00
    }
  }
}'
# Auto-computes: capacity=8999, sampleRate=50%, maxSamples=10
```

**Disable targeted deletions (only whole-partition):**

```bash
gemini --statement-ratios='{
  "mutation": {
    "delete_subtypes": {
      "whole_partition": 0.90,
      "single_row": 0.00,
      "clustering_subset": 0.00,
      "multiple_partitions": 0.10
    }
  }
}'
# Auto-computes: capacity=0, sampleRate=0%
```

---

## Data Flow

### Row Tracking (Validation → Tracker)

```
1. Validation worker calls store.Check()
   └─ Executes SELECT on both clusters in parallel (both goroutines tracked
      by inflight WaitGroup so Close() waits for them)
   └─ Compares results → success (rows match)
   └─ Returns (matchCount, testRows, nil) — testRows are free, no extra SELECT

2. Sampling gate (fill-zone switch):
   ├─ fill ≥ 90%  → skip
   ├─ fill < 30%  → sampleRowsForTracker(stmt, testRows, forceAll=true)
   ├─ fill ≥ 70%  → if rand < sampleRate: sampleRowsForTracker(..., forceAll=false)
   └─ 30–70%      → no sampling (queue is healthy)

3. sampleRowsForTracker(stmt, rows, forceAll):
   a. Pick limit = 1 + random.IntN(len(rows))   — between 1 and N rows
      If !forceAll: limit = min(limit, maxSamplesPerRun)
   b. Shuffle rows with random.Perm(len(rows))
   c. For each row (up to limit):
      - Extract clustering key values from the row
      - Push TrackedRow{PartitionID, PartitionValues, ClusteringValues}
        into the RowTracker FIFO queue
```

### Row Consumption (Tracker → Mutation)

```
1. Mutation worker calls MutateStatement() → ratio selects DELETE
2. RatioController selects subtype: DeleteSingleRow or DeleteClusteringSubset
3. Generator calls PopTrackedRow() on the RowTracker
   └─ Got a row → build DELETE with real PK + CK values from TrackedRow
   └─ Empty    → return ErrNoTrackedRows → worker backs off 100ms and retries
4. Statement is executed against both clusters
5. Partition key is released via defer in Mutation.run()
   (TrackedRow itself carries no Release — the partition lifecycle is
    managed by the original validation statement's PartitionKeys)
```

### Partition Invalidation

```
1. Mutation worker calls Replace(idx) on a partition slot
2. Partitions.Replace() calls RowTracker.Invalidate(oldID)
3. Invalidate() scans the buffer under the lock, sets invalid bits for
   all slots matching oldID
4. Future Pop() calls skip those slots (decrementing count as they go)
   — stale rows for replaced partitions are never returned
```

---

## Concurrency Model

### Thread Safety

| Component | Synchronization | Access Pattern |
|-----------|----------------|----------------|
| RowTracker.Push() | `sync.Mutex` | Multiple validation workers |
| RowTracker.Pop() | `sync.Mutex` | Multiple mutation workers |
| RowTracker.Invalidate() | `sync.Mutex` | Partition replace path |
| RowTracker.Len() / FillRatio() | `atomic.Uint64` | Any goroutine (lock-free) |
| Validation.random | Single goroutine | Each validation worker owns its RNG |
| Generator.random | Single goroutine | Each worker owns its statement generator |

### No Shared RNG

Each validation worker and each mutation worker creates its own `rand.Rand` instance from a unique seed. The RNG inside a `Validation` struct is the same instance used by its `statements.Generator`, but both are only accessed from the same goroutine (the worker's `Do()` loop). There is no cross-goroutine sharing of RNG state.

### inflight WaitGroup

Both the mutate path (`Mutate`) and the check path (`Check`) register goroutines with `delegatingStore.inflight` before launching. `delegatingStore.Close()` calls `inflight.Wait()` before closing CQL sessions, preventing "use of closed network connection" races on both mutation and SELECT goroutines.

---

## Fallback Behavior

When the row tracker is empty (no rows have been sampled yet, or consumption has temporarily exceeded supply), targeted delete operations return `ErrNoTrackedRows`:

```
deleteSingleRow() → Pop() returns false → return ErrNoTrackedRows
deleteClusteringSubset()   → Pop() returns false → return ErrNoTrackedRows
```

The mutation worker handles `ErrNoTrackedRows` identically to `ErrNoStatement`: it backs off for 100 ms and retries. This ensures:

- No tombstones are produced against rows that almost certainly do not exist
- The worker does not stall — it retries once validation has had a chance to refill the queue
- Cold-start scenarios are handled gracefully (queue fills up after the first validation pass)

### Defensive Bounds Checks

If a tracked row's values don't match the current table schema (theoretically impossible since CQL doesn't support ALTER TABLE on clustering keys, but guarded defensively):

```go
if pkValIdx+lenVal > len(trackedRow.PartitionValues) {
    return nil, ErrNoTrackedRows
}
if ckValIdx+lenVal > len(trackedRow.ClusteringValues) {
    return nil, ErrNoTrackedRows
}
```

No `Release` is called — `TrackedRow` carries none. The row is simply discarded.

---

## Performance Characteristics

### Memory

| Component | Memory Usage |
|-----------|-------------|
| RowTracker buffer | `capacity × sizeof(TrackedRow)` ≈ capacity × ~56 bytes |
| invalidBits | `ceil(capacity / 64) × 8 bytes` (negligible) |
| At default auto (1000) | ~56 KB |
| At cap (100000) | ~5.6 MB |
| When disabled (0) | 0 bytes |

### CPU Overhead

| Operation | Cost | Frequency |
|-----------|------|-----------|
| Fill-zone check | 1 atomic load | Every successful validation |
| sampleRowsForTracker | Perm shuffle + slice reads | Per fill-zone trigger |
| Push to tracker | Mutex lock + array write | Per sampled row (1–N per validation) |
| Pop from tracker | Mutex lock + array read | Per targeted delete mutation |
| Invalidate | O(capacity) scan under lock | Per partition replace (infrequent) |

### Throughput Impact

At the default configuration:
- Sampling reuses `testRows` already returned by `store.Check()` — **zero extra SELECTs**
- Push/Pop operations are sub-microsecond (mutex + array index)
- Net impact: negligible additional load from sampling

When deletions are disabled (`deleteRatio=0`):
- Sample rate = 0%, no sampling attempted
- Tracker capacity = 0, no memory allocated
- Zero additional overhead
