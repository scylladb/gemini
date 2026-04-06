# Gemini Project

Gemini is an automated randomized testing suite for ScyllaDB and Apache Cassandra.
It generates random CQL mutations (INSERT, UPDATE, DELETE) and applies them to two clusters
simultaneously: an **oracle** (source of truth) and a **system under test** (SUT).
It then runs SELECT queries on both clusters and compares results row-by-row.
Any difference indicates a bug in the SUT.

## How Gemini Works

1. **Schema**: Generated from a random seed or loaded from JSON. Defines keyspace, tables,
   partition/clustering keys, columns, indexes, materialized views, and UDTs.
2. **Partitions**: A pool of partition key values is initialized with a configurable statistical
   distribution (uniform, zipfian, sequential, lognormal) that controls access patterns.
3. **Warmup** (optional): Write-only phase with no deletes to populate initial data.
4. **Main phase**: Runs in one of four modes:
   - `mixed` (default) -- concurrent mutation + validation workers
   - `write` -- mutation workers only
   - `read` -- validation workers only
   - `warmup` -- writes without deletes
5. **Workers**: Per-table pools managed by `errgroup`:
   - `mutationConcurrency` Mutation workers generate random INSERT/UPDATE/DELETE statements
   - `readConcurrency` Validation workers run SELECT queries and compare results
6. **Mutations**: Applied to both clusters in parallel with retry and exponential backoff.
   Each cluster is retried independently.
7. **Validation**: Rows loaded from both clusters in parallel, sorted by PK+CK, compared via
   `CompareCollectedRows`. List values are deduplicated (LWT retry artifact workaround).
8. **Delete tracking**: Deleted partitions are tracked in time buckets via a min-heap.
   After configurable delays, they are re-validated (expecting zero rows on both clusters).
9. **Error budget**: When max errors reached, workers stop gracefully via `stop.Flag`.

## Project Structure

- `pkg/` -- all packages for the gemini project
- `docs/` -- project documentation (architecture, partitions, metrics, etc.)
- `scripts/` -- development and testing scripts
- `docker/` -- Dockerfiles and docker-compose files for dev environment and CI

### Key Packages

| Package | Purpose |
|---|---|
| `pkg/cmd` | CLI entry point, flag parsing, root command |
| `pkg/jobs` | Mutation/Validation worker orchestration |
| `pkg/store` | Dual-cluster CQL operations and row comparison |
| `pkg/partitions` | Thread-safe partition pool with distributions |
| `pkg/statements` | CQL statement generation with cached queries |
| `pkg/stmtlogger` | Async statement logging framework |
| `pkg/stmtlogger/scylla` | ScyllaDB + file statement writer |
| `pkg/typedef` | Core types: Schema, Table, Column, Type interface, Stmt |
| `pkg/builders` | Schema builder pattern |
| `pkg/distributions` | Statistical distributions for partition selection |
| `pkg/status` | Global ops/error counters |
| `pkg/stop` | Graceful shutdown flag (soft/hard) |
| `pkg/metrics` | Prometheus metric definitions |
| `pkg/joberror` | Structured error types with comparison results |
| `pkg/utils` | Backoff, timers, file helpers |
| `pkg/random` | Goroutine-safe random number generation |
| `pkg/replication` | Replication strategy configuration |
| `pkg/murmur` | Murmur3 hash for token-aware routing |
| `pkg/inflight` | In-flight request tracking |
| `pkg/tableopts` | Table options for CQL CREATE TABLE |
| `pkg/schema` | Schema generation from seeds |
| `pkg/services` | Supporting services (pprof, etc.) |
| `pkg/benchmarks` | Performance benchmarks |
| `pkg/testutils` | Test helpers (ScyllaDB containers, etc.) |

## Language & Framework

- **Language**: Go 1.25
- **Architecture**: CLI application with modular package structure
- **Database**: Scylla/Cassandra using CQL protocol
- **Testing**: Randomized testing with statistical distributions

## Go Instructions (1.24, 1.25)

1. Always use `for range` instead of `for` with `i++` when iterating, this is the new syntax from Go 1.25
   Example: `for i := 0; i < 10; i++` should be replaced with `for i := range 10`. If the `i` is not needed it can be
   omitted.
   This is also true when using some integer variable as a counter. `for i := 0; i < VARIABLE; i++` can be replaced with
   `for i := range VARIABLE`, also `i` can be omitted.
2. Always assume `go` **1.25**. Prefer `go` commands that work with Go 1.25.
3. Keep `go.mod` `go 1.25`. Do **not** add a `toolchain` line when updating the `go` line (Go 1.25 no longer auto-adds
   it).
4. Use the new `go.mod` **`ignore`** directive to exclude non-packages (e.g., examples, scratch) from `./...`
5. Prefer standard library first; avoid third-party deps unless asked.
6. Slice stack-allocation opportunities (new in 1.25) and avoid unsafe pointer aliasing.
7. Use testing/synctest for flaky/racy tests.
8. Consider the container-aware GOMAXPROCS defaults when benchmarking.
9. **DWARF 5** debug info by default
10. Add a `synctest` based test that removes `time.Sleep` and waits deterministically.
11. Use `go test -race` to detect data races.
12. Always use in tests for context `t.Context()` and for benchmarking `b.Context()`, there are new go 1.24 function,
    and they better and avoid linting errors.
13. Use `t.Cleanup()` to register cleanup functions in tests instead of `defer` to ensure proper execution order.
14. Use `t.Parallel()` to run tests in parallel.

## Vet & Static Checks

Always run go vet ./... and address:

- waitgroup analyzer: fix misplaced (*sync.WaitGroup).Add calls.
- hostport analyzer: replace fmt.Sprintf("%s:%d", host, port) with net.JoinHostPort(host, strconv.Itoa(port)).

## Testing Instructions

1. IMPORTANT: Always use `-tags testing` and `-race` when running tests locally and in CI.
    * `go test -tags testing -race ./pkg/...`
2. When writing new tests, always use `t.Context()` for context in tests.
3. IMPORTANT: Running tests locally and in CI should be deterministic, so use environment variables to set up
   ScyllaDB clusters:
    * `GEMINI_ORACLE_CLUSTER_IP=192.168.100.2 GEMINI_TEST_CLUSTER_IP=192.168.100.3 GEMINI_USE_DOCKER_SCYLLA=true`

### Integration Testing

Integration tests require two running ScyllaDB nodes (oracle + test).

**Setup on Linux:**

1. Start the docker compose environment with 2 nodes:
   ```bash
   docker compose -f docker/docker-compose-scylla.yml up -d
   ```
   This creates two ScyllaDB containers on a bridge network:
   - `gemini-oracle` at `192.168.100.2`
   - `gemini-test` at `192.168.100.3`

2. Wait for both nodes to be ready (CQL port responsive).

3. Run integration tests with the required environment variables:
   ```bash
   GEMINI_USE_DOCKER_SCYLLA=true \
   GEMINI_TEST_CLUSTER_IP=192.168.100.3 \
   GEMINI_ORACLE_CLUSTER_IP=192.168.100.2 \
   go test -tags testing -race ./pkg/...
   ```

**Environment variables:**

| Variable | Purpose | Example |
|---|---|---|
| `GEMINI_USE_DOCKER_SCYLLA` | Enables Docker-based ScyllaDB for tests | `true` |
| `GEMINI_TEST_CLUSTER_IP` | IP of the system under test node | `192.168.100.3` |
| `GEMINI_ORACLE_CLUSTER_IP` | IP of the oracle node | `192.168.100.2` |

For cluster tests (3-node), use `docker/docker-compose-scylla-cluster.yml` instead.

---

## Skill: Partitions System

**Package**: `pkg/partitions/` | **Key files**: `partitions.go`, `deleted.go`

### Purpose

Thread-safe management of partition key values used by mutation and validation workers.
Partitions are the central data structure -- every mutation and validation operation starts
by obtaining partition keys from this pool.

### Interface

```go
type Interface interface {
    Get(idx uint64) typedef.PartitionKeys     // Get partition at index
    Next() typedef.PartitionKeys              // Get next partition via distribution
    Extend() typedef.PartitionKeys            // Add a new partition to the pool
    Replace(idx uint64) typedef.PartitionKeys // Replace partition at index (returns old for delete tracking)
    Deleted() <-chan typedef.PartitionKeys     // Channel of partitions ready for delete validation
    ValidationSuccess(values *typedef.PartitionKeys)
    ValidationFailure(values *typedef.PartitionKeys)
    MarkInvalid(keys *typedef.PartitionKeys) bool
    IsInvalid(idx uint64) bool
    Len() uint64
    Close()
}
```

### Architecture

- **Storage**: Flat slice of `*Partition` pointers stored via `atomic.Pointer[[]*Partition]`.
  Individual partitions hold data in `atomic.Pointer[partitionData]` for lock-free reads.
- **Thread safety**: No mutex for reading/writing individual partitions (atomic pointer swaps).
  A mutex (`extendMu`) is only held during `Extend()` which copies the slice (rare operation).
- **Partition selection**: `idxFunc` (distribution function) picks the next index. `pickValidIdx()`
  skips invalid partitions with three strategies: fast-path (no invalids), linear scan (>50% invalid),
  random retry.
- **Validation tracking**: `sync.Map` keyed by UUID. `ValidationData` tracks first/last success
  timestamps, failure timestamps, and a ring buffer of recent timestamps -- all using atomics.
- **Invalid partitions**: `MarkInvalid()` uses CAS loop on `invalidCount` + `LoadOrStore` on
  `invalidByIdx` for thread-safe, idempotent marking. `maxInvalid` acts as error budget.
- **Reference counting**: `PartitionKeys.Release` function decrements `refCount`; when it hits
  zero, validation data is cleaned up.

### Delete Tracking (`deleted.go`)

- Min-heap sorted by `readyAt` time, with inlined heap operations (bit-shift arithmetic).
- Background goroutine emits ready partitions to a buffered channel (50K capacity).
- Time buckets define re-validation schedule with jitter (20% of bucket duration, max 5s).
- Adaptive check intervals and heap shrinking when utilization drops below 25%.

### Gotcha

**Must call `MarkInvalid()` BEFORE `releaseKeys()`** because release may trigger
`deleteValidation` which removes the UUID from `uuidToIdx`.

---

## Skill: Statement Logger

**Package**: `pkg/stmtlogger/`, `pkg/stmtlogger/scylla/` | **Key files**: `logger.go`, `scylla/scylla.go`

### Purpose

Async logging of all CQL mutations to files and a ScyllaDB `_logs` keyspace for post-mortem
analysis when validation failures occur.

### Architecture

- **Logger**: Wraps a buffered channel (`chan Item`, default 131072 = 128K items) and an inner
  `io.Closer` (the ScyllaDB writer).
- **Item struct**: Contains query string, values, host, duration, partition keys, error,
  validation timestamps (first/last success, last failure, recent successes), attempt counters.
- **Filtering**: Schema statements are never logged. SELECT statements are only logged if they
  carry validation timestamp data (indicating a notable event).
- **Blocking behavior**: `LogStmt` blocks until the item is accepted or the logger is closed.
  No drops in normal operation.

### ScyllaDB Logger (`stmtlogger/scylla/`)

- Reads from the shared channel and writes statements to ScyllaDB tables in batches
  (`committerBatchSize = 256`).
- Also writes to JSON files with buffered writers (64KB buffer).
- Manages its own CQL sessions to the oracle cluster.

### Lifecycle

- `Close()` is idempotent (via `sync.Once`). Signals via `done` channel, closes the data
  channel, then closes the inner logger.
- Created in `store.New()`. The shared channel is passed to both the `stmtlogger.Logger` and
  the `scylla.Logger`. CQL observers on each `cqlStore` feed items into the logger.

---

## Skill: Store

**Package**: `pkg/store/` | **Key files**: `store.go`, `cqlstore.go`, `compare.go`

### Purpose

Dual-cluster database abstraction that runs CQL against test + oracle clusters and compares
results for validation.

### Interface

```go
type Store interface {
    io.Closer
    Create(context.Context, *typedef.Stmt, *typedef.Stmt) error
    Mutate(context.Context, *typedef.Stmt) error
    Check(context.Context, *typedef.Table, *typedef.Stmt, int) (int, error)
    CheckOnce(ctx context.Context, table *typedef.Table, stmt *typedef.Stmt, attempt int) (int, error)
}
```

### Architecture

- **delegatingStore**: Wraps two `cqlStore` instances (test + optional oracle).
- **cqlStore**: Wraps a `gocql.Session`. Handles `mutate()` (CQL write) and `load()` (CQL read,
  returns `Rows`). Pre-resolves Prometheus counters per statement type to avoid per-call label
  lookups. Tracks prepared statements via `stmtTracker`.

### Mutation Flow

1. Runs test and oracle mutations in parallel via goroutines and a pooled channel (`sync.Pool`).
2. Retries with exponential backoff (`utils.Backoff`).
3. Only retries the store that failed (tracks `retryTest`/`retryOracle` independently).
4. Returns `MutationError` with per-store success tracking.

### Validation Flow

1. Loads rows from both stores in parallel via pooled channel.
2. Calls `CompareCollectedRows()` which sorts rows by PK+CK, deduplicates list values
   (Scylla LWT retry artifact), then compares.
3. Three comparison cases: both empty = success, different row counts = reports missing/extra
   rows, same count = per-row column-level diff.
4. Returns `ValidationError` with `ComparisonResults` attached.

### Timestamps

Supports client-side timestamps (default) or server-side timestamps. Client-side uses
`mo.Option[time.Time]` passed to both stores for consistency.

### Gotcha

**`deepCopyValue` in cqlstore.go is critical** -- gocql reuses internal buffers, so byte slices
and `big.Int`/`inf.Dec` values must be deep-copied before storing.

---

## Skill: Jobs

**Package**: `pkg/jobs/` | **Key files**: `jobs.go`, `mutation.go`, `validation.go`, `retryqueue.go`

### Purpose

Worker orchestration that ties together partitions, statement generation, store operations,
and error budgeting into concurrent test execution.

### Architecture

- **Entry point**: `Jobs.Run()` takes a mode, duration, distribution function, and partition
  config. Creates `errgroup` with timeout context.
- **Per-table workers**: For each table in the schema, spawns `mutationConcurrency` Mutation
  workers and/or `readConcurrency` Validation workers depending on mode.

### Mutation Worker (`mutation.go`)

- Loop calls `statement.MutateStatement()` then `store.Mutate()`.
- On success: increments write ops counter.
- On `MutationError`: creates `joberror.JobError`, adds to write errors, checks error budget.
- Handles `context.Canceled` and `context.DeadlineExceeded` gracefully.

### Validation Worker (`validation.go`)

- Main loop uses `select` with four cases: context done, deleted partition ready (from
  `generator.Deleted()` channel), retry timer fired, or new work (default).
- `checkOnce` calls `store.CheckOnce`. On success, records `ValidationSuccess` on partition.
- On failure, can either schedule retry (into `retryQueue`) or call `handleCheckFailure`.

### handleCheckFailure

1. Records `ValidationFailure` on partition.
2. Calls `MarkInvalid` on all partition keys (**must happen before `releaseKeys`**).
3. Builds `JobError` only for newly-marked partitions (deduplication).

### Deleted Partition Validation

Validation workers consume from `generator.Deleted()` channel. Creates SELECT statement for
the partition keys and validates via `store.Check()` (with retries). Expects zero rows on
both test and oracle.

### Retry Queue (`retryqueue.go`)

Single-goroutine queue with backoff timers. Not thread-safe by design (one per validation
worker). Stores `pendingRetry` with `*typedef.Stmt`, attempt count, and timer.

### Stop Conditions

`stop.Flag` with soft/hard modes. Soft stop triggered by error budget or duration timeout.
Workers check `stopFlag.IsHardOrSoft()` at loop top.

### Modes

| Mode | Workers | Behavior |
|---|---|---|
| `mixed` | Mutation + Validation | Default: concurrent writes and reads |
| `write` | Mutation only | Writes without validation |
| `read` | Validation only | Reads without mutations |
| `warmup` | Mutation (no deletes) | Populates data without deletes |

---

## Skill: Statement Generation

**Package**: `pkg/statements/` | **Key files**: `statements.go`, `mutations.go`, `select.go`, `insert.go`, `delete.go`

### Purpose

Generates random CQL statements (INSERT, UPDATE, DELETE, SELECT) for a given table with
pre-cached query strings and pre-resolved metrics.

### Architecture

- **Generator struct**: Holds partitions interface, table metadata, random source, ratio
  controller, and pre-cached query strings.
- **Cached queries**: `buildCachedQueries()` pre-builds CQL strings at construction time.
  Same table schema always produces the same CQL, so queries are cached for the lifetime of
  the generator.
- **Pre-resolved metrics**: Prometheus counters resolved at construction to avoid
  `WithLabelValues()` map lookups in the hot path.
- **Ratio controller**: Configurable ratios for mutation types (insert/update/delete) and
  validation types (single/multi partition, index, clustering range). Each category sums to 1.0.

### Statement Types

| Category | Types |
|---|---|
| SELECT | SinglePartition, MultiplePartition, ClusteringRange, MultiPartitionClusteringRange, SingleIndex |
| DELETE | WholePartition, SingleRow, SingleColumn, MultiplePartitions |
| INSERT | Regular, JSON |
| UPDATE | Counter table updates |

### Multi-Partition Queries

Uses Cartesian product calculation bounded by `MaxCartesianProductCount` (100) to limit IN
clause sizes for multi-partition SELECT and DELETE queries.
