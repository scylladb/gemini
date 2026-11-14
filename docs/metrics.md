# Metrics Reference

Gemini exposes Prometheus metrics for monitoring test runs. This guide covers the available metrics and how to use them.

## Accessing Metrics

Metrics are available via HTTP:

```bash
# Default endpoint
curl http://localhost:2112/metrics

# Custom bind address
./gemini --bind=0.0.0.0:9090 ...
curl http://localhost:9090/metrics
```

## Key Metrics

### CQL Operations

| Metric | Type | Description |
|--------|------|-------------|
| `cql_requests` | Counter | Total CQL requests by system (oracle/test) and method |
| `cql_error_requests` | Counter | Failed CQL requests |
| `cql_query_timeouts` | Counter | Query timeouts by cluster and query type |
| `cql_queries` | Counter | Queries by cluster, host, and query type |
| `cql_query_errors` | Counter | Query errors with error type |
| `cql_batches` | Counter | Batch operations |
| `cql_batched_queries` | Counter | Queries within batches |
| `cql_rows_returned_total` | Counter | Rows returned by read queries, by system |
| `cql_rows_per_query` | Histogram | Rows returned per read query, by system |
| `cql_prepared_stmts_unique` | Gauge | Unique prepared statement strings seen, by system |
| `cql_prepared_stmts_ratio` | Gauge | Unique prepared statements divided by the driver cache size, capped at 1.0 |
| `cql_prepared_stmts_new_total` | Counter | Prepared statement strings seen for the first time, by system |
| `context_cancellations_total` | Counter | Mutations and loads that ended on a cancelled or expired context, by operation and reason |

### Driver

The cluster observer records these metrics. Gemini installs the observer only
when an oracle is configured and both `--test-statement-log-file` and
`--oracle-statement-log-file` are set. Without it, these metrics stay at zero.

| Metric | Type | Description |
|--------|------|-------------|
| `cql_driver_retry_attempts` | Histogram | Driver attempt number per query, by cluster and query type. Attempt 0 means no retry |
| `cql_driver_query_latency_by_attempt` | Histogram | Query latency in microseconds, by cluster and attempt (`0` to `3`, then `4+`) |
| `cql_driver_query_rows` | Histogram | Rows per page of a read query, by cluster |
| `cql_driver_errors_by_type_total` | Counter | Driver errors by cluster and category, for example `write_timeout` or `connect_host_down` |
| `cql_driver_host_up` | Gauge | 1 when the driver considers a host up, 0 when down, by cluster and host |

### Query Timing

| Metric | Type | Description |
|--------|------|-------------|
| `cql_query_time` | Histogram | Query execution time in seconds |
| `cql_connect_time` | Histogram | Connection establishment time |
| `execution_time` | Histogram | Task execution time |

### Connections

| Metric | Type | Description |
|--------|------|-------------|
| `cql_connections` | Gauge | Active connections by cluster and host |
| `cql_connections_errors` | Counter | Connection errors |

### Validation

| Metric | Type | Description |
|--------|------|-------------|
| `validated_rows` | Counter | Successfully validated rows by table |
| `execution_errors` | Counter | Execution errors by type |
| `validation_rows_matched_total` | Counter | Rows equal on test and oracle |
| `validation_rows_missing_in_test_total` | Counter | Rows on the oracle that the test cluster does not return |
| `validation_rows_missing_in_oracle_total` | Counter | Rows on the test cluster that the oracle does not return |
| `validation_rows_different_total` | Counter | Rows on both clusters with different values |
| `validation_rows_deduplicated_total` | Counter | Duplicate list values removed before comparison, counted on the first attempt only |
| `validation_retries_scheduled_total` | Counter | Validations put in the retry queue |
| `validation_retries_exhausted_total` | Counter | Validations that used all retry attempts |
| `validation_retries_pending` | Gauge | Validations waiting in the retry queue |
| `validation_retries_abandoned_total` | Counter | Retries left unrun because validation stopped hard or its context ended. After a soft stop, the workers run the queued retries first |
| `validation_retries_dropped_total` | Counter | Retries dropped because the retry queue was full. These do not count as validation failures |
| `mutation_retries_total` | Counter | Mutation retries, by outcome (`success` adds the retries before success, `exhausted` adds one per failed mutation) |

The four row-comparison counters add up on every validation attempt. A row
that differs on the first attempt and matches after a retry counts in both
`validation_rows_different_total` and `validation_rows_matched_total`.

### Partitions

| Metric | Type | Description |
|--------|------|-------------|
| `partitions_extended_total` | Counter | Partition slots added to the pool |
| `partitions_replaced_total` | Counter | Partition slots replaced after a whole-partition delete |
| `partitions_invalid` | Gauge | Partitions marked invalid and skipped by validation |
| `deleted_partitions_pending` | Gauge | Deleted partitions that wait for re-validation |
| `deleted_partitions_emitted_total` | Counter | Deleted partitions sent to the validators, once per time bucket |

### Statements

| Metric | Type | Description |
|--------|------|-------------|
| `statements_generated_total` | Counter | Generated statements, by type (`select` or `mutation`) and subtype (the statement type) |
| `statement_ratio_configured` | Gauge | Configured statement ratios, set once at startup |

### Dual-Write Divergence

| Metric | Type | Description |
|--------|------|-------------|
| `mutation_asymmetric_acks_total` | Counter | Dual-write mutations acknowledged by one cluster but not the other |
| `mutation_compensation_failures_total` | Counter | Compensating whole-partition `DELETE`s that failed, by the cluster whose delete failed |

Labels:

| Label | Values | Meaning |
|-------|--------|---------|
| `outcome` | `compensated` | A best-effort whole-partition `DELETE` was issued to **both** clusters, making the partition deterministically empty regardless of which side actually committed. The run continues and the partition stays valid. |
| `outcome` | `uncompensated` | Compensation did not apply or failed. The affected partitions were marked invalid so validation skips them. |
| `acked_store` | `test` / `oracle` | The cluster that **did** acknowledge the write. |

**This counts acknowledgements, not confirmed divergence.** A non-nil error from a
store proves it did not acknowledge; it does *not* prove the write was never
applied, because a timed-out server may have committed and lost the response.
Treat a non-zero value as "the dual write may not have stayed symmetric — verify
before trusting validation results from this run", **not** as "the clusters are
definitely inconsistent". The metric is derived from gemini's own per-store
bookkeeping, not from the statement log, so it stays accurate even when the
logger is stalled or back-pressured.

Only emitted when an oracle cluster is configured.

Example alert — any asymmetry at all is worth a look, since a healthy run should
produce none:

```promql
sum(increase(gemini_mutation_asymmetric_acks_total[10m])) by (outcome, acked_store) > 0
```

Uncompensated asymmetry is the more serious case (partitions were dropped from
validation coverage rather than repaired):

```promql
sum(rate(gemini_mutation_asymmetric_acks_total{outcome="uncompensated"}[5m])) > 0
```

#### Compensation failures

`mutation_compensation_failures_total` is the companion signal. Compensation
runs after a write times out and issues a whole-partition `DELETE` to **both**
clusters, forcing the partition to a known-empty state so an ambiguous timeout
cannot become a divergence. A failure means that collapse did not happen — the
partition may hold a committed-but-timed-out write on one cluster and nothing on
the other.

This matters even when the acknowledgement metric stays flat. If *both* original
writes time out, the two success flags are equal and no asymmetric ack is
recorded, yet each server may independently have committed. A half-successful
compensation in that state leaves the clusters genuinely different. Gemini marks
those partitions invalid so validation skips them, so this never produces a false
bug report — but a sustained non-zero rate means the run is steadily losing
partition coverage and its results are correspondingly weaker.

Invalidation is driven by the asymmetry itself, not by the error kind: a write
that commits on one cluster and fails on the other with any error — timeout,
`Unavailable`, `WriteFailure` — takes its partitions out of validation coverage.
Only the *error accounting* distinguishes timeouts (exempt, to keep a slow
runner from exhausting the error budget) from real failures (charged).

```promql
sum(rate(gemini_mutation_compensation_failures_total[5m])) by (store) > 0
```

> **Note on naming:** metric names in the tables above are written without the
> registry prefix. All gemini metrics are exported with a `gemini_` prefix, so
> the counter above is queried as `gemini_mutation_asymmetric_acks_total`.

### Statement Logger

| Metric | Type | Description |
|--------|------|-------------|
| `statement_logger_enqueued_total` | Counter | Items sent to statement logger |
| `statement_logger_dequeued_total` | Counter | Items processed by statement logger |
| `statement_logger_items` | Gauge | Current items in logger |
| `statement_logger_flushes_total` | Counter | File flush operations |
| `statement_logger_unpaired_total` | Counter | Statement log pairs emitted with only one cluster. This occurs when the other cluster's item does not arrive within 2 seconds, when one cluster reports the same execution twice, or when the logger shuts down before the other cluster's item arrives |
| `stmt_error_last_timestamp_seconds` | Gauge | Last error timestamp per partition |

### Workers

| Metric | Type | Description |
|--------|------|-------------|
| `workers_current` | Gauge | Active workers by job type |
| `concurrency_limit` | Gauge | Workers of each pool that run at a time. The limiter lowers it while latency shows the clusters queue requests |
| `worker_stop_events_total` | Counter | Worker stops, by job and reason |

### Go Runtime

Standard Go metrics are included:
- `go_goroutines` - Current goroutines
- `go_memstats_*` - Memory statistics
- `go_gc_*` - Garbage collection stats
- `go_sync_mutex_wait_total_seconds` - Mutex contention

## Useful Queries

### PromQL Examples

Operations per second:
```promql
rate(cql_requests[1m])
```

Error rate:
```promql
rate(cql_error_requests[1m]) / rate(cql_requests[1m])
```

Query latency (p99):
```promql
histogram_quantile(0.99, rate(cql_query_time_bucket[5m]))
```

Active workers:
```promql
sum(workers_current) by (job)
```

Validation throughput:
```promql
rate(validated_rows[1m])
```

### Grafana Dashboard

A pre-built Grafana dashboard is available at `docker/monitoring/Gemini.json`. It has a panel for every
metric in this document, in these rows:
- Overview: errors, workers, requests and the running workers per pool
- Throughput: operations, requests and execution time
- Concurrency: the worker limit, the mean query latency it follows, and the started workers
- Validation & Data Correctness: compared rows, rows per query, asymmetric writes and compensation
- CQL Driver: query time, errors, connections, prepared statements, retries, host state and batches
- Statement Ratios: configured and actual statement ratios
- Partitions & Retries: invalid, replaced and deleted partitions, and mutation and validation retries
- Statement Logger: unpaired rows, malformed items, the overflow ring and file flushes
- Go Runtime and Go Process: memory, goroutines, GC and CPU

## Setting Up Monitoring

### With Docker Compose

The monitoring stack is included in the cluster setup:

```bash
make scylla-setup-cluster
```

Access:
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000

### Manual Setup

1. Add Gemini to Prometheus targets:
```yaml
scrape_configs:
  - job_name: 'gemini'
    static_configs:
      - targets: ['localhost:2112']
```

2. Import the Grafana dashboard from `docker/monitoring/Gemini.json`

## Alerting Examples

### High Error Rate

```yaml
alert: GeminiHighErrorRate
expr: rate(cql_error_requests[5m]) / rate(cql_requests[5m]) > 0.01
for: 5m
labels:
  severity: warning
annotations:
  summary: "Gemini error rate above 1%"
```

### Query Latency

```yaml
alert: GeminiSlowQueries
expr: histogram_quantile(0.99, rate(cql_query_time_bucket[5m])) > 1
for: 5m
labels:
  severity: warning
annotations:
  summary: "Gemini p99 latency above 1 second"
```

### Connection Issues

```yaml
alert: GeminiConnectionErrors
expr: rate(cql_connections_errors[5m]) > 0
for: 1m
labels:
  severity: critical
annotations:
  summary: "Gemini experiencing connection errors"
```

