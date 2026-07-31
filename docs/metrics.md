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
| `stmt_error_last_timestamp_seconds` | Gauge | Last error timestamp per partition |

### Workers

| Metric | Type | Description |
|--------|------|-------------|
| `workers_current` | Gauge | Active workers by job type |

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

A pre-built Grafana dashboard is available at `docker/monitoring/Gemini.json`. Import it to visualize:
- Request rates and errors
- Query latencies
- Connection status
- Worker activity
- Memory usage

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

