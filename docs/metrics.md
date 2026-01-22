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

