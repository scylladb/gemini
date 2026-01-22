# Quick Reference

Common Gemini commands and options for everyday use.

## Basic Commands

```bash
# Standard test run (30 seconds)
./gemini --oracle-cluster=ORACLE_IP --test-cluster=TEST_IP

# Extended run with statement logging
./gemini \
  --oracle-cluster=ORACLE_IP \
  --test-cluster=TEST_IP \
  --duration=1h \
  --test-statement-log-file=test.json \
  --oracle-statement-log-file=oracle.json

# Stress test (no validation)
./gemini --test-cluster=TEST_IP --mode=write --duration=2h

# Read-only validation
./gemini --oracle-cluster=ORACLE_IP --test-cluster=TEST_IP --mode=read
```

## Common Options

| Option | Example | Description |
|--------|---------|-------------|
| `--duration` | `1h`, `30m`, `24h` | How long to run |
| `--concurrency` | `10`, `50`, `100` | Parallel workers |
| `--mode` | `mixed`, `write`, `read` | Operation mode |
| `--seed` | `12345` | Reproducible randomness |
| `--schema-seed` | `67890` | Reproducible schema |
| `--fail-fast` | (flag) | Stop on first error |
| `--warmup` | `5m` | Warmup period before validation |

## Statement Logging

```bash
# Enable statement logging for debugging
--test-statement-log-file=test.json
--oracle-statement-log-file=oracle.json

# With compression for long runs
--statement-log-file-compression=gzip
```

## Schema Control

```bash
# Use custom schema
--schema=my_schema.json

# Generate specific schema size
--max-tables=3
--max-partition-keys=4
--max-clustering-keys=2
--max-columns=8

# Feature level
--cql-features=all  # basic, normal, all
```

## Special Features

```bash
# Counter tables
--use-counters

# Lightweight transactions
--use-lwt

# Server-side timestamps
--use-server-timestamps

# Custom statement ratios
--statement-ratios='{"mutation":{"insert":0.8,"update":0.15,"delete":0.05}}'
```

## Authentication

```bash
--test-username=user --test-password=pass
--oracle-username=user --oracle-password=pass
```

## Timeouts

```bash
--request-timeout=30s
--connect-timeout=30s
```

## Partition Distribution

```bash
# Distribution types
--partition-key-distribution=uniform  # Even distribution
--partition-key-distribution=zipf     # Hot spots (default)
--partition-key-distribution=normal   # Bell curve

# Partition count
--partition-count=2000000
```

## Output

```bash
# Write results to file
--outfile=results.json

# Log level
--level=debug  # debug, info, warn, error
```

## Docker

```bash
# Basic run
docker run -it scylladb/gemini:latest \
  --oracle-cluster=ORACLE_IP \
  --test-cluster=TEST_IP

# With mounted volume for logs
docker run -it \
  -v $(pwd)/logs:/logs \
  scylladb/gemini:latest \
  --oracle-cluster=ORACLE_IP \
  --test-cluster=TEST_IP \
  --test-statement-log-file=/logs/test.json
```

## Reproducing a Run

When Gemini starts, it prints the seed values:
```
Seed:           12345678901234
Schema seed:    98765432109876
```

To reproduce:
```bash
./gemini \
  --seed=12345678901234 \
  --schema-seed=98765432109876 \
  --oracle-cluster=... \
  --test-cluster=...
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success, no errors |
| 1 | Validation errors found |
| 2 | Configuration/startup error |

## Metrics

Prometheus metrics available at `http://localhost:2112/metrics`

Change bind address:
```bash
--bind=0.0.0.0:9090
```

## Files Generated

| File | Description |
|------|-------------|
| `gemini.log` | JSON application logs |
| `test.json` | Test cluster statement logs (if enabled) |
| `oracle.json` | Oracle cluster statement logs (if enabled) |
| ScyllaDB `<ks>_logs` | Statements stored in database |

