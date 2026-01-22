# Investigation Guide

When Gemini reports a failure, it means the test cluster (SUT) returned different data than the oracle cluster. This guide walks you through investigating these discrepancies.

## Where to Find Errors

### 1. Console Output

Gemini prints errors directly to stdout as they occur. You'll see something like:

```
validation failed for partition [123, "user_abc"]
oracle returned 5 rows, test returned 3 rows
```

### 2. Log File (gemini.log)

Gemini writes detailed JSON logs to `gemini.log` in the current directory. Each line is a JSON object:

```json
{"level":"error","ts":"2025-01-20T10:15:30.123Z","msg":"validation failed","partition":[123,"user_abc"],"error":"row mismatch"}
```

Use `jq` to filter errors:
```bash
cat gemini.log | jq 'select(.level == "error")'
```

### 3. Statement Logs

When enabled, statement logs capture mutations for partitions that had errors. Statements are stored both in ScyllaDB (for querying) and optionally written to files.

## Enabling Statement Logging

Enable statement logging with file output for investigation runs:

```bash
./gemini \
  --test-cluster=192.168.1.10 \
  --oracle-cluster=192.168.1.20 \
  --test-statement-log-file=test_statements.json \
  --oracle-statement-log-file=oracle_statements.json \
  --duration=1h
```

For long runs, enable compression:
```bash
--statement-log-file-compression=gzip
```

## Statement Log Storage

Gemini stores statements in two ways:

### ScyllaDB Tables

Statements are automatically stored in a separate keyspace `<keyspace>_logs` with table `<table>_statements`. This allows querying historical statements directly from ScyllaDB.

### File Output

When statement log files are configured, Gemini writes error context to JSON files. Each line represents a partition that had an error, containing all relevant statements for that partition.

## Reading Statement Log Files

Each log entry contains information about a partition that encountered an error:

| Field | Description |
|-------|-------------|
| `partitionKeys` | Map of partition key column names to their values |
| `timestamp` | When the error occurred |
| `err` | Error message |
| `query` | The query that triggered the error |
| `message` | Additional error context |
| `mutationFragments` | Low-level mutation data from MUTATION_FRAGMENTS |
| `statements` | All statements executed on this partition |

Example entry:
```json
{
  "partitionKeys": {"col1": 5, "col2": "test_value"},
  "timestamp": "2025-01-20T10:15:30Z",
  "err": "row mismatch: oracle has 5 rows, test has 3",
  "query": "SELECT * FROM ks.table1 WHERE col1 = ? AND col2 = ?",
  "message": "validation failed",
  "mutationFragments": [...],
  "statements": [
    {"ts": "2025-01-20T10:14:00Z", "statement": "INSERT INTO...", "values": [...], "host": "192.168.1.10", "attempt": 1, "gemini_attempt": 1, "dur": "1ms"},
    {"ts": "2025-01-20T10:14:30Z", "statement": "UPDATE...", "values": [...], "host": "192.168.1.10", "attempt": 1, "gemini_attempt": 1, "dur": "2ms"}
  ]
}
```

### Statement Entry Fields

Each entry in the `statements` array contains:

| Field | Description |
|-------|-------------|
| `ts` | Timestamp when statement was executed |
| `statement` | The CQL query |
| `values` | Bound parameter values |
| `host` | Host that executed the query |
| `attempt` | Driver retry attempt number |
| `gemini_attempt` | Gemini-level retry attempt number |
| `error` | Error message (if the statement failed) |
| `dur` | Execution duration |

## Investigation Steps

### Step 1: Find the Failed Partition

Look at the error output to identify which partition key failed validation. The error will include the partition key values.

### Step 2: Examine the Statement Log Entry

Find the relevant entry in the statement log file:

```bash
# Search for a specific partition key value
cat test_statements.json | jq 'select(.partitionKeys.col1 == 123)'
```

### Step 3: Review All Statements for That Partition

The `statements` array contains the full history of mutations:

```bash
# Get all statements for a partition
cat test_statements.json | jq 'select(.partitionKeys.col1 == 123) | .statements'
```

### Step 4: Query Both Clusters Manually

Connect to both clusters and run the same SELECT:

```bash
# Oracle cluster
cqlsh 192.168.1.20 -e "SELECT * FROM ks.table1 WHERE col1 = 123;"

# Test cluster
cqlsh 192.168.1.10 -e "SELECT * FROM ks.table1 WHERE col1 = 123;"
```

Compare the results to understand the discrepancy.

### Step 5: Check Mutation Fragments

For deeper analysis, examine the `mutationFragments` which show the low-level mutation data from ScyllaDB:

```bash
cat test_statements.json | jq 'select(.partitionKeys.col1 == 123) | .mutationFragments'
```

### Step 6: Query Statement Logs in ScyllaDB

You can also query the statement logs directly from ScyllaDB:

```bash
cqlsh 192.168.1.10 -e "SELECT * FROM ks_logs.table1_statements WHERE col1 = 123;"
```

## Common Issues

### Row Count Mismatch

When oracle returns more rows than test, it usually means:
- A DELETE was applied on test but failed on oracle
- An INSERT succeeded on oracle but failed on test
- Data corruption or replication issues on test

### Value Differences

When rows exist on both but have different values:
- An UPDATE was applied differently
- Timestamp issues (check `--use-server-timestamps`)
- Type conversion problems

### Timeout Errors

If you see timeout errors in statement logs:
- Increase `--request-timeout`
- Check cluster health
- Reduce `--concurrency`

### Too Many Errors

If Gemini stops with many errors:
- Check if the test cluster is healthy
- Review network connectivity
- Look at Scylla/Cassandra logs on the test cluster

## Useful Commands

Find all error entries:
```bash
cat test_statements.json | jq 'select(.err != null and .err != "")'
```

Count statements per partition:
```bash
cat test_statements.json | jq '{pk: .partitionKeys, count: (.statements | length)}'
```

Extract just the CQL statements:
```bash
cat test_statements.json | jq '.statements[].statement'
```

Find slow statements:
```bash
cat test_statements.json | jq '.statements[] | select(.dur | test("^[0-9]+s"))'
```

Extract schema from logs:
```bash
cat gemini.log | jq 'select(.msg | contains("Schema"))' | head -1
```

## Reporting Bugs

When reporting a potential Scylla bug:

1. **Seed values** - Note the `--seed` and `--schema-seed` used (printed at startup)
2. **Schema** - Include the generated schema from logs
3. **Statement logs** - Attach the relevant statement log file entries
4. **Scylla version** - Include versions of both clusters
5. **Gemini version** - Run `./gemini --version`

With the seed values, the exact same test can be replayed:
```bash
./gemini --seed=12345 --schema-seed=67890 \
  --test-cluster=... --oracle-cluster=...
```
