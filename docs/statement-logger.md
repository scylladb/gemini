# Gemini Statement Logger

The Gemini Statement Logger captures mutation statements (INSERT, DELETE) executed during testing sessions and stores them both in ScyllaDB and optionally in files. When validation errors occur, Gemini provides the full mutation history for the affected partitions.

> **Note**: Only mutations are logged, not SELECT queries or DDL statements.

For practical guidance on using statement logs to debug failures, see the [Investigation Guide](investigation.md).

## How It Works

Gemini logs mutation statements (INSERT, DELETE) during test execution:

1. **ScyllaDB Storage**: Mutations are automatically stored in a dedicated logs keyspace (`<keyspace>_logs`) with a table named `<table>_statements`
2. **Error Detection**: When a validation error occurs, Gemini fetches all mutations for the affected partition
3. **File Output**: If statement log files are configured, error context is written to JSON files with full mutation history
4. **Mutation Fragments**: For deeper analysis, Gemini can also fetch low-level mutation data using ScyllaDB's MUTATION_FRAGMENTS function

## Enabling Statement Logging

### Basic Setup

Statement logging to ScyllaDB happens automatically. For file output, specify log files:

```bash
gemini --test-statement-log-file=test_statements.json \
       --oracle-statement-log-file=oracle_statements.json \
       --oracle-cluster=192.168.1.10 \
       --test-cluster=192.168.1.20
```

### With Compression

For long-running tests, enable compression to save disk space:

```bash
gemini --test-statement-log-file=test_statements.json.gz \
       --oracle-statement-log-file=oracle_statements.json.gz \
       --statement-log-file-compression=gzip \
       --oracle-cluster=192.168.1.10 \
       --test-cluster=192.168.1.20
```

## CLI Flags

| Flag | Description |
|------|-------------|
| `--test-statement-log-file` | File path to write test cluster error context |
| `--oracle-statement-log-file` | File path to write oracle cluster error context |
| `--statement-log-file-compression` | Compression: `none` (default), `gzip`, `zstd` |

## Log File Format

Each line in the log file represents a partition that had a validation error. The format is:

```json
{
  "partitionKeys": [{"partitionKeys": {"col1": 5, "col2": "value"}}],
  "timestamp": "2026-01-20T10:15:30Z",
  "err": "row mismatch: oracle has 5 rows, test has 3",
  "query": "SELECT * FROM ks.table1 WHERE col1 = ? AND col2 = ?",
  "message": "validation failed",
  "mutationFragments": [...],
  "statements": [...],
  "partial": true
}
```

### Top-Level Fields

| Field | Description |
|-------|-------------|
| `partitionKeys` | Array of partitions. Each entry holds its key columns and, if present, `lastValidations` |
| `timestamp` | When the error was detected |
| `err` | Error message describing the validation failure |
| `query` | The SELECT query that detected the mismatch |
| `message` | Additional context about the error |
| `mutationFragments` | Low-level mutation data from MUTATION_FRAGMENTS |
| `statements` | Array of mutations executed on this partition |
| `partial` | Present and `true` only when a read of `_logs` broke part way. The two arrays above are then a lower bound, not the whole history |

### Mutations Array Format

Each entry in the `statements` array is one execution on the cluster of the
file. Gemini builds it from the `data` cell of the `_logs` row and the CQL text
in the `queries` table. It skips the rows that hold only the other cluster. The
partition key is in the `partitionKeys` field of the line.

```json
{
  "ts": 1768903440000000000,
  "seq": 41,
  "statement": "INSERT INTO ks.table1 (col1, col2, val) VALUES (?, ?, ?)",
  "values": [5, "value", "0x64617461"],
  "host": "192.168.1.10",
  "attempt": 1,
  "gemini_attempt": 1,
  "dur": "1ms"
}
```

| Field | Description |
|-------|-------------|
| `ts` | Time the statement started on this cluster, as UTC nanoseconds. Not a CQL timestamp |
| `seq` | Per-writer counter. It keeps two statements apart that share the same `ts`, `attempt` and `gemini_attempt` |
| `statement` | The CQL query |
| `values` | Bound parameter values as a JSON array (see [Values Format](#values-format)) |
| `host` | Host that executed the query |
| `attempt` | Driver retry attempt number (1-indexed) |
| `gemini_attempt` | Gemini-level retry attempt number (0-indexed, will be aligned with `attempt` in future release) |
| `error` | Error message (only present if statement failed) |
| `dur` | Execution duration, in Go duration format |

### Values Format

The `values` array holds the bound values of the statement in bind order:

| CQL type | JSON form |
|----------|-----------|
| `text`, `ascii`, `varchar` | string |
| integer types, `float`, `double` | number |
| `varint`, `decimal` | number with every digit |
| `boolean` | `true` or `false` |
| `blob` | string with the `0x` prefix of a CQL blob literal |
| `uuid`, `timeuuid`, `inet` | string |
| `timestamp`, `date` | RFC 3339 string |
| `time`, `duration` | number of nanoseconds |
| `list`, `set`, `tuple` | array |
| `map`, UDT | object with sorted keys. A key that is not a string becomes the string of its JSON form |

A `float` or `double` that JSON cannot hold (`NaN`, `Infinity`) is a string.

## Querying Mutation Logs in ScyllaDB

Mutations are stored in ScyllaDB for direct querying. A row stores the query id
of its statement, and the `queries` table maps every id to its CQL text:

```bash
# Find all mutations for a specific partition, on both clusters
cqlsh -e "SELECT ts, attempt, gemini_attempt, seq, data FROM ks_logs.table1_statements WHERE col1 = 5 AND col2 = 'value';"

# Read the CQL text of a query id from the "q" field of data
cqlsh -e "SELECT statement FROM ks_logs.queries WHERE id = 4417529160735123462;"
```

One row holds one execution of a statement on both clusters. The committer
pairs the test and the oracle observation of the same statement, partition,
gemini attempt and driver attempt, and writes them as one row. A cluster with
no observation within 2 seconds has no key in `data`, and
`statement_logger_unpaired_total` counts that row.

The logs table schema includes:
- Partition key columns (matching the original table)
- `ts` - Earlier start of the two clusters, as UTC nanoseconds (`bigint`)
- `attempt` - Driver attempt
- `gemini_attempt` - Gemini attempt
- `seq` - Per-writer counter that keeps statements of the same `ts` apart
- `data` - One JSON object with everything else about the execution

The primary key is `((<partition key columns>), ts, attempt, gemini_attempt, seq)`.
`seq` is the last clustering column. Without it, two statements that share every
other key column overwrite each other, and the history loses rows.

The `data` object has these fields:

| Field | Description |
|-------|-------------|
| `q` | Query id. The `queries` table holds its CQL text |
| `v` | Bound values (see [Values Format](#values-format)) |
| `t`, `o` | Observation of the test and the oracle cluster. A cluster that gemini did not observe has no key |

Each observation has these fields:

| Field | Description |
|-------|-------------|
| `ts` | Start on that cluster, as UTC nanoseconds |
| `h` | Executing host |
| `e` | Error message. Left out when the statement succeeded |
| `d` | Duration, in nanoseconds |

## Performance Considerations

- **Asynchronous Processing**: Mutation logging uses background workers and doesn't block query execution
- **Batched Writes**: Mutations are batched before writing to ScyllaDB for efficiency
- **Compact Rows**: A row writes one `data` cell. The CQL text of a statement is written once per run to the `queries` table, not once per execution
- **Table Options**: The log tables use LZ4 compression and no row cache, because gemini reads them back only when a validation fails
- **Memory Usage**: A fixed-size channel buffers mutations before processing
- **Disk Usage**: File output only contains error context, not all statements
