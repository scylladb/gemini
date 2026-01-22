# Gemini Statement Logger

The Gemini Statement Logger captures database statements executed during testing sessions and stores them both in ScyllaDB and optionally in files. When validation errors occur, Gemini provides the full statement history for the affected partitions, making it easy to reproduce and investigate issues.

For practical guidance on using statement logs to debug failures, see the [Investigation Guide](investigation.md).

## How It Works

Gemini logs all mutation statements (INSERT, UPDATE, DELETE) during test execution:

1. **ScyllaDB Storage**: Statements are automatically stored in a dedicated logs keyspace (`<keyspace>_logs`) with a table named `<table>_statements`
2. **Error Detection**: When a validation error occurs, Gemini fetches all statements for the affected partition
3. **File Output**: If statement log files are configured, error context is written to JSON files with full statement history
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
  "partitionKeys": {"col1": 5, "col2": "value"},
  "timestamp": "2025-01-20T10:15:30Z",
  "err": "row mismatch: oracle has 5 rows, test has 3",
  "query": "SELECT * FROM ks.table1 WHERE col1 = ? AND col2 = ?",
  "message": "validation failed",
  "mutationFragments": [...],
  "statements": [...]
}
```

### Top-Level Fields

| Field | Description |
|-------|-------------|
| `partitionKeys` | Map of partition key column names to their values |
| `timestamp` | When the error was detected |
| `err` | Error message describing the validation failure |
| `query` | The SELECT query that detected the mismatch |
| `message` | Additional context about the error |
| `mutationFragments` | Low-level mutation data from MUTATION_FRAGMENTS |
| `statements` | Array of all statements executed on this partition |

### Statement Array Format

Each entry in the `statements` array:

```json
{
  "ts": "2025-01-20T10:14:00Z",
  "statement": "INSERT INTO ks.table1 (col1, col2, val) VALUES (?, ?, ?)",
  "values": ["5", "\"value\"", "\"data\""],
  "host": "192.168.1.10",
  "attempt": 1,
  "gemini_attempt": 1,
  "dur": "1ms"
}
```

| Field | Description |
|-------|-------------|
| `ts` | Timestamp when statement was executed |
| `statement` | The CQL query |
| `values` | Bound parameter values (as strings) |
| `host` | Host that executed the query |
| `attempt` | Driver retry attempt number |
| `gemini_attempt` | Gemini-level retry attempt number |
| `error` | Error message (only present if statement failed) |
| `dur` | Execution duration |

## Querying Statement Logs in ScyllaDB

Statements are stored in ScyllaDB for direct querying:

```bash
# Find all statements for a specific partition
cqlsh -e "SELECT * FROM ks_logs.table1_statements WHERE col1 = 5 AND col2 = 'value';"

# Filter by type (oracle or test)
cqlsh -e "SELECT * FROM ks_logs.table1_statements WHERE col1 = 5 AND col2 = 'value' AND ty = 'test';"
```

The logs table schema includes:
- Partition key columns (matching the original table)
- `ty` - Type: 'oracle' or 'test'
- `ddl` - Whether it's a DDL statement
- `ts` - Timestamp
- `statement` - The CQL query
- `values` - Bound parameters
- `host` - Executing host
- `attempt` - Driver attempt
- `gemini_attempt` - Gemini attempt
- `error` - Error message
- `dur` - Duration

## Performance Considerations

- **Asynchronous Processing**: Statement logging uses background workers and doesn't block query execution
- **Batched Writes**: Statements are batched before writing to ScyllaDB for efficiency
- **Memory Usage**: A fixed-size channel buffers statements before processing
- **Disk Usage**: File output only contains error context, not all statements
