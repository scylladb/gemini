# Gemini Statement Logger

The Gemini Statement Logger is a comprehensive logging system that captures detailed information about all database statements executed during testing sessions. This feature is particularly valuable for debugging, performance analysis, and understanding the exact behavior of test workloads.

## Purpose

The statement logger serves several critical purposes:

- **Debugging**: When errors occur, the logger immediately captures the failed statements with full context, making it easier to reproduce and analyze issues
- **Performance Analysis**: Track execution times and patterns for all statement types
- **Audit Trail**: Maintain a complete record of all database operations for compliance and review
- **Test Validation**: Compare statement execution between Oracle (reference) and Test (system under test) clusters

## Overview

The statement logger is opt-in. It activates only when you pass at least one of the file flags. When enabled, statements are enqueued asynchronously with minimal overhead and written out by background workers. File writes are buffered and flushed periodically to reduce I/O overhead.

### Key Features

- **Immediate Error Logging**: When an error occurs, the statement is immediately logged with full context
- **Real-time Logging**: All statements are logged as they execute, not buffered until the end
- **Dual Cluster Support**: Separate logging for Oracle and Test clusters
- **Multiple Output Formats**: Support for JSON format with optional compression
- **Comprehensive Data**: Captures statement text, parameters, timing, host information, and error details

### How It Works

The statement logger uses an asynchronous channel-based architecture:

1. **Statement Execution**: When Gemini executes a statement, it creates a log item with relevant information.
2. **Asynchronous Enqueue**: The item is enqueued to a bounded channel using a non-blocking send. If the channel is full, the item is dropped to avoid impacting workload throughput. Drops are counted in Prometheus metrics.
3. **Worker Pool**: A fixed-size worker pool reads from the channel and, for each item, writes into the Scylla statement-log table. This avoids per-item goroutine churn.
4. **File Writing (optional)**: If file paths are provided, background goroutines fetch error-related statement context and write JSON Lines (JSONL) entries into the configured files.
5. **Batched Flush**: Writes are buffered and flushed periodically (time-based), rather than after every record, to reduce syscall pressure.
6. **Compression**: Optional compression (none, gzip, zstd) reduces file sizes for large test runs.

## CLI Flags

### Required Flags

For statement logging to be active, you must specify at least one output file:

- **`--test-statement-log-file`**: File path to write Test cluster statements
- **`--oracle-statement-log-file`**: File path to write Oracle cluster statements

### Optional Flags

- **`--statement-log-file-compression`**: Compression algorithm to use
  - Options: `none` (default), `gzip`, `zstd`
  - Example: `--statement-log-file-compression=gzip`

### Usage Examples

```bash
# Basic statement logging for both clusters
gemini --test-statement-log-file=/tmp/test_statements.json \
       --oracle-statement-log-file=/tmp/oracle_statements.json \
       --oracle-cluster=192.168.1.10 \
       --test-cluster=192.168.1.20

# With compression to save disk space
gemini --test-statement-log-file=/tmp/test_statements.json.gz \
       --oracle-statement-log-file=/tmp/oracle_statements.json.gz \
       --statement-log-file-compression=gzip \
       --oracle-cluster=192.168.1.10 \
       --test-cluster=192.168.1.20

# Only test cluster logging (no validation)
gemini --test-statement-log-file=/tmp/statements.json \
       --test-cluster=192.168.1.20
```

## Log File Format

Each log entry is a JSON object containing, for example:

```json
{
  "s": "2025-01-01T12:00:00Z",
  "partitionKeys": [1, "key"],
  "e": "error message",
  "q": "INSERT INTO...",
  "h": "192.168.1.10",
  "v": [1, "value"],
  "d": 1000000,
  "d_a": 1,
  "g_a": 1
}
```

## Error Handling and Logging

When an error occurs during statement execution and file logging is enabled:

1. The failed statement’s context is fetched from both Oracle and Test clusters.
2. JSONL entries are written to the configured files using buffered writers.
3. Flushes happen periodically or on shutdown, rather than per-record, to reduce overhead.

## Performance Impact

The statement logger is designed to minimize performance impact:

- **Opt-in**: Disabled by default unless file flags are provided.
- **Non-blocking enqueue**: Hot path never blocks on logging; when overloaded, items are dropped and counted.
- **Worker pool**: Avoids per-item goroutines when inserting into the Scylla statement-log table.
- **Buffered writes**: File I/O is batched and flushed periodically.
- **Compression (optional)**: Reduces disk I/O at the cost of CPU.

Prometheus metrics exposed:
- `gemini_statement_logger_enqueued_total`
- `gemini_statement_logger_dropped_total{reason="channel_full"}`
- `gemini_statement_logger_flushes_total{sink="oracle_file|test_file"}`

## Limitations

- **Disk Space**: Large test runs can generate significant log files
- **File I/O**: Intensive logging may impact system I/O performance
- **Memory Usage**: Log items are queued in memory before writing
- **Single Format**: Currently only supports JSON format output
