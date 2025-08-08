# Gemini Statement Logger

The Gemini Statement Logger is a comprehensive logging system that captures detailed information about all database statements executed during testing sessions. This feature is particularly valuable for debugging, performance analysis, and understanding the exact behavior of test workloads.

## Purpose

The statement logger serves several critical purposes:

- **Debugging**: When errors occur, the logger immediately captures the failed statements with full context, making it easier to reproduce and analyze issues
- **Performance Analysis**: Track execution times and patterns for all statement types
- **Audit Trail**: Maintain a complete record of all database operations for compliance and review
- **Test Validation**: Compare statement execution between Oracle (reference) and Test (system under test) clusters

## Overview

The statement logger operates in real-time, immediately writing statement information to log files when statements are executed or when errors occur. This immediate logging ensures that critical information is captured even if the test run is interrupted unexpectedly.

### Key Features

- **Immediate Error Logging**: When an error occurs, the statement is immediately logged with full context
- **Real-time Logging**: All statements are logged as they execute, not buffered until the end
- **Dual Cluster Support**: Separate logging for Oracle and Test clusters
- **Multiple Output Formats**: Support for JSON format with optional compression
- **Comprehensive Data**: Captures statement text, parameters, timing, host information, and error details

### How It Works

The statement logger uses an asynchronous channel-based architecture:

1. **Statement Execution**: When Gemini executes a statement, it creates a log item with all relevant information
2. **Channel Processing**: Log items are sent through dedicated channels for immediate processing
3. **File Writing**: Background goroutines continuously write log items to the configured files
4. **Error Handling**: Failed statements trigger immediate logging to both Oracle and Test statement files
5. **Compression**: Optional compression (none, gzip, zstd) reduces file sizes for large test runs

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

Each log entry is a JSON object containing:

```json
{
  "s": "2025-01-01T12:00:00Z",           // Start timestamp
  "partitionKeys": [1, "key"],           // Partition key values
  "e": "error message",                  // Error (if any)
  "q": "INSERT INTO...",                 // Statement query
  "h": "192.168.1.10",                  // Host that executed
  "v": [1, "value"],                     // Statement parameters
  "d": 1000000,                         // Duration in nanoseconds
  "d_a": 1,                             // Driver attempt number
  "g_a": 1                              // Gemini attempt number
}
```

## Error Handling and Immediate Logging

When an error occurs during statement execution:

1. **Immediate Capture**: The failed statement is immediately logged with full error context
2. **Dual Logging**: Error information is written to both Oracle and Test statement files (if configured)
3. **No Buffering**: Error statements bypass any buffering and are written directly to disk
4. **Complete Context**: Includes the exact statement, parameters, timing, and error message

This immediate error logging ensures that even if Gemini crashes or is interrupted, the failed statements are preserved for analysis.

## Performance Impact

The statement logger is designed for minimal performance impact:

- **Asynchronous Processing**: Logging doesn't block statement execution
- **Buffered Writes**: File I/O is buffered and batched for efficiency
- **Optional Compression**: Reduces I/O overhead for large test runs
- **Channel-based**: Uses Go channels for efficient concurrent processing

## Limitations

- **Disk Space**: Large test runs can generate significant log files
- **File I/O**: Intensive logging may impact system I/O performance
- **Memory Usage**: Log items are queued in memory before writing
- **Single Format**: Currently only supports JSON format output
