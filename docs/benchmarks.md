# Benchmark System

This document describes the benchmark infrastructure for tracking and comparing performance over time.

## Overview

The benchmark system provides:
- **Automated benchmark execution** - Run Go benchmarks and capture results
- **Historical tracking** - Store benchmark results with metadata (git commit, timestamp, CPU info, etc.)
- **Regression detection** - Compare current results with previous runs and detect performance regressions
- **JSON storage** - Persistent storage in JSON format for easy analysis and integration with CI/CD

## Components

### 1. Benchmark Results Package (`pkg/benchmarks`)

Core functionality for storing, parsing, and comparing benchmark results.

**Key types:**
- `BenchmarkResult` - Single benchmark test result (ns/op, allocations, etc.)
- `BenchmarkRun` - Complete benchmark run with metadata
- `BenchmarkHistory` - Historical collection of benchmark runs
- `ComparisonResult` - Comparison between two benchmark results

**Key functions:**
- `ParseBenchmarkOutput()` - Parse Go benchmark output
- `CompareRuns()` - Compare two benchmark runs
- `LoadHistory()` - Load benchmark history from file

### 2. Benchmark Runner Tool (`cmd/benchrunner`)

Command-line tool for running benchmarks and managing results.

## Usage

### Running Benchmarks

#### Basic usage - run all benchmarks:
```bash
go run ./cmd/benchrunner/main.go
```

#### Run specific benchmarks:
```bash
# Run benchmarks matching a pattern
go run ./cmd/benchrunner/main.go -bench=BenchmarkPartitions

# Run benchmarks from specific package
go run ./cmd/benchrunner/main.go -pkg=./pkg/partitions/...
```

#### Customize benchmark execution:
```bash
# Run for 5 seconds
go run ./cmd/benchrunner/main.go -benchtime=5s

# Run for specific number of iterations
go run ./cmd/benchrunner/main.go -benchtime=100x

# Disable memory statistics
go run ./cmd/benchrunner/main.go -benchmem=false
```

#### Add metadata:
```bash
# Add tags for categorization
go run ./cmd/benchrunner/main.go -tags="version=1.0,env=ci"

# Add notes
go run ./cmd/benchrunner/main.go -notes="After optimization X"

# Specify CPU info manually
go run ./cmd/benchrunner/main.go -cpu="Intel Core i7-9700K"
```

#### Specify history file:
```bash
go run ./cmd/benchrunner/main.go -history=/path/to/benchmarks.json
```

### Comparing Results

#### Compare with last run:
```bash
go run ./cmd/benchrunner/main.go -compare=last
```

This will:
1. Run benchmarks
2. Save results to history
3. Compare with the previous run
4. Print comparison report
5. Exit with error if regressions detected

#### Configure regression threshold:
```bash
# Default threshold is 10% - adjust as needed
go run ./cmd/benchrunner/main.go -compare=last -threshold=5.0
```

### Example Output

#### Benchmark execution:
```
Running benchmarks...

Parsed 14 benchmark results
✓ Benchmark results saved to benchmark_history.json
```

#### Comparison output:
```
=== Benchmark Comparison ===

Benchmark: BenchmarkPartitionsConcurrentMixed-8
  Speed:       1234.56 ns/op → 1100.23 ns/op (10.88% faster)
  Allocations: 45 → 42 (-6.67%)
  Memory:      2048 B → 1920 B (-6.25%)

Benchmark: BenchmarkMemoryAllocation-8
  Speed:       5678.90 ns/op → 6123.45 ns/op (-7.82% slower)
  Allocations: 100 → 110 (10.00%)
  Memory:      4096 B → 4500 B (9.86%)
  ⚠️  REGRESSION DETECTED

⚠️  WARNING: Performance regressions detected!
```

## Benchmark History Format

Results are stored in JSON format:

```json
{
  "runs": [
    {
      "timestamp": "2025-10-26T17:14:00Z",
      "git_commit": "abc123def456",
      "git_branch": "main",
      "go_version": "go1.25.2",
      "os": "linux",
      "arch": "amd64",
      "cpu": "Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz",
      "results": [
        {
          "name": "BenchmarkPartitionsConcurrentMixed-8",
          "ns_per_op": 1234.56,
          "ops_per_sec": 809999.35,
          "allocs_per_op": 45,
          "bytes_per_op": 2048,
          "iterations": 1000000,
          "parallelism": 8
        }
      ],
      "tags": {
        "version": "1.0",
        "env": "ci"
      },
      "notes": "Baseline before optimization"
    }
  ]
}
```

## Integration with CI/CD

### Example GitHub Actions Workflow

```yaml
name: Benchmark

on:
  push:
    branches: [main]
  pull_request:

jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
        with:
          fetch-depth: 0  # Full history for git info
      
      - uses: actions/setup-go@v4
        with:
          go-version: '1.25'
      
      - name: Run Benchmarks
        run: |
          go run ./cmd/benchrunner/main.go \
            -history=benchmark_history.json \
            -tags="env=ci,branch=${{ github.ref_name }}" \
            -notes="CI run for ${{ github.sha }}"
      
      - name: Compare with baseline
        if: github.event_name == 'pull_request'
        run: |
          # Download baseline from main branch
          git fetch origin main
          git show origin/main:benchmark_history.json > baseline.json
          
          # Compare
          go run ./cmd/benchrunner/main.go \
            -history=benchmark_history.json \
            -compare=last \
            -threshold=5.0
      
      - name: Upload Results
        uses: actions/upload-artifact@v3
        with:
          name: benchmark-results
          path: benchmark_history.json
```

## Best Practices

1. **Consistent Environment**
   - Run benchmarks on the same hardware when comparing
   - Use `-cpu` flag to track different machines
   - Close resource-intensive applications during benchmarking

2. **Sufficient Run Time**
   - Use `-benchtime=5s` or more for stable results
   - Longer runs reduce variance in measurements

3. **Regular Baseline Updates**
   - Commit benchmark history to track trends over time
   - Update baseline after intentional performance changes

4. **Tag Your Runs**
   - Use tags to categorize runs (e.g., `env=ci`, `version=1.0`)
   - Add notes for significant changes

5. **Regression Thresholds**
   - Set realistic thresholds (5-10% is typical)
   - Adjust based on benchmark variance
   - Consider separate thresholds for speed vs memory

## Available Benchmarks

The project includes benchmarks for:

- **Partitions** (`pkg/partitions`)
  - `BenchmarkPartitionsConcurrentMixed` - Mixed concurrent operations
  - `BenchmarkMemoryAllocation` - Memory allocation patterns

- **In-flight tracking** (`pkg/inflight`)
  - `BenchmarkBloom` - Bloom filter operations
  - `BenchmarkBloomHashFunctions` - Hash function performance
  - `BenchmarkBloomConcurrent` - Concurrent bloom filter access

- **Hashing** (`pkg/murmur`)
  - `BenchmarkMurmur3H1` - Murmur3 hash function

- **Statement logging** (`pkg/stmtlogger`)
  - `BenchmarkLogger` - Statement logging performance
  - `BenchmarkLogStatement` - Individual statement logging
  - `BenchmarkPrepareValues` - Value preparation

- **Storage** (`pkg/store`)
  - `BenchmarkPks` - Partition key operations
  - `BenchmarkFormatRows` - Row formatting

- **Utilities** (`pkg/utils`)
  - `BenchmarkRandString` - Random string generation
  - `BenchmarkTimeDurationToScyllaDuration` - Time conversion

## Troubleshooting

### No benchmark results found
- Ensure benchmark functions start with `Benchmark`
- Check that the package pattern matches existing tests
- Verify the benchmark pattern with `-bench=.`

### Inconsistent results
- Increase `-benchtime` for more stable measurements
- Ensure system is not under load
- Run multiple times and compare variance

### Git information not captured
- Ensure you're in a git repository
- Check that git is installed and in PATH
- Commit changes before running benchmarks for accurate tracking

## Future Enhancements

Potential improvements:
- Web dashboard for visualizing trends
- Statistical analysis (mean, variance, confidence intervals)
- Automated performance regression alerts
- Integration with external monitoring systems
- Support for custom metrics beyond Go's built-in benchmarking
