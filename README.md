![Gemini](docs/images/gemini.png)

<p align="center"><i>An automatic randomized testing suite for Scylla and Apache Cassandra.</i></p>

## What is Gemini?

Gemini is a tool for testing ScyllaDB (and Apache Cassandra) clusters by running randomized workloads against two clusters simultaneously - a reference "oracle" cluster and the system under test (SUT). It generates random mutations (INSERT, UPDATE, DELETE) and validates their results using SELECT queries. When discrepancies appear between the oracle and SUT, Gemini reports them for investigation.

## Getting Started

### Installation

Download the latest release for your platform from [GitHub Releases](https://github.com/scylladb/gemini/releases).

Available builds:
- Linux (amd64, arm64)
- Docker: `scylladb/gemini:latest` (supports amd64 and arm64)

### Oracle Cluster Sizing

The oracle cluster serves as the source of truth for validation. For reliable testing:

- **Single node recommended**: Use a single-node oracle to avoid eventual consistency effects that could cause false positives
- **Resource allocation**: The oracle node should have at least **2x the CPU/RAM** of a single test cluster node
- **Replication factor consideration**: If your test cluster's RF is less than the number of nodes, increase oracle resources proportionally
- **Consistency level**: Consider using `--consistency=QUORUM` or `--consistency=ALL` to minimize replication lag issues

### Quick Start

```bash
# With two clusters (oracle + test)
./gemini --oracle-cluster=<ORACLE_IP> --test-cluster=<TEST_IP>

# Test-only mode (no oracle validation)
./gemini --test-cluster=<TEST_IP>

# Using Docker
docker run -it scylladb/gemini:latest \
  --oracle-cluster=<ORACLE_IP> \
  --test-cluster=<TEST_IP>
```

### Basic Options

```bash
./gemini \
  --test-cluster=192.168.1.10,192.168.1.11 \
  --oracle-cluster=192.168.1.20 \
  --duration=1h \
  --concurrency=10 \
  --mode=mixed
```

See `./gemini --help` or [CLI arguments documentation](docs/cli-arguments.md) for all options.

## Features

### Available Now

- **Random Schema Generation** - Automatically generates table schemas with various column types, partition keys, and clustering keys
- **Multiple Operation Modes** - Write-only, read-only, or mixed workloads
- **Partition Key Distributions** - Uniform, normal, and zipf distributions for realistic access patterns
- **Counters Support** - Test counter tables with `--use-counters`
- **LWT (Lightweight Transactions)** - Enable with `--use-lwt`
- **Statement Logging** - Capture all executed statements for debugging (see [Statement Logger](docs/statement-logger.md))
- **Custom Statement Ratios** - Fine-tune insert/update/delete/select distributions (see [Statement Ratios](docs/statement-ratio.md))
- **Prometheus Metrics** - Built-in metrics endpoint on port 2112
- **Authentication** - Username/password auth for both clusters
- **Compression** - gzip/zstd compression for statement logs
- **Configurable Concurrency** - Separate mutation and read concurrency settings
- **Warmup Period** - Optional warmup phase before validation starts
- **Reproducible Runs** - Use `--seed` and `--schema-seed` to replay exact test scenarios

## Modes of Operation

| Mode | Description |
|------|-------------|
| `mixed` | (default) Runs both mutations and validations |
| `write` | Only mutations, no validation queries |
| `read` | Only validation queries on existing data |

## When Something Fails

When Gemini detects a mismatch between oracle and test cluster, it reports the error to:
1. **Console/stdout** - Immediate error output
2. **gemini.log** - JSON-formatted log file with full details
3. **Statement logs** - If enabled, captures all statements for replay

For detailed debugging steps, see [Investigation Guide](docs/investigation.md).

## Documentation

- [Quick Reference](docs/quickref.md) - Common commands and options
- [Getting Started](docs/getting-started.md) - First-time setup
- [Schema Configuration](docs/schema.md) - Custom schemas and data types
- [Investigation Guide](docs/investigation.md) - Debugging failures
- [Statement Logger](docs/statement-logger.md) - Capturing statements
- [Statement Ratios](docs/statement-ratio.md) - Workload distribution
- [Metrics](docs/metrics.md) - Prometheus metrics reference
- [CLI Arguments](docs/cli-arguments.md) - All command line options
- [Troubleshooting](docs/troubleshooting.md) - Common issues and fixes
- [Architecture](docs/architecture.md) - Internal design (contributors)

## Contributing

Contributions are welcome! Fork the repository and submit a pull request.
See [Development Guide](docs/development.md) for setup instructions.

## License

Apache 2.0 - see [LICENSE](LICENSE).
