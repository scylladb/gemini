# Getting started with Gemini

## Quick Start

1. Download the latest release from [GitHub Releases](https://github.com/scylladb/gemini/releases)

2. Set up two Scylla clusters - we'll call them ORACLE_CLUSTER (reference) and TEST_CLUSTER (system under test)

3. Run Gemini:

   ```bash
   # Single node per cluster
   ./gemini --oracle-cluster=192.168.1.10 --test-cluster=192.168.1.20

   # Multiple nodes (comma-separated IP addresses)
   ./gemini --oracle-cluster=192.168.1.10 --test-cluster=192.168.1.20,192.168.1.21,192.168.1.22
   ```

   The `--oracle-cluster` and `--test-cluster` flags accept IP addresses. For multi-node clusters, provide comma-separated addresses.

That's it! Gemini will generate a random schema and start running tests.

## Test-Only Mode

If you don't have an oracle cluster, you can run in test-only mode:

```bash
./gemini --test-cluster=192.168.1.20,192.168.1.21,192.168.1.22
```

This runs mutations without validation - useful for stress testing.

## Using Docker

```bash
docker run -it scylladb/gemini:latest \
  --oracle-cluster=192.168.1.10 \
  --test-cluster=192.168.1.20,192.168.1.21,192.168.1.22
```

## CLI Arguments

Run `./gemini --help` to see all available options.

For detailed explanations, see [CLI arguments documentation](cli-arguments.md).

## Next Steps

- [Investigation Guide](investigation.md) - How to debug failures
- [Statement Logger](statement-logger.md) - Capturing statements for analysis
- [Architecture](architecture.md) - Internal design for contributors
