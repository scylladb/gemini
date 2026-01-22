# Getting started with Gemini

## Quick Start

1. Download the latest release from [GitHub Releases](https://github.com/scylladb/gemini/releases)

2. Set up two Scylla clusters - we'll call them ORACLE_CLUSTER (reference) and TEST_CLUSTER (system under test)

3. Run Gemini:

   ```bash
   ./gemini --oracle-cluster=<ORACLE_CLUSTER> --test-cluster=<TEST_CLUSTER>
   ```

That's it! Gemini will generate a random schema and start running tests.

## Test-Only Mode

If you don't have an oracle cluster, you can run in test-only mode:

```bash
./gemini --test-cluster=<TEST_CLUSTER>
```

This runs mutations without validation - useful for stress testing.

## Using Docker

```bash
docker run -it scylladb/gemini:latest \
  --oracle-cluster=<ORACLE_CLUSTER> \
  --test-cluster=<TEST_CLUSTER>
```

## CLI Arguments

Run `./gemini --help` to see all available options.

For detailed explanations, see [CLI arguments documentation](cli-arguments.md).

## Next Steps

- [Investigation Guide](investigation.md) - How to debug failures
- [Statement Logger](statement-logger.md) - Capturing statements for analysis
- [Architecture](architecture.md) - Internal design for contributors
