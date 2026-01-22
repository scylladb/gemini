# Troubleshooting Guide

Common issues and how to resolve them.

## Connection Issues

### Cannot connect to cluster

**Symptoms:**
```
failed to connect to cluster: dial tcp: connection refused
```

**Solutions:**
1. Verify cluster is running: `cqlsh <IP>`
2. Check firewall rules (port 9042)
3. For Docker, use host network or correct IP
4. Increase connect timeout: `--connect-timeout=60s`

### Authentication failed

**Symptoms:**
```
authentication failed: invalid credentials
```

**Solutions:**
1. Verify username/password
2. Use correct flags:
   ```bash
   --test-username=user --test-password=pass
   --oracle-username=user --oracle-password=pass
   ```

## Runtime Errors

### Timeouts during test

**Symptoms:**
```
request timeout: operation timed out
```

**Solutions:**
1. Increase timeout: `--request-timeout=60s`
2. Reduce concurrency: `--concurrency=5`
3. Check cluster load and resources
4. Verify network latency between Gemini and clusters

### Out of memory

**Symptoms:**
```
runtime: out of memory
```

**Solutions:**
1. Reduce partition count: `--partition-count=500000`
2. Use smaller dataset: `--dataset-size=small`
3. Reduce concurrency
4. Enable statement log compression: `--statement-log-file-compression=gzip`

### Too many open files

**Symptoms:**
```
too many open files
```

**Solutions:**
1. Increase ulimit: `ulimit -n 65535`
2. Reduce concurrency
3. Check for connection leaks in cluster

## Validation Errors

### False positives during startup

**Symptoms:**
Errors during the first few seconds of the test.

**Solutions:**
1. Add warmup period: `--warmup=2m`
2. Ensure clusters are fully synchronized before testing
3. Check for ongoing compactions or repairs

### Consistency errors

**Symptoms:**
Intermittent mismatches that resolve on retry.

**Solutions:**
1. Use stronger consistency: `--consistency=ALL`
2. Increase retry attempts: `--max-mutation-retries=20`
3. Add delay between mutations: `--minimum-delay=100ms`
4. Check cluster replication status

### Row count differences

**Symptoms:**
```
oracle returned 5 rows, test returned 3 rows
```

**Causes:**
- Failed mutations on one cluster
- Replication lag
- Data corruption

**Investigation:**
1. Check statement logs for failed operations
2. Query both clusters manually
3. See [Investigation Guide](investigation.md)

## Schema Issues

### Schema file parsing error

**Symptoms:**
```
cannot parse schema file: invalid JSON
```

**Solutions:**
1. Validate JSON syntax
2. Check for trailing commas
3. Ensure all required fields are present
4. See [Schema Guide](schema.md) for format

### Unsupported type

**Symptoms:**
```
unsupported column type
```

**Solutions:**
1. Check CQL feature level: `--cql-features=all`
2. Verify type is supported by your Scylla version
3. Use simple types for partition keys

## Performance Issues

### Slow throughput

**Symptoms:**
Low operations per second.

**Solutions:**
1. Increase concurrency: `--concurrency=50`
2. Increase IO worker pool: `--io-worker-pool=256`
3. Use token-aware policy (default)
4. Check cluster resource utilization

### High latency

**Solutions:**
1. Check network between Gemini and clusters
2. Reduce batch sizes
3. Monitor cluster metrics
4. Use local datacenter if multi-DC

## Statement Logger Issues

### Large log files

**Solutions:**
1. Enable compression: `--statement-log-file-compression=gzip`
2. Use shorter test duration
3. Only error context is written to files (not all statements)

### Cannot read compressed logs

```bash
# For gzip
zcat test.json.gz | jq '.'

# For zstd
zstd -d -c test.json.zst | jq '.'
```

### Missing statement logs

**Causes:**
- Logs only created when errors occur
- File path not writable

**Solutions:**
1. Ensure directory exists and is writable
2. Statement logs contain error context, not all statements
3. Query ScyllaDB logs table directly:
   ```bash
   cqlsh -e "SELECT * FROM ks_logs.table1_statements LIMIT 10;"
   ```

## Docker Issues

### Container cannot reach clusters

**Solutions:**
1. Use host network:
   ```bash
   docker run --network=host scylladb/gemini:latest ...
   ```
2. Use container IP addresses
3. Ensure clusters are accessible from Docker network

### Logs not persisted

**Solutions:**
Mount a volume:
```bash
docker run -v $(pwd)/logs:/logs scylladb/gemini:latest \
  --test-statement-log-file=/logs/test.json \
  ...
```

## Getting Help

1. Check logs: `cat gemini.log | jq 'select(.level == "error")'`
2. Enable debug logging: `--level=debug`
3. Reproduce with specific seed: `--seed=... --schema-seed=...`
4. File an issue: https://github.com/scylladb/gemini/issues

Include:
- Gemini version: `./gemini --version`
- Command used
- Error message
- Seed values from output
- Scylla/Cassandra versions

