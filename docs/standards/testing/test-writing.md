## Test writing

### Test flags

Always run tests with `-tags testing -race`, locally and in CI.

```bash
go test -tags testing -race ./pkg/...
```

### Parallel tests

Call `t.Parallel()` at the start of each test and each subtest, with two
exceptions:

- A test that calls `t.Setenv()` must not be parallel, and none of its
  parent tests can be parallel. `t.Setenv()` panics in a parallel test.
- Do not call `t.Parallel()`, `t.Run()`, or `t.Deadline()` on the
  `*testing.T` that `synctest.Test` gives to its function. Call
  `t.Parallel()` on the outer test only.

### Test context

Use `t.Context()` in tests and `b.Context()` in benchmarks. Do not use
`context.Background()` or `context.TODO()`. Use `t.TempDir()` for
temporary directories and `t.Setenv()` for environment variables. The
`usetesting` linter checks this.

### Cleanup

Register cleanup with `t.Cleanup()`. Do not use `defer` for test cleanup.

### Deterministic timing

Use `testing/synctest` for a test that depends on time. Do not use
`time.Sleep` to wait for a result.

### Testify

Use `require` for a precondition that must stop the test. Use `assert`
for the other checks. The `testifylint` linter checks the usage.

### Scylla environment

Tests that need a cluster read these environment variables:
`GEMINI_USE_DOCKER_SCYLLA=true`, `GEMINI_TEST_CLUSTER_IP=192.168.100.3`,
`GEMINI_ORACLE_CLUSTER_IP=192.168.100.2`. Start the two nodes with
`make scylla-setup` before you run them.
