# Gemini Developers Guide

## Setting Up the Environment

Before building Gemini, you need first to have ScyllaDB clusters running, for both `oracle` and `test` clusters.
There are two modes for running clusters. Running `single node oracle` and `3 node test cluster` and `single node oracle` and `single node test cluster`. The second mode is useful for debugging and development purposes, but the first mode is recommended for running tests as it mimic the production environment more closely. Everything in gemini is organized with `Makefile` scripts, so you can run the following commands to start the clusters:

When running scylla multi-node clusters, the script will setup prometheus and grafana for monitoring the clusters. You can access the prometheus and grafana dashboards at `http://localhost:9090` and `http://localhost:3000` respectively.

```bash
make scylla-setup-cluster # For starting 1 node oracle and 3 node test cluster
make scylla-shutdown-cluster # For shutting down the clusters
```

```bash
make scylla-setup # For starting 1 node oracle and 1 node test cluster
make scylla-shutdown # For shutting down the clusters
```

## Building Gemini for Development

```bash
make debug-build
```

This will build the Gemini binary in debug mode, which is useful for development and debugging purposes. The binary will be located in the `bin/` directory.

### Building Gemini for Production

```bash
make build
```

### Building Gemini Docker image

```bash
make build-docker
```

## Running Tests

### Running Unit tests

Running unit tests with `-race` and `-tags testing` to catch issues related to concurrency and to include testing tags.
This is the default way to run tests in Gemini and in CI.

```bash
make test
```

### Running Integration tests

As Gemini has two running modes for clusters (`3 node` and `1 node`). There are two commands for integration tests:

- Running tests with `3 node` test cluster

```bash
make integration-cluster-test
```

- Running tests with `1 node` test cluster

```bash
make integration-test
```

- Running integration tests in Docker

```bash
make docker-integration-test
```

## Running other tools

- Formatting the code

Using `golangci-lint` to format the code. (under the hood it used `gofumt`, `goimports`, and `gci` to format the code)

```bash
make fmt
```

- Linting the code
Using `golangci-lint` to lint the code. It will run multiple linters to catch issues in the code.

```bash
make check
```

- Struct alignment fixes

```bash
make fieldalign
```

- Profiling the code

```bash
make pprof-profile # Runs pprof /debug/profile on :8080 port
make pprof-goroutine # Runs pprof /debug/goroutine on :8082 port
make pprof-block # Runs pprof /debug/block on :8083 port
make pprof-heap # Runs pprof /debug/heap on :8085 port
```
