# Gemini Project

Gemini is an automatic randomized testing suite for
the Scylla database. It operates by generating random mutations (INSERT, UPDATE, DELETE) and
verifying them (SELECT) using the CQL protocol across two clusters:
a system under test (SUT) and a test oracle.

## Project Structure
- `pkg/` contains everything that is needed for `gemini` project
- `docs/` contain project documentation and always keep it up to date, especially the architecture diagrams and if something is new, add it to the documentation
- `scripts/` contain useful scripts for development and testing
- `docker/` contains dockerfiles for building the project, and it contains `docker-compose` files for easy development environment setup, also used in CI

## Language & Framework
- **Language**: Go 1.25
- **Architecture**: CLI application with modular package structure
- **Database**: Scylla/Cassandra using CQL protocol
- **Testing**: Randomized testing with statistical distributions

## Go instructions (1.24, 1.25)

1. Always use `for range` instead of `for` with `i++` when iterating, this is the new syntax from Go 1.25
   Example: `for i := 0; i < 10; i++` should be replaced with `for i := range 10`. If the `i` is not needed it can be omitted.
   This is also true when using some integer variable as a counter. `for i := 0; i < VARIABLE; i++` can be replaced with `for i := range VARIABLE`, also `i` can be omitted.
2. Always assume `go` **1.25**. Prefer `go` commands that work with Go 1.25.
3. Keep `go.mod` `go 1.25`. Do **not** add a `toolchain` line when updating the `go` line (Go 1.25 no longer auto-adds it).
4. Use the new `go.mod` **`ignore`** directive to exclude non-packages (e.g., examples, scratch) from `./...`
5. Prefer standard library first; avoid third-party deps unless asked.
6. Slice stack-allocation opportunities (new in 1.25) and avoid unsafe pointer aliasing.
7. Use testing/synctest for flaky/racy tests.
8. Consider the container-aware GOMAXPROCS defaults when benchmarking.
9. **DWARF 5** debug info by default
10. Add a `synctest` based test that removes `time.Sleep` and waits deterministically.
11. Use `go test -race` to detect data races.
12. Always use in tests for context `t.Context()` and for benchmarking `b.Context()`, there are new go 1.24 function, and they better and avoid linting errors.
13. Use `t.Cleanup()` to register cleanup functions in tests instead of `defer` to ensure proper execution order.
14. Use `t.Parallel()` to run tests in parallel.

## Vet & Static Checks
Always run go vet ./... and address:
- waitgroup analyzer: fix misplaced (*sync.WaitGroup).Add calls.
- hostport analyzer: replace fmt.Sprintf("%s:%d", host, port) with net.JoinHostPort(host, strconv.Itoa(port)).

## Running tests and writing new tests

1. IMPORTANT THING: Always use `-tags testing` and `-race` when running tests locally and in CI.
   Example: `go test -tags testing -race ./pkg/...`
2. When writing new tests, always use `t.Context()` for context in tests
