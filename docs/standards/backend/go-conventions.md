## Go conventions

### Go version

Use Go 1.27. Keep `go 1.27.0` in `go.mod`. Do not add a `toolchain` or a
`godebug` line. Use the `ignore` directive to exclude non-packages from
`./...`.

### Range over int

Write `for i := range n` for a counter loop. Omit `i` when the body does
not use it. The `intrange` linter checks this.

### Standard library first

Use the standard library first. Do not add a third-party dependency unless
someone asks for it. The only `replace` directive in `go.mod` is
`github.com/gocql/gocql` to `github.com/scylladb/gocql`.

### No print statements

Log with `zap`. Do not use `fmt.Print*`, `print`, or `println`. Only
`pkg/cmd`, `pkg/status`, and `pkg/benchmarks` can print to the terminal.
An `Example` function can print, because `go test` compares its output.
forbidigo skips `Example` functions.

### Error handling

Compare errors with `errors.Is` and `errors.As`, not with `==` or a type
assertion. Use the standard `errors` package. Give an error type a name
that ends with `Error`. Give an exported sentinel error variable a name
that starts with `Err`, as in `ErrNoStatement`. An unexported one starts
with `err`. The `errorlint` and `errname` linters
check this.

### Vet analyzers

`make check` runs the `govet` analyzers with the `testing` tag. Outside
`make check`, run `go vet -tags testing ./...`. Fix every report. Start a goroutine
with `wg.Go(f)`. When you use `wg.Add`, call it before the `go` statement.
Build a host and port with `net.JoinHostPort`. Do not shadow variables
outside tests.

### Field alignment

Order struct fields to keep the padding small. The `fieldalignment`
analyzer of `govet` checks this outside tests. `make fieldalign` shows the
reports, including reports for test files. `make fieldalign-fix` rewrites
the structs. Keep only its changes to non-test files. It removes the
comments inside each struct it changes, so read the diff.

### Cyclomatic complexity

Keep the cyclomatic complexity of a function at 20 or lower. The `gocyclo`
linter checks this.

### Documentation in sync

Update `docs/` when you change a behavior or add a feature. This includes
the architecture diagrams.
