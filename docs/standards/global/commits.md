## Commits

### Commit messages

Write the header as `type(scope): subject`. The `commit-msg` pre-commit hook
runs commitlint and gitlint on your machine. CI does not check commit
messages. Run `pre-commit install` in each clone. Run it again when the hook
types in `.pre-commit-config.yaml` change. Follow these rules:

- The type is one of `ci`, `docs`, `feature`, `fix`, `improvement`, `perf`,
  `refactor`, `revert`, `style`, `test`, `unit-test`, `build`.
- Give a scope of three characters or more. commitlint warns when the
  scope is empty.
- The header has 72 characters or fewer. The subject has 10 characters or more.
- The subject has no final period.
- The title does not contain `WIP`.
- Every commit has a body of 20 characters or more.
- A blank line comes before the body. Body lines have 80 characters or fewer.

Dependency update bots use the type `chore`. Their commits do not go through
the hook. Do not use `chore` for a commit you write.
