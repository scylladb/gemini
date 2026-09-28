# Documentation Index

Read this file at the start of any task. It indexes the standards of this
repository.

## Standards

The conventions the team decided on. Follow them when you write code. When a
standard conflicts with the task, ask the user.

### Global standards

Located in `docs/standards/global/`.

#### Coding style (`standards/global/coding-style.md`)
License header, formatting through `make fmt`, import groups, and line length.

#### Commits (`standards/global/commits.md`)
Commit message format, allowed types, scope, length limits, and the `chore`
type for bots.

### Backend standards

Located in `docs/standards/backend/`.

#### Go conventions (`standards/backend/go-conventions.md`)
Go version, range over int, standard library first, and no print statements.
Also error handling, vet analyzers, cyclomatic complexity, field alignment,
and documentation in sync.

### Testing standards

Located in `docs/standards/testing/`.

#### Test writing (`standards/testing/test-writing.md`)
Test flags, parallel tests and their exceptions, test context, and cleanup.
Also deterministic timing, testify, and the Scylla environment.

## Updating this documentation

- Update a standard when a team convention changes, through
  `/qatools-sdlc:standards-update`.
- Update this index when you add, remove, or change a file.
