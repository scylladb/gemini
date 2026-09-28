## Coding style

### License header

Start every `.go` file with the Apache 2.0 license header. The `goheader`
linter checks it. A new file uses the current year.

```go
// Copyright 2026 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// ...
```

### Formatting

Run `make fmt` before you commit. It applies gofumpt with `group-params`,
goimports, gci, and golines. Do not format the code by hand.

### Import groups

Put the imports in three groups, in this order: standard library, third
party, `github.com/scylladb/gemini`. Separate the groups with a blank line.

```go
import (
	"context"

	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/typedef"
)
```

### Line length

The maximum line length is 180 characters. The `lll` linter and golines
use this limit. Test files are exempt from `lll`.
