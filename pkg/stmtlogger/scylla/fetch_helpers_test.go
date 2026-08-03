//go:build testing

// Copyright 2026 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scylla

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/stmtlogger"
)

// parseLines decodes the JSONL a FetchTo call produced.
func parseLines(tb testing.TB, data []byte) []Line {
	tb.Helper()

	dec := json.NewDecoder(bytes.NewReader(data))
	lines := make([]Line, 0, 1)

	for {
		var l Line

		err := dec.Decode(&l)
		if err == io.EOF {
			break
		}

		require.NoError(tb, err, "statement log line must be valid JSON: %s", data)
		lines = append(lines, l)
	}

	return lines
}

// fetchLines streams a job error's statements into memory and parses them. Only
// tests buffer the whole thing; production streams straight to the file.
func fetchLines(
	ctx context.Context,
	tb testing.TB,
	c *cqlStatements,
	ty stmtlogger.Type,
	jobErr *joberror.JobError,
) []Line {
	tb.Helper()

	var buf bytes.Buffer

	n, err := c.FetchTo(ctx, ty, jobErr, &buf)
	require.NoError(tb, err)
	require.EqualValues(tb, buf.Len(), n, "reported byte count must match what was written")

	return parseLines(tb, buf.Bytes())
}

// partitionKeyValues pulls one key's values out of a line's partition infos.
func partitionKeyValues(lines []Line, name string) []any {
	var out []any

	for _, l := range lines {
		for _, pi := range l.PartitionKeys {
			if v, ok := pi.PartitionKeys[name]; ok {
				out = append(out, v)
			}
		}
	}

	return out
}
