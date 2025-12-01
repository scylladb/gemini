// Copyright 2025 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scylla

import (
	"encoding/json"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/typedef"
)

func RecordErrorMetrics(j *joberror.JobError, lineBytes []byte, storage string) {
	if j == nil {
		return
	}

	ks, tbl := extractKeyspaceTable(j.Query)
	ph := partitionHashFromValues(j.PartitionKeys)
	et := j.StmtType.String()

	var errStr string
	if j.Err != nil {
		errStr = j.Err.Error()
	}

	labels := map[string]string{
		"keyspace":       ks,
		"table":          tbl,
		"stmt_type":      et,
		"error":          errStr,
		"partition_hash": ph,
		"stmt_storage":   storage,
		"stmt_logger":    string(lineBytes),
	}

	metrics.StatementErrorLastTS.With(labels).Set(float64(time.Now().Unix()))
}

func partitionHashFromValues(v *typedef.Values) string {
	if v == nil || v.Len() == 0 {
		return "unknown"
	}
	// Canonicalize map: keys sorted, values JSON-marshaled
	m := v.ToMap()
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	// Build a deterministic JSON string
	b := strings.Builder{}
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		// Marshal key
		kb, _ := json.Marshal(k)
		b.Write(kb)
		b.WriteByte(':')
		// Marshal values
		vb, _ := json.Marshal(m[k])
		b.Write(vb)
	}
	b.WriteByte('}')

	// Reuse JobError#Hash on a temporary to get a stable SHA256.
	tmp := &joberror.JobError{
		PartitionKeys: typedef.NewValuesFromMap(map[string][]any{}),
	}
	// Build Values back to ensure Hash() uses Keys/Get in deterministic order.
	// We can safely reuse m since we already sorted keys for JSON canonicalization.
	tmp.PartitionKeys = typedef.NewValuesFromMap(m)
	sum := tmp.HashHex()
	if len(sum) >= 16 {
		return sum[:16]
	}
	return sum
}

var (
	reInto  = regexp.MustCompile(`(?i)\binto\s+([a-z0-9_]+)\.([a-z0-9_]+)\b`)
	reFrom  = regexp.MustCompile(`(?i)\bfrom\s+([a-z0-9_]+)\.([a-z0-9_]+)\b`)
	reTable = regexp.MustCompile(`(?i)\btable\s+([a-z0-9_]+)\.([a-z0-9_]+)\b`)
)

func extractKeyspaceTable(query string) (string, string) {
	q := strings.TrimSpace(query)
	if q == "" {
		return "unknown", "unknown"
	}
	if m := reInto.FindStringSubmatch(q); len(m) == 3 {
		return m[1], m[2]
	}
	if m := reFrom.FindStringSubmatch(q); len(m) == 3 {
		return m[1], m[2]
	}
	if m := reTable.FindStringSubmatch(q); len(m) == 3 {
		return m[1], m[2]
	}
	// Fallback: try to split on first dot-like token after space
	// This is intentionally conservative.
	return "unknown", "unknown"
}
