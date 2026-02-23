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

package joberror

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/google/uuid"
)

const (
	notAvailable = "-"
	tsLayout     = "2006-01-02 15:04:05.000 UTC"
)

type stmtLogEntry struct {
	Timestamp time.Time `json:"ts"`
	Statement string    `json:"statement"`
	Error     string    `json:"error,omitempty"`
	Host      string    `json:"host,omitempty"`
	Type      string    `json:"ty,omitempty"`
}
type stmtLogLine struct {
	Query      string            `json:"query,omitempty"`
	Message    string            `json:"message,omitempty"`
	Err        string            `json:"err,omitempty"`
	Statements []json.RawMessage `json:"statements,omitempty"`
}
type (
	stmtIndex          map[string][]stmtLogLine
	StmtClusterSummary struct {
		FirstWriteTime *time.Time `json:"first_write_time,omitempty"`
		LastWriteTime  *time.Time `json:"last_write_time,omitempty"`
		Hosts          []string   `json:"hosts,omitempty"`
		WriteCount     int        `json:"write_count"`
		DeleteCount    int        `json:"delete_count"`
		WriteErrors    int        `json:"write_errors"`
	}
)

type CorruptionEntry struct {
	CorruptionDetectedAt     time.Time          `json:"corruption_detected_at"`
	LastSuccessfulValidation *time.Time         `json:"last_successful_validation,omitempty"`
	DeletedAt                *time.Time         `json:"deleted_at,omitempty"`
	InsertedAt               *time.Time         `json:"inserted_at,omitempty"`
	Message                  string             `json:"message,omitempty"`
	PartitionKeys            string             `json:"partition_keys"`
	WriteToCorruptionGap     string             `json:"write_to_corruption_gap,omitempty"`
	ErrorKind                string             `json:"error_kind"`
	FailingQuery             string             `json:"failing_query,omitempty"`
	FieldDiffs               []string           `json:"field_diffs,omitempty"`
	TestCluster              StmtClusterSummary `json:"test_cluster"`
	OracleCluster            StmtClusterSummary `json:"oracle_cluster"`
	SuccessfulValidations    uint64             `json:"successful_validations"`
	DiffOracleOnlyRows       int                `json:"diff_oracle_only_rows,omitempty"`
	DiffTestOnlyRows         int                `json:"diff_test_only_rows,omitempty"`
	DiffFieldMismatches      int                `json:"diff_field_mismatches,omitempty"`
}
type partitionKeyReader interface {
	Keys() []string
	Get(string) []any
}

func buildStmtIndex(path string) stmtIndex {
	if path == "" {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer func() { _ = f.Close() }()
	idx := make(stmtIndex)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 4*1024*1024), 4*1024*1024)
	for sc.Scan() {
		raw := sc.Bytes()
		if len(raw) == 0 {
			continue
		}
		var top map[string]json.RawMessage
		if err = json.Unmarshal(raw, &top); err != nil {
			continue
		}
		pkRaw, ok := top["partitionKeys"]
		if !ok {
			continue
		}
		var partInfos []struct {
			PartitionKeys map[string]json.RawMessage `json:"partitionKeys"`
		}
		if err = json.Unmarshal(pkRaw, &partInfos); err != nil || len(partInfos) == 0 {
			continue
		}
		var line stmtLogLine
		if q, ok2 := top["query"]; ok2 {
			_ = json.Unmarshal(q, &line.Query)
		}
		if m, ok2 := top["message"]; ok2 {
			_ = json.Unmarshal(m, &line.Message)
		}
		if e, ok2 := top["err"]; ok2 {
			_ = json.Unmarshal(e, &line.Err)
		}
		if stmts, ok2 := top["statements"]; ok2 {
			var arr []json.RawMessage
			if err = json.Unmarshal(stmts, &arr); err == nil {
				line.Statements = arr
			}
		}
		for _, pi := range partInfos {
			k := stmtFileKeyFromRaw(pi.PartitionKeys)
			idx[k] = append(idx[k], line)
		}
	}
	return idx
}

func stmtFileKeyFromRaw(m map[string]json.RawMessage) string {
	if len(m) == 0 {
		return ""
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.Write(m[k])
	}
	return sb.String()
}

func buildStmtKeyFromValues(v partitionKeyReader) string {
	if v == nil {
		return ""
	}
	keys := v.Keys()
	if len(keys) == 0 {
		return ""
	}
	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(k)
		sb.WriteByte('=')
		vals := v.Get(k)
		if len(vals) == 0 {
			continue
		}
		b, _ := json.Marshal(vals[0])
		sb.Write(b)
	}
	return sb.String()
}

func summariseStatements(lines []stmtLogLine) StmtClusterSummary {
	s := StmtClusterSummary{}
	seen := make(map[string]struct{})
	for _, line := range lines {
		for _, raw := range line.Statements {
			var entry stmtLogEntry
			if err := json.Unmarshal(raw, &entry); err != nil {
				continue
			}
			upper := strings.ToUpper(strings.TrimSpace(entry.Statement))
			isWrite := strings.HasPrefix(upper, "INSERT") || strings.HasPrefix(upper, "UPDATE")
			isDel := strings.HasPrefix(upper, "DELETE")
			if !isWrite && !isDel {
				continue
			}
			if !entry.Timestamp.IsZero() {
				ts := entry.Timestamp.UTC()
				if s.FirstWriteTime == nil || ts.Before(*s.FirstWriteTime) {
					s.FirstWriteTime = &ts
				}
				if s.LastWriteTime == nil || ts.After(*s.LastWriteTime) {
					s.LastWriteTime = &ts
				}
			}
			if isWrite {
				s.WriteCount++
			}
			if isDel {
				s.DeleteCount++
			}
			if entry.Error != "" {
				s.WriteErrors++
			}
			if entry.Host != "" {
				if _, exists := seen[entry.Host]; !exists {
					seen[entry.Host] = struct{}{}
					s.Hosts = append(s.Hosts, entry.Host)
				}
			}
		}
	}
	sort.Strings(s.Hosts)
	return s
}

//nolint:gocyclo
func BuildCorruptionEntries(errors []JobError, testIdx, oracleIdx stmtIndex) []CorruptionEntry {
	if errors == nil {
		return nil
	}
	out := make([]CorruptionEntry, 0, len(errors))
	for _, e := range errors {
		stmtKey := buildStmtKeyFromValues(e.PartitionKeys)
		var testLines, oracleLines []stmtLogLine
		if testIdx != nil {
			testLines = testIdx[stmtKey]
		}
		if oracleIdx != nil {
			oracleLines = oracleIdx[stmtKey]
		}
		testStats := summariseStatements(testLines)
		oracleStats := summariseStatements(oracleLines)
		var insertTime *time.Time
		if t := uuidv7Time(e.PartitionID); !t.IsZero() {
			insertTime = &t
		}
		if insertTime == nil {
			insertTime = testStats.FirstWriteTime
		}
		if insertTime == nil {
			insertTime = oracleStats.FirstWriteTime
		}
		if insertTime == nil {
			for _, pv := range e.LastValidations {
				if pv.FirstSuccessNS != 0 {
					t := nsToTime(pv.FirstSuccessNS)
					if insertTime == nil || t.Before(*insertTime) {
						insertTime = &t
					}
				}
			}
		}
		var lastSuccess *time.Time
		var successCount uint64
		for _, pv := range e.LastValidations {
			if pv.LastSuccessNS != 0 {
				t := nsToTime(pv.LastSuccessNS)
				if lastSuccess == nil || t.After(*lastSuccess) {
					lastSuccess = &t
				}
			}
			successCount += pv.SuccessCount
		}
		var deletedAt *time.Time
		if t := nsToTime(e.DeletionTimeNS); !t.IsZero() {
			deletedAt = &t
		}
		entry := CorruptionEntry{
			PartitionKeys:            formatPartitionKeys(e.PartitionKeys),
			InsertedAt:               insertTime,
			LastSuccessfulValidation: lastSuccess,
			SuccessfulValidations:    successCount,
			CorruptionDetectedAt:     e.Timestamp.UTC(),
			DeletedAt:                deletedAt,
			ErrorKind:                errorKindFromResults(e.Results),
			FailingQuery:             e.Query,
			Message:                  e.Message,
			TestCluster:              testStats,
			OracleCluster:            oracleStats,
		}
		if insertTime != nil {
			gap := e.Timestamp.UTC().Sub(*insertTime)
			entry.WriteToCorruptionGap = gap.Round(time.Millisecond).String()
		}
		if e.Results != nil {
			entry.DiffOracleOnlyRows = len(e.Results.OracleOnlyRows)
			entry.DiffTestOnlyRows = len(e.Results.TestOnlyRows)
			entry.DiffFieldMismatches = len(e.Results.DifferentRows)
			for _, diff := range e.Results.DifferentRows {
				if diff.Diff != "" {
					entry.FieldDiffs = append(entry.FieldDiffs, diff.Diff)
				}
			}
		}
		out = append(out, entry)
	}
	return out
}

func ComputeSummary(errors []JobError, testStmtFile, oracleStmtFile string) []CorruptionEntry {
	if len(errors) == 0 {
		return nil
	}
	return BuildCorruptionEntries(errors, buildStmtIndex(testStmtFile), buildStmtIndex(oracleStmtFile))
}

//nolint:forbidigo,gocyclo
func PrintCorruptionSummary(w io.Writer, entries []CorruptionEntry) {
	if len(entries) == 0 {
		return
	}
	_, _ = fmt.Fprintln(w)
	_, _ = fmt.Fprintln(w, "╔══════════════════════════════════════════════════════════════════════════════════╗")
	_, _ = fmt.Fprintln(w, "║                         CORRUPTION SUMMARY                                      ║")
	_, _ = fmt.Fprintln(w, "╚══════════════════════════════════════════════════════════════════════════════════╝")
	_, _ = fmt.Fprintf(w, "  %d corrupted partition(s) detected\n\n", len(entries))
	for i, e := range entries {
		_, _ = fmt.Fprintf(w, "─── Error #%d ─────────────────────────────────────────────────────────────────────\n", i+1)
		_, _ = fmt.Fprintf(w, "  Partition keys  : %s\n", e.PartitionKeys)
		_, _ = fmt.Fprintf(w, "  Error kind      : %s\n", e.ErrorKind)
		_, _ = fmt.Fprintf(w, "  Detected at     : %s\n", e.CorruptionDetectedAt.Format(tsLayout))
		_, _ = fmt.Fprintln(w)
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "  EVENT\tTIME\tDETAIL")
		_, _ = fmt.Fprintln(tw, "  ─────\t────\t──────")
		_, _ = fmt.Fprintf(tw, "  First write (test)\t%s\t%d writes, %d deletes, %d errors\n",
			optionalTime(e.TestCluster.FirstWriteTime), e.TestCluster.WriteCount, e.TestCluster.DeleteCount, e.TestCluster.WriteErrors)
		_, _ = fmt.Fprintf(tw, "  Last write  (test)\t%s\t\n", optionalTime(e.TestCluster.LastWriteTime))
		_, _ = fmt.Fprintf(tw, "  First write (oracle)\t%s\t%d writes, %d deletes, %d errors\n",
			optionalTime(e.OracleCluster.FirstWriteTime), e.OracleCluster.WriteCount, e.OracleCluster.DeleteCount, e.OracleCluster.WriteErrors)
		_, _ = fmt.Fprintf(tw, "  Last write  (oracle)\t%s\t\n", optionalTime(e.OracleCluster.LastWriteTime))
		_, _ = fmt.Fprintf(tw, "  Partition created\t%s\t(UUIDv7 / first write)\n", optionalTime(e.InsertedAt))
		_, _ = fmt.Fprintf(tw, "  Last validation ok\t%s\t%d successful validation(s)\n",
			optionalTime(e.LastSuccessfulValidation), e.SuccessfulValidations)
		_, _ = fmt.Fprintf(tw, "  Corruption detected\t%s\t%s\n",
			e.CorruptionDetectedAt.Format(tsLayout), e.ErrorKind)
		_, _ = fmt.Fprintf(tw, "  Partition deleted\t%s\t\n", optionalTime(e.DeletedAt))
		_ = tw.Flush()
		if e.WriteToCorruptionGap != "" {
			_, _ = fmt.Fprintf(w, "\n  Write → corruption gap : %s\n", e.WriteToCorruptionGap)
		}
		allHosts := mergeHosts(e.TestCluster.Hosts, e.OracleCluster.Hosts)
		if len(allHosts) > 0 {
			_, _ = fmt.Fprintf(w, "  Hosts seen in writes   : %s\n", strings.Join(allHosts, ", "))
		}
		hasDiff := e.DiffOracleOnlyRows > 0 || e.DiffTestOnlyRows > 0 || len(e.FieldDiffs) > 0
		if hasDiff {
			_, _ = fmt.Fprintln(w)
			_, _ = fmt.Fprintln(w, "  Diff details:")
			if e.DiffOracleOnlyRows > 0 {
				_, _ = fmt.Fprintf(w, "    rows in oracle but missing from test : %d\n", e.DiffOracleOnlyRows)
			}
			if e.DiffTestOnlyRows > 0 {
				_, _ = fmt.Fprintf(w, "    rows in test but missing from oracle : %d\n", e.DiffTestOnlyRows)
			}
			for j, diff := range e.FieldDiffs {
				_, _ = fmt.Fprintf(w, "    field diff #%d:\n", j+1)
				for _, line := range strings.Split(diff, "\n") {
					if line != "" {
						_, _ = fmt.Fprintf(w, "      %s\n", line)
					}
				}
			}
		}
		if e.FailingQuery != "" {
			_, _ = fmt.Fprintln(w)
			_, _ = fmt.Fprintf(w, "  Failing query : %s\n", e.FailingQuery)
		}
		if e.Message != "" {
			_, _ = fmt.Fprintf(w, "  Message       : %s\n", e.Message)
		}
		_, _ = fmt.Fprintln(w)
	}
	_, _ = fmt.Fprintln(w, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	_, _ = fmt.Fprintln(w, "  Quick triage")
	_, _ = fmt.Fprintln(w, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	tw2 := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
	_, _ = fmt.Fprintln(tw2, "  #\tPartition\tInserted\tLast OK Validation\tCorruption\tDeleted\tKind")
	_, _ = fmt.Fprintln(tw2, "  ─\t─────────\t────────\t──────────────────\t───────────\t───────\t────")
	for i, e := range entries {
		_, _ = fmt.Fprintf(tw2, "  %d\t%s\t%s\t%s\t%s\t%s\t%s\n",
			i+1,
			e.PartitionKeys,
			optionalTime(e.InsertedAt),
			optionalTime(e.LastSuccessfulValidation),
			e.CorruptionDetectedAt.Format(tsLayout),
			optionalTime(e.DeletedAt),
			e.ErrorKind,
		)
	}
	_ = tw2.Flush()
	_, _ = fmt.Fprintln(w)
}

func formatPartitionKeys(v partitionKeyReader) string {
	if v == nil {
		return notAvailable
	}
	keys := v.Keys()
	if len(keys) == 0 {
		return notAvailable
	}
	var sb strings.Builder
	sb.Grow(64)
	for i, key := range keys {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(key)
		sb.WriteByte('=')
		vals := v.Get(key)
		if len(vals) == 0 {
			continue
		}
		_, _ = fmt.Fprintf(&sb, "%v", vals[0])
	}
	return sb.String()
}

func optionalTime(t *time.Time) string {
	if t == nil {
		return notAvailable
	}
	return t.UTC().Format(tsLayout)
}

func nsToTime(ns uint64) time.Time {
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, int64(ns)).UTC()
}

func uuidv7Time(id uuid.UUID) time.Time {
	if id == (uuid.UUID{}) || id[6]>>4 != 7 {
		return time.Time{}
	}
	ms := int64(id[0])<<40 | int64(id[1])<<32 | int64(id[2])<<24 |
		int64(id[3])<<16 | int64(id[4])<<8 | int64(id[5])
	return time.Unix(ms/1000, (ms%1000)*int64(time.Millisecond)).UTC()
}

func errorKindFromResults(r *ComparisonResults) string {
	if r == nil {
		return "unknown"
	}
	switch {
	case len(r.OracleOnlyRows) > 0 && len(r.TestOnlyRows) > 0:
		return "row count mismatch (missing in both test and oracle)"
	case len(r.OracleOnlyRows) > 0:
		return "missing in test"
	case len(r.TestOnlyRows) > 0:
		return "missing in oracle"
	case len(r.DifferentRows) > 0:
		return "field value mismatch"
	default:
		return "unknown"
	}
}

func mergeHosts(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	for _, h := range a {
		seen[h] = struct{}{}
	}
	for _, h := range b {
		seen[h] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for h := range seen {
		out = append(out, h)
	}
	sort.Strings(out)
	return out
}
