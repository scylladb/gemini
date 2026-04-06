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
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/google/uuid"
)

const (
	notAvailable   = "-"
	tsLayout       = "2006-01-02 15:04:05.000 UTC"
	scannerInitBuf = 32 * 1024 * 1024
	scannerMaxBuf  = 256 * 1024 * 1024
)

type stmtLogEntry struct {
	Timestamp cqlTimestamp `json:"ts"`
	Statement string       `json:"statement"`
	Error     string       `json:"error,omitempty"`
	Host      string       `json:"host,omitempty"`
	Type      string       `json:"ty,omitempty"`
}

// cqlTimestamp wraps time.Time with JSON unmarshaling that accepts both
// Go's RFC3339 format and Scylla/Cassandra's SELECT JSON timestamp format
// (e.g. "2024-11-25 12:34:56.789+0000" or "2024-11-25 12:34:56.789Z").
type cqlTimestamp struct {
	time.Time
}

// cqlTSFormats lists timestamp layouts emitted by Scylla / Cassandra
// SELECT JSON, in order from most to least common.
var cqlTSFormats = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05.999999999Z0700",  // space separator, compact offset
	"2006-01-02 15:04:05.999999999Z07:00", // space separator, colon offset
	"2006-01-02T15:04:05.999999999Z0700",  // T separator, compact offset (+0000)
	"2006-01-02T15:04:05.999999999-0700",  // T separator, compact offset
	"2006-01-02 15:04:05.999999999+0000",  // explicit UTC with compact offset
}

func (ct *cqlTimestamp) UnmarshalJSON(data []byte) error {
	// Strip surrounding quotes.
	s := string(data)
	if len(s) < 2 || s[0] != '"' || s[len(s)-1] != '"' {
		return fmt.Errorf("cqlTimestamp: expected quoted string, got %s", s)
	}
	s = s[1 : len(s)-1]
	if s == "" || s == "null" {
		ct.Time = time.Time{}
		return nil
	}
	for _, layout := range cqlTSFormats {
		if t, err := time.Parse(layout, s); err == nil {
			ct.Time = t
			return nil
		}
	}
	return fmt.Errorf("cqlTimestamp: unable to parse %q", s)
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
		FirstWriteTime  *time.Time `json:"first_write_time,omitempty"`
		LastWriteTime   *time.Time `json:"last_write_time,omitempty"`
		FirstDeleteTime *time.Time `json:"first_delete_time,omitempty"`
		LastDeleteTime  *time.Time `json:"last_delete_time,omitempty"`
		Hosts           []string   `json:"hosts,omitempty"`
		WriteCount      int        `json:"write_count"`
		DeleteCount     int        `json:"delete_count"`
		WriteErrors     int        `json:"write_errors"`
		DeleteErrors    int        `json:"delete_errors"`
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

	sc.Buffer(make([]byte, scannerInitBuf), scannerMaxBuf)
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
	// Values.Keys() already returns sorted keys, but we sort here explicitly to
	// match the contract of stmtFileKeyFromRaw and guard against future
	// partitionKeyReader implementations that may not guarantee order.
	sort.Strings(keys)
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
		// Encode the first value as canonical JSON.
		// When partition-key values have been round-tripped through
		// json.Unmarshal (e.g. via Values.UnmarshalJSON), numeric types
		// arrive as float64.  For whole-number floats we must emit an
		// integer representation so the key matches what stmtFileKeyFromRaw
		// produces from the raw bytes written by the statement flusher
		// (which marshals the original int32/int64 directly).
		// Example: float64(4200000000000000) → json.Marshal → "4.2e+15"
		//          int64(4200000000000000)   → json.Marshal → "4200000000000000"
		// Using json.Number("4200000000000000") makes both paths produce
		// identical bytes.
		val := vals[0]
		if f, ok := val.(float64); ok {
			if i64 := int64(f); float64(i64) == f {
				val = json.Number(strconv.FormatInt(i64, 10))
			}
		}
		b, _ := json.Marshal(val)
		sb.Write(b)
	}
	return sb.String()
}

// updateTimeWindow expands the [*first, *last] closed time interval to include ts.
// Calling it with a zero ts is a no-op.
func updateTimeWindow(first, last **time.Time, ts time.Time) {
	if ts.IsZero() {
		return
	}
	if *first == nil || ts.Before(**first) {
		*first = &ts
	}
	if *last == nil || ts.After(**last) {
		*last = &ts
	}
}

func parseStatements(lines []stmtLogLine) []stmtLogEntry {
	var out []stmtLogEntry
	for _, line := range lines {
		for _, raw := range line.Statements {
			var e stmtLogEntry
			if err := json.Unmarshal(raw, &e); err == nil {
				out = append(out, e)
			}
		}
	}
	return out
}

// statementKind reports whether stmt is a write (INSERT/UPDATE) or a delete.
func statementKind(stmt string) (isWrite, isDel bool) {
	upper := strings.ToUpper(strings.TrimSpace(stmt))
	isWrite = strings.HasPrefix(upper, "INSERT") || strings.HasPrefix(upper, "UPDATE")
	isDel = strings.HasPrefix(upper, "DELETE")
	return isWrite, isDel
}

func trackHost(seen map[string]struct{}, hosts *[]string, host string) {
	if host == "" {
		return
	}
	if _, exists := seen[host]; !exists {
		seen[host] = struct{}{}
		*hosts = append(*hosts, host)
	}
}

func summariseStatements(lines []stmtLogLine) StmtClusterSummary {
	s := StmtClusterSummary{}
	seen := make(map[string]struct{})
	for _, entry := range parseStatements(lines) {
		isWrite, isDel := statementKind(entry.Statement)
		if !isWrite && !isDel {
			continue
		}
		ts := entry.Timestamp.UTC()
		if isWrite {
			updateTimeWindow(&s.FirstWriteTime, &s.LastWriteTime, ts)
			s.WriteCount++
			if entry.Error != "" {
				s.WriteErrors++
			}
		}
		if isDel {
			updateTimeWindow(&s.FirstDeleteTime, &s.LastDeleteTime, ts)
			s.DeleteCount++
			if entry.Error != "" {
				s.DeleteErrors++
			}
		}
		trackHost(seen, &s.Hosts, entry.Host)
	}
	sort.Strings(s.Hosts)
	return s
}

func resolveInsertTime(e JobError, testStats, oracleStats StmtClusterSummary) *time.Time {
	// Try each recorded partition ID; for multi-partition statements use the
	// earliest valid UUIDv7 timestamp so the write→corruption gap is as accurate
	// as possible (it reflects when the first partition was written).
	var earliest *time.Time
	for _, id := range e.PartitionIDs {
		if t := uuidv7Time(id); !t.IsZero() {
			if earliest == nil || t.Before(*earliest) {
				t := t // capture loop variable
				earliest = &t
			}
		}
	}
	if earliest != nil {
		return earliest
	}
	if testStats.FirstWriteTime != nil {
		return testStats.FirstWriteTime
	}
	if oracleStats.FirstWriteTime != nil {
		return oracleStats.FirstWriteTime
	}
	for _, pv := range e.LastValidations {
		if pv.FirstSuccessNS != 0 {
			t := nsToTime(pv.FirstSuccessNS)
			if earliest == nil || t.Before(*earliest) {
				earliest = &t
			}
		}
	}
	return earliest
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
		insertTime := resolveInsertTime(e, testStats, oracleStats)
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
		_, _ = fmt.Fprintf(tw, "  First write  (test)\t%s\t%d writes, %d write errors\n",
			optionalTime(e.TestCluster.FirstWriteTime), e.TestCluster.WriteCount, e.TestCluster.WriteErrors)
		_, _ = fmt.Fprintf(tw, "  Last write   (test)\t%s\t\n", optionalTime(e.TestCluster.LastWriteTime))
		_, _ = fmt.Fprintf(tw, "  First delete (test)\t%s\t%d deletes, %d delete errors\n",
			optionalTime(e.TestCluster.FirstDeleteTime), e.TestCluster.DeleteCount, e.TestCluster.DeleteErrors)
		_, _ = fmt.Fprintf(tw, "  Last delete  (test)\t%s\t\n", optionalTime(e.TestCluster.LastDeleteTime))
		_, _ = fmt.Fprintf(tw, "  First write  (oracle)\t%s\t%d writes, %d write errors\n",
			optionalTime(e.OracleCluster.FirstWriteTime), e.OracleCluster.WriteCount, e.OracleCluster.WriteErrors)
		_, _ = fmt.Fprintf(tw, "  Last write   (oracle)\t%s\t\n", optionalTime(e.OracleCluster.LastWriteTime))
		_, _ = fmt.Fprintf(tw, "  First delete (oracle)\t%s\t%d deletes, %d delete errors\n",
			optionalTime(e.OracleCluster.FirstDeleteTime), e.OracleCluster.DeleteCount, e.OracleCluster.DeleteErrors)
		_, _ = fmt.Fprintf(tw, "  Last delete  (oracle)\t%s\t\n", optionalTime(e.OracleCluster.LastDeleteTime))
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
	if id == uuid.Nil || id[6]>>4 != 7 {
		return time.Time{}
	}

	return time.Unix(id.Time().UnixTime())
}

func errorKindFromResults(r *ComparisonResults) string {
	if r == nil {
		return "unknown"
	}
	switch {
	case len(r.OracleOnlyRows) > 0 && len(r.TestOnlyRows) > 0:
		return "missing rows in test and oracle"
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
