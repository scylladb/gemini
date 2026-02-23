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
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/typedef"
)

func tp(t time.Time) *time.Time { return &t }
func writeStmtFile(t *testing.T, lines ...string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "stmts-*.jsonl")
	require.NoError(t, err)
	for _, l := range lines {
		_, err = f.WriteString(l + "\n")
		require.NoError(t, err)
	}
	require.NoError(t, f.Close())
	return f.Name()
}

func stmtLine(t *testing.T, pkMap map[string]any, stmts []map[string]any) string {
	t.Helper()
	raws := make([]json.RawMessage, 0, len(stmts))
	for _, s := range stmts {
		b, err := json.Marshal(s)
		require.NoError(t, err)
		raws = append(raws, b)
	}
	stmtsJSON, err := json.Marshal(raws)
	require.NoError(t, err)
	line := map[string]any{
		"partitionKeys": []map[string]any{{"partitionKeys": pkMap}},
		"statements":    json.RawMessage(stmtsJSON),
		"query":         "SELECT pk0 FROM ks.tbl WHERE pk0=?",
		"message":       "test",
	}
	b, err := json.Marshal(line)
	require.NoError(t, err)
	return string(b)
}

func rawEntries(t *testing.T, entries []stmtLogEntry) []json.RawMessage {
	t.Helper()
	out := make([]json.RawMessage, len(entries))
	for i, e := range entries {
		b, err := json.Marshal(e)
		require.NoError(t, err)
		out[i] = b
	}
	return out
}

func mustMarshalRawSlice(t *testing.T, items []map[string]any) json.RawMessage {
	t.Helper()
	raws := make([]json.RawMessage, 0, len(items))
	for _, item := range items {
		b, err := json.Marshal(item)
		require.NoError(t, err)
		raws = append(raws, b)
	}
	b, err := json.Marshal(raws)
	require.NoError(t, err)
	return b
}

func TestUuidv7Time_ZeroUUID(t *testing.T) {
	t.Parallel()
	assert.True(t, uuidv7Time(uuid.UUID{}).IsZero())
}

func TestUuidv7Time_V4UUID(t *testing.T) {
	t.Parallel()
	assert.True(t, uuidv7Time(uuid.Must(uuid.NewRandom())).IsZero())
}

func TestUuidv7Time_V7UUID(t *testing.T) {
	t.Parallel()
	before := time.Now().UTC().Truncate(time.Millisecond)
	id, err := uuid.NewV7()
	require.NoError(t, err)
	after := time.Now().UTC().Add(time.Millisecond)
	got := uuidv7Time(id)
	assert.False(t, got.IsZero())
	assert.True(t, !got.Before(before) && !got.After(after))
}

func TestNsToTime_Zero(t *testing.T) {
	t.Parallel()
	assert.True(t, nsToTime(0).IsZero())
}

func TestNsToTime_Roundtrip(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 2, 23, 10, 0, 0, 0, time.UTC)
	assert.Equal(t, ts, nsToTime(uint64(ts.UnixNano())))
}

func TestErrorKindFromResults_Nil(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "unknown", errorKindFromResults(nil))
}

func TestErrorKindFromResults_MissingInTest(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "missing in test", errorKindFromResults(
		&ComparisonResults{OracleOnlyRows: []json.RawMessage{[]byte(`{}`)}},
	))
}

func TestErrorKindFromResults_MissingInOracle(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "missing in oracle", errorKindFromResults(
		&ComparisonResults{TestOnlyRows: []json.RawMessage{[]byte(`{}`)}},
	))
}

func TestErrorKindFromResults_FieldMismatch(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "field value mismatch", errorKindFromResults(
		&ComparisonResults{DifferentRows: []RowDiff{{Diff: "- a\n+ b"}}},
	))
}

func TestErrorKindFromResults_BothMissing(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "row count mismatch (missing in both test and oracle)", errorKindFromResults(
		&ComparisonResults{
			OracleOnlyRows: []json.RawMessage{[]byte(`{}`)},
			TestOnlyRows:   []json.RawMessage{[]byte(`{}`)},
		},
	))
}

func TestErrorKindFromResults_Empty(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "unknown", errorKindFromResults(&ComparisonResults{}))
}

func TestMergeHosts_Dedup(t *testing.T) {
	t.Parallel()
	assert.Equal(t, []string{"a", "b", "c"}, mergeHosts([]string{"a", "b"}, []string{"b", "c"}))
}

func TestMergeHosts_BothEmpty(t *testing.T) {
	t.Parallel()
	assert.Empty(t, mergeHosts(nil, nil))
}

func TestStmtFileKeyFromRaw_Empty(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "", stmtFileKeyFromRaw(nil))
	assert.Equal(t, "", stmtFileKeyFromRaw(map[string]json.RawMessage{}))
}

func TestStmtFileKeyFromRaw_Sorted(t *testing.T) {
	t.Parallel()
	m := map[string]json.RawMessage{
		"pk1": json.RawMessage(`"v1"`),
		"pk0": json.RawMessage(`"v0"`),
	}
	assert.Equal(t, `pk0="v0",pk1="v1"`, stmtFileKeyFromRaw(m))
}

func TestBuildStmtKeyFromValues_Nil(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "", buildStmtKeyFromValues(nil))
}

func TestBuildStmtKeyFromValues_Values(t *testing.T) {
	t.Parallel()
	v := typedef.NewValuesFromMap(map[string][]any{"pk0": {int32(42)}, "pk1": {"hello"}})
	got := buildStmtKeyFromValues(v)
	assert.Equal(t, `pk0=42,pk1="hello"`, got)
}

func TestBuildStmtKeyFromValues_MultiColumn(t *testing.T) {
	t.Parallel()
	v := typedef.NewValuesFromMap(map[string][]any{"pk0": {int32(1)}, "pk1": {int32(2)}, "pk2": {"foo"}})
	got := buildStmtKeyFromValues(v)
	assert.Equal(t, `pk0=1,pk1=2,pk2="foo"`, got)
}

func TestFormatPartitionKeys_MultiColumn(t *testing.T) {
	t.Parallel()
	v := typedef.NewValuesFromMap(map[string][]any{"pk0": {int32(10)}, "pk1": {"bar"}})
	assert.Equal(t, "pk0=10, pk1=bar", formatPartitionKeys(v))
}

func TestFormatPartitionKeys_Single(t *testing.T) {
	t.Parallel()
	v := typedef.NewValuesFromMap(map[string][]any{"pk0": {int32(99)}})
	assert.Equal(t, "pk0=99", formatPartitionKeys(v))
}

func TestFormatPartitionKeys_Nil(t *testing.T) {
	t.Parallel()
	assert.Equal(t, notAvailable, formatPartitionKeys(nil))
}

func TestBuildStmtIndex_EmptyPath(t *testing.T) {
	t.Parallel()
	assert.Nil(t, buildStmtIndex(""))
}

func TestBuildStmtIndex_MissingFile(t *testing.T) {
	t.Parallel()
	assert.Nil(t, buildStmtIndex(filepath.Join(t.TempDir(), "no-such-file.jsonl")))
}

func TestBuildStmtIndex_SkipsMalformed(t *testing.T) {
	t.Parallel()
	idx := buildStmtIndex(writeStmtFile(t, "not json at all", `{"partitionKeys":null}`))
	assert.NotNil(t, idx)
	assert.Empty(t, idx)
}

func TestBuildStmtIndex_ParsesValidLines(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 2, 23, 12, 0, 0, 0, time.UTC)
	path := writeStmtFile(t, stmtLine(t,
		map[string]any{"pk0": "abc"},
		[]map[string]any{{"ts": ts.Format(time.RFC3339), "statement": "INSERT INTO ks.tbl (pk0) VALUES (?)", "host": "10.0.0.1"}},
	))
	idx := buildStmtIndex(path)
	require.NotNil(t, idx)
	require.Len(t, idx, 1)
	for k, lines := range idx {
		assert.Contains(t, k, "pk0")
		require.Len(t, lines, 1)
		require.Len(t, lines[0].Statements, 1)
	}
}

func TestBuildStmtIndex_MultiplePartitionsPerLine(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 2, 23, 12, 0, 0, 0, time.UTC)
	line := map[string]any{
		"partitionKeys": []map[string]any{
			{"partitionKeys": map[string]any{"pk0": "aaa"}},
			{"partitionKeys": map[string]any{"pk0": "bbb"}},
		},
		"statements": mustMarshalRawSlice(t, []map[string]any{
			{"ts": ts.Format(time.RFC3339), "statement": "INSERT INTO ks.tbl (pk0) VALUES (?)"},
		}),
	}
	raw, err := json.Marshal(line)
	require.NoError(t, err)
	require.Len(t, buildStmtIndex(writeStmtFile(t, string(raw))), 2)
}

func TestSummariseStatements_Empty(t *testing.T) {
	t.Parallel()
	s := summariseStatements(nil)
	assert.Zero(t, s.WriteCount)
	assert.Nil(t, s.FirstWriteTime)
}

func TestSummariseStatements_CountsWrites(t *testing.T) {
	t.Parallel()
	ts1 := time.Date(2026, 2, 23, 10, 0, 0, 0, time.UTC)
	ts2 := ts1.Add(time.Minute)
	ts3 := ts2.Add(time.Second)
	s := summariseStatements([]stmtLogLine{{
		Statements: rawEntries(t, []stmtLogEntry{
			{Timestamp: ts1, Statement: "INSERT INTO ks.t (pk0) VALUES (?)"},
			{Timestamp: ts2, Statement: "UPDATE ks.t SET c=1 WHERE pk0=?"},
			{Timestamp: ts3, Statement: "DELETE FROM ks.t WHERE pk0=?"},
		}),
	}})
	assert.Equal(t, 2, s.WriteCount)
	assert.Equal(t, 1, s.DeleteCount)
	require.NotNil(t, s.FirstWriteTime)
	assert.Equal(t, ts1.UTC(), *s.FirstWriteTime)
	require.NotNil(t, s.LastWriteTime)
	assert.Equal(t, ts3.UTC(), *s.LastWriteTime)
}

func TestSummariseStatements_IgnoresSelect(t *testing.T) {
	t.Parallel()
	s := summariseStatements([]stmtLogLine{{
		Statements: rawEntries(t, []stmtLogEntry{
			{Timestamp: time.Now(), Statement: "SELECT * FROM ks.t WHERE pk0=?"},
		}),
	}})
	assert.Zero(t, s.WriteCount)
	assert.Nil(t, s.FirstWriteTime)
}

func TestSummariseStatements_TracksHosts(t *testing.T) {
	t.Parallel()
	s := summariseStatements([]stmtLogLine{{
		Statements: rawEntries(t, []stmtLogEntry{
			{Timestamp: time.Now(), Statement: "INSERT INTO ks.t (pk0) VALUES (?)", Host: "10.0.0.1"},
			{Timestamp: time.Now(), Statement: "INSERT INTO ks.t (pk0) VALUES (?)", Host: "10.0.0.2"},
			{Timestamp: time.Now(), Statement: "INSERT INTO ks.t (pk0) VALUES (?)", Host: "10.0.0.1"},
		}),
	}})
	assert.Equal(t, []string{"10.0.0.1", "10.0.0.2"}, s.Hosts)
}

func TestSummariseStatements_CountsWriteErrors(t *testing.T) {
	t.Parallel()
	s := summariseStatements([]stmtLogLine{{
		Statements: rawEntries(t, []stmtLogEntry{
			{Timestamp: time.Now(), Statement: "INSERT INTO ks.t (pk0) VALUES (?)", Error: "timeout"},
			{Timestamp: time.Now(), Statement: "INSERT INTO ks.t (pk0) VALUES (?)"},
		}),
	}})
	assert.Equal(t, 2, s.WriteCount)
	assert.Equal(t, 1, s.WriteErrors)
}

func TestBuildCorruptionEntries_Empty(t *testing.T) {
	t.Parallel()
	assert.Nil(t, BuildCorruptionEntries(nil, nil, nil))
	assert.Empty(t, BuildCorruptionEntries([]JobError{}, nil, nil))
}

func TestBuildCorruptionEntries_InsertionTimeFromUUIDv7(t *testing.T) {
	t.Parallel()
	id, err := uuid.NewV7()
	require.NoError(t, err)
	entries := BuildCorruptionEntries([]JobError{{
		Timestamp:     time.Now().UTC(),
		PartitionID:   id,
		PartitionKeys: typedef.NewValuesFromMap(map[string][]any{"pk0": {int32(1)}}),
	}}, nil, nil)
	require.Len(t, entries, 1)
	require.NotNil(t, entries[0].InsertedAt)
	assert.WithinDuration(t, time.Now().UTC(), *entries[0].InsertedAt, 5*time.Second)
}

func TestBuildCorruptionEntries_InsertionTimeFallbackToStmtLog(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 2, 23, 9, 0, 0, 0, time.UTC)
	path := writeStmtFile(t, stmtLine(t,
		map[string]any{"pk0": int32(7)},
		[]map[string]any{{"ts": ts.Format(time.RFC3339Nano), "statement": "INSERT INTO ks.t (pk0) VALUES (?)"}},
	))
	entries := BuildCorruptionEntries([]JobError{{
		Timestamp:     time.Now().UTC(),
		PartitionKeys: typedef.NewValuesFromMap(map[string][]any{"pk0": {int32(7)}}),
	}}, buildStmtIndex(path), nil)
	require.Len(t, entries, 1)
	require.NotNil(t, entries[0].InsertedAt)
	assert.Equal(t, ts.UTC(), *entries[0].InsertedAt)
}

func TestBuildCorruptionEntries_InsertionTimeFallbackToFirstSuccessNS(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 2, 23, 8, 30, 0, 0, time.UTC)
	entries := BuildCorruptionEntries([]JobError{{
		Timestamp:     time.Now().UTC(),
		PartitionKeys: typedef.NewValuesFromMap(map[string][]any{"pk0": {int32(99)}}),
		LastValidations: map[string]PartitionValidation{
			"some-id": {FirstSuccessNS: uint64(ts.UnixNano())},
		},
	}}, nil, nil)
	require.Len(t, entries, 1)
	require.NotNil(t, entries[0].InsertedAt)
	assert.Equal(t, ts.UTC(), *entries[0].InsertedAt)
}

func TestBuildCorruptionEntries_WriteToCorruptionGap(t *testing.T) {
	t.Parallel()
	insertTime := time.Date(2026, 2, 23, 10, 0, 0, 0, time.UTC)
	entries := BuildCorruptionEntries([]JobError{{
		Timestamp:     insertTime.Add(2 * time.Minute),
		PartitionKeys: typedef.NewValuesFromMap(map[string][]any{"pk0": {int32(1)}}),
		LastValidations: map[string]PartitionValidation{
			"id": {FirstSuccessNS: uint64(insertTime.UnixNano())},
		},
	}}, nil, nil)
	require.Len(t, entries, 1)
	assert.Equal(t, "2m0s", entries[0].WriteToCorruptionGap)
}

func TestBuildCorruptionEntries_DiffFields(t *testing.T) {
	t.Parallel()
	entries := BuildCorruptionEntries([]JobError{{
		Timestamp:     time.Now().UTC(),
		PartitionKeys: typedef.NewValuesFromMap(map[string][]any{"pk0": {int32(1)}}),
		Results: &ComparisonResults{
			OracleOnlyRows: []json.RawMessage{[]byte(`{}`)},
			DifferentRows:  []RowDiff{{Diff: "- col: a\n+ col: b"}},
		},
	}}, nil, nil)
	require.Len(t, entries, 1)
	assert.Equal(t, "missing in test", entries[0].ErrorKind)
	assert.Equal(t, 1, entries[0].DiffOracleOnlyRows)
	assert.Equal(t, 1, entries[0].DiffFieldMismatches)
	assert.Equal(t, []string{"- col: a\n+ col: b"}, entries[0].FieldDiffs)
}

func TestBuildCorruptionEntries_DeletionTime(t *testing.T) {
	t.Parallel()
	delTime := time.Date(2026, 2, 23, 11, 0, 0, 0, time.UTC)
	entries := BuildCorruptionEntries([]JobError{{
		Timestamp:      time.Now().UTC(),
		PartitionKeys:  typedef.NewValuesFromMap(map[string][]any{"pk0": {int32(1)}}),
		DeletionTimeNS: uint64(delTime.UnixNano()),
	}}, nil, nil)
	require.Len(t, entries, 1)
	require.NotNil(t, entries[0].DeletedAt)
	assert.Equal(t, delTime.UTC(), *entries[0].DeletedAt)
}

func TestBuildCorruptionEntries_SuccessCount(t *testing.T) {
	t.Parallel()
	ts1 := time.Date(2026, 2, 23, 10, 30, 0, 0, time.UTC)
	ts2 := ts1.Add(time.Minute)
	entries := BuildCorruptionEntries([]JobError{{
		Timestamp:     time.Now().UTC(),
		PartitionKeys: typedef.NewValuesFromMap(map[string][]any{"pk0": {int32(1)}}),
		LastValidations: map[string]PartitionValidation{
			"id0": {LastSuccessNS: uint64(ts1.UnixNano()), SuccessCount: 5},
			"id1": {LastSuccessNS: uint64(ts2.UnixNano()), SuccessCount: 3},
		},
	}}, nil, nil)
	require.Len(t, entries, 1)
	assert.EqualValues(t, 8, entries[0].SuccessfulValidations)
	require.NotNil(t, entries[0].LastSuccessfulValidation)
	assert.Equal(t, ts2.UTC(), *entries[0].LastSuccessfulValidation)
}

func TestComputeSummary_EmptyErrors(t *testing.T) {
	t.Parallel()
	assert.Nil(t, ComputeSummary(nil, "", ""))
	assert.Nil(t, ComputeSummary([]JobError{}, "", ""))
}

func TestComputeSummary_WithFiles(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 2, 23, 12, 0, 0, 0, time.UTC)
	testFile := writeStmtFile(t, stmtLine(t,
		map[string]any{"pk0": int32(55)},
		[]map[string]any{{"ts": ts.Format(time.RFC3339), "statement": "INSERT INTO ks.t (pk0) VALUES (?)", "host": "10.0.0.5"}},
	))
	summary := ComputeSummary([]JobError{{
		Timestamp:     ts.Add(time.Minute),
		PartitionKeys: typedef.NewValuesFromMap(map[string][]any{"pk0": {int32(55)}}),
	}}, testFile, "")
	require.Len(t, summary, 1)
	assert.Equal(t, 1, summary[0].TestCluster.WriteCount)
	assert.Equal(t, []string{"10.0.0.5"}, summary[0].TestCluster.Hosts)
}

func TestPrintCorruptionSummary_Empty(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	PrintCorruptionSummary(&buf, nil)
	assert.Empty(t, buf.String())
	buf.Reset()
	PrintCorruptionSummary(&buf, []CorruptionEntry{})
	assert.Empty(t, buf.String())
}

func TestPrintCorruptionSummary_Header(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	PrintCorruptionSummary(&buf, []CorruptionEntry{{
		PartitionKeys:        "pk0=1",
		ErrorKind:            "missing in test",
		CorruptionDetectedAt: time.Date(2026, 2, 23, 12, 0, 0, 0, time.UTC),
	}})
	out := buf.String()
	assert.Contains(t, out, "CORRUPTION SUMMARY")
	assert.Contains(t, out, "1 corrupted partition(s) detected")
	assert.Contains(t, out, "Error #1")
	assert.Contains(t, out, "pk0=1")
	assert.Contains(t, out, "missing in test")
	assert.Contains(t, out, "Quick triage")
}

func TestPrintCorruptionSummary_MultipleErrors(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	PrintCorruptionSummary(&buf, []CorruptionEntry{
		{PartitionKeys: "pk0=1", ErrorKind: "missing in test", CorruptionDetectedAt: time.Date(2026, 2, 23, 10, 0, 0, 0, time.UTC)},
		{PartitionKeys: "pk0=2", ErrorKind: "field value mismatch", CorruptionDetectedAt: time.Date(2026, 2, 23, 11, 0, 0, 0, time.UTC)},
	})
	out := buf.String()
	assert.Contains(t, out, "2 corrupted partition(s) detected")
	assert.Contains(t, out, "Error #1")
	assert.Contains(t, out, "Error #2")
}

func TestPrintCorruptionSummary_DiffDetails(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	PrintCorruptionSummary(&buf, []CorruptionEntry{{
		PartitionKeys:        "pk0=5",
		ErrorKind:            "field value mismatch",
		CorruptionDetectedAt: time.Now().UTC(),
		DiffFieldMismatches:  1,
		FieldDiffs:           []string{"- col: hello\n+ col: world"},
	}})
	out := buf.String()
	assert.Contains(t, out, "field diff #1")
	assert.Contains(t, out, "col: hello")
	assert.Contains(t, out, "col: world")
}

func TestPrintCorruptionSummary_WriteToCorruptionGap(t *testing.T) {
	t.Parallel()
	insertTime := time.Date(2026, 2, 23, 10, 0, 0, 0, time.UTC)
	var buf bytes.Buffer
	PrintCorruptionSummary(&buf, []CorruptionEntry{{
		PartitionKeys:        "pk0=1",
		ErrorKind:            "missing in oracle",
		InsertedAt:           tp(insertTime),
		CorruptionDetectedAt: insertTime.Add(5 * time.Minute),
		WriteToCorruptionGap: "5m0s",
	}})
	assert.Contains(t, buf.String(), "5m0s")
}

func TestPrintCorruptionSummary_DeletedAt(t *testing.T) {
	t.Parallel()
	deletedAt := time.Date(2026, 2, 23, 10, 5, 0, 0, time.UTC)
	var buf bytes.Buffer
	PrintCorruptionSummary(&buf, []CorruptionEntry{{
		PartitionKeys:        "pk0=1",
		ErrorKind:            "missing in test",
		CorruptionDetectedAt: deletedAt.Add(-time.Minute),
		DeletedAt:            tp(deletedAt),
	}})
	out := buf.String()
	assert.Contains(t, out, deletedAt.UTC().Format(tsLayout))
	found := false
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, "pk0=1") && strings.Contains(l, deletedAt.UTC().Format(tsLayout)) {
			found = true
			break
		}
	}
	assert.True(t, found, "quick-triage table should show deletion time")
}

func TestPrintCorruptionSummary_NotDeletedShowsDash(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	PrintCorruptionSummary(&buf, []CorruptionEntry{{
		PartitionKeys:        "pk0=1",
		ErrorKind:            "missing in test",
		CorruptionDetectedAt: time.Now().UTC(),
	}})
	for _, l := range strings.Split(buf.String(), "\n") {
		if strings.Contains(l, "pk0=1") && strings.Contains(l, "missing in test") {
			assert.Contains(t, l, "-", "deleted column should be '-'")
		}
	}
}

func TestCorruptionEntry_JSONRoundtrip(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 2, 23, 10, 0, 0, 0, time.UTC)
	entry := CorruptionEntry{
		PartitionKeys:            "pk0=42",
		InsertedAt:               tp(ts),
		LastSuccessfulValidation: tp(ts.Add(30 * time.Second)),
		SuccessfulValidations:    7,
		CorruptionDetectedAt:     ts.Add(time.Minute),
		DeletedAt:                tp(ts.Add(2 * time.Minute)),
		WriteToCorruptionGap:     "1m0s",
		ErrorKind:                "field value mismatch",
		FailingQuery:             "SELECT pk0 FROM ks.t WHERE pk0=?",
		Message:                  "row differs",
		DiffFieldMismatches:      1,
		FieldDiffs:               []string{"- col: a\n+ col: b"},
		TestCluster: StmtClusterSummary{
			FirstWriteTime: tp(ts),
			LastWriteTime:  tp(ts.Add(50 * time.Second)),
			WriteCount:     10,
			DeleteCount:    1,
			Hosts:          []string{"10.0.0.1"},
		},
	}
	b, err := json.Marshal(entry)
	require.NoError(t, err)
	var got CorruptionEntry
	require.NoError(t, json.Unmarshal(b, &got))
	assert.Equal(t, entry.PartitionKeys, got.PartitionKeys)
	assert.Equal(t, entry.ErrorKind, got.ErrorKind)
	assert.Equal(t, entry.WriteToCorruptionGap, got.WriteToCorruptionGap)
	assert.Equal(t, entry.SuccessfulValidations, got.SuccessfulValidations)
	assert.Equal(t, entry.DiffFieldMismatches, got.DiffFieldMismatches)
	assert.Equal(t, entry.FieldDiffs, got.FieldDiffs)
	assert.Equal(t, entry.TestCluster.WriteCount, got.TestCluster.WriteCount)
	assert.Equal(t, entry.TestCluster.Hosts, got.TestCluster.Hosts)
	require.NotNil(t, got.InsertedAt)
	assert.WithinDuration(t, *entry.InsertedAt, *got.InsertedAt, time.Millisecond)
	assert.WithinDuration(t, entry.CorruptionDetectedAt, got.CorruptionDetectedAt, time.Millisecond)
	require.NotNil(t, got.DeletedAt)
	assert.WithinDuration(t, *entry.DeletedAt, *got.DeletedAt, time.Millisecond)
}

func TestCorruptionEntry_ZeroTimesOmittedFromJSON(t *testing.T) {
	t.Parallel()
	b, err := json.Marshal(CorruptionEntry{
		PartitionKeys:        "pk0=1",
		ErrorKind:            "unknown",
		CorruptionDetectedAt: time.Date(2026, 2, 23, 10, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	var m map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(b, &m))
	_, hasInserted := m["inserted_at"]
	_, hasDeleted := m["deleted_at"]
	assert.False(t, hasInserted)
	assert.False(t, hasDeleted)
}

func TestCorruptionEntry_JSONHasExpectedKeys(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 2, 23, 10, 0, 0, 0, time.UTC)
	b, err := json.Marshal(CorruptionEntry{
		PartitionKeys:        "pk0=1",
		ErrorKind:            "missing in test",
		CorruptionDetectedAt: ts,
		InsertedAt:           tp(ts.Add(-time.Minute)),
		WriteToCorruptionGap: "1m0s",
	})
	require.NoError(t, err)
	var m map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(b, &m))
	for _, key := range []string{
		"partition_keys", "error_kind", "corruption_detected_at",
		"inserted_at", "write_to_corruption_gap",
		"test_cluster", "oracle_cluster",
	} {
		assert.Contains(t, m, key)
	}
}

func TestBuildStmtIndex_RoundTripWithBuildCorruptionEntries(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 2, 23, 9, 0, 0, 0, time.UTC)
	detectedAt := ts.Add(3 * time.Minute)
	pkVal := int32(777)
	testFile := writeStmtFile(t, stmtLine(t,
		map[string]any{"pk0": pkVal},
		[]map[string]any{
			{"ts": ts.Format(time.RFC3339Nano), "statement": "INSERT INTO ks.t (pk0) VALUES (?)", "host": "192.168.0.1"},
			{"ts": ts.Add(time.Minute).Format(time.RFC3339Nano), "statement": "UPDATE ks.t SET c0=1 WHERE pk0=?", "host": "192.168.0.2"},
		},
	))
	entries := BuildCorruptionEntries([]JobError{{
		Timestamp:     detectedAt,
		PartitionKeys: typedef.NewValuesFromMap(map[string][]any{"pk0": {pkVal}}),
		Results:       &ComparisonResults{OracleOnlyRows: []json.RawMessage{[]byte(`{}`)}},
		Message:       "row missing in test",
		Query:         "SELECT pk0 FROM ks.t WHERE pk0=?",
	}}, buildStmtIndex(testFile), nil)
	require.Len(t, entries, 1)
	entry := entries[0]
	assert.Equal(t, "missing in test", entry.ErrorKind)
	assert.Equal(t, 2, entry.TestCluster.WriteCount)
	assert.Equal(t, 0, entry.TestCluster.DeleteCount)
	require.NotNil(t, entry.TestCluster.FirstWriteTime)
	assert.Equal(t, ts.UTC(), *entry.TestCluster.FirstWriteTime)
	assert.Contains(t, entry.WriteToCorruptionGap, "m")
	assert.Equal(t, []string{"192.168.0.1", "192.168.0.2"}, entry.TestCluster.Hosts)
	assert.Equal(t, "SELECT pk0 FROM ks.t WHERE pk0=?", entry.FailingQuery)
	assert.Equal(t, "row missing in test", entry.Message)
	assert.Equal(t, 1, entry.DiffOracleOnlyRows)
	b, err := json.Marshal(entry)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m))
	assert.Contains(t, m, "test_cluster")
	assert.Contains(t, m, "corruption_detected_at")
}

func TestBuildStmtIndex_RoundTripMultiColumnPK(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 2, 23, 9, 0, 0, 0, time.UTC)
	detectedAt := ts.Add(2 * time.Minute)

	testFile := writeStmtFile(t, stmtLine(t,
		map[string]any{"pk0": int32(10), "pk1": "foo"},
		[]map[string]any{
			{"ts": ts.Format(time.RFC3339Nano), "statement": "INSERT INTO ks.t (pk0, pk1) VALUES (?, ?)", "host": "10.0.0.1"},
		},
	))

	entries := BuildCorruptionEntries([]JobError{{
		Timestamp:     detectedAt,
		PartitionKeys: typedef.NewValuesFromMap(map[string][]any{"pk0": {int32(10)}, "pk1": {"foo"}}),
		Results:       &ComparisonResults{OracleOnlyRows: []json.RawMessage{[]byte(`{}`)}},
	}}, buildStmtIndex(testFile), nil)

	require.Len(t, entries, 1)
	entry := entries[0]
	assert.Equal(t, "pk0=10, pk1=foo", entry.PartitionKeys)
	assert.Equal(t, 1, entry.TestCluster.WriteCount)
	require.NotNil(t, entry.TestCluster.FirstWriteTime)
	assert.Equal(t, ts.UTC(), *entry.TestCluster.FirstWriteTime)
}
