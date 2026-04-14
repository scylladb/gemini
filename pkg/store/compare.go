// Copyright 2025 ScyllaDB
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

package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"go.uber.org/multierr"
	"gopkg.in/inf.v0"

	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/typedef"
)

// CompareCollectedRows compares already collected rows from both sides
func CompareCollectedRows(table *typedef.Table, testRows, oracleRows Rows) ComparisonResult {
	result := ComparisonResult{Table: table}

	// Handle empty result sets
	if len(testRows) == 0 && len(oracleRows) == 0 {
		return result
	}

	// Deduplicate list values on both sides. Duplicate list items can appear
	// on either cluster when inserts are retried (non-idempotent list appends).
	deduplicateListValues(table, testRows)
	deduplicateListValues(table, oracleRows)

	// Sort both sides (stable) to aid deterministic behavior when rendering diffs.
	// We sort by partition keys first, then clustering keys to ensure stable
	// and consistent comparison regardless of the order returned by the database.
	cmp := func(a, b Row) int {
		for _, pk := range table.PartitionKeys {
			if c := compareValues(a.Get(pk.Name), b.Get(pk.Name)); c != 0 {
				return c
			}
		}
		for _, ck := range table.ClusteringKeys {
			if c := compareValues(a.Get(ck.Name), b.Get(ck.Name)); c != 0 {
				return c
			}
		}
		return rowsCmp(a, b)
	}
	if len(testRows) > 1 {
		slices.SortStableFunc(testRows, cmp)
	}
	if len(oracleRows) > 1 {
		slices.SortStableFunc(oracleRows, cmp)
	}

	// Fast-path: if rows are deeply equal after sort, all match.
	if reflect.DeepEqual(testRows, oracleRows) {
		return ComparisonResult{Table: table, MatchCount: len(testRows)}
	}

	// If row counts differ, report only set differences and keep MatchCount = 0.
	if len(testRows) != len(oracleRows) {
		testSet := pks(table, testRows)
		oracleSet := pks(table, oracleRows)

		// Build pk -> row maps for both sides
		testRowMap := buildRowMap(table, testRows)
		oracleRowMap := buildRowMap(table, oracleRows)

		// Rows present only in oracle
		for _, pk := range oracleSet.List() {
			if !testSet.Has(pk) {
				if row, ok := oracleRowMap[pk]; ok {
					result.OracleOnlyRows = append(result.OracleOnlyRows, row)
				}
			}
		}

		// Rows present only in test
		for _, pk := range testSet.List() {
			if !oracleSet.Has(pk) {
				if row, ok := testRowMap[pk]; ok {
					result.TestOnlyRows = append(result.TestOnlyRows, row)
				}
			}
		}

		return result
	}

	// Same number of rows: compare by index after sorting, producing value diffs or matches.
	for i := range testRows {
		tRow := testRows[i]
		oRow := oracleRows[i]
		if diff := diffRows(table, oRow, tRow); diff != "" {
			result.DifferentRows = append(result.DifferentRows, RowDifference{
				TestRow:   tRow,
				OracleRow: oRow,
				Diff:      diff,
			})
		} else {
			result.MatchCount++
		}
	}

	return result
}

// buildRowMap creates a map from pk string to Row for efficient lookup
func buildRowMap(table *typedef.Table, rows Rows) map[string]Row {
	result := make(map[string]Row, len(rows))

	for _, row := range rows {
		var sb strings.Builder
		sb.Grow(64)

		// Build composite key from all partition and clustering keys
		for _, pk := range table.PartitionKeys {
			formatRows(&sb, pk.Name, row.Get(pk.Name))
			sb.WriteByte(',')
		}
		for _, ck := range table.ClusteringKeys {
			formatRows(&sb, ck.Name, row.Get(ck.Name))
			sb.WriteByte(',')
		}

		// Trim trailing comma to keep consistency with pks()
		pkStr := strings.TrimRight(sb.String(), ",")
		result[pkStr] = row
	}

	return result
}

// ToError converts ComparisonResult to an error if there are differences
func (cr ComparisonResult) ToError() error {
	if cr.TestError != nil || cr.OracleError != nil {
		return multierr.Combine(cr.TestError, cr.OracleError)
	}

	var err error

	if len(cr.TestOnlyRows) > 0 || len(cr.OracleOnlyRows) > 0 {
		// For row-count mismatches we report only the unmatched counts,
		// excluding matched rows from the totals to satisfy existing tests.
		err = multierr.Append(err, ErrorRowDifference{
			MissingInTest:   rowsToKeyStrings(cr.Table, cr.OracleOnlyRows),
			MissingInOracle: rowsToKeyStrings(cr.Table, cr.TestOnlyRows),
			TestRows:        len(cr.TestOnlyRows),
			OracleRows:      len(cr.OracleOnlyRows),
		})
	}

	for _, diff := range cr.DifferentRows {
		err = multierr.Append(err, ErrorRowDifference{
			Diff:      diff.Diff,
			OracleRow: diff.OracleRow,
			TestRow:   diff.TestRow,
		})
	}

	return err
}

// rowsToKeyStrings renders only the primary and clustering key values for compact error reporting.
func rowsToKeyStrings(table *typedef.Table, rows []Row) []string {
	if len(rows) == 0 {
		return nil
	}

	result := make([]string, len(rows))
	for i, row := range rows {
		result[i] = rowKeyString(table, row)
	}

	return result
}

func rowKeyString(table *typedef.Table, row Row) string {
	if table == nil {
		bytes, _ := json.Marshal(row)
		return string(bytes)
	}

	var sb strings.Builder
	sb.Grow(64)

	for _, pk := range table.PartitionKeys {
		formatRows(&sb, pk.Name, row.Get(pk.Name))
		sb.WriteByte(',')
	}

	for _, ck := range table.ClusteringKeys {
		formatRows(&sb, ck.Name, row.Get(ck.Name))
		sb.WriteByte(',')
	}

	return strings.TrimRight(sb.String(), ",")
}

// diffRows produces an editor-friendly unified diff between two rows.
// It compares values by content (not pointer identity) and avoids leaking
// pointer addresses. Only columns that differ are included.
func diffRows(table *typedef.Table, oracleRow, testRow Row) string {
	// Header with PK/CK context for quick identification
	var header strings.Builder
	header.Grow(128)
	header.WriteString("pk:")
	if table != nil {
		for _, pk := range table.PartitionKeys {
			header.WriteString(" ")
			var tmp strings.Builder
			formatRows(&tmp, pk.Name, oracleRow.Get(pk.Name))
			header.WriteString(tmp.String())
			header.WriteString(",")
		}
		for _, ck := range table.ClusteringKeys {
			header.WriteString(" ")
			var tmp strings.Builder
			formatRows(&tmp, ck.Name, oracleRow.Get(ck.Name))
			header.WriteString(tmp.String())
			header.WriteString(",")
		}
	}
	hdr := strings.TrimRight(header.String(), ",")

	// Build canonical maps of column->string value
	oMap := canonicalizeRow(oracleRow)
	tMap := canonicalizeRow(testRow)

	// Union of keys
	keys := make([]string, 0, len(oMap)+len(tMap))
	keySeen := make(map[string]struct{}, len(oMap)+len(tMap))
	for k := range oMap {
		keys = append(keys, k)
		keySeen[k] = struct{}{}
	}
	for k := range tMap {
		if _, ok := keySeen[k]; !ok {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return ""
	}
	slices.Sort(keys)

	var b strings.Builder
	b.Grow(256)
	b.WriteString(hdr)
	b.WriteByte('\n')

	diffCount := 0
	for _, k := range keys {
		oVal, oOK := oMap[k]
		tVal, tOK := tMap[k]

		switch {
		case oOK && !tOK:
			b.WriteString("- ")
			b.WriteString(k)
			b.WriteString(": ")
			b.WriteString(oVal)
			b.WriteByte('\n')
			diffCount++
		case !oOK && tOK:
			b.WriteString("+ ")
			b.WriteString(k)
			b.WriteString(": ")
			b.WriteString(tVal)
			b.WriteByte('\n')
			diffCount++
		case oOK && oVal != tVal:
			b.WriteString("- ")
			b.WriteString(k)
			b.WriteString(": ")
			b.WriteString(oVal)
			b.WriteByte('\n')
			b.WriteString("+ ")
			b.WriteString(k)
			b.WriteString(": ")
			b.WriteString(tVal)
			b.WriteByte('\n')
			diffCount++
		default:
			// equal, skip
		}
	}

	if diffCount == 0 {
		return ""
	}
	return b.String()
}

// canonicalizeRow converts a Row into a map of column name -> stable string value.
// Pointers are dereferenced and common types are rendered in a readable way.
func canonicalizeRow(row Row) map[string]string {
	out := make(map[string]string, len(row.columns))
	for name := range row.columns {
		v := row.Get(name)
		out[name] = canonicalValueString(v)
	}
	return out
}

func deduplicateListValues(table *typedef.Table, rows Rows) {
	if table == nil || len(rows) == 0 {
		return
	}

	listCols := table.ListColumns()
	if len(listCols) == 0 {
		return
	}

	for _, row := range rows {
		for i := range listCols {
			val := row.Get(listCols[i].Name)
			newVal, before, after := deduplicateSlice(val)
			if after != before {
				row.Set(listCols[i].Name, newVal)
				metrics.ValidationRowsDeduplicated.Add(float64(before - after))
			}
		}
	}
}


//nolint:cyclop
func deduplicateSlice(val any) (newVal any, before, after int) {
	if val == nil {
		return nil, 0, 0
	}

	switch s := val.(type) {
	// ── []any ────────────────────────────────────────────────────────────────
	case []any:
		n := deduplicateAny(s)
		return s[:n], len(s), n

	// ── bool ─────────────────────────────────────────────────────────────────
	case []bool:
		n := deduplicateInPlace(s)
		return s[:n], len(s), n
	case *[]bool:
		if s == nil {
			return val, 0, 0
		}
		b := len(*s)
		n := deduplicateInPlace(*s)
		*s = (*s)[:n]
		return val, b, n

	// ── int (CQL int → Go int) ────────────────────────────────────────────────
	case []int:
		n := deduplicateInPlace(s)
		return s[:n], len(s), n
	case *[]int:
		if s == nil {
			return val, 0, 0
		}
		b := len(*s)
		n := deduplicateInPlace(*s)
		*s = (*s)[:n]
		return val, b, n

	// ── int8 (tinyint) ───────────────────────────────────────────────────────
	case []int8:
		n := deduplicateInPlace(s)
		return s[:n], len(s), n
	case *[]int8:
		if s == nil {
			return val, 0, 0
		}
		b := len(*s)
		n := deduplicateInPlace(*s)
		*s = (*s)[:n]
		return val, b, n

	// ── int16 (smallint) ─────────────────────────────────────────────────────
	case []int16:
		n := deduplicateInPlace(s)
		return s[:n], len(s), n
	case *[]int16:
		if s == nil {
			return val, 0, 0
		}
		b := len(*s)
		n := deduplicateInPlace(*s)
		*s = (*s)[:n]
		return val, b, n

	// ── int32 (not produced by gocql for list elements, but guard anyway) ────
	case []int32:
		n := deduplicateInPlace(s)
		return s[:n], len(s), n
	case *[]int32:
		if s == nil {
			return val, 0, 0
		}
		b := len(*s)
		n := deduplicateInPlace(*s)
		*s = (*s)[:n]
		return val, b, n

	// ── int64 (bigint, counter) ───────────────────────────────────────────────
	case []int64:
		n := deduplicateInPlace(s)
		return s[:n], len(s), n
	case *[]int64:
		if s == nil {
			return val, 0, 0
		}
		b := len(*s)
		n := deduplicateInPlace(*s)
		*s = (*s)[:n]
		return val, b, n

	// ── float32 (float) ──────────────────────────────────────────────────────
	case []float32:
		n := deduplicateInPlace(s)
		return s[:n], len(s), n
	case *[]float32:
		if s == nil {
			return val, 0, 0
		}
		b := len(*s)
		n := deduplicateInPlace(*s)
		*s = (*s)[:n]
		return val, b, n

	// ── float64 (double) ─────────────────────────────────────────────────────
	case []float64:
		n := deduplicateInPlace(s)
		return s[:n], len(s), n
	case *[]float64:
		if s == nil {
			return val, 0, 0
		}
		b := len(*s)
		n := deduplicateInPlace(*s)
		*s = (*s)[:n]
		return val, b, n

	// ── string (text, ascii, varchar, inet) ──────────────────────────────────
	case []string:
		n := deduplicateInPlace(s)
		return s[:n], len(s), n
	case *[]string:
		if s == nil {
			return val, 0, 0
		}
		b := len(*s)
		n := deduplicateInPlace(*s)
		*s = (*s)[:n]
		return val, b, n

	// ── gocql.UUID (uuid, timeuuid) — [16]byte, comparable ───────────────────
	case []gocql.UUID:
		n := deduplicateInPlace(s)
		return s[:n], len(s), n
	case *[]gocql.UUID:
		if s == nil {
			return val, 0, 0
		}
		b := len(*s)
		n := deduplicateInPlace(*s)
		*s = (*s)[:n]
		return val, b, n

	// ── time.Time (timestamp, date) ───────────────────────────────────────────
	case []time.Time:
		n := deduplicateTimeSlice(s)
		return s[:n], len(s), n
	case *[]time.Time:
		if s == nil {
			return val, 0, 0
		}
		b := len(*s)
		n := deduplicateTimeSlice(*s)
		*s = (*s)[:n]
		return val, b, n

	// ── time.Duration (CQL time → nanoseconds) ────────────────────────────────
	case []time.Duration:
		n := deduplicateInPlace(s)
		return s[:n], len(s), n
	case *[]time.Duration:
		if s == nil {
			return val, 0, 0
		}
		b := len(*s)
		n := deduplicateInPlace(*s)
		*s = (*s)[:n]
		return val, b, n

	// ── gocql.Duration (CQL duration: months/days/nanoseconds struct) ─────────
	case []gocql.Duration:
		n := deduplicateInPlace(s)
		return s[:n], len(s), n
	case *[]gocql.Duration:
		if s == nil {
			return val, 0, 0
		}
		b := len(*s)
		n := deduplicateInPlace(*s)
		*s = (*s)[:n]
		return val, b, n

	// ── []byte (blob) — not comparable, use bytes.Equal ──────────────────────
	case [][]byte:
		n := deduplicateBlobSlice(s)
		return s[:n], len(s), n
	case *[][]byte:
		if s == nil {
			return val, 0, 0
		}
		b := len(*s)
		n := deduplicateBlobSlice(*s)
		*s = (*s)[:n]
		return val, b, n

	// ── *big.Int (varint) ────────────────────────────────────────────────────
	case []*big.Int:
		n := deduplicateBigIntSlice(s)
		return s[:n], len(s), n
	case *[]*big.Int:
		if s == nil {
			return val, 0, 0
		}
		b := len(*s)
		n := deduplicateBigIntSlice(*s)
		*s = (*s)[:n]
		return val, b, n

	// ── *inf.Dec (decimal) ───────────────────────────────────────────────────
	case []*inf.Dec:
		n := deduplicateDecSlice(s)
		return s[:n], len(s), n
	case *[]*inf.Dec:
		if s == nil {
			return val, 0, 0
		}
		b := len(*s)
		n := deduplicateDecSlice(*s)
		*s = (*s)[:n]
		return val, b, n

	default:
		// Reflection fallback for any unknown/future gocql type.
		newV, b, a := deduplicateReflect(val)
		return newV, b, a
	}
}

const dedupSmallThreshold = 16

func deduplicateInPlace[T comparable](s []T) int {
	n := len(s)
	if n <= 1 {
		return n
	}

	if n <= dedupSmallThreshold {
		// Allocation-free linear scan — no heap pressure for the common case.
		w := 1 // s[0] is always kept
	outer:
		for i := 1; i < n; i++ {
			for j := range w {
				if s[j] == s[i] {
					continue outer
				}
			}
			s[w] = s[i]
			w++
		}
		return w
	}

	// Hash-map path for large slices — O(n) amortised.
	seen := make(map[T]struct{}, n)
	w := 0
	for _, v := range s {
		if _, exists := seen[v]; !exists {
			seen[v] = struct{}{}
			s[w] = v
			w++
		}
	}
	return w
}

func deduplicateAny(s []any) int {
	if len(s) <= 1 {
		return len(s)
	}
	w := 0
	for _, v := range s {
		found := false
		for j := range w {
			if compareValues(s[j], v) == 0 {
				found = true
				break
			}
		}
		if !found {
			s[w] = v
			w++
		}
	}
	return w
}

func deduplicateTimeSlice(s []time.Time) int {
	if len(s) <= 1 {
		return len(s)
	}
	w := 0
outer:
	for _, v := range s {
		for j := range w {
			if s[j].Equal(v) {
				continue outer
			}
		}
		s[w] = v
		w++
	}
	return w
}

// deduplicateBlobSlice deduplicates [][]byte in-place using bytes.Equal.
func deduplicateBlobSlice(s [][]byte) int {
	if len(s) <= 1 {
		return len(s)
	}
	w := 0
outer:
	for _, v := range s {
		for j := range w {
			if bytes.Equal(s[j], v) {
				continue outer
			}
		}
		s[w] = v
		w++
	}
	return w
}

// deduplicateBigIntSlice deduplicates []*big.Int in-place using Cmp.
func deduplicateBigIntSlice(s []*big.Int) int {
	if len(s) <= 1 {
		return len(s)
	}
	w := 0
outer:
	for _, v := range s {
		for j := range w {
			if s[j].Cmp(v) == 0 {
				continue outer
			}
		}
		s[w] = v
		w++
	}
	return w
}

// deduplicateDecSlice deduplicates []*inf.Dec in-place using Cmp.
func deduplicateDecSlice(s []*inf.Dec) int {
	if len(s) <= 1 {
		return len(s)
	}
	w := 0
outer:
	for _, v := range s {
		for j := range w {
			if s[j].Cmp(v) == 0 {
				continue outer
			}
		}
		s[w] = v
		w++
	}
	return w
}

// deduplicateReflect is a reflection-based fallback for unknown slice types.
// It handles *[]T and []T by extracting the underlying slice and comparing
// elements via compareValues. Returns (val, 0, 0) if val is not a slice.
func deduplicateReflect(val any) (newVal any, before, after int) {
	rv := reflect.ValueOf(val)
	isPtr := rv.Kind() == reflect.Ptr
	if isPtr {
		if rv.IsNil() {
			return val, 0, 0
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Slice {
		return val, 0, 0
	}
	n := rv.Len()
	if n <= 1 {
		return val, n, n
	}

	w := 0
outer:
	for i := range n {
		v := rv.Index(i).Interface()
		for j := range w {
			if compareValues(rv.Index(j).Interface(), v) == 0 {
				continue outer
			}
		}
		rv.Index(w).Set(rv.Index(i))
		w++
	}

	resliced := rv.Slice(0, w)
	if isPtr {
		// Truncate the slice behind the pointer in-place.
		rv.Set(resliced)
		return val, n, w
	}
	return resliced.Interface(), n, w
}

// canonicalValueString returns a stable, pointer-safe string for a value.
func canonicalValueString(v any) string {
	if v == nil {
		return "null"
	}
	// Fully dereference pointers
	rv := reflect.ValueOf(v)
	for rv.IsValid() && rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return "null"
		}
		rv = rv.Elem()
	}
	if !rv.IsValid() {
		return "null"
	}
	v = rv.Interface()

	switch val := v.(type) {
	case []byte:
		// Render bytes as hex to avoid binary noise
		return fmt.Sprintf("0x%x", val)
	case string:
		return val
	case bool:
		if val {
			return "true"
		}
		return "false"
	case time.Time:
		return val.Format("2006-01-02 15:04:05.999999999")
	case gocql.UUID:
		return val.String()
	default:
		// Fall back to %v, which prints underlying values of numbers/structs cleanly
		return fmt.Sprintf("%v", val)
	}
}
