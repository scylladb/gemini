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
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"go.uber.org/multierr"

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

	// Only deduplicate list values on the test side — oracle never has
	// duplicate list items. This is a workaround for non-idempotent list
	// appends in Scylla when inserts are retried.
	deduplicateListValues(table, testRows)

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
		metrics.ValidationRowsMatched.Add(float64(len(testRows)))
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

		metrics.ValidationRowsMissingInTest.Add(float64(len(result.OracleOnlyRows)))
		metrics.ValidationRowsMissingInOracle.Add(float64(len(result.TestOnlyRows)))
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

	metrics.ValidationRowsMatched.Add(float64(result.MatchCount))
	metrics.ValidationRowsDifferent.Add(float64(len(result.DifferentRows)))

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

// deduplicateListValues deduplicates values in CQL list columns in-place.
// This works around a Scylla issue where insert retries can cause list columns
// to accumulate duplicate values. Uses cached list column info from the table
// and type-aware comparison based on the list's ValueType.
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
			slice, ok := val.([]any)
			if !ok || len(slice) <= 1 {
				continue
			}
			deduped := deduplicateByType(slice, listCols[i].ValueType)
			if len(deduped) != len(slice) {
				row.Set(listCols[i].Name, deduped)
				metrics.ValidationRowsDeduplicated.Add(float64(len(slice) - len(deduped)))
			}
		}
	}
}

// deduplicateByType removes duplicate elements from a list using type-aware
// comparison. For types that are directly comparable (int, string, etc.), it
// uses a typed set with zero string conversion. For complex types it falls
// back to compareValues-based dedup.
func deduplicateByType(s []any, vt typedef.SimpleType) []any {
	switch vt {
	case typedef.TypeInt:
		return deduplicateTyped[int32](s)
	case typedef.TypeBigint:
		return deduplicateTyped[int64](s)
	case typedef.TypeSmallint:
		return deduplicateTyped[int16](s)
	case typedef.TypeTinyint:
		return deduplicateTyped[int8](s)
	case typedef.TypeFloat:
		return deduplicateTyped[float32](s)
	case typedef.TypeDouble:
		return deduplicateTyped[float64](s)
	case typedef.TypeBoolean:
		return deduplicateTyped[bool](s)
	case typedef.TypeText, typedef.TypeAscii, typedef.TypeVarchar:
		return deduplicateTyped[string](s)
	case typedef.TypeUuid, typedef.TypeTimeuuid:
		return deduplicateUUID(s)
	default:
		return deduplicateGeneric(s)
	}
}

// deduplicateTyped uses a typed map for O(1) lookup without any string conversion.
func deduplicateTyped[T comparable](s []any) []any {
	seen := make(map[T]struct{}, len(s))
	result := s[:0] // reuse backing array
	for _, v := range s {
		tv, ok := v.(T)
		if !ok {
			result = append(result, v)
			continue
		}
		if _, exists := seen[tv]; exists {
			continue
		}
		seen[tv] = struct{}{}
		result = append(result, v)
	}
	return result
}

// deduplicateUUID handles gocql.UUID which is [16]byte and comparable.
func deduplicateUUID(s []any) []any {
	seen := make(map[gocql.UUID]struct{}, len(s))
	result := s[:0]
	for _, v := range s {
		u, ok := v.(gocql.UUID)
		if !ok {
			result = append(result, v)
			continue
		}
		if _, exists := seen[u]; exists {
			continue
		}
		seen[u] = struct{}{}
		result = append(result, v)
	}
	return result
}

// deduplicateGeneric falls back to compareValues for types that aren't
// directly map-keyable ([]byte, *big.Int, *inf.Dec, time.Duration, etc.).
func deduplicateGeneric(s []any) []any {
	result := s[:0]
	for _, v := range s {
		found := false
		for _, existing := range result {
			if compareValues(existing, v) == 0 {
				found = true
				break
			}
		}
		if !found {
			result = append(result, v)
		}
	}
	return result
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
