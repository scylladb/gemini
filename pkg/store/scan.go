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

package store

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/gocql/gocql"
)

const (
	// maxScanPlans bounds scanPlans. gocql keeps the result metadata of a
	// prepared statement, so a run has one plan per statement shape. The bound
	// only matters if a driver change starts to return new metadata per query.
	maxScanPlans = 4096
	// maxScanChunkRows bounds the rows that one chunk of scan destinations
	// holds, so one large page does not allocate one very large slice.
	maxScanChunkRows = 256
)

// scanPlan describes the scan destinations of one result layout: the column
// index of its rows and the Go type that each scan column decodes into. The
// types are the ones gocql's Iter.RowData allocates, so a row holds the same
// *T values either way.
type scanPlan struct {
	columns   *columnIndex
	elemTypes []reflect.Type
}

var (
	// scanPlans caches a *scanPlan per result layout. The key is the first
	// *gocql.ColumnInfo of the metadata. The cache holds the key, so the
	// address cannot be reused by another layout while the plan exists.
	scanPlans     sync.Map
	scanPlanCount atomic.Int64
)

// scanPlanFor returns the scan plan of the current result of iter.
func scanPlanFor(iter *gocql.Iter) (*scanPlan, error) {
	cols := iter.Columns()
	if len(cols) == 0 {
		return nil, errors.New("gocql: result has no column metadata")
	}

	key := &cols[0]
	if cached, ok := scanPlans.Load(key); ok {
		return cached.(*scanPlan), nil //nolint:forcetypeassert // scanPlanFor is the only writer and stores *scanPlan
	}

	plan, err := newScanPlan(cols)
	if err != nil {
		return nil, err
	}

	if scanPlanCount.Load() < maxScanPlans {
		actual, loaded := scanPlans.LoadOrStore(key, plan)
		if !loaded {
			scanPlanCount.Add(1)
		}

		return actual.(*scanPlan), nil //nolint:forcetypeassert // scanPlanFor is the only writer and stores *scanPlan
	}

	return plan, nil
}

// newScanPlan builds the plan of cols. A tuple column scans as one column per
// element, named as gocql names it, the same as Iter.RowData does.
func newScanPlan(cols []gocql.ColumnInfo) (*scanPlan, error) {
	names := make([]string, 0, len(cols))
	types := make([]reflect.Type, 0, len(cols))

	add := func(name string, info gocql.TypeInfo) error {
		dst, err := info.NewWithError()
		if err != nil {
			return err
		}

		t := reflect.TypeOf(dst)
		if t == nil || t.Kind() != reflect.Pointer {
			return fmt.Errorf("gocql: column %q scans into %T, want a pointer", name, dst)
		}

		names = append(names, name)
		types = append(types, t.Elem())

		return nil
	}

	for _, col := range cols {
		tuple, ok := col.TypeInfo.(gocql.TupleTypeInfo)
		if !ok {
			if err := add(col.Name, col.TypeInfo); err != nil {
				return nil, err
			}

			continue
		}

		for i, elem := range tuple.Elems {
			if err := add(gocql.TupleColumnName(col.Name, i), elem); err != nil {
				return nil, err
			}
		}
	}

	return &scanPlan{columns: getColumnIndex(names), elemTypes: types}, nil
}

// rowScanner hands out the scan destinations of one row at a time. It
// allocates them a chunk of rows at once: one slice per column and one []any
// for the whole chunk, in place of one allocation per column per row.
//
// Every row still gets its own zeroed destinations, never ones that an earlier
// row used. gocql reuses the buffers of a destination it scans into again,
// which would alias byte slices, big.Int and inf.Dec values across rows.
type rowScanner struct {
	plan  *scanPlan
	slabs []reflect.Value
	vals  []any
	next  int
	size  int
}

func newRowScanner(plan *scanPlan) *rowScanner {
	return &rowScanner{
		plan:  plan,
		slabs: make([]reflect.Value, len(plan.elemTypes)),
	}
}

// row returns the destinations of the next row. pageRows is the row count of
// the current page, which sizes a new chunk.
func (s *rowScanner) row(pageRows int) []any {
	if s.next == s.size {
		s.size = min(max(pageRows, 1), maxScanChunkRows)
		s.next = 0

		for i, t := range s.plan.elemTypes {
			s.slabs[i] = reflect.MakeSlice(reflect.SliceOf(t), s.size, s.size)
		}

		s.vals = make([]any, s.size*len(s.plan.elemTypes))
	}

	width := len(s.plan.elemTypes)
	start := s.next * width
	dest := s.vals[start : start+width : start+width]

	for i := range s.slabs {
		dest[i] = s.slabs[i].Index(s.next).Addr().Interface()
	}

	s.next++

	return dest
}
