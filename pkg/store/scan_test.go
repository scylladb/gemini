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
	"fmt"
	"reflect"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/testutils"
)

func scanTestColumns() []gocql.ColumnInfo {
	native := func(t gocql.Type) gocql.TypeInfo { return gocql.NewNativeType(4, t) }

	return []gocql.ColumnInfo{
		{Name: "pk0", TypeInfo: native(gocql.TypeVarchar)},
		{Name: "ck0", TypeInfo: native(gocql.TypeBigInt)},
		{Name: "col0", TypeInfo: native(gocql.TypeBlob)},
		{Name: "col1", TypeInfo: native(gocql.TypeVarint)},
		{Name: "col2", TypeInfo: gocql.TupleTypeInfo{
			NativeType: gocql.NewNativeType(4, gocql.TypeTuple),
			Elems:      []gocql.TypeInfo{native(gocql.TypeInt), native(gocql.TypeVarchar)},
		}},
		{Name: "col3", TypeInfo: gocql.CollectionType{
			NativeType: gocql.NewNativeType(4, gocql.TypeList),
			Elem:       native(gocql.TypeVarchar),
		}},
	}
}

// TestNewScanPlanMatchesRowData checks that a plan gives every scan column the
// type that gocql's RowData allocates, with tuples expanded into one column per
// element under gocql's names.
func TestNewScanPlanMatchesRowData(t *testing.T) {
	t.Parallel()

	plan, err := newScanPlan(scanTestColumns())
	require.NoError(t, err)

	wantNames := []string{"pk0", "ck0", "col0", "col1", "col2[0]", "col2[1]", "col3"}
	wantInfos := []gocql.TypeInfo{}
	for _, col := range scanTestColumns() {
		if tuple, ok := col.TypeInfo.(gocql.TupleTypeInfo); ok {
			wantInfos = append(wantInfos, tuple.Elems...)
			continue
		}
		wantInfos = append(wantInfos, col.TypeInfo)
	}

	require.Len(t, plan.elemTypes, len(wantNames))

	for i, name := range wantNames {
		idx, ok := plan.columns.byName[name]
		require.True(t, ok, "column %q must be in the index", name)
		assert.Equal(t, i, idx, "column %q", name)

		dst, newErr := wantInfos[i].NewWithError()
		require.NoError(t, newErr)
		assert.Equal(t, reflect.TypeOf(dst).Elem(), plan.elemTypes[i], "column %q", name)
	}
}

// TestRowScannerGivesEveryRowItsOwnDestinations pins the aliasing rule of the
// scan path: gocql reuses the buffers of a destination it scans into again, so
// no two rows may share one, within a chunk or across chunks.
func TestRowScannerGivesEveryRowItsOwnDestinations(t *testing.T) {
	t.Parallel()

	plan, err := newScanPlan(scanTestColumns())
	require.NoError(t, err)

	scanner := newRowScanner(plan)
	seen := map[any]int{}

	const rows = 3*maxScanChunkRows + 7

	for r := range rows {
		dest := scanner.row(maxScanChunkRows)
		require.Len(t, dest, len(plan.elemTypes))
		assert.Equal(t, len(dest), cap(dest), "a row's destinations must not reach into the next row")

		for i, d := range dest {
			assert.Equal(t, reflect.PointerTo(plan.elemTypes[i]), reflect.TypeOf(d))
			require.True(t, reflect.ValueOf(d).Elem().IsZero(), "row %d column %d must start zeroed", r, i)

			if prev, dup := seen[d]; dup {
				t.Fatalf("row %d column %d shares its destination with row %d", r, i, prev)
			}
			seen[d] = r
		}
	}
}

// TestScanPlanReusedForPreparedStatement checks the assumption the plan cache
// rests on: gocql returns the same result metadata for every execution of one
// prepared statement.
func TestScanPlanReusedForPreparedStatement(t *testing.T) {
	t.Parallel()

	containers := testutils.TestContainers(t)
	keyspace := testutils.GenerateUniqueKeyspaceName(t)

	require.NoError(t, containers.Test.Query(fmt.Sprintf(
		"CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}",
		keyspace)).Exec())
	t.Cleanup(func() { _ = containers.Test.Query("DROP KEYSPACE IF EXISTS " + keyspace).Exec() })

	require.NoError(t, containers.Test.Query(fmt.Sprintf(
		"CREATE TABLE %s.t (pk int, ck int, v text, PRIMARY KEY (pk, ck))", keyspace)).Exec())

	for ck := range 3 {
		require.NoError(t, containers.Test.Query(fmt.Sprintf(
			"INSERT INTO %s.t (pk, ck, v) VALUES (?, ?, ?)", keyspace), 1, ck, "v").Exec())
	}

	plans := make([]*scanPlan, 0, 2)

	for range 2 {
		q := containers.Test.Query(fmt.Sprintf("SELECT pk, ck, v FROM %s.t WHERE pk = ?", keyspace), 1)
		iter := q.Iter()
		require.Positive(t, iter.NumRows())

		plan, err := scanPlanFor(iter)
		require.NoError(t, err)
		plans = append(plans, plan)

		require.NoError(t, iter.Close())
		q.Release()
	}

	assert.Same(t, plans[0], plans[1], "a second execution must reuse the cached plan")
}
