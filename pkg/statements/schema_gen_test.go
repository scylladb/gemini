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

package statements

import (
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/typedef"
)

// TestInsertMarksOnlyLWTNonIdempotent: an LWT INSERT takes its timestamp from
// the Paxos ballot, so a compensating DELETE at the client timestamp can miss
// it. The store must invalidate such a write instead of compensating it.
func TestInsertMarksOnlyLWTNonIdempotent(t *testing.T) {
	t.Parallel()

	table := singleClusteringKeyTable()
	mp := newMockPartitionsWithTracker(10)
	t.Cleanup(mp.Close)

	rng := rand.New(rand.NewChaCha8([32]byte{3}))
	rc, err := NewRatioController(DefaultStatementRatios(), rng)
	require.NoError(t, err)
	vc := &typedef.ValueRangeConfig{MaxBlobLength: 8, MinBlobLength: 1, MaxStringLength: 8, MinStringLength: 1}
	gen := New("ks", mp, table, rng, vc, rc, true)

	var lwt, plain int
	for range 500 {
		stmt, insertErr := gen.Insert(t.Context())
		require.NoError(t, insertErr)

		isLWT := strings.Contains(stmt.Query, "IF NOT EXISTS")
		assert.Equal(t, isLWT, stmt.NonIdempotent, "query %q", stmt.Query)
		if isLWT {
			lwt++
		} else {
			plain++
		}
	}

	assert.Positive(t, lwt, "no LWT INSERT in 500 draws")
	assert.Positive(t, plain, "no plain INSERT in 500 draws")
}

func TestInsertWithoutLWTIsIdempotent(t *testing.T) {
	t.Parallel()

	table := singleClusteringKeyTable()
	mp := newMockPartitionsWithTracker(10)
	t.Cleanup(mp.Close)

	gen := newSelectRangeTestGenerator(t, table, mp)
	for range 100 {
		stmt, err := gen.Insert(t.Context())
		require.NoError(t, err)
		assert.False(t, stmt.NonIdempotent)
	}
}

// TestGenTupleTypeLengthRange: every length from 2 to MaxTupleParts must be
// possible, with an equal chance for each.
func TestGenTupleTypeLengthRange(t *testing.T) {
	t.Parallel()

	for _, maxParts := range []int{0, 1, 2, 3, 5} {
		sc := &typedef.SchemaConfig{MaxTupleParts: maxParts}
		r := rand.New(rand.NewPCG(uint64(maxParts), 11))
		want := max(maxParts, 2)

		const draws = 4000
		counts := make(map[int]int)
		for range draws {
			tuple, ok := GenTupleType(sc, r).(*typedef.TupleType)
			require.True(t, ok)
			counts[len(tuple.ValueTypes)]++
		}

		for n := range counts {
			assert.GreaterOrEqual(t, n, 2, "MaxTupleParts %d", maxParts)
			assert.LessOrEqual(t, n, want, "MaxTupleParts %d", maxParts)
		}

		lengths := want - 1
		for n := 2; n <= want; n++ {
			// Each length gets about draws/lengths. Half of that is a
			// generous lower bound that a uniform draw never misses.
			assert.Greater(t, counts[n], draws/lengths/2, "MaxTupleParts %d: length %d drawn %d times", maxParts, n, counts[n])
		}
	}
}

// TestGetCreateTypesIsStable: CREATE TYPE lists the UDT fields in sorted order,
// so two runs with one schema send the same DDL.
func TestGetCreateTypesIsStable(t *testing.T) {
	t.Parallel()

	fields := map[string]typedef.SimpleType{}
	for i := range 12 {
		fields["f"+itoaStmt(i)] = typedef.AllTypes[i%len(typedef.AllTypes)]
	}

	newTable := func() *typedef.Table {
		return &typedef.Table{
			Name:          "t",
			PartitionKeys: typedef.Columns{{Name: "pk", Type: typedef.TypeInt}},
			Columns: typedef.Columns{{Name: "u", Type: &typedef.UDTType{
				ComplexType: typedef.TypeUdt,
				ValueTypes:  fields,
				TypeName:    "udt_1",
				Frozen:      true,
			}}},
		}
	}

	ks := typedef.Keyspace{Name: "ks"}
	want := GetCreateTypes(newTable(), ks)
	require.Len(t, want, 1)
	assert.Equal(t,
		"CREATE TYPE IF NOT EXISTS ks.udt_1 (f0 ascii,f1 bigint,f10 text,f11 time,"+
			"f2 blob,f3 date,f4 decimal,f5 double,f6 float,f7 inet,f8 int,f9 smallint)",
		want[0])

	for range 20 {
		assert.Equal(t, want, GetCreateTypes(newTable(), ks))
	}
}
