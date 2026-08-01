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

package statements

import (
	"context"

	"github.com/scylladb/gocqlx/v3/qb"

	"github.com/scylladb/gemini/pkg/partitions"
	"github.com/scylladb/gemini/pkg/typedef"
)

// maxUpdateVariants bounds the number of distinct cached UPDATE statements per
// table. Different variants SET different column subsets so updates exercise
// different fields, but the count is capped on purpose: a per-column pool (one
// query per column) makes the number of distinct prepared statements grow with
// table width, which thrashes gocql's prepared-statement cache and stalls the
// run on wide schemas. Capping keeps it constant regardless of column count.
const maxUpdateVariants = 8

// updateVariant is one cached single-row UPDATE shape: a pre-built CQL string,
// the table-column indices its SET clause covers, and the number of bind values
// those columns expect. Counter columns use the literal `col + 1` form and bind
// no value, so they are excluded from setValuesLen.
type updateVariant struct {
	query        string
	columns      []int
	setValuesLen int
}

// buildCachedUpdateQueries pre-builds a small, bounded pool of single-row UPDATE
// shapes for this table. The WHERE shape is fixed — CQL only permits an UPDATE
// to target a full single row, every partition and clustering key bound with
// equality. The SET clause is what varies: the full-row update plus up to
// maxUpdateVariants-1 disjoint column groups, so different statements write
// different fields while the number of distinct cached (and prepared) queries
// stays bounded regardless of how many columns the table has.
func (g *Generator) buildCachedUpdateQueries() {
	cols := g.table.Columns
	if len(cols) == 0 {
		// No non-key columns to SET; Update() falls back to Insert().
		return
	}

	// Full-row update is always available.
	all := make([]int, len(cols))
	for i := range cols {
		all[i] = i
	}
	g.updateVariants = []updateVariant{g.buildUpdateVariant(all)}

	if len(cols) == 1 {
		// The single column already is the full-row update.
		return
	}

	// Partition the columns into disjoint contiguous groups — one variant each —
	// so every column is covered by some subset update. nGroups is capped (one
	// slot is reserved for the full-row variant above) so wide tables produce a
	// few multi-column groups rather than one query per column.
	nGroups := min(len(cols), maxUpdateVariants-1)
	base, rem := len(cols)/nGroups, len(cols)%nGroups
	start := 0
	for gi := range nGroups {
		size := base
		if gi < rem {
			size++ // spread the remainder across the first groups
		}
		group := make([]int, size)
		for j := range size {
			group[j] = start + j
		}
		start += size
		g.updateVariants = append(g.updateVariants, g.buildUpdateVariant(group))
	}
}

// buildUpdateVariant builds one updateVariant whose SET clause covers exactly
// the table columns at the given indices, in the order provided.
func (g *Generator) buildUpdateVariant(indices []int) updateVariant {
	builder := qb.Update(g.keyspaceAndTable)
	setValuesLen := 0
	for _, ci := range indices {
		col := g.table.Columns[ci]
		switch ty := col.Type.(type) {
		case *typedef.TupleType:
			builder.SetTuple(col.Name, len(ty.ValueTypes))
			setValuesLen += col.Type.LenValue()
		case *typedef.CounterType:
			builder.SetLit(col.Name, col.Name+"+1")
		default:
			builder.Set(col.Name)
			setValuesLen += col.Type.LenValue()
		}
	}

	for _, pk := range g.table.PartitionKeys {
		builder.Where(qb.Eq(pk.Name))
	}
	for _, ck := range g.table.ClusteringKeys {
		builder.Where(qb.Eq(ck.Name))
	}

	query, _ := builder.ToCql()
	return updateVariant{query: query, columns: indices, setValuesLen: setValuesLen}
}

// pickUpdateVariant returns a cached UPDATE shape for this statement: which
// columns to write (and the query that binds them). This is the single place
// that decides "what to update".
func (g *Generator) pickUpdateVariant() updateVariant {
	if len(g.updateVariants) == 1 {
		return g.updateVariants[0]
	}
	return g.updateVariants[g.random.IntN(len(g.updateVariants))]
}

// appendUpdateSetValues appends the variant's SET-clause bind values to dst, in
// the variant's column order, and returns the extended slice. Counter columns
// are skipped: their SET uses the literal `col + 1` form and binds no value.
func (g *Generator) appendUpdateSetValues(dst []any, variant updateVariant) []any {
	for _, ci := range variant.columns {
		col := g.table.Columns[ci]
		if _, ok := col.Type.(*typedef.CounterType); ok {
			continue
		}
		dst = col.Type.GenValueOut(dst, g.random, g.valueRangeConfig)
	}
	return dst
}

// Update generates a single-row UPDATE statement. It prefers to update a real
// row pulled from the row tracker (populated during validation), mirroring the
// targeted single-row DELETE flow, so the update modifies data that validation
// can then verify. When the tracker is empty it falls back to a random-key
// upsert so updates keep flowing in write-only/warmup modes.
func (g *Generator) Update(ctx context.Context) (*typedef.Stmt, error) {
	if len(g.table.Columns) == 0 {
		// Nothing to SET; still exercise the row via an insert.
		return g.Insert(ctx)
	}

	variant := g.pickUpdateVariant()

	// One bind slice, sized for this variant's SET values + PK + CK; the tracked
	// and random paths both append their key values into this same backing array.
	values := make([]any, 0, variant.setValuesLen+
		g.table.PartitionKeysLenValues()+g.table.ClusteringKeysLenValues())
	values = g.appendUpdateSetValues(values, variant)

	if trackedRow, ok := g.generator.PopTrackedRow(); ok {
		if stmt := g.updateFromTracked(values, variant.query, trackedRow); stmt != nil {
			return stmt, nil
		}
		// On a tracked miss appendTrackedKeys appends nothing (it validates
		// lengths first), so values still holds only the SET bind values.
	}

	return g.updateRandomRow(values, variant.query), nil
}

// updateFromTracked builds a single-row UPDATE targeting trackedRow (full
// partition + clustering key equality). values must already hold the SET bind
// values; the key values are appended in place. Returns nil when trackedRow is
// too short for the schema, so the caller can fall back to a random-row update.
func (g *Generator) updateFromTracked(values []any, query string, trackedRow partitions.TrackedRow) *typedef.Stmt {
	values, pkVals, ok := g.appendTrackedKeys(values, trackedRow, len(g.table.ClusteringKeys))
	if !ok {
		g.trackedMisses.Update++
		return nil
	}

	return &typedef.Stmt{
		PartitionKeys: []typedef.PartitionKeys{{ID: trackedRow.PartitionID, Values: pkVals}},
		Values:        values,
		QueryType:     typedef.UpdateStatementType,
		Query:         query,
	}
}

// updateRandomRow builds a single-row UPDATE against a distribution-picked
// partition with freshly generated clustering key values, used when no tracked
// row is available. values must already hold the SET bind values; the key
// values are appended in place. UPDATE is an upsert, so this is valid even if
// the row did not previously exist.
func (g *Generator) updateRandomRow(values []any, query string) *typedef.Stmt {
	pks := g.generator.Next()

	for _, pk := range g.table.PartitionKeys {
		values = append(values, pks.Values.Get(pk.Name)...)
	}
	for _, ck := range g.table.ClusteringKeys {
		values = ck.Type.GenValueOut(values, g.random, g.valueRangeConfig)
	}

	return &typedef.Stmt{
		PartitionKeys: []typedef.PartitionKeys{pks},
		Values:        values,
		QueryType:     typedef.UpdateStatementType,
		Query:         query,
	}
}
