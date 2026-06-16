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

	"github.com/scylladb/gemini/pkg/typedef"
)

// pkValuesFromTracked reconstructs a *typedef.Values map from the flat
// PartitionValues slice stored in a TrackedRow. This is needed so that the
// statement logger (which calls PartitionKeys.Values.ToCQLValues) receives
// the correct number of bind values for the batch INSERT into the statements
// table — a nil Values causes "expected N values got 9" errors there.
//
// The function mirrors the loop already used to build the DELETE bind slice:
// it walks the table's PartitionKeys in order and slices off LenValue() values
// for each column, building the column-name → values map.
// Returns nil when the flat slice is too short (caller should fall back).
func pkValuesFromTracked(partitionKeys typedef.Columns, flatValues []any) *typedef.Values {
	m := make(map[string][]any, len(partitionKeys))
	idx := 0
	for _, pk := range partitionKeys {
		lenVal := pk.Type.LenValue()
		if idx+lenVal > len(flatValues) {
			return nil
		}
		m[pk.Name] = flatValues[idx : idx+lenVal]
		idx += lenVal
	}
	return typedef.NewValuesFromMap(m)
}

func (g *Generator) buildCachedDeleteQueries() {
	wholeBuilder := qb.Delete(g.keyspaceAndTable)
	for _, pk := range g.table.PartitionKeys {
		wholeBuilder = wholeBuilder.Where(qb.Eq(pk.Name))
	}
	g.deleteWholePartitionQuery, _ = wholeBuilder.ToCql()

	cks := g.table.ClusteringKeys
	if len(cks) == 0 {
		return
	}

	// Single-row delete: equality on every partition key AND every clustering key.
	rowBuilder := qb.Delete(g.keyspaceAndTable)
	for _, pk := range g.table.PartitionKeys {
		rowBuilder = rowBuilder.Where(qb.Eq(pk.Name))
	}
	for _, ck := range cks {
		rowBuilder = rowBuilder.Where(qb.Eq(ck.Name))
	}
	g.deleteSingleRowQuery, _ = rowBuilder.ToCql()

	// Cluster (prefix) deletes: equality on all partition keys plus the first n
	// clustering keys, for every prefix length n in [1, len(cks)-1]. Indexed by n
	// (slot 0 and the full-length slot are unused; full length is a single-row
	// delete handled above).
	g.deleteClusteringSubsetQueries = make([]string, len(cks))
	for n := 1; n < len(cks); n++ {
		b := qb.Delete(g.keyspaceAndTable)
		for _, pk := range g.table.PartitionKeys {
			b = b.Where(qb.Eq(pk.Name))
		}
		for _, ck := range cks[:n] {
			b = b.Where(qb.Eq(ck.Name))
		}
		g.deleteClusteringSubsetQueries[n], _ = b.ToCql()
	}
}

func (g *Generator) Delete(ctx context.Context) (*typedef.Stmt, error) {
	switch g.ratioController.GetDeleteSubtype() {
	case DeleteWholePartition:
		return g.deleteSinglePartition(ctx)
	case DeleteSingleRow:
		return g.deleteSingleRow(ctx)
	case DeleteClusteringSubset:
		return g.deleteClusteringSubset(ctx)
	case DeleteMultiplePartitions:
		// deleteMultiplePartitions is intentionally NOT used: it builds a
		// per-column IN delete (pk1 IN (...) AND pk2 IN (...)), which CQL
		// evaluates as the CARTESIAN PRODUCT of the values. For a composite
		// partition key (the only case where getMultiplePartitionKeys returns
		// >1) that tombstones up to MaxCartesianProductCount partition
		// combinations per statement — almost all of which never existed — a
		// tombstone storm that collapses SCT throughput. A tuple IN
		// ((pk1,pk2,...) IN ((..),(..))) would delete only the real partitions
		// but ScyllaDB rejects multi-column relations on partition keys
		// ("Multi-column relations can only be applied to clustering columns").
		// Until a safe multi-partition delete exists, fall back to a single
		// whole-partition delete.
		return g.deleteSinglePartition(ctx)
	default:
		panic("unknown delete statement type")
	}
}

//nolint:unused // kept for reference; see DeleteMultiplePartitions case above for why it is not called
func (g *Generator) deleteMultiplePartitions(_ context.Context) (*typedef.Stmt, error) {
	builder := qb.Delete(g.keyspaceAndTable)

	numQueryPKs := g.getMultiplePartitionKeys()
	pks := make([]typedef.PartitionKeys, 0, numQueryPKs)
	combined := typedef.NewValues(g.table.PartitionKeys.Len())

	for range numQueryPKs {
		pk := g.generator.ReplaceNext()
		pks = append(pks, pk)
		combined.Merge(pk.Values)
	}

	for _, pk := range g.table.PartitionKeys {
		builder.Where(qb.InTuple(pk.Name, numQueryPKs))
	}

	query, _ := builder.ToCql()

	return &typedef.Stmt{
		PartitionKeys: pks,
		Values:        combined.ToCQLValues(g.table.PartitionKeys),
		QueryType:     typedef.DeleteMultiplePartitionsType,
		Query:         query,
	}, nil
}

func (g *Generator) deleteSinglePartition(_ context.Context) (*typedef.Stmt, error) {
	pks := g.generator.ReplaceNext()

	values := make([]any, 0, g.table.PartitionKeys.LenValues())
	for _, pk := range g.table.PartitionKeys {
		values = append(values, pks.Values.Get(pk.Name)...)
	}

	return &typedef.Stmt{
		PartitionKeys: []typedef.PartitionKeys{pks},
		Values:        values,
		QueryType:     typedef.DeleteWholePartitionType,
		Query:         g.deleteWholePartitionQuery,
	}, nil
}

// deleteSingleRow deletes a specific row identified by partition key + all clustering key values.
// It consumes a tracked row from the row tracker (populated during validation SELECTs).
func (g *Generator) deleteSingleRow(ctx context.Context) (*typedef.Stmt, error) {
	if len(g.table.ClusteringKeys) == 0 {
		// No clustering keys — a single row IS the whole partition.
		return g.deleteSinglePartition(ctx)
	}

	trackedRow, ok := g.generator.PopTrackedRow()
	if !ok {
		return g.deleteSinglePartition(ctx)
	}

	values := make([]any, 0, g.table.PartitionKeys.LenValues()+g.table.ClusteringKeys.LenValues())

	// Add partition key conditions using the flat PartitionValues slice.
	pkValIdx := 0
	for _, pk := range g.table.PartitionKeys {
		lenVal := pk.Type.LenValue()
		if pkValIdx+lenVal > len(trackedRow.PartitionValues) {
			return g.deleteSinglePartition(ctx)
		}
		values = append(values, trackedRow.PartitionValues[pkValIdx:pkValIdx+lenVal]...)
		pkValIdx += lenVal
	}

	// Add ALL clustering key conditions (single row).
	ckValIdx := 0
	for _, ck := range g.table.ClusteringKeys {
		lenVal := ck.Type.LenValue()
		if ckValIdx+lenVal > len(trackedRow.ClusteringValues) {
			return g.deleteSinglePartition(ctx)
		}
		values = append(values, trackedRow.ClusteringValues[ckValIdx:ckValIdx+lenVal]...)
		ckValIdx += lenVal
	}

	// Reconstruct the Values map so the statement logger can bind the correct
	// number of partition-key values in its batch INSERT.
	pkVals := pkValuesFromTracked(g.table.PartitionKeys, trackedRow.PartitionValues)
	if pkVals == nil {
		return g.deleteSinglePartition(ctx)
	}

	stmtPK := typedef.PartitionKeys{
		ID:     trackedRow.PartitionID,
		Values: pkVals,
	}

	return &typedef.Stmt{
		PartitionKeys: []typedef.PartitionKeys{stmtPK},
		Values:        values,
		QueryType:     typedef.DeleteSingleRowType,
		Query:         g.deleteSingleRowQuery,
	}, nil
}

func (g *Generator) deleteClusteringSubset(ctx context.Context) (*typedef.Stmt, error) {
	numCKs := len(g.table.ClusteringKeys)
	if numCKs == 0 {
		// No clustering keys — cluster delete is same as partition delete.
		return g.deleteSinglePartition(ctx)
	}
	if numCKs == 1 {
		// Only one CK: any prefix delete == single row delete.
		return g.deleteSingleRow(ctx)
	}

	trackedRow, ok := g.generator.PopTrackedRow()
	if !ok {
		return g.deleteSinglePartition(ctx)
	}

	values := make([]any, 0, g.table.PartitionKeys.LenValues()+g.table.ClusteringKeys.LenValues())

	// Add partition key equality conditions.
	pkValIdx := 0
	for _, pk := range g.table.PartitionKeys {
		lenVal := pk.Type.LenValue()
		if pkValIdx+lenVal > len(trackedRow.PartitionValues) {
			return g.deleteSinglePartition(ctx)
		}
		values = append(values, trackedRow.PartitionValues[pkValIdx:pkValIdx+lenVal]...)
		pkValIdx += lenVal
	}

	// Pick n in [1, numCKs-1]: n equality-bound CK columns, leaving at least one
	// unbound so this is a cluster (prefix) delete rather than a single-row delete.
	n := 1 + g.random.IntN(numCKs-1)

	ckValIdx := 0
	for _, ck := range g.table.ClusteringKeys[:n] {
		lenVal := ck.Type.LenValue()
		if ckValIdx+lenVal > len(trackedRow.ClusteringValues) {
			return g.deleteSinglePartition(ctx)
		}
		values = append(values, trackedRow.ClusteringValues[ckValIdx:ckValIdx+lenVal]...)
		ckValIdx += lenVal
	}

	// Reconstruct the Values map so the statement logger can bind the correct
	// number of partition-key values in its batch INSERT.
	pkVals := pkValuesFromTracked(g.table.PartitionKeys, trackedRow.PartitionValues)
	if pkVals == nil {
		return g.deleteSinglePartition(ctx)
	}

	stmtPK := typedef.PartitionKeys{
		ID:     trackedRow.PartitionID,
		Values: pkVals,
	}

	return &typedef.Stmt{
		PartitionKeys: []typedef.PartitionKeys{stmtPK},
		Values:        values,
		QueryType:     typedef.DeleteClusteringSubsetType,
		Query:         g.deleteClusteringSubsetQueries[n],
	}, nil
}
