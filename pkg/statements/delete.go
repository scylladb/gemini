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

// appendTrackedKeys appends the equality bind values for a targeted mutation
// against trackedRow to dst: every partition-key value followed by the first nCK
// clustering-key values (nCK == len(ClusteringKeys) for a single-row target, a
// smaller nCK for a clustering-prefix target). It also returns a *typedef.Values
// of just the partition-key values, which the statement logger binds into the
// _logs INSERT (a nil Values there fails with a bind-arity mismatch).
//
// ok is false when trackedRow is too short for the schema; the caller should
// fall back. Lengths are validated before any append, so on ok=false dst is
// returned unchanged and is safe to reuse.
//
// Shared by single-row UPDATE, single-row DELETE, and clustering-prefix DELETE.
func (g *Generator) appendTrackedKeys(dst []any, trackedRow partitions.TrackedRow, nCK int) ([]any, *typedef.Values, bool) {
	pkLen := g.table.PartitionKeysLenValues()
	if len(trackedRow.PartitionValues) < pkLen {
		return dst, nil, false
	}
	ckLen := g.table.ClusteringKeys[:nCK].LenValues()
	if len(trackedRow.ClusteringValues) < ckLen {
		return dst, nil, false
	}

	dst = append(dst, trackedRow.PartitionValues[:pkLen]...)
	dst = append(dst, trackedRow.ClusteringValues[:ckLen]...)

	// Partition-key Values for the logger; the slices alias trackedRow (no copy),
	// built map-free directly from the flat partition-value slice.
	return dst, typedef.NewPartitionValues(g.table.PartitionKeys, trackedRow.PartitionValues), true
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
	switch g.ratioController.GetTargetedSubtype() {
	case TargetedWholePartition:
		return g.deleteSinglePartition(ctx)
	case TargetedSingleRow:
		return g.deleteSingleRow(ctx)
	case TargetedClusteringSubset:
		return g.deleteClusteringSubset(ctx)
	case TargetedMultiplePartitions:
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

//nolint:unused // kept for reference; see TargetedMultiplePartitions case above for why it is not called
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

	values := make([]any, 0, g.table.PartitionKeysLenValues())
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

	// Single-row delete binds every partition key AND every clustering key.
	values := make([]any, 0, g.table.PartitionKeysLenValues()+g.table.ClusteringKeysLenValues())
	values, pkVals, ok := g.appendTrackedKeys(values, trackedRow, len(g.table.ClusteringKeys))
	if !ok {
		g.trackedMisses.DeleteSingleRow++
		return g.deleteSinglePartition(ctx)
	}

	return &typedef.Stmt{
		PartitionKeys: []typedef.PartitionKeys{{ID: trackedRow.PartitionID, Values: pkVals}},
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

	// Pick n in [1, numCKs-1]: n equality-bound CK columns, leaving at least one
	// unbound so this is a cluster (prefix) delete rather than a single-row delete.
	n := 1 + g.random.IntN(numCKs-1)

	values := make([]any, 0, g.table.PartitionKeysLenValues()+g.table.ClusteringKeysLenValues())
	values, pkVals, ok := g.appendTrackedKeys(values, trackedRow, n)
	if !ok {
		g.trackedMisses.DeleteClusteringSubset++
		return g.deleteSinglePartition(ctx)
	}

	return &typedef.Stmt{
		PartitionKeys: []typedef.PartitionKeys{{ID: trackedRow.PartitionID, Values: pkVals}},
		Values:        values,
		QueryType:     typedef.DeleteClusteringSubsetType,
		Query:         g.deleteClusteringSubsetQueries[n],
	}, nil
}
