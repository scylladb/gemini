// Copyright 2019 ScyllaDB
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

package generators

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2/qb"
	"golang.org/x/exp/rand"

	"github.com/scylladb/gemini/pkg/builders"
	"github.com/scylladb/gemini/pkg/coltypes"
	"github.com/scylladb/gemini/pkg/testschema"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

func GenMutateStmt(s *testschema.Schema, t *testschema.Table, g *Generator, r *rand.Rand, p *typedef.PartitionRangeConfig, deletes bool) (*typedef.Stmt, error) {
	t.RLock()
	defer t.RUnlock()

	valuesWithToken := g.Get()
	if valuesWithToken == nil {
		return nil, nil
	}
	useLWT := false
	if p.UseLWT && r.Uint32()%10 == 0 {
		useLWT = true
	}

	if !deletes {
		return genInsertOrUpdateStmt(s, t, valuesWithToken, r, p, useLWT)
	}
	switch n := rand.Intn(1000); n {
	case 10, 100:
		return genDeleteRows(s, t, valuesWithToken, r, p)
	default:
		switch rand.Intn(2) {
		case 0:
			if t.KnownIssues[typedef.KnownIssuesJSONWithTuples] {
				return genInsertOrUpdateStmt(s, t, valuesWithToken, r, p, useLWT)
			}
			return genInsertJSONStmt(s, t, valuesWithToken, r, p)
		default:
			return genInsertOrUpdateStmt(s, t, valuesWithToken, r, p, useLWT)
		}
	}
}

func GenCheckStmt(s *testschema.Schema, t *testschema.Table, g *Generator, r *rand.Rand, p *typedef.PartitionRangeConfig) *typedef.Stmt {
	var n int
	if len(t.Indexes) > 0 {
		n = r.Intn(5)
	} else {
		n = r.Intn(4)
	}
	switch n {
	case 0:
		return genSinglePartitionQuery(s, t, g, r, p)
	case 1:
		return genMultiplePartitionQuery(s, t, g, r, p)
	case 2:
		return genClusteringRangeQuery(s, t, g, r, p)
	case 3:
		return genMultiplePartitionClusteringRangeQuery(s, t, g, r, p)
	case 4:
		// Reducing the probability to hit these since they often take a long time to run
		switch r.Intn(5) {
		case 0:
			return genSingleIndexQuery(s, t, g, r, p)
		default:
			return genSinglePartitionQuery(s, t, g, r, p)
		}
	}
	return nil
}

func genSinglePartitionQuery(s *testschema.Schema, t *testschema.Table, g *Generator, r *rand.Rand, p *typedef.PartitionRangeConfig) *typedef.Stmt {
	t.RLock()
	defer t.RUnlock()
	valuesWithToken := g.GetOld()
	if valuesWithToken == nil {
		return nil
	}
	var (
		mvCol    testschema.ColumnDef
		mvValues []interface{}
	)

	tableName := t.Name
	partitionKeys := t.PartitionKeys
	values := valuesWithToken.Value.Copy()
	if len(t.MaterializedViews) > 0 && r.Int()%2 == 0 {
		view := r.Intn(len(t.MaterializedViews))
		tableName = t.MaterializedViews[view].Name
		partitionKeys = t.MaterializedViews[view].PartitionKeys
		mvCol = t.MaterializedViews[view].NonPrimaryKey
	}
	builder := qb.Select(s.Keyspace.Name + "." + tableName)
	typs := make([]typedef.Type, 0, 10)
	for _, pk := range partitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
		typs = append(typs, pk.Type)
	}
	if (testschema.ColumnDef{}) != mvCol {
		mvValues = appendValue(mvCol.Type, r, p, mvValues)
		values = append(mvValues, values...)
	}

	return &typedef.Stmt{
		ValuesWithToken: valuesWithToken,
		Query:           builder,
		Values:          values,
		Types:           typs,
		QueryType:       typedef.SelectStatementType,
	}
}

func genMultiplePartitionQuery(s *testschema.Schema, t *testschema.Table, g *Generator, r *rand.Rand, p *typedef.PartitionRangeConfig) *typedef.Stmt {
	t.RLock()
	defer t.RUnlock()

	var (
		values []interface{}
		typs   []typedef.Type
	)
	tableName := t.Name
	partitionKeys := t.PartitionKeys
	if len(t.MaterializedViews) > 0 && r.Int()%2 == 0 {
		view := r.Intn(len(t.MaterializedViews))
		tableName = t.MaterializedViews[view].Name
		partitionKeys = t.MaterializedViews[view].PartitionKeys
	}
	numQueryPKs := r.Intn(len(partitionKeys))
	if numQueryPKs == 0 {
		numQueryPKs = 1
	}
	multiplier := int(math.Pow(float64(numQueryPKs), float64(len(partitionKeys))))
	if multiplier > 100 {
		numQueryPKs = 1
	}

	builder := qb.Select(s.Keyspace.Name + "." + tableName)
	for i, pk := range partitionKeys {
		builder = builder.Where(qb.InTuple(pk.Name, numQueryPKs))
		for j := 0; j < numQueryPKs; j++ {
			vs := g.GetOld()
			if vs == nil {
				return nil
			}
			numMVKeys := len(partitionKeys) - len(vs.Value)
			if i < numMVKeys {
				values = appendValue(pk.Type, r, p, values)
				typs = append(typs, pk.Type)
			} else {
				values = append(values, vs.Value[i-numMVKeys])
				typs = append(typs, pk.Type)
			}
		}
	}
	return &typedef.Stmt{
		Query:     builder,
		Values:    values,
		Types:     typs,
		QueryType: typedef.SelectStatementType,
	}
}

func genClusteringRangeQuery(s *testschema.Schema, t *testschema.Table, g *Generator, r *rand.Rand, p *typedef.PartitionRangeConfig) *typedef.Stmt {
	t.RLock()
	defer t.RUnlock()
	vs := g.GetOld()
	if vs == nil {
		return nil
	}

	var (
		allTypes []typedef.Type
		mvCol    testschema.ColumnDef
		mvValues []interface{}
	)
	tableName := t.Name
	partitionKeys := t.PartitionKeys
	clusteringKeys := t.ClusteringKeys
	if len(t.MaterializedViews) > 0 && r.Int()%2 == 0 {
		view := r.Intn(len(t.MaterializedViews))
		tableName = t.MaterializedViews[view].Name
		partitionKeys = t.MaterializedViews[view].PartitionKeys
		clusteringKeys = t.MaterializedViews[view].ClusteringKeys
		mvCol = t.MaterializedViews[view].NonPrimaryKey
	}
	builder := qb.Select(s.Keyspace.Name + "." + tableName)
	values := vs.Value.Copy()
	for _, pk := range partitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
		allTypes = append(allTypes, pk.Type)
	}
	if (testschema.ColumnDef{}) != mvCol {
		mvValues = appendValue(mvCol.Type, r, p, mvValues)
		values = append(mvValues, values...)
	}
	if len(clusteringKeys) > 0 {
		maxClusteringRels := r.Intn(len(clusteringKeys))
		for i := 0; i < maxClusteringRels; i++ {
			builder = builder.Where(qb.Eq(clusteringKeys[i].Name))
			values = appendValue(clusteringKeys[i].Type, r, p, values)
			allTypes = append(allTypes, clusteringKeys[i].Type)
		}
		builder = builder.Where(qb.Gt(clusteringKeys[maxClusteringRels].Name)).Where(qb.Lt(clusteringKeys[maxClusteringRels].Name))
		values = appendValue(t.ClusteringKeys[maxClusteringRels].Type, r, p, values)
		values = appendValue(t.ClusteringKeys[maxClusteringRels].Type, r, p, values)
		allTypes = append(allTypes, clusteringKeys[maxClusteringRels].Type, clusteringKeys[maxClusteringRels].Type)
	}
	return &typedef.Stmt{
		Query:     builder,
		Values:    values,
		Types:     allTypes,
		QueryType: typedef.SelectRangeStatementType,
	}
}

func genMultiplePartitionClusteringRangeQuery(s *testschema.Schema, t *testschema.Table, g *Generator, r *rand.Rand, p *typedef.PartitionRangeConfig) *typedef.Stmt {
	t.RLock()
	defer t.RUnlock()

	var (
		values []interface{}
		typs   []typedef.Type
	)
	tableName := t.Name
	partitionKeys := t.PartitionKeys
	clusteringKeys := t.ClusteringKeys
	if len(t.MaterializedViews) > 0 && r.Int()%2 == 0 {
		view := r.Intn(len(t.MaterializedViews))
		tableName = t.MaterializedViews[view].Name
		partitionKeys = t.MaterializedViews[view].PartitionKeys
		clusteringKeys = t.MaterializedViews[view].ClusteringKeys
	}
	numQueryPKs := r.Intn(len(partitionKeys))
	if numQueryPKs == 0 {
		numQueryPKs = 1
	}
	multiplier := int(math.Pow(float64(numQueryPKs), float64(len(partitionKeys))))
	if multiplier > 100 {
		numQueryPKs = 1
	}
	builder := qb.Select(s.Keyspace.Name + "." + tableName)
	for i, pk := range partitionKeys {
		builder = builder.Where(qb.InTuple(pk.Name, numQueryPKs))
		for j := 0; j < numQueryPKs; j++ {
			vs := g.GetOld()
			if vs == nil {
				return nil
			}
			numMVKeys := len(partitionKeys) - len(vs.Value)
			if i < numMVKeys {
				values = appendValue(pk.Type, r, p, values)
				typs = append(typs, pk.Type)
			} else {
				values = append(values, vs.Value[i-numMVKeys])
				typs = append(typs, pk.Type)
			}
		}
	}
	if len(clusteringKeys) > 0 {
		maxClusteringRels := r.Intn(len(clusteringKeys))
		for i := 0; i < maxClusteringRels; i++ {
			builder = builder.Where(qb.Eq(clusteringKeys[i].Name))
			values = appendValue(clusteringKeys[i].Type, r, p, values)
			typs = append(typs, clusteringKeys[i].Type)
		}
		builder = builder.Where(qb.Gt(clusteringKeys[maxClusteringRels].Name)).Where(qb.Lt(clusteringKeys[maxClusteringRels].Name))
		values = appendValue(clusteringKeys[maxClusteringRels].Type, r, p, values)
		values = appendValue(clusteringKeys[maxClusteringRels].Type, r, p, values)
		typs = append(typs, clusteringKeys[maxClusteringRels].Type, clusteringKeys[maxClusteringRels].Type)
	}
	return &typedef.Stmt{
		Query:     builder,
		Values:    values,
		Types:     typs,
		QueryType: typedef.SelectRangeStatementType,
	}
}

func genSingleIndexQuery(s *testschema.Schema, t *testschema.Table, g *Generator, r *rand.Rand, p *typedef.PartitionRangeConfig) *typedef.Stmt {
	t.RLock()
	defer t.RUnlock()

	var (
		values []interface{}
		typs   []typedef.Type
	)

	if len(t.Indexes) == 0 {
		return nil
	}

	pkNum := r.Intn(len(t.Indexes))
	if pkNum == 0 {
		pkNum = 1
	}
	indexes := t.Indexes[:pkNum]
	builder := qb.Select(s.Keyspace.Name + "." + t.Name)
	builder.AllowFiltering()
	for _, idx := range indexes {
		builder = builder.Where(qb.Eq(idx.Column))
		values = appendValue(t.Columns[idx.ColumnIdx].Type, r, p, values)
		typs = append(typs, t.Columns[idx.ColumnIdx].Type)
	}

	return &typedef.Stmt{
		Query:     builder,
		Values:    values,
		Types:     typs,
		QueryType: typedef.SelectByIndexStatementType,
	}
}

func genInsertOrUpdateStmt(
	s *testschema.Schema,
	t *testschema.Table,
	valuesWithToken *typedef.ValueWithToken,
	r *rand.Rand,
	p *typedef.PartitionRangeConfig,
	useLWT bool,
) (*typedef.Stmt, error) {
	if t.IsCounterTable() {
		return genUpdateStmt(s, t, valuesWithToken, r, p)
	}
	return genInsertStmt(s, t, valuesWithToken, r, p, useLWT)
}

func genUpdateStmt(s *testschema.Schema, t *testschema.Table, valuesWithToken *typedef.ValueWithToken, r *rand.Rand, p *typedef.PartitionRangeConfig) (*typedef.Stmt, error) {
	var typs []typedef.Type
	builder := qb.Update(s.Keyspace.Name + "." + t.Name)
	for _, pk := range t.PartitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
		typs = append(typs, pk.Type)
	}
	values := valuesWithToken.Value.Copy()
	for _, ck := range t.ClusteringKeys {
		builder = builder.Where(qb.Eq(ck.Name))
		values = appendValue(ck.Type, r, p, values)
		typs = append(typs, ck.Type)
	}
	var (
		colValues typedef.Values
		colTyps   []typedef.Type
	)
	for _, cdef := range t.Columns {
		switch t := cdef.Type.(type) {
		case *coltypes.TupleType:
			builder = builder.SetTuple(cdef.Name, len(t.Types))
		case *coltypes.CounterType:
			builder = builder.SetLit(cdef.Name, cdef.Name+"+1")
			continue
		default:
			builder = builder.Set(cdef.Name)
		}
		colValues = appendValue(cdef.Type, r, p, colValues)
		colTyps = append(colTyps, cdef.Type)
	}
	return &typedef.Stmt{
		ValuesWithToken: valuesWithToken,
		Query:           builder,
		Values:          append(colValues, values...),
		Types:           append(colTyps, typs...),
		QueryType:       typedef.Updatetatement,
	}, nil
}

func genInsertStmt(
	s *testschema.Schema,
	t *testschema.Table,
	valuesWithToken *typedef.ValueWithToken,
	r *rand.Rand,
	p *typedef.PartitionRangeConfig,
	useLWT bool,
) (*typedef.Stmt, error) {
	var typs []typedef.Type
	builder := qb.Insert(s.Keyspace.Name + "." + t.Name)
	for _, pk := range t.PartitionKeys {
		builder = builder.Columns(pk.Name)
		typs = append(typs, pk.Type)
	}
	values := valuesWithToken.Value.Copy()
	for _, ck := range t.ClusteringKeys {
		builder = builder.Columns(ck.Name)
		values = appendValue(ck.Type, r, p, values)
		typs = append(typs, ck.Type)
	}
	for _, cdef := range t.Columns {
		switch t := cdef.Type.(type) {
		case *coltypes.TupleType:
			builder = builder.TupleColumn(cdef.Name, len(t.Types))
		default:
			builder = builder.Columns(cdef.Name)
		}
		values = appendValue(cdef.Type, r, p, values)
		typs = append(typs, cdef.Type)
	}
	if useLWT {
		builder = builder.Unique()
	}

	return &typedef.Stmt{
		ValuesWithToken: valuesWithToken,
		Query:           builder,
		Values:          values,
		Types:           typs,
		QueryType:       typedef.InsertStatement,
	}, nil
}

func genInsertJSONStmt(
	s *testschema.Schema,
	table *testschema.Table,
	valuesWithToken *typedef.ValueWithToken,
	r *rand.Rand,
	p *typedef.PartitionRangeConfig,
) (*typedef.Stmt, error) {
	var v string
	var ok bool
	if table.IsCounterTable() {
		return nil, nil
	}
	vs := valuesWithToken.Value.Copy()
	values := make(map[string]interface{})
	for i, pk := range table.PartitionKeys {
		switch t := pk.Type.(type) {
		case coltypes.SimpleType:
			if t != coltypes.TYPE_BLOB {
				values[pk.Name] = vs[i]
				continue
			}
			v, ok = vs[i].(string)
			if ok {
				values[pk.Name] = "0x" + v
			}
		case *coltypes.TupleType:
			tupVals := make([]interface{}, len(t.Types))
			for j := 0; j < len(t.Types); j++ {
				if t.Types[j] == coltypes.TYPE_BLOB {
					v, ok = vs[i+j].(string)
					if ok {
						v = "0x" + v
					}
					vs[i+j] = v
				}
				tupVals[i] = vs[i+j]
				i++
			}
			values[pk.Name] = tupVals
		default:
			panic(fmt.Sprintf("unknown type: %s", t.Name()))
		}
	}
	values = table.ClusteringKeys.ToJSONMap(values, r, p)
	values = table.Columns.ToJSONMap(values, r, p)

	jsonString, err := json.Marshal(values)
	if err != nil {
		return nil, err
	}

	builder := qb.Insert(s.Keyspace.Name + "." + table.Name).Json()
	return &typedef.Stmt{
		ValuesWithToken: valuesWithToken,
		Query:           builder,
		Values:          []interface{}{string(jsonString)},
		Types:           []typedef.Type{coltypes.TYPE_TEXT},
		QueryType:       typedef.InsertStatement,
	}, nil
}

func genDeleteRows(s *testschema.Schema, t *testschema.Table, valuesWithToken *typedef.ValueWithToken, r *rand.Rand, p *typedef.PartitionRangeConfig) (*typedef.Stmt, error) {
	var typs []typedef.Type
	builder := qb.Delete(s.Keyspace.Name + "." + t.Name)
	for _, pk := range t.PartitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
		typs = append(typs, pk.Type)
	}

	values := valuesWithToken.Value.Copy()
	if len(t.ClusteringKeys) > 0 {
		ck := t.ClusteringKeys[0]
		builder = builder.Where(qb.GtOrEq(ck.Name)).Where(qb.LtOrEq(ck.Name))
		values = appendValue(ck.Type, r, p, values)
		values = appendValue(ck.Type, r, p, values)
		typs = append(typs, ck.Type, ck.Type)
	}
	return &typedef.Stmt{
		ValuesWithToken: valuesWithToken,
		Query:           builder,
		Values:          values,
		Types:           typs,
		QueryType:       typedef.DeleteStatementType,
	}, nil
}

func GenDDLStmt(s *testschema.Schema, t *testschema.Table, r *rand.Rand, p *typedef.PartitionRangeConfig, sc *typedef.SchemaConfig) (*typedef.Stmts, error) {
	switch n := r.Intn(3); n {
	// case 0: // Alter column not supported in Cassandra from 3.0.11
	//	return t.alterColumn(s.Keyspace.Name)
	case 1:
		return genDropColumnStmt(t, s.Keyspace.Name)
	default:
		return genAddColumnStmt(t, s.Keyspace.Name, sc)
	}
}

func appendValue(columnType typedef.Type, r *rand.Rand, p *typedef.PartitionRangeConfig, values []interface{}) []interface{} {
	return append(values, columnType.GenValue(r, p)...)
}

func genAddColumnStmt(t *testschema.Table, keyspace string, sc *typedef.SchemaConfig) (*typedef.Stmts, error) {
	var stmts []*typedef.Stmt
	column := testschema.ColumnDef{Name: GenColumnName("col", len(t.Columns)+1), Type: GenColumnType(len(t.Columns)+1, sc)}
	if c, ok := column.Type.(*coltypes.UDTType); ok {
		createType := "CREATE TYPE IF NOT EXISTS %s.%s (%s);"
		var typs []string
		for name, typ := range c.Types {
			typs = append(typs, name+" "+typ.CQLDef())
		}
		stmt := fmt.Sprintf(createType, keyspace, c.TypeName, strings.Join(typs, ","))
		stmts = append(stmts, &typedef.Stmt{
			Query: &builders.AlterTableBuilder{
				Stmt: stmt,
			},
		})
	}
	stmt := "ALTER TABLE " + keyspace + "." + t.Name + " ADD " + column.Name + " " + column.Type.CQLDef()
	stmts = append(stmts, &typedef.Stmt{
		Query: &builders.AlterTableBuilder{
			Stmt: stmt,
		},
	})
	return &typedef.Stmts{
		List: stmts,
		PostStmtHook: func() {
			t.Columns = append(t.Columns, &column)
		},
	}, nil
}

//nolint:unused
func alterColumn(t *testschema.Table, keyspace string) ([]*typedef.Stmt, func(), error) {
	var stmts []*typedef.Stmt
	idx := rand.Intn(len(t.Columns))
	column := t.Columns[idx]
	oldType, isSimpleType := column.Type.(coltypes.SimpleType)
	if !isSimpleType {
		return nil, func() {}, errors.Errorf("complex type=%s cannot be altered", column.Name)
	}
	compatTypes := coltypes.CompatibleColumnTypes[oldType]
	if len(compatTypes) == 0 {
		return nil, func() {}, errors.Errorf("simple type=%s has no compatible coltypes so it cannot be altered", column.Name)
	}
	newType := compatTypes.Random()
	newColumn := testschema.ColumnDef{Name: column.Name, Type: newType}
	stmt := "ALTER TABLE " + keyspace + "." + t.Name + " ALTER " + column.Name + " TYPE " + column.Type.CQLDef()
	stmts = append(stmts, &typedef.Stmt{
		Query: &builders.AlterTableBuilder{
			Stmt: stmt,
		},
		QueryType: typedef.AlterColumnStatementType,
	})
	return stmts, func() {
		t.Columns[idx] = &newColumn
	}, nil
}

func genDropColumnStmt(t *testschema.Table, keyspace string) (*typedef.Stmts, error) {
	var stmts []*typedef.Stmt
	idx := rand.Intn(len(t.Columns))
	column := t.Columns[idx]
	stmt := "ALTER TABLE " + keyspace + "." + t.Name + " DROP " + column.Name
	stmts = append(stmts, &typedef.Stmt{
		Query: &builders.AlterTableBuilder{
			Stmt: stmt,
		},
		QueryType: typedef.DropColumnStatementType,
	})
	return &typedef.Stmts{
		List: stmts,
		PostStmtHook: func() {
			t.Columns = append(t.Columns[:idx], t.Columns[idx+1:]...)
		},
	}, nil
}

func GenSchema(sc typedef.SchemaConfig) *testschema.Schema {
	builder := builders.NewSchemaBuilder()
	keyspace := typedef.Keyspace{
		Name:              "ks1",
		Replication:       sc.ReplicationStrategy,
		OracleReplication: sc.OracleReplicationStrategy,
	}
	builder.Keyspace(keyspace)
	numTables := utils.RandInt(1, sc.GetMaxTables())
	for i := 0; i < numTables; i++ {
		table := genTable(sc, fmt.Sprintf("table%d", i+1))
		builder.Table(table)
	}
	return builder.Build()
}

func genTable(sc typedef.SchemaConfig, tableName string) *testschema.Table {
	partitionKeys := make(testschema.Columns, utils.RandInt(sc.GetMinPartitionKeys(), sc.GetMaxPartitionKeys()))
	for i := 0; i < len(partitionKeys); i++ {
		partitionKeys[i] = &testschema.ColumnDef{Name: GenColumnName("pk", i), Type: GenPartitionKeyColumnType()}
	}
	clusteringKeys := make(testschema.Columns, utils.RandInt(sc.GetMinClusteringKeys(), sc.GetMaxClusteringKeys()))
	for i := 0; i < len(clusteringKeys); i++ {
		clusteringKeys[i] = &testschema.ColumnDef{Name: GenColumnName("ck", i), Type: GenPrimaryKeyColumnType()}
	}
	table := testschema.Table{
		Name:           tableName,
		PartitionKeys:  partitionKeys,
		ClusteringKeys: clusteringKeys,
		KnownIssues: map[string]bool{
			typedef.KnownIssuesJSONWithTuples: true,
		},
	}
	for _, option := range sc.TableOptions {
		table.TableOptions = append(table.TableOptions, option.ToCQL())
	}
	if sc.UseCounters {
		table.Columns = testschema.Columns{
			{
				Name: GenColumnName("col", 0),
				Type: &coltypes.CounterType{
					Value: 0,
				},
			},
		}
		return &table
	}
	columns := make(testschema.Columns, utils.RandInt(sc.GetMinColumns(), sc.GetMaxColumns()))
	for i := 0; i < len(columns); i++ {
		columns[i] = &testschema.ColumnDef{Name: GenColumnName("col", i), Type: GenColumnType(len(columns), &sc)}
	}
	var indexes []typedef.IndexDef
	if sc.CQLFeature > typedef.CQL_FEATURE_BASIC && len(columns) > 0 {
		indexes = CreateIndexesForColumn(columns, tableName, utils.RandInt(1, len(columns)))
	}

	var mvs []testschema.MaterializedView
	if sc.CQLFeature > typedef.CQL_FEATURE_BASIC && len(clusteringKeys) > 0 {
		mvs = columns.CreateMaterializedViews(table.Name, partitionKeys, clusteringKeys)
	}

	table.Columns = columns
	table.MaterializedViews = mvs
	table.Indexes = indexes
	return &table
}

func GetCreateKeyspaces(s *testschema.Schema) (string, string) {
	return fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = %s", s.Keyspace.Name, s.Keyspace.Replication.ToCQL()),
		fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = %s", s.Keyspace.Name, s.Keyspace.OracleReplication.ToCQL())
}

func GetCreateSchema(s *testschema.Schema) []string {
	var stmts []string

	for _, t := range s.Tables {
		createTypes := GetCreateTypes(t, s.Keyspace)
		stmts = append(stmts, createTypes...)
		createTable := GetCreateTable(t, s.Keyspace)
		stmts = append(stmts, createTable)
		for _, idef := range t.Indexes {
			stmts = append(stmts, fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s.%s (%s)", idef.Name, s.Keyspace.Name, t.Name, idef.Column))
		}
		for _, mv := range t.MaterializedViews {
			var (
				mvPartitionKeys      []string
				mvPrimaryKeysNotNull []string
			)
			for _, pk := range mv.PartitionKeys {
				mvPartitionKeys = append(mvPartitionKeys, pk.Name)
				mvPrimaryKeysNotNull = append(mvPrimaryKeysNotNull, fmt.Sprintf("%s IS NOT NULL", pk.Name))
			}
			for _, ck := range mv.ClusteringKeys {
				mvPrimaryKeysNotNull = append(mvPrimaryKeysNotNull, fmt.Sprintf("%s IS NOT NULL", ck.Name))
			}
			var createMaterializedView string
			if len(mv.PartitionKeys) == 1 {
				createMaterializedView = "CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s AS SELECT * FROM %s.%s WHERE %s PRIMARY KEY (%s"
			} else {
				createMaterializedView = "CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s AS SELECT * FROM %s.%s WHERE %s PRIMARY KEY ((%s)"
			}
			createMaterializedView = createMaterializedView + ",%s)"
			stmts = append(stmts, fmt.Sprintf(createMaterializedView,
				s.Keyspace.Name, mv.Name, s.Keyspace.Name, t.Name,
				strings.Join(mvPrimaryKeysNotNull, " AND "),
				strings.Join(mvPartitionKeys, ","), strings.Join(t.ClusteringKeys.Names(), ",")))
		}
	}
	return stmts
}

func GetDropSchema(s *testschema.Schema) []string {
	return []string{
		fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", s.Keyspace.Name),
	}
}
