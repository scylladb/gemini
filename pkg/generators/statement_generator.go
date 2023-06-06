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

func GenCheckStmt(
	s *testschema.Schema,
	table *testschema.Table,
	g *Generator,
	rnd *rand.Rand,
	p *typedef.PartitionRangeConfig,
) *typedef.Stmt {
	n := 0
	mvNum := -1
	if len(table.Indexes) > 0 {
		n = rnd.Intn(5)
	} else {
		n = rnd.Intn(4)
	}
	if len(table.MaterializedViews) > 0 && rnd.Int()%2 == 0 {
		mvNum = utils.RandInt2(rnd, 0, len(table.MaterializedViews))
	}

	maxClusteringRels := 0
	numQueryPKs := 0
	switch n {
	case 0:
		return genSinglePartitionQuery(s, table, g, rnd, p, mvNum)
	case 1:
		lenPartitionKeys := len(table.PartitionKeys)
		if mvNum >= 0 {
			lenPartitionKeys = len(table.MaterializedViews[mvNum].PartitionKeys)
		}
		numQueryPKs = utils.RandInt2(rnd, 1, lenPartitionKeys)
		multiplier := int(math.Pow(float64(numQueryPKs), float64(lenPartitionKeys)))
		if multiplier > 100 {
			numQueryPKs = 1
		}
		return genMultiplePartitionQuery(s, table, g, rnd, p, mvNum, numQueryPKs)
	case 2:
		lenClusteringKeys := len(table.ClusteringKeys)
		if mvNum >= 0 {
			lenClusteringKeys = len(table.MaterializedViews[mvNum].ClusteringKeys)
		}
		maxClusteringRels = utils.RandInt2(rnd, 0, lenClusteringKeys)
		return genClusteringRangeQuery(s, table, g, rnd, p, mvNum, maxClusteringRels)
	case 3:
		lenPartitionKeys := len(table.PartitionKeys)
		lenClusteringKeys := len(table.ClusteringKeys)
		if mvNum >= 0 {
			lenPartitionKeys = len(table.MaterializedViews[mvNum].PartitionKeys)
			lenClusteringKeys = len(table.MaterializedViews[mvNum].ClusteringKeys)
		}
		numQueryPKs = utils.RandInt2(rnd, 1, lenPartitionKeys)
		multiplier := int(math.Pow(float64(numQueryPKs), float64(lenPartitionKeys)))
		if multiplier > 100 {
			numQueryPKs = 1
		}
		maxClusteringRels = utils.RandInt2(rnd, 0, lenClusteringKeys)
		return genMultiplePartitionClusteringRangeQuery(s, table, g, rnd, p, mvNum, numQueryPKs, maxClusteringRels)
	case 4:
		// Reducing the probability to hit these since they often take a long time to run
		switch rnd.Intn(5) {
		case 0:
			idxCount := utils.RandInt2(rnd, 1, len(table.Indexes))
			return genSingleIndexQuery(s, table, g, rnd, p, idxCount)
		default:
			return genSinglePartitionQuery(s, table, g, rnd, p, mvNum)
		}
	}
	return nil
}

func genSinglePartitionQuery(
	s *testschema.Schema,
	t *testschema.Table,
	g GeneratorInterface,
	r *rand.Rand,
	p *typedef.PartitionRangeConfig,
	mvNum int,
) *typedef.Stmt {
	t.RLock()
	defer t.RUnlock()
	valuesWithToken := g.GetOld()
	if valuesWithToken == nil {
		return nil
	}
	var mvValues []interface{}

	tableName := t.Name
	partitionKeys := t.PartitionKeys
	values := valuesWithToken.Value.Copy()
	if mvNum >= 0 {
		tableName = t.MaterializedViews[mvNum].Name
		partitionKeys = t.MaterializedViews[mvNum].PartitionKeys
		if t.MaterializedViews[mvNum].NonPrimaryKey != (testschema.ColumnDef{}) {
			mvValues = append(mvValues, t.MaterializedViews[mvNum].NonPrimaryKey.Type.GenValue(r, p)...)
			values = append(mvValues, values...)
		}

	}
	builder := qb.Select(s.Keyspace.Name + "." + tableName)
	typs := make([]typedef.Type, 0, 10)
	for _, pk := range partitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
		typs = append(typs, pk.Type)
	}

	return &typedef.Stmt{
		StmtCache: &typedef.StmtCache{
			Query:     builder,
			Types:     typs,
			QueryType: typedef.SelectStatementType,
		},
		ValuesWithToken: valuesWithToken,
		Values:          values,
	}
}

func genMultiplePartitionQuery(
	s *testschema.Schema,
	t *testschema.Table,
	g GeneratorInterface,
	r *rand.Rand,
	p *typedef.PartitionRangeConfig,
	mvNum, numQueryPKs int,
) *typedef.Stmt {
	t.RLock()
	defer t.RUnlock()

	var (
		values []interface{}
		typs   []typedef.Type
	)
	tableName := t.Name
	partitionKeys := t.PartitionKeys
	if mvNum >= 0 {
		tableName = t.MaterializedViews[mvNum].Name
		partitionKeys = t.MaterializedViews[mvNum].PartitionKeys
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
				values = append(values, pk.Type.GenValue(r, p)...)
				typs = append(typs, pk.Type)
			} else {
				values = append(values, vs.Value[i-numMVKeys])
				typs = append(typs, pk.Type)
			}
		}
	}
	return &typedef.Stmt{
		StmtCache: &typedef.StmtCache{
			Query:     builder,
			Types:     typs,
			QueryType: typedef.SelectStatementType,
		},
		Values: values,
	}
}

func genClusteringRangeQuery(
	s *testschema.Schema,
	t *testschema.Table,
	g GeneratorInterface,
	r *rand.Rand,
	p *typedef.PartitionRangeConfig,
	mvNum, maxClusteringRels int,
) *typedef.Stmt {
	t.RLock()
	defer t.RUnlock()
	vs := g.GetOld()
	if vs == nil {
		return nil
	}

	var (
		allTypes []typedef.Type
		mvValues []interface{}
	)
	tableName := t.Name
	partitionKeys := t.PartitionKeys
	clusteringKeys := t.ClusteringKeys
	values := vs.Value.Copy()

	if mvNum >= 0 {
		tableName = t.MaterializedViews[mvNum].Name
		partitionKeys = t.MaterializedViews[mvNum].PartitionKeys
		clusteringKeys = t.MaterializedViews[mvNum].ClusteringKeys
		if t.MaterializedViews[mvNum].NonPrimaryKey != (testschema.ColumnDef{}) {
			mvValues = append(mvValues, t.MaterializedViews[mvNum].NonPrimaryKey.Type.GenValue(r, p)...)
			values = append(mvValues, values...)
		}
	}
	builder := qb.Select(s.Keyspace.Name + "." + tableName)

	for _, pk := range partitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
		allTypes = append(allTypes, pk.Type)
	}
	if len(clusteringKeys) > 0 {
		for i := 0; i < maxClusteringRels; i++ {
			builder = builder.Where(qb.Eq(clusteringKeys[i].Name))
			values = append(values, clusteringKeys[i].Type.GenValue(r, p)...)
			allTypes = append(allTypes, clusteringKeys[i].Type)
		}
		builder = builder.Where(qb.Gt(clusteringKeys[maxClusteringRels].Name)).Where(qb.Lt(clusteringKeys[maxClusteringRels].Name))
		values = append(values, t.ClusteringKeys[maxClusteringRels].Type.GenValue(r, p)...)
		values = append(values, t.ClusteringKeys[maxClusteringRels].Type.GenValue(r, p)...)
		allTypes = append(allTypes, clusteringKeys[maxClusteringRels].Type, clusteringKeys[maxClusteringRels].Type)
	}
	return &typedef.Stmt{
		StmtCache: &typedef.StmtCache{
			Query:     builder,
			QueryType: typedef.SelectRangeStatementType,
			Types:     allTypes,
		},
		Values: values,
	}
}

func genMultiplePartitionClusteringRangeQuery(
	s *testschema.Schema,
	t *testschema.Table,
	g GeneratorInterface,
	r *rand.Rand,
	p *typedef.PartitionRangeConfig,
	mvNum, numQueryPKs, maxClusteringRels int,
) *typedef.Stmt {
	t.RLock()
	defer t.RUnlock()

	var (
		values []interface{}
		typs   []typedef.Type
	)
	tableName := t.Name
	partitionKeys := t.PartitionKeys
	clusteringKeys := t.ClusteringKeys
	if mvNum >= 0 {
		tableName = t.MaterializedViews[mvNum].Name
		partitionKeys = t.MaterializedViews[mvNum].PartitionKeys
		clusteringKeys = t.MaterializedViews[mvNum].ClusteringKeys
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
		for i := 0; i < maxClusteringRels; i++ {
			builder = builder.Where(qb.Eq(clusteringKeys[i].Name))
			values = append(values, clusteringKeys[i].Type.GenValue(r, p)...)
			typs = append(typs, clusteringKeys[i].Type)
		}
		builder = builder.Where(qb.Gt(clusteringKeys[maxClusteringRels].Name)).Where(qb.Lt(clusteringKeys[maxClusteringRels].Name))
		values = append(values, clusteringKeys[maxClusteringRels].Type.GenValue(r, p)...)
		values = append(values, clusteringKeys[maxClusteringRels].Type.GenValue(r, p)...)
		typs = append(typs, clusteringKeys[maxClusteringRels].Type, clusteringKeys[maxClusteringRels].Type)
	}
	return &typedef.Stmt{
		StmtCache: &typedef.StmtCache{
			Query:     builder,
			Types:     typs,
			QueryType: typedef.SelectRangeStatementType,
		},
		Values: values,
	}
}

func genSingleIndexQuery(
	s *testschema.Schema,
	t *testschema.Table,
	g GeneratorInterface,
	r *rand.Rand,
	p *typedef.PartitionRangeConfig,
	idxCount int,
) *typedef.Stmt {
	t.RLock()
	defer t.RUnlock()

	var (
		values []interface{}
		typs   []typedef.Type
	)

	builder := qb.Select(s.Keyspace.Name + "." + t.Name)
	builder.AllowFiltering()
	for i := 0; i < idxCount; i++ {
		builder = builder.Where(qb.Eq(t.Indexes[i].Column))
		values = append(values, t.Columns[t.Indexes[i].ColumnIdx].Type.GenValue(r, p)...)
		typs = append(typs, t.Columns[t.Indexes[i].ColumnIdx].Type)
	}

	return &typedef.Stmt{
		StmtCache: &typedef.StmtCache{
			Query:     builder,
			Types:     typs,
			QueryType: typedef.SelectByIndexStatementType,
		},
		Values: values,
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
	stmtCache := t.GetQueryCache(typedef.CacheUpdate)
	nonCounters := t.Columns.NonCounters()
	values := make(typedef.Values, 0, t.PartitionKeys.LenValues()+t.ClusteringKeys.LenValues()+nonCounters.LenValues())
	for _, cdef := range nonCounters {
		values = appendValue(cdef.Type, r, p, values)
	}
	values = values.CopyFrom(valuesWithToken.Value)
	for _, ck := range t.ClusteringKeys {
		values = appendValue(ck.Type, r, p, values)
	}
	return &typedef.Stmt{
		StmtCache:       stmtCache,
		ValuesWithToken: valuesWithToken,
		Values:          values,
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
	values := make(typedef.Values, 0, t.PartitionKeys.LenValues()+t.ClusteringKeys.LenValues()+t.Columns.LenValues())
	values = values.CopyFrom(valuesWithToken.Value)
	for _, ck := range t.ClusteringKeys {
		values = append(values, ck.Type.GenValue(r, p)...)
	}
	for _, col := range t.Columns {
		values = append(values, col.Type.GenValue(r, p)...)
	}
	cacheType := typedef.CacheInsert
	if useLWT {
		cacheType = typedef.CacheInsertIfNotExists
	}
	stmtCache := t.GetQueryCache(cacheType)
	return &typedef.Stmt{
		StmtCache:       stmtCache,
		ValuesWithToken: valuesWithToken,
		Values:          values,
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
		StmtCache: &typedef.StmtCache{
			Query:     builder,
			Types:     []typedef.Type{coltypes.TYPE_TEXT},
			QueryType: typedef.InsertStatement,
		},
		ValuesWithToken: valuesWithToken,
		Values:          []interface{}{string(jsonString)},
	}, nil
}

func genDeleteRows(s *testschema.Schema, t *testschema.Table, valuesWithToken *typedef.ValueWithToken, r *rand.Rand, p *typedef.PartitionRangeConfig) (*typedef.Stmt, error) {
	stmtCache := t.GetQueryCache(typedef.CacheDelete)
	values := valuesWithToken.Value.Copy()
	if len(t.ClusteringKeys) > 0 {
		ck := t.ClusteringKeys[0]
		values = appendValue(ck.Type, r, p, values)
		values = appendValue(ck.Type, r, p, values)
	}
	return &typedef.Stmt{
		StmtCache:       stmtCache,
		ValuesWithToken: valuesWithToken,
		Values:          values,
	}, nil
}

func GenDDLStmt(s *testschema.Schema, t *testschema.Table, r *rand.Rand, p *typedef.PartitionRangeConfig, sc *typedef.SchemaConfig) (*typedef.Stmts, error) {
	maxVariant := 1
	if len(t.Columns) > 0 {
		maxVariant = 2
	}
	switch n := r.Intn(maxVariant + 2); n {
	// case 0: // Alter column not supported in Cassandra from 3.0.11
	//	return t.alterColumn(s.Keyspace.Name)
	case 2:
		colNum := r.Intn(len(t.Columns))
		return genDropColumnStmt(t, s.Keyspace.Name, colNum)
	default:
		column := testschema.ColumnDef{Name: GenColumnName("col", len(t.Columns)+1), Type: GenColumnType(len(t.Columns)+1, sc)}
		return genAddColumnStmt(t, s.Keyspace.Name, &column)
	}
}

func appendValue(columnType typedef.Type, r *rand.Rand, p *typedef.PartitionRangeConfig, values []interface{}) []interface{} {
	return append(values, columnType.GenValue(r, p)...)
}

func genAddColumnStmt(t *testschema.Table, keyspace string, column *testschema.ColumnDef) (*typedef.Stmts, error) {
	var stmts []*typedef.Stmt
	if c, ok := column.Type.(*coltypes.UDTType); ok {
		createType := "CREATE TYPE IF NOT EXISTS %s.%s (%s);"
		var typs []string
		for name, typ := range c.Types {
			typs = append(typs, name+" "+typ.CQLDef())
		}
		stmt := fmt.Sprintf(createType, keyspace, c.TypeName, strings.Join(typs, ","))
		stmts = append(stmts, &typedef.Stmt{
			StmtCache: &typedef.StmtCache{
				Query: &builders.AlterTableBuilder{
					Stmt: stmt,
				},
			},
		})
	}
	stmt := "ALTER TABLE " + keyspace + "." + t.Name + " ADD " + column.Name + " " + column.Type.CQLDef()
	stmts = append(stmts, &typedef.Stmt{
		StmtCache: &typedef.StmtCache{
			Query: &builders.AlterTableBuilder{
				Stmt: stmt,
			},
		},
	})
	return &typedef.Stmts{
		List: stmts,
		PostStmtHook: func() {
			t.Columns = append(t.Columns, column)
			t.ResetQueryCache()
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
		StmtCache: &typedef.StmtCache{
			Query: &builders.AlterTableBuilder{
				Stmt: stmt,
			},
			QueryType: typedef.AlterColumnStatementType,
		},
	})
	return stmts, func() {
		t.Columns[idx] = &newColumn
		t.ResetQueryCache()
	}, nil
}

func genDropColumnStmt(t *testschema.Table, keyspace string, colNum int) (*typedef.Stmts, error) {
	var stmts []*typedef.Stmt

	column := t.Columns[colNum]
	stmt := "ALTER TABLE " + keyspace + "." + t.Name + " DROP " + column.Name
	stmts = append(stmts, &typedef.Stmt{
		StmtCache: &typedef.StmtCache{
			Query: &builders.AlterTableBuilder{
				Stmt: stmt,
			},
			QueryType: typedef.DropColumnStatementType,
		},
	})
	return &typedef.Stmts{
		List: stmts,
		PostStmtHook: func() {
			t.Columns = t.Columns.Remove(colNum)
			t.ResetQueryCache()
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
