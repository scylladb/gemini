package gemini

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/qb"
	"golang.org/x/exp/rand"
)

type CQLFeature int

const (
	CQL_FEATURE_BASIC CQLFeature = iota + 1
	CQL_FEATURE_NORMAL
	CQL_FEATURE_ALL

	KnownIssuesJsonWithTuples = "https://github.com/scylladb/scylla/issues/3708"
)

type SchemaConfig struct {
	CompactionStrategy *CompactionStrategy
	MaxPartitionKeys   int
	MaxClusteringKeys  int
	MaxColumns         int
	MaxUDTParts        int
	MaxTupleParts      int
	MaxBlobLength      int
	MaxStringLength    int
	MinBlobLength      int
	MinStringLength    int
	CQLFeature         CQLFeature
}

type Keyspace struct {
	Name string `json:"name"`
}

type ColumnDef struct {
	Name string `json:"name"`
	Type Type   `json:"type"`
}

type Type interface {
	Name() string
	CQLDef() string
	CQLHolder() string
	CQLPretty(string, []interface{}) (string, int)
	GenValue(*rand.Rand, PartitionRangeConfig) []interface{}
	Indexable() bool
	CQLType() gocql.TypeInfo
}

type IndexDef struct {
	Name   string `json:"name"`
	Column string `json:"column"`
}

type Columns []ColumnDef

func (c Columns) Names() []string {
	names := make([]string, 0, len(c))
	for _, col := range c {
		names = append(names, col.Name)
	}
	return names
}

func (cs Columns) ToJSONMap(values map[string]interface{}, r *rand.Rand, p PartitionRangeConfig) map[string]interface{} {
	for _, k := range cs {
		switch t := k.Type.(type) {
		case SimpleType:
			if t != TYPE_BLOB {
				values[k.Name] = t.GenValue(r, p)[0]
				continue
			}
			v, ok := t.GenValue(r, p)[0].(string)
			if ok {
				values[k.Name] = "0x" + v
			}
		case TupleType:
			vv := t.GenValue(r, p)
			for i, val := range vv {
				if t.Types[i] == TYPE_BLOB {
					v, ok := val.(string)
					if ok {
						v = "0x" + v
					}
					vv[i] = v
				}
			}
			values[k.Name] = vv
		default:
			panic(fmt.Sprintf("unknown type: %s", t.Name()))
		}
	}
	return values
}

type Table struct {
	Name               string              `json:"name"`
	PartitionKeys      Columns             `json:"partition_keys"`
	ClusteringKeys     Columns             `json:"clustering_keys"`
	Columns            Columns             `json:"columns"`
	CompactionStrategy *CompactionStrategy `json:"compaction_strategy"`
	Indexes            []IndexDef          `json:"indexes"`
	MaterializedViews  []MaterializedView  `json:"materialized_views"`
	KnownIssues        map[string]bool     `json:"known_issues"`

	// mu protects the table during schema changes
	mu sync.RWMutex
}

func (t *Table) Lock() {
	t.mu.Lock()
}

func (t *Table) Unlock() {
	t.mu.Unlock()
}

func (t *Table) GetCreateTable(ks Keyspace) string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var (
		partitionKeys  []string
		clusteringKeys []string
		columns        []string
	)
	for _, pk := range t.PartitionKeys {
		partitionKeys = append(partitionKeys, pk.Name)
		columns = append(columns, fmt.Sprintf("%s %s", pk.Name, pk.Type.CQLDef()))
	}
	for _, ck := range t.ClusteringKeys {
		clusteringKeys = append(clusteringKeys, ck.Name)
		columns = append(columns, fmt.Sprintf("%s %s", ck.Name, ck.Type.CQLDef()))
	}
	for _, cdef := range t.Columns {
		columns = append(columns, fmt.Sprintf("%s %s", cdef.Name, cdef.Type.CQLDef()))
	}

	var stmt string
	if len(clusteringKeys) == 0 {
		stmt = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY ((%s)))", ks.Name, t.Name, strings.Join(columns, ","), strings.Join(partitionKeys, ","))
	} else {
		stmt = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY ((%s), %s))", ks.Name, t.Name, strings.Join(columns, ","),
			strings.Join(partitionKeys, ","), strings.Join(clusteringKeys, ","))
	}
	if t.CompactionStrategy != nil {
		stmt = stmt + " WITH compaction = " + t.CompactionStrategy.ToCQL() + ";"
	}
	return stmt
}

func (t *Table) GetCreateTypes(keyspace Keyspace) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var stmts []string
	for _, column := range t.Columns {
		switch c := column.Type.(type) {
		case UDTType:
			createType := "CREATE TYPE %s.%s (%s)"
			var typs []string
			for name, typ := range c.Types {
				typs = append(typs, name+" "+typ.CQLDef())
			}
			stmts = append(stmts, fmt.Sprintf(createType, keyspace.Name, c.TypeName, strings.Join(typs, ",")))
		}
	}
	return stmts
}

type AlterTableBuilder struct {
	stmt string
}

func (atb *AlterTableBuilder) ToCql() (string, []string) {
	return atb.stmt, nil
}

func (t *Table) addColumn(keyspace string, sc *SchemaConfig) ([]*Stmt, func(), error) {
	var stmts []*Stmt
	column := ColumnDef{Name: genColumnName("col", len(t.Columns)+1), Type: genColumnType(len(t.Columns)+1, sc)}
	if c, ok := column.Type.(UDTType); ok {
		createType := "CREATE TYPE %s.%s (%s);"
		var typs []string
		for name, typ := range c.Types {
			typs = append(typs, name+" "+typ.CQLDef())
		}
		stmt := fmt.Sprintf(createType, keyspace, c.TypeName, strings.Join(typs, ","))
		stmts = append(stmts, &Stmt{
			Query: &AlterTableBuilder{
				stmt: stmt,
			},
			Values: func() []interface{} {
				return nil
			},
		})
	}
	stmt := "ALTER TABLE " + keyspace + "." + t.Name + " ADD " + column.Name + " " + column.Type.CQLDef()
	stmts = append(stmts, &Stmt{
		Query: &AlterTableBuilder{
			stmt: stmt,
		},
		Values: func() []interface{} {
			return nil
		},
	})
	return stmts, func() {
		t.Columns = append(t.Columns, column)
	}, nil
}

func (t *Table) alterColumn(keyspace string) ([]*Stmt, func(), error) {
	var stmts []*Stmt
	idx := rand.Intn(len(t.Columns))
	column := t.Columns[idx]
	oldType, isSimpleType := column.Type.(SimpleType)
	if !isSimpleType {
		return nil, func() {}, errors.Errorf("complex type=%s cannot be altered", column.Name)
	}
	if compatTypes, ok := compatibleColumnTypes[oldType]; ok {
		newType := compatTypes[rand.Intn(len(compatTypes))]
		newColumn := ColumnDef{Name: column.Name, Type: newType}
		stmt := "ALTER TABLE " + keyspace + "." + t.Name + " ALTER " + column.Name + " TYPE " + column.Type.CQLDef()
		stmts = append(stmts, &Stmt{
			Query: &AlterTableBuilder{
				stmt: stmt,
			},
			Values: func() []interface{} {
				return nil
			},
		})
		fmt.Println(stmt)
		return stmts, func() {
			t.Columns[idx] = newColumn
		}, nil
	}
	return nil, func() {}, errors.Errorf("simple type=%s has no compatible types so it cannot be altered", column.Name)
}

func (t *Table) dropColumn(keyspace string) ([]*Stmt, func(), error) {
	var stmts []*Stmt
	idx := rand.Intn(len(t.Columns))
	column := t.Columns[idx]
	stmt := "ALTER TABLE " + keyspace + "." + t.Name + " DROP " + column.Name
	stmts = append(stmts, &Stmt{
		Query: &AlterTableBuilder{
			stmt: stmt,
		},
		Values: func() []interface{} {
			return nil
		},
	})
	return stmts, func() {
		t.Columns = append(t.Columns[:idx], t.Columns[idx+1:]...)
	}, nil
}

type MaterializedView struct {
	Name           string  `json:"name"`
	PartitionKeys  Columns `json:"partition_keys"`
	ClusteringKeys Columns `json:"clustering_keys"`
}

type Stmt struct {
	Query  qb.Builder
	Values func() []interface{}
	Types  []Type
}

func (s *Stmt) PrettyCQL() string {
	var replaced int
	query, _ := s.Query.ToCql()
	if len(s.Values()) == 0 {
		return query
	}
	values := s.Values()
	for _, typ := range s.Types {
		query, replaced = typ.CQLPretty(query, values)
		if len(values) >= replaced {
			values = values[replaced:]
		} else {
			break
		}
	}
	return query
}

type Schema struct {
	Keyspace Keyspace `json:"keyspace"`
	Tables   []*Table `json:"tables"`
}

type PartitionRangeConfig struct {
	MaxBlobLength   int
	MinBlobLength   int
	MaxStringLength int
	MinStringLength int
}

func (s *Schema) GetDropSchema() []string {
	return []string{
		fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", s.Keyspace.Name),
	}
}

func GenSchema(sc *SchemaConfig) *Schema {
	builder := NewSchemaBuilder()
	keyspace := Keyspace{
		Name: "ks1",
	}
	builder.Keyspace(keyspace)
	var partitionKeys []ColumnDef
	numPartitionKeys := rand.Intn(sc.MaxPartitionKeys-1) + 1
	for i := 0; i < numPartitionKeys; i++ {
		partitionKeys = append(partitionKeys, ColumnDef{Name: genColumnName("pk", i), Type: TYPE_INT})
	}
	var clusteringKeys []ColumnDef
	numClusteringKeys := rand.Intn(sc.MaxClusteringKeys)
	for i := 0; i < numClusteringKeys; i++ {
		clusteringKeys = append(clusteringKeys, ColumnDef{Name: genColumnName("ck", i), Type: genPrimaryKeyColumnType()})
	}
	var columns []ColumnDef
	numColumns := rand.Intn(sc.MaxColumns)
	for i := 0; i < numColumns; i++ {
		columns = append(columns, ColumnDef{Name: genColumnName("col", i), Type: genColumnType(numColumns, sc)})
	}
	var indexes []IndexDef
	if sc.CQLFeature > CQL_FEATURE_BASIC {
		indexes = createIndexes(numColumns, columns)
	}

	var mvs []MaterializedView
	if sc.CQLFeature > CQL_FEATURE_BASIC {
		mvs = createMaterializedViews(partitionKeys, clusteringKeys, columns)
	}

	table := Table{
		Name:              "table1",
		PartitionKeys:     partitionKeys,
		ClusteringKeys:    clusteringKeys,
		Columns:           columns,
		MaterializedViews: mvs,
		Indexes:           indexes,
		KnownIssues: map[string]bool{
			KnownIssuesJsonWithTuples: true,
		},
	}
	if sc.CompactionStrategy == nil {
		table.CompactionStrategy = randomCompactionStrategy()
	} else {
		table.CompactionStrategy = &(*sc.CompactionStrategy)
	}

	builder.Table(&table)
	return builder.Build()
}

func createIndexes(numColumns int, columns []ColumnDef) []IndexDef {
	var indexes []IndexDef
	if numColumns > 0 {
		numIndexes := rand.Intn(numColumns)
		for i := 0; i < numIndexes; i++ {
			if columns[i].Type.Indexable() {
				indexes = append(indexes, IndexDef{Name: genIndexName("col", i), Column: columns[i].Name})
			}
		}
	}
	return indexes
}

func createMaterializedViews(partitionKeys []ColumnDef, clusteringKeys []ColumnDef, columns []ColumnDef) []MaterializedView {
	validMVColumn := func() (ColumnDef, error) {
		validCols := make([]ColumnDef, 0, len(columns))
		for _, col := range columns {
			valid := false
			for _, pkType := range pkTypes {
				if col.Type.Name() == pkType.Name() {
					valid = true
					break
				}
			}
			if valid {
				validCols = append(validCols, col)
			}
		}
		if len(validCols) == 0 {
			return ColumnDef{}, errors.New("no valid MV columns found")
		}
		return validCols[rand.Intn(len(validCols))], nil
	}
	var mvs []MaterializedView
	numMvs := 1
	for i := 0; i < numMvs; i++ {
		col, err := validMVColumn()
		if err != nil {
			fmt.Printf("unable to generate valid columns for materialized view, error=%s", err)
			continue
		}

		cols := []ColumnDef{
			col,
		}
		mv := MaterializedView{
			Name:           "table1_mv_" + strconv.Itoa(i),
			PartitionKeys:  append(cols, partitionKeys...),
			ClusteringKeys: clusteringKeys,
		}
		mvs = append(mvs, mv)
	}
	return mvs
}

func randomCompactionStrategy() *CompactionStrategy {
	switch rand.Intn(3) {
	case 0:
		return NewLeveledCompactionStrategy()
	case 1:
		return NewTimeWindowCompactionStrategy()
	default:
		return NewSizeTieredCompactionStrategy()
	}
}

func (s *Schema) GetCreateSchema() []string {
	createKeyspace := fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}", s.Keyspace.Name)

	stmts := []string{createKeyspace}

	for _, t := range s.Tables {
		createTypes := t.GetCreateTypes(s.Keyspace)
		stmts = append(stmts, createTypes...)
		createTable := t.GetCreateTable(s.Keyspace)
		stmts = append(stmts, createTable)
		for _, idef := range t.Indexes {
			stmts = append(stmts, fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s)", idef.Name, s.Keyspace.Name, t.Name, idef.Column))
		}
		for _, mv := range t.MaterializedViews {
			var (
				mvPartitionKeys      []string
				mvPrimaryKeysNotNull []string
				mvClusteringKeys     []string
			)
			for _, pk := range mv.PartitionKeys {
				mvPartitionKeys = append(mvPartitionKeys, pk.Name)
				mvPrimaryKeysNotNull = append(mvPrimaryKeysNotNull, fmt.Sprintf("%s IS NOT NULL", pk.Name))
			}
			for _, ck := range mv.ClusteringKeys {
				mvClusteringKeys = append(mvClusteringKeys, ck.Name)
				mvPrimaryKeysNotNull = append(mvPrimaryKeysNotNull, fmt.Sprintf("%s IS NOT NULL", ck.Name))
			}
			var createMaterializedView string
			if len(mv.PartitionKeys) == 1 {
				createMaterializedView = "CREATE MATERIALIZED VIEW %s.%s AS SELECT * FROM %s.%s WHERE %s PRIMARY KEY (%s"
			} else {
				createMaterializedView = "CREATE MATERIALIZED VIEW %s.%s AS SELECT * FROM %s.%s WHERE %s PRIMARY KEY ((%s)"
			}
			if len(mvClusteringKeys) > 0 {
				createMaterializedView = createMaterializedView + ",%s)"
				stmts = append(stmts, fmt.Sprintf(createMaterializedView,
					s.Keyspace.Name, mv.Name, s.Keyspace.Name, t.Name,
					strings.Join(mvPrimaryKeysNotNull, " AND "),
					strings.Join(mvPartitionKeys, ","), strings.Join(t.ClusteringKeys.Names(), ",")))
			} else {
				createMaterializedView = createMaterializedView + ")"
				stmts = append(stmts, fmt.Sprintf(createMaterializedView,
					s.Keyspace.Name, mv.Name, s.Keyspace.Name, t.Name,
					strings.Join(mvPrimaryKeysNotNull, " AND "),
					strings.Join(mvPartitionKeys, ",")))
			}
		}
	}
	return stmts
}

func (s *Schema) GenInsertStmt(t *Table, partitionValues <-chan Value, r *rand.Rand, p PartitionRangeConfig) (*Stmt, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var (
		typs []Type
	)
	builder := qb.Insert(s.Keyspace.Name + "." + t.Name)
	for _, pk := range t.PartitionKeys {
		builder = builder.Columns(pk.Name)
		typs = append(typs, pk.Type)
	}

	values, ok := <-partitionValues
	if !ok {
		return nil, nil
	}

	for _, ck := range t.ClusteringKeys {
		builder = builder.Columns(ck.Name)
		values = appendValue(ck.Type, r, p, values)
		typs = append(typs, ck.Type)
	}
	for _, cdef := range t.Columns {
		switch t := cdef.Type.(type) {
		case TupleType:
			builder = builder.TupleColumn(cdef.Name, len(t.Types))
		default:
			builder = builder.Columns(cdef.Name)
		}
		values = appendValue(cdef.Type, r, p, values)
		typs = append(typs, cdef.Type)
	}
	return &Stmt{
		Query: builder,
		Values: func() []interface{} {
			return values
		},
		Types: typs,
	}, nil
}

func (s *Schema) GenInsertJsonStmt(t *Table, partitionValues <-chan Value, r *rand.Rand, p PartitionRangeConfig) (*Stmt, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	vs, ok := <-partitionValues
	if !ok {
		return nil, nil
	}
	values := make(map[string]interface{})
	for i, pk := range t.PartitionKeys {
		switch t := pk.Type.(type) {
		case SimpleType:
			if t != TYPE_BLOB {
				values[pk.Name] = vs[i]
				continue
			}
			v, ok := vs[i].(string)
			if ok {
				values[pk.Name] = "0x" + v
			}
		case TupleType:
			tupVals := make([]interface{}, len(t.Types))
			for j := 0; j < len(t.Types); j++ {
				if t.Types[j] == TYPE_BLOB {
					v, ok := vs[i+j].(string)
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
	values = t.ClusteringKeys.ToJSONMap(values, r, p)
	values = t.Columns.ToJSONMap(values, r, p)

	jsonString, err := json.Marshal(values)
	if err != nil {
		return nil, err
	}

	builder := qb.Insert(s.Keyspace.Name + "." + t.Name).Json()
	return &Stmt{
		Query: builder,
		Values: func() []interface{} {
			return []interface{}{string(jsonString)}
		},
		Types: []Type{TYPE_TEXT},
	}, nil
}

func (s *Schema) GenDeleteRows(t *Table, partitionValues <-chan Value, r *rand.Rand, p PartitionRangeConfig) (*Stmt, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var (
		values []interface{}
		typs   []Type
	)
	builder := qb.Delete(s.Keyspace.Name + "." + t.Name)
	for _, pk := range t.PartitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
		typs = append(typs, pk.Type)
	}

	values, ok := <-partitionValues
	if !ok {
		return nil, nil
	}

	if len(t.ClusteringKeys) > 0 {
		ck := t.ClusteringKeys[0]
		builder = builder.Where(qb.GtOrEq(ck.Name)).Where(qb.LtOrEq(ck.Name))
		values = appendValue(ck.Type, r, p, values)
		values = appendValue(ck.Type, r, p, values)
		typs = append(typs, ck.Type, ck.Type)
	}
	return &Stmt{
		Query: builder,
		Values: func() []interface{} {
			return values
		},
		Types: typs,
	}, nil
}

func (s *Schema) GenDDLStmt(t *Table, r *rand.Rand, p PartitionRangeConfig, sc *SchemaConfig) ([]*Stmt, func(), error) {
	switch n := r.Intn(3); n {
	//case 0: // Alter column not supported in Cassandra from 3.0.11
	//	return t.alterColumn(s.Keyspace.Name)
	case 1: // Delete column
		return t.dropColumn(s.Keyspace.Name)
	default: // Alter column
		return t.addColumn(s.Keyspace.Name, sc)
	}
}

func (s *Schema) GenMutateStmt(t *Table, partitionValues <-chan Value, r *rand.Rand, p PartitionRangeConfig, deletes bool) (*Stmt, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if !deletes {
		return s.GenInsertStmt(t, partitionValues, r, p)
	}
	switch n := rand.Intn(1000); n {
	case 10, 100:
		return s.GenDeleteRows(t, partitionValues, r, p)
	default:
		switch n := rand.Intn(2); n {
		case 0:
			if t.KnownIssues[KnownIssuesJsonWithTuples] {
				return s.GenInsertStmt(t, partitionValues, r, p)
			}
			return s.GenInsertJsonStmt(t, partitionValues, r, p)
		default:
			return s.GenInsertStmt(t, partitionValues, r, p)
		}
	}
}

func (s *Schema) GenCheckStmt(t *Table, partitionValues <-chan Value, r *rand.Rand, p PartitionRangeConfig) *Stmt {
	var n int
	if len(t.Indexes) > 0 {
		n = r.Intn(5)
	} else {
		n = r.Intn(4)
	}
	switch n {
	case 0:
		return s.genSinglePartitionQuery(t, partitionValues, r, p)
	case 1:
		return s.genMultiplePartitionQuery(t, partitionValues, r, p)
	case 2:
		return s.genClusteringRangeQuery(t, partitionValues, r, p)
	case 3:
		return s.genMultiplePartitionClusteringRangeQuery(t, partitionValues, r, p)
	case 4:
		return s.genSingleIndexQuery(t, partitionValues, r, p)
	}
	return nil
}

func (s *Schema) genSinglePartitionQuery(t *Table, partitionValues <-chan Value, r *rand.Rand, p PartitionRangeConfig) *Stmt {
	t.mu.RLock()
	defer t.mu.RUnlock()

	tableName := t.Name
	partitionKeys := t.PartitionKeys
	// TODO: Support materialized views
	/*
		if len(t.MaterializedViews) > 0 && r.Int()%2 == 0 {
			view := r.Intn(len(t.MaterializedViews))
			tableName = t.MaterializedViews[view].Name
			partitionKeys = t.MaterializedViews[view].PartitionKeys
		}*/
	builder := qb.Select(s.Keyspace.Name + "." + tableName)
	typs := make([]Type, 0, 10)
	for _, pk := range partitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
		typs = append(typs, pk.Type)
	}
	values, ok := <-partitionValues
	if !ok {
		return nil
	}
	return &Stmt{
		Query: builder,
		Values: func() []interface{} {
			return values
		},
		Types: typs,
	}
}

func (s *Schema) genMultiplePartitionQuery(t *Table, partitionValues <-chan Value, r *rand.Rand, p PartitionRangeConfig) *Stmt {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var (
		values []interface{}
		typs   []Type
	)
	tableName := t.Name
	partitionKeys := t.PartitionKeys
	// TODO: Support materialized views
	/*
		if len(t.MaterializedViews) > 0 && r.Int()%2 == 0 {
			view := r.Intn(len(t.MaterializedViews))
			tableName = t.MaterializedViews[view].Name
			partitionKeys = t.MaterializedViews[view].PartitionKeys
		}
	*/
	pkNum := r.Intn(len(partitionKeys))
	if pkNum == 0 {
		pkNum = 1
	}
	builder := qb.Select(s.Keyspace.Name + "." + tableName)
	for i, pk := range partitionKeys {
		builder = builder.Where(qb.InTuple(pk.Name, pkNum))
		for j := 0; j < pkNum; j++ {
			vs, ok := <-partitionValues
			if !ok {
				return nil
			}
			values = append(values, vs[i])
			typs = append(typs, pk.Type)
		}
	}
	return &Stmt{
		Query: builder,
		Values: func() []interface{} {
			return values
		},
		Types: typs,
	}
}

func (s *Schema) genClusteringRangeQuery(t *Table, partitionValues <-chan Value, r *rand.Rand, p PartitionRangeConfig) *Stmt {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var (
		typs []Type
	)
	tableName := t.Name
	partitionKeys := t.PartitionKeys
	clusteringKeys := t.ClusteringKeys
	// TODO: Support materialized views
	/*
		if len(t.MaterializedViews) > 0 && p.Rand.Int()%2 == 0 {
			view := p.Rand.Intn(len(t.MaterializedViews))
			tableName = t.MaterializedViews[view].Name
			partitionKeys = t.MaterializedViews[view].PartitionKeys
			clusteringKeys = t.MaterializedViews[view].ClusteringKeys
		}*/
	builder := qb.Select(s.Keyspace.Name + "." + tableName)
	values, ok := <-partitionValues
	if !ok {
		return nil
	}
	for _, pk := range partitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
		typs = append(typs, pk.Type)
	}
	maxClusteringRels := 0
	if len(clusteringKeys) > 1 {
		maxClusteringRels = r.Intn(len(clusteringKeys) - 1)
		for i := 0; i < maxClusteringRels; i++ {
			builder = builder.Where(qb.Eq(clusteringKeys[i].Name))
			values = appendValue(clusteringKeys[i].Type, r, p, values)
			typs = append(typs, clusteringKeys[i].Type)
		}
	}
	builder = builder.Where(qb.Gt(clusteringKeys[maxClusteringRels].Name)).Where(qb.Lt(clusteringKeys[maxClusteringRels].Name))
	values = appendValue(t.ClusteringKeys[maxClusteringRels].Type, r, p, values)
	values = appendValue(t.ClusteringKeys[maxClusteringRels].Type, r, p, values)
	typs = append(typs, clusteringKeys[maxClusteringRels].Type, clusteringKeys[maxClusteringRels].Type)
	return &Stmt{
		Query: builder,
		Values: func() []interface{} {
			return values
		},
		Types: typs,
	}
}

func (s *Schema) genMultiplePartitionClusteringRangeQuery(t *Table, partitionValues <-chan Value, r *rand.Rand, p PartitionRangeConfig) *Stmt {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var (
		values []interface{}
		typs   []Type
	)
	tableName := t.Name
	partitionKeys := t.PartitionKeys
	clusteringKeys := t.ClusteringKeys
	// TODO: Support materialized views
	/*
		if len(t.MaterializedViews) > 0 && r.Int()%2 == 0 {
			view := r.Intn(len(t.MaterializedViews))
			tableName = t.MaterializedViews[view].Name
			partitionKeys = t.MaterializedViews[view].PartitionKeys
			clusteringKeys = t.MaterializedViews[view].ClusteringKeys
		}*/
	pkNum := r.Intn(len(partitionKeys))
	if pkNum == 0 {
		pkNum = 1
	}
	builder := qb.Select(s.Keyspace.Name + "." + tableName)
	for i, pk := range partitionKeys {
		builder = builder.Where(qb.InTuple(pk.Name, pkNum))
		for j := 0; j < pkNum; j++ {
			vs, ok := <-partitionValues
			if !ok {
				return nil
			}
			values = append(values, vs[i])
			typs = append(typs, pk.Type)
		}
	}
	maxClusteringRels := 0
	if len(clusteringKeys) > 1 {
		maxClusteringRels = r.Intn(len(clusteringKeys) - 1)
		for i := 0; i < maxClusteringRels; i++ {
			builder = builder.Where(qb.Eq(clusteringKeys[i].Name))
			values = appendValue(clusteringKeys[i].Type, r, p, values)
			typs = append(typs, clusteringKeys[i].Type)
		}
	}
	builder = builder.Where(qb.Gt(clusteringKeys[maxClusteringRels].Name)).Where(qb.Lt(clusteringKeys[maxClusteringRels].Name))
	values = appendValue(clusteringKeys[maxClusteringRels].Type, r, p, values)
	values = appendValue(clusteringKeys[maxClusteringRels].Type, r, p, values)
	typs = append(typs, clusteringKeys[maxClusteringRels].Type, clusteringKeys[maxClusteringRels].Type)
	return &Stmt{
		Query: builder,
		Values: func() []interface{} {
			return values
		},
		Types: typs,
	}
}

func (s *Schema) genSingleIndexQuery(t *Table, partitionValues <-chan Value, r *rand.Rand, p PartitionRangeConfig) *Stmt {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var (
		typs []Type
	)

	if len(t.Indexes) == 0 {
		return nil
	}

	/* Once we have ALLOW FILTERING SUPPORT this can be applied
	pkNum := p.Rand.Intn(len(t.PartitionKeys))
	if pkNum == 0 {
		pkNum = 1
	}
	*/
	values, ok := <-partitionValues
	if !ok {
		return nil
	}
	pkNum := len(t.PartitionKeys)
	builder := qb.Select(s.Keyspace.Name + "." + t.Name)
	partitionKeys := t.PartitionKeys
	for i := 0; i < pkNum; i++ {
		pk := partitionKeys[i]
		builder = builder.Where(qb.Eq(pk.Name))
		typs = append(typs, pk.Type)
	}
	idx := r.Intn(len(t.Indexes))
	builder = builder.Where(qb.Eq(t.Indexes[idx].Column))
	values = appendValue(t.Columns[idx].Type, r, p, values)
	typs = append(typs, t.Columns[idx].Type)
	return &Stmt{
		Query: builder,
		Values: func() []interface{} {
			return values
		},
		Types: typs,
	}
}

type SchemaBuilder interface {
	Keyspace(Keyspace) SchemaBuilder
	Table(*Table) SchemaBuilder
	Build() *Schema
}

type schemaBuilder struct {
	keyspace Keyspace
	tables   []*Table
}

func (s *schemaBuilder) Keyspace(keyspace Keyspace) SchemaBuilder {
	s.keyspace = keyspace
	return s
}

func (s *schemaBuilder) Table(table *Table) SchemaBuilder {
	s.tables = append(s.tables, table)
	return s
}

func (s *schemaBuilder) Build() *Schema {
	return &Schema{Keyspace: s.keyspace, Tables: s.tables}
}

func NewSchemaBuilder() SchemaBuilder {
	return &schemaBuilder{}
}
