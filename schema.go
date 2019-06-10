package gemini

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"github.com/scylladb/gocqlx/qb"
)

const (
	KnownIssuesJsonWithTuples = "https://github.com/scylladb/scylla/issues/3708"
)

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
	GenValue(*PartitionRange) []interface{}
	GenValueRange(p *PartitionRange) ([]interface{}, []interface{})
	Indexable() bool
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

func (cs Columns) ToJSONMap(values map[string]interface{}, p *PartitionRange) map[string]interface{} {
	for _, k := range cs {
		switch t := k.Type.(type) {
		case SimpleType:
			if t != TYPE_BLOB {
				values[k.Name] = t.GenValue(p)[0]
				continue
			}
			v, ok := t.GenValue(p)[0].(string)
			if ok {
				values[k.Name] = "0x" + v
			}
		case TupleType:
			vv := t.GenValue(p)
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
}

func (t *Table) GetCreateTable(ks Keyspace) string {
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

type PartitionRange struct {
	Min  int `default:0`
	Max  int `default:100`
	Rand *rand.Rand
}

func (s *Schema) GetDropSchema() []string {
	return []string{
		fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", s.Keyspace.Name),
	}
}

const (
	MaxPartitionKeys  = 2
	MaxClusteringKeys = 4
	MaxColumns        = 16
)

func GenSchema(cs *CompactionStrategy) *Schema {
	builder := NewSchemaBuilder()
	keyspace := Keyspace{
		Name: "ks1",
	}
	builder.Keyspace(keyspace)
	var partitionKeys []ColumnDef
	numPartitionKeys := rand.Intn(MaxPartitionKeys-1) + 1
	for i := 0; i < numPartitionKeys; i++ {
		partitionKeys = append(partitionKeys, ColumnDef{Name: genColumnName("pk", i), Type: TYPE_INT})
	}
	var clusteringKeys []ColumnDef
	numClusteringKeys := rand.Intn(MaxClusteringKeys)
	for i := 0; i < numClusteringKeys; i++ {
		clusteringKeys = append(clusteringKeys, ColumnDef{Name: genColumnName("ck", i), Type: genPrimaryKeyColumnType()})
	}
	var columns []ColumnDef
	numColumns := rand.Intn(MaxColumns)
	for i := 0; i < numColumns; i++ {
		columns = append(columns, ColumnDef{Name: genColumnName("col", i), Type: genColumnType(numColumns)})
	}
	var indexes []IndexDef
	if numColumns > 0 {
		numIndexes := rand.Intn(numColumns)
		for i := 0; i < numIndexes; i++ {
			if columns[i].Type.Indexable() {
				indexes = append(indexes, IndexDef{Name: genIndexName("col", i), Column: columns[i].Name})
			}
		}
	}
	validMVColumn := func() ColumnDef {
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
		return validCols[rand.Intn(len(validCols))]
	}
	var mvs []MaterializedView
	numMvs := 1
	for i := 0; i < numMvs; i++ {
		cols := []ColumnDef{
			validMVColumn(),
		}
		mv := MaterializedView{
			Name:           "table1_mv_" + strconv.Itoa(i),
			PartitionKeys:  append(cols, partitionKeys...),
			ClusteringKeys: clusteringKeys,
		}
		mvs = append(mvs, mv)
	}

	table := Table{
		Name:               "table1",
		PartitionKeys:      partitionKeys,
		ClusteringKeys:     clusteringKeys,
		Columns:            columns,
		CompactionStrategy: cs,
		MaterializedViews:  mvs,
		Indexes:            indexes,
		KnownIssues: map[string]bool{
			KnownIssuesJsonWithTuples: true,
		},
	}
	if cs == nil {
		table.CompactionStrategy = randomCompactionStrategy()
	}

	builder.Table(&table)
	return builder.Build()
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

func (s *Schema) GenInsertStmt(t *Table, p *PartitionRange) (*Stmt, error) {
	var (
		typs []Type
	)
	builder := qb.Insert(s.Keyspace.Name + "." + t.Name)
	values := make([]interface{}, 0)
	for _, pk := range t.PartitionKeys {
		builder = builder.Columns(pk.Name)
		values = appendValue(pk.Type, p, values)
		typs = append(typs, pk.Type)
	}
	for _, ck := range t.ClusteringKeys {
		builder = builder.Columns(ck.Name)
		values = appendValue(ck.Type, p, values)
		typs = append(typs, ck.Type)
	}
	for _, cdef := range t.Columns {
		switch t := cdef.Type.(type) {
		case TupleType:
			builder = builder.TupleColumn(cdef.Name, len(t.Types))
		default:
			builder = builder.Columns(cdef.Name)
		}
		values = appendValue(cdef.Type, p, values)
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

func (s *Schema) GenInsertJsonStmt(t *Table, p *PartitionRange) (*Stmt, error) {
	values := make(map[string]interface{})
	values = t.PartitionKeys.ToJSONMap(values, p)
	values = t.ClusteringKeys.ToJSONMap(values, p)
	values = t.Columns.ToJSONMap(values, p)

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

func (s *Schema) GenDeleteRows(t *Table, p *PartitionRange) (*Stmt, error) {
	var (
		values []interface{}
		typs   []Type
	)
	builder := qb.Delete(s.Keyspace.Name + "." + t.Name)
	for _, pk := range t.PartitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
		values = appendValue(pk.Type, p, values)
		typs = append(typs, pk.Type)
	}
	if len(t.ClusteringKeys) > 0 {
		ck := t.ClusteringKeys[0]
		builder = builder.Where(qb.GtOrEq(ck.Name)).Where(qb.LtOrEq(ck.Name))
		values = appendValueRange(ck.Type, p, values)
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

func (s *Schema) GenMutateStmt(t *Table, p *PartitionRange, deletes bool) (*Stmt, error) {
	if !deletes {
		return s.GenInsertStmt(t, p)
	}
	switch n := p.Rand.Intn(1000); n {
	case 10, 100:
		return s.GenDeleteRows(t, p)
	default:
		switch n := p.Rand.Intn(2); n {
		case 0:
			if t.KnownIssues[KnownIssuesJsonWithTuples] {
				return s.GenInsertStmt(t, p)
			}
			return s.GenInsertJsonStmt(t, p)
		default:
			return s.GenInsertStmt(t, p)
		}
	}
}

func (s *Schema) GenCheckStmt(t *Table, p *PartitionRange) *Stmt {
	var n int
	if len(t.Indexes) > 0 {
		n = p.Rand.Intn(5)
	} else {
		n = p.Rand.Intn(4)
	}
	switch n {
	case 0:
		return s.genSinglePartitionQuery(t, p)
	case 1:
		return s.genMultiplePartitionQuery(t, p)
	case 2:
		return s.genClusteringRangeQuery(t, p)
	case 3:
		return s.genMultiplePartitionClusteringRangeQuery(t, p)
	case 4:
		return s.genSingleIndexQuery(t, p)
	}
	return nil
}

func (s *Schema) genSinglePartitionQuery(t *Table, p *PartitionRange) *Stmt {
	tableName := t.Name
	partitionKeys := t.PartitionKeys
	if len(t.MaterializedViews) > 0 && p.Rand.Int()%2 == 0 {
		view := p.Rand.Intn(len(t.MaterializedViews))
		tableName = t.MaterializedViews[view].Name
		partitionKeys = t.MaterializedViews[view].PartitionKeys
	}
	builder := qb.Select(s.Keyspace.Name + "." + tableName)
	values := make([]interface{}, 0)
	typs := make([]Type, 0, 10)
	for _, pk := range partitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
		values = appendValue(pk.Type, p, values)
		typs = append(typs, pk.Type)
	}
	return &Stmt{
		Query: builder,
		Values: func() []interface{} {
			return values
		},
		Types: typs,
	}
}

func (s *Schema) genMultiplePartitionQuery(t *Table, p *PartitionRange) *Stmt {
	var (
		values []interface{}
		typs   []Type
	)
	tableName := t.Name
	partitionKeys := t.PartitionKeys
	if len(t.MaterializedViews) > 0 && p.Rand.Int()%2 == 0 {
		view := p.Rand.Intn(len(t.MaterializedViews))
		tableName = t.MaterializedViews[view].Name
		partitionKeys = t.MaterializedViews[view].PartitionKeys
	}
	pkNum := p.Rand.Intn(len(partitionKeys))
	if pkNum == 0 {
		pkNum = 1
	}
	builder := qb.Select(s.Keyspace.Name + "." + tableName)
	for _, pk := range partitionKeys {
		builder = builder.Where(qb.InTuple(pk.Name, pkNum))
		for i := 0; i < pkNum; i++ {
			values = appendValue(pk.Type, p, values)
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

func (s *Schema) genClusteringRangeQuery(t *Table, p *PartitionRange) *Stmt {
	var (
		values []interface{}
		typs   []Type
	)
	tableName := t.Name
	partitionKeys := t.PartitionKeys
	clusteringKeys := t.ClusteringKeys
	view := p.Rand.Intn(len(t.MaterializedViews))
	if len(t.MaterializedViews) > 0 && p.Rand.Int()%2 == 0 {
		tableName = t.MaterializedViews[view].Name
		partitionKeys = t.MaterializedViews[view].PartitionKeys
		clusteringKeys = t.MaterializedViews[view].ClusteringKeys
	}
	builder := qb.Select(s.Keyspace.Name + "." + tableName)
	for _, pk := range partitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
		values = appendValue(pk.Type, p, values)
		typs = append(typs, pk.Type)
	}
	maxClusteringRels := 0
	if len(clusteringKeys) > 1 {
		maxClusteringRels = p.Rand.Intn(len(clusteringKeys) - 1)
		for i := 0; i < maxClusteringRels; i++ {
			builder = builder.Where(qb.Eq(clusteringKeys[i].Name))
			values = appendValue(clusteringKeys[i].Type, p, values)
			typs = append(typs, clusteringKeys[i].Type)
		}
	}
	builder = builder.Where(qb.Gt(clusteringKeys[maxClusteringRels].Name)).Where(qb.Lt(clusteringKeys[maxClusteringRels].Name))
	values = appendValueRange(t.ClusteringKeys[maxClusteringRels].Type, p, values)
	typs = append(typs, clusteringKeys[maxClusteringRels].Type, clusteringKeys[maxClusteringRels].Type)
	return &Stmt{
		Query: builder,
		Values: func() []interface{} {
			return values
		},
		Types: typs,
	}
}

func (s *Schema) genMultiplePartitionClusteringRangeQuery(t *Table, p *PartitionRange) *Stmt {
	var (
		values []interface{}
		typs   []Type
	)
	tableName := t.Name
	partitionKeys := t.PartitionKeys
	clusteringKeys := t.ClusteringKeys
	view := p.Rand.Intn(len(t.MaterializedViews))
	if len(t.MaterializedViews) > 0 && p.Rand.Int()%2 == 0 {
		tableName = t.MaterializedViews[view].Name
		partitionKeys = t.MaterializedViews[view].PartitionKeys
		clusteringKeys = t.MaterializedViews[view].ClusteringKeys
	}
	pkNum := p.Rand.Intn(len(partitionKeys))
	if pkNum == 0 {
		pkNum = 1
	}
	builder := qb.Select(s.Keyspace.Name + "." + tableName)
	for _, pk := range partitionKeys {
		builder = builder.Where(qb.InTuple(pk.Name, pkNum))
		for i := 0; i < pkNum; i++ {
			values = appendValue(pk.Type, p, values)
			typs = append(typs, pk.Type)
		}
	}
	maxClusteringRels := 0
	if len(clusteringKeys) > 1 {
		maxClusteringRels = p.Rand.Intn(len(clusteringKeys) - 1)
		for i := 0; i < maxClusteringRels; i++ {
			builder = builder.Where(qb.Eq(clusteringKeys[i].Name))
			values = appendValue(clusteringKeys[i].Type, p, values)
			typs = append(typs, clusteringKeys[i].Type)
		}
	}
	builder = builder.Where(qb.Gt(clusteringKeys[maxClusteringRels].Name)).Where(qb.Lt(clusteringKeys[maxClusteringRels].Name))
	values = appendValueRange(clusteringKeys[maxClusteringRels].Type, p, values)
	typs = append(typs, clusteringKeys[maxClusteringRels].Type, clusteringKeys[maxClusteringRels].Type)
	return &Stmt{
		Query: builder,
		Values: func() []interface{} {
			return values
		},
		Types: typs,
	}
}

func (s *Schema) genSingleIndexQuery(t *Table, p *PartitionRange) *Stmt {
	var (
		values []interface{}
		typs   []Type
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
	pkNum := len(t.PartitionKeys)
	builder := qb.Select(s.Keyspace.Name + "." + t.Name)
	partitionKeys := t.PartitionKeys
	for i := 0; i < pkNum; i++ {
		pk := partitionKeys[i]
		builder = builder.Where(qb.Eq(pk.Name))
		values = appendValue(pk.Type, p, values)
		typs = append(typs, pk.Type)
	}
	idx := p.Rand.Intn(len(t.Indexes))
	builder = builder.Where(qb.Eq(t.Indexes[idx].Column))
	values = appendValue(t.Columns[idx].Type, p, values)
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
