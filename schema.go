package gemini

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
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
	Name              string             `json:"name"`
	PartitionKeys     Columns            `json:"partition_keys"`
	ClusteringKeys    Columns            `json:"clustering_keys"`
	Columns           Columns            `json:"columns"`
	Indexes           []IndexDef         `json:"indexes"`
	MaterializedViews []MaterializedView `json:"materialized_views"`
	KnownIssues       map[string]bool    `json:"known_issues"`
}

type MaterializedView struct {
	Name           string  `json:"name"`
	PartitionKeys  Columns `json:"partition_keys"`
	ClusteringKeys Columns `json:"clustering_keys"`
}

type Stmt struct {
	Query  string
	Values func() []interface{}
	Types  []Type
}

func (s *Stmt) PrettyCQL() string {
	var replaced int
	query := s.Query
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
	Tables   []Table  `json:"tables"`
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

func GenSchema() *Schema {
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
	builder.Table(table)
	return builder.Build()
}

func (s *Schema) GetCreateSchema() []string {
	createKeyspace := fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}", s.Keyspace.Name)

	stmts := []string{createKeyspace}

	for _, t := range s.Tables {
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
		for _, column := range t.Columns {
			switch c := column.Type.(type) {
			case UDTType:
				createType := "CREATE TYPE %s.%s (%s)"
				var typs []string
				for name, typ := range c.Types {
					typs = append(typs, name+" "+typ.CQLDef())
				}
				stmts = append(stmts, fmt.Sprintf(createType, s.Keyspace.Name, c.TypeName, strings.Join(typs, ",")))
			}
		}
		var createTable string
		if len(clusteringKeys) == 0 {
			createTable = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY (%s))", s.Keyspace.Name, t.Name, strings.Join(columns, ","), strings.Join(partitionKeys, ","))
		} else {
			createTable = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY ((%s), %s))", s.Keyspace.Name, t.Name, strings.Join(columns, ","),
				strings.Join(partitionKeys, ","), strings.Join(clusteringKeys, ","))
		}
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
					strings.Join(mvPartitionKeys, ","), strings.Join(clusteringKeys, ",")))
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

func (s *Schema) GenInsertStmt(t Table, p *PartitionRange) (*Stmt, error) {
	var (
		columns      []string
		placeholders []string
		typs         []Type
	)
	values := make([]interface{}, 0)
	for _, pk := range t.PartitionKeys {
		columns = append(columns, pk.Name)
		placeholders = append(placeholders, pk.Type.CQLHolder())
		values = appendValue(pk.Type, p, values)
		typs = append(typs, pk.Type)
	}
	for _, ck := range t.ClusteringKeys {
		columns = append(columns, ck.Name)
		placeholders = append(placeholders, ck.Type.CQLHolder())
		values = appendValue(ck.Type, p, values)
		typs = append(typs, ck.Type)
	}
	for _, cdef := range t.Columns {
		columns = append(columns, cdef.Name)
		placeholders = append(placeholders, cdef.Type.CQLHolder())
		values = appendValue(cdef.Type, p, values)
		typs = append(typs, cdef.Type)
	}
	query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", s.Keyspace.Name, t.Name, strings.Join(columns, ","), strings.Join(placeholders, ","))
	return &Stmt{
		Query: query,
		Values: func() []interface{} {
			return values
		},
		Types: typs,
	}, nil
}

func (s *Schema) GenInsertJsonStmt(t Table, p *PartitionRange) (*Stmt, error) {
	values := make(map[string]interface{})
	values = t.PartitionKeys.ToJSONMap(values, p)
	values = t.ClusteringKeys.ToJSONMap(values, p)
	values = t.Columns.ToJSONMap(values, p)

	jsonString, err := json.Marshal(values)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf("INSERT INTO %s.%s JSON ?", s.Keyspace.Name, t.Name)
	return &Stmt{
		Query: query,
		Values: func() []interface{} {
			return []interface{}{string(jsonString)}
		},
		Types: []Type{TYPE_TEXT},
	}, nil
}

func (s *Schema) GenDeleteRows(t Table, p *PartitionRange) (*Stmt, error) {
	var (
		relations []string
		values    []interface{}
		typs      []Type
	)
	for _, pk := range t.PartitionKeys {
		relations = append(relations, fmt.Sprintf("%s = ?", pk.Name))
		values = appendValue(pk.Type, p, values)
		typs = append(typs, pk.Type)
	}
	if len(t.ClusteringKeys) == 1 {
		for _, ck := range t.ClusteringKeys {
			relations = append(relations, fmt.Sprintf("%s >= ? AND %s <= ?", ck.Name, ck.Name))
			values = appendValueRange(ck.Type, p, values)
			typs = append(typs, ck.Type, ck.Type)
		}
	}
	query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s", s.Keyspace.Name, t.Name, strings.Join(relations, " AND "))
	return &Stmt{
		Query: query,
		Values: func() []interface{} {
			return values
		},
		Types: typs,
	}, nil
}

func (s *Schema) GenMutateStmt(t Table, p *PartitionRange) (*Stmt, error) {
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

func (s *Schema) GenCheckStmt(t Table, p *PartitionRange) *Stmt {
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

func (s *Schema) genSinglePartitionQuery(t Table, p *PartitionRange) *Stmt {
	tableName := t.Name
	partitionKeys := t.PartitionKeys
	if len(t.MaterializedViews) > 0 && p.Rand.Int()%2 == 0 {
		view := p.Rand.Intn(len(t.MaterializedViews))
		tableName = t.MaterializedViews[view].Name
		partitionKeys = t.MaterializedViews[view].PartitionKeys
	}
	var relations []string
	values := make([]interface{}, 0)
	typs := make([]Type, 0, 10)
	for _, pk := range partitionKeys {
		relations = append(relations, fmt.Sprintf("%s = ?", pk.Name))
		values = appendValue(pk.Type, p, values)
		typs = append(typs, pk.Type)
	}
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", s.Keyspace.Name, tableName, strings.Join(relations, " AND "))
	return &Stmt{
		Query: query,
		Values: func() []interface{} {
			return values
		},
		Types: typs,
	}
}

func (s *Schema) genMultiplePartitionQuery(t Table, p *PartitionRange) *Stmt {
	var (
		relations []string
		values    []interface{}
		typs      []Type
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
	for _, pk := range partitionKeys {
		relations = append(relations, fmt.Sprintf("%s IN (%s)", pk.Name, strings.TrimRight(strings.Repeat("?,", pkNum), ",")))
		for i := 0; i < pkNum; i++ {
			values = appendValue(pk.Type, p, values)
			typs = append(typs, pk.Type)
		}
	}
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", s.Keyspace.Name, tableName, strings.Join(relations, " AND "))
	return &Stmt{
		Query: query,
		Values: func() []interface{} {
			return values
		},
		Types: typs,
	}
}

func (s *Schema) genClusteringRangeQuery(t Table, p *PartitionRange) *Stmt {
	var (
		relations []string
		values    []interface{}
		typs      []Type
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
	for _, pk := range partitionKeys {
		relations = append(relations, fmt.Sprintf("%s = ?", pk.Name))
		values = appendValue(pk.Type, p, values)
		typs = append(typs, pk.Type)
	}
	maxClusteringRels := 0
	if len(clusteringKeys) > 1 {
		maxClusteringRels = p.Rand.Intn(len(clusteringKeys) - 1)
		for i := 0; i < maxClusteringRels; i++ {
			relations = append(relations, fmt.Sprintf("%s = ?", clusteringKeys[i].Name))
			values = appendValue(clusteringKeys[i].Type, p, values)
			typs = append(typs, clusteringKeys[i].Type)
		}
	}
	relations = append(relations, fmt.Sprintf("%s > ? AND %s < ?", clusteringKeys[maxClusteringRels].Name, clusteringKeys[maxClusteringRels].Name))
	values = appendValueRange(t.ClusteringKeys[maxClusteringRels].Type, p, values)
	typs = append(typs, clusteringKeys[maxClusteringRels].Type, clusteringKeys[maxClusteringRels].Type)
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", s.Keyspace.Name, tableName, strings.Join(relations, " AND "))
	return &Stmt{
		Query: query,
		Values: func() []interface{} {
			return values
		},
		Types: typs,
	}
}

func (s *Schema) genMultiplePartitionClusteringRangeQuery(t Table, p *PartitionRange) *Stmt {
	var (
		relations []string
		values    []interface{}
		typs      []Type
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
	for _, pk := range partitionKeys {
		relations = append(relations, fmt.Sprintf("%s IN (%s)", pk.Name, strings.TrimRight(strings.Repeat("?,", pkNum), ",")))
		for i := 0; i < pkNum; i++ {
			values = appendValue(pk.Type, p, values)
			typs = append(typs, pk.Type)
		}
	}
	maxClusteringRels := 0
	if len(clusteringKeys) > 1 {
		maxClusteringRels = p.Rand.Intn(len(clusteringKeys) - 1)
		for i := 0; i < maxClusteringRels; i++ {
			relations = append(relations, fmt.Sprintf("%s = ?", clusteringKeys[i].Name))
			values = appendValue(clusteringKeys[i].Type, p, values)
			typs = append(typs, clusteringKeys[i].Type)
		}
	}
	relations = append(relations, fmt.Sprintf("%s > ? AND %s < ?", clusteringKeys[maxClusteringRels].Name, clusteringKeys[maxClusteringRels].Name))
	values = appendValueRange(clusteringKeys[maxClusteringRels].Type, p, values)
	typs = append(typs, clusteringKeys[maxClusteringRels].Type, clusteringKeys[maxClusteringRels].Type)
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", s.Keyspace.Name, tableName, strings.Join(relations, " AND "))
	return &Stmt{
		Query: query,
		Values: func() []interface{} {
			return values
		},
		Types: typs,
	}
}

func (s *Schema) genSingleIndexQuery(t Table, p *PartitionRange) *Stmt {
	var (
		relations []string
		values    []interface{}
		typs      []Type
	)

	if len(t.Indexes) == 0 {
		return nil
	}
	pkNum := p.Rand.Intn(len(t.PartitionKeys))
	if pkNum == 0 {
		pkNum = 1
	}
	for _, pk := range t.PartitionKeys {
		for i := 0; i < pkNum; i++ {
			relations = append(relations, fmt.Sprintf("%s = ?", pk.Name))
			values = appendValue(pk.Type, p, values)
			typs = append(typs, pk.Type)
		}
	}
	idx := p.Rand.Intn(len(t.Indexes))
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s AND %s=?", s.Keyspace.Name, t.Name, strings.Join(relations, " AND "), t.Indexes[idx].Column)
	values = appendValue(t.Columns[idx].Type, p, values)
	typs = append(typs, t.Columns[idx].Type)
	return &Stmt{
		Query: query,
		Values: func() []interface{} {
			return values
		},
		Types: typs,
	}
}

type SchemaBuilder interface {
	Keyspace(Keyspace) SchemaBuilder
	Table(Table) SchemaBuilder
	Build() *Schema
}

type schemaBuilder struct {
	keyspace Keyspace
	tables   []Table
}

func (s *schemaBuilder) Keyspace(keyspace Keyspace) SchemaBuilder {
	s.keyspace = keyspace
	return s
}

func (s *schemaBuilder) Table(table Table) SchemaBuilder {
	s.tables = append(s.tables, table)
	return s
}

func (s *schemaBuilder) Build() *Schema {
	return &Schema{Keyspace: s.keyspace, Tables: s.tables}
}

func NewSchemaBuilder() SchemaBuilder {
	return &schemaBuilder{}
}
