package gemini

import (
	"fmt"
	"math/rand"
	"strings"
)

type Keyspace struct {
	Name string `json:"name"`
}

type ColumnDef struct {
	Name string
	Type string
}

type IndexDef struct {
	Name   string
	Column ColumnDef
}

type Table struct {
	Name           string      `json:"name"`
	PartitionKeys  []ColumnDef `json:"partition_keys"`
	ClusteringKeys []ColumnDef `json:"clustering_keys"`
	Columns        []ColumnDef `json:"columns"`
	Indexes        []IndexDef  `json:"indexes"`
}

type Stmt struct {
	Query  string
	Values func() []interface{}
}

type Schema struct {
	Keyspace Keyspace `json:"keyspace"`
	Tables   []Table  `json:"tables"`
}

type PartitionRange struct {
	Min int `default:0`
	Max int `default:100`
}

func (s *Schema) GetDropSchema() []string {
	return []string{
		fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", s.Keyspace.Name),
	}
}

// TODO: Add support for time when gocql bug is fixed.
var (
	pkTypes = []string{"ascii", "bigint", "blob", "date", "decimal", "double", "float", "inet", "int", "smallint", "text" /*"time",*/, "timestamp", "timeuuid", "tinyint", "uuid", "varchar", "varint"}
	types   = append(append([]string{}, pkTypes...), "boolean", "duration")
)

func genColumnName(prefix string, idx int) string {
	return fmt.Sprintf("%s%d", prefix, idx)
}

func genColumnType() string {
	n := rand.Intn(len(types))
	return types[n]
}

func genPrimaryKeyColumnType() string {
	n := rand.Intn(len(pkTypes))
	return types[n]
}

func genColumnDef(prefix string, idx int) ColumnDef {
	return ColumnDef{
		Name: genColumnName(prefix, idx),
		Type: genColumnType(),
	}
}

func genIndexName(prefix string, idx int) string {
	return fmt.Sprintf("%s_idx", genColumnName(prefix, idx))
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
		partitionKeys = append(partitionKeys, ColumnDef{Name: genColumnName("pk", i), Type: "int"})
	}
	var clusteringKeys []ColumnDef
	numClusteringKeys := rand.Intn(MaxClusteringKeys)
	for i := 0; i < numClusteringKeys; i++ {
		clusteringKeys = append(clusteringKeys, ColumnDef{Name: genColumnName("ck", i), Type: genPrimaryKeyColumnType()})
	}
	var columns []ColumnDef
	numColumns := rand.Intn(MaxColumns)
	for i := 0; i < numColumns; i++ {
		columns = append(columns, ColumnDef{Name: genColumnName("col", i), Type: genColumnType()})
	}
	var indexes []IndexDef
	if numColumns > 0 {
		numIndexes := rand.Intn(numColumns)
		for i := 0; i < numIndexes; i++ {
			indexes = append(indexes, IndexDef{Name: genIndexName("col", i), Column: columns[i]})
		}
	}
	table := Table{
		Name:           "table1",
		PartitionKeys:  partitionKeys,
		ClusteringKeys: clusteringKeys,
		Columns:        columns,
		Indexes:        indexes,
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
			columns = append(columns, fmt.Sprintf("%s %s", pk.Name, pk.Type))
		}
		for _, ck := range t.ClusteringKeys {
			clusteringKeys = append(clusteringKeys, ck.Name)
			columns = append(columns, fmt.Sprintf("%s %s", ck.Name, ck.Type))
		}
		for _, cdef := range t.Columns {
			columns = append(columns, fmt.Sprintf("%s %s", cdef.Name, cdef.Type))
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
			stmts = append(stmts, fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s)", idef.Name, s.Keyspace.Name, t.Name, idef.Column.Name))
		}
	}
	return stmts
}

func (s *Schema) GenInsertStmt(t Table, p *PartitionRange) *Stmt {
	var (
		columns      []string
		placeholders []string
	)
	values := make([]interface{}, 0)
	for _, pk := range t.PartitionKeys {
		columns = append(columns, pk.Name)
		placeholders = append(placeholders, "?")
		values = appendValue(pk.Type, p, values)
	}
	for _, ck := range t.ClusteringKeys {
		columns = append(columns, ck.Name)
		placeholders = append(placeholders, "?")
		values = appendValue(ck.Type, p, values)
	}
	for _, cdef := range t.Columns {
		columns = append(columns, cdef.Name)
		placeholders = append(placeholders, "?")
		values = appendValue(cdef.Type, p, values)
	}
	query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", s.Keyspace.Name, t.Name, strings.Join(columns, ","), strings.Join(placeholders, ","))
	return &Stmt{
		Query: query,
		Values: func() []interface{} {
			return values
		},
	}
}

func (s *Schema) GenDeleteRows(t Table, p *PartitionRange) *Stmt {
	var (
		relations []string
		values    []interface{}
	)
	for _, pk := range t.PartitionKeys {
		relations = append(relations, fmt.Sprintf("%s = ?", pk.Name))
		values = appendValue(pk.Type, p, values)
	}
	if len(t.ClusteringKeys) == 1 {
		for _, ck := range t.ClusteringKeys {
			relations = append(relations, fmt.Sprintf("%s >= ? AND %s <= ?", ck.Name, ck.Name))
			values = appendValueRange(ck.Type, p, values)
		}
	}
	query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s", s.Keyspace.Name, t.Name, strings.Join(relations, " AND "))
	return &Stmt{
		Query: query,
		Values: func() []interface{} {
			return values
		},
	}
}

func (s *Schema) GenMutateStmt(t Table, p *PartitionRange) *Stmt {
	switch n := rand.Intn(1000); n {
	case 10, 100:
		return s.GenDeleteRows(t, p)
	default:
		return s.GenInsertStmt(t, p)
	}
}

func (s *Schema) GenCheckStmt(t Table, p *PartitionRange) *Stmt {
	var n int
	if len(t.Indexes) > 0 {
		n = rand.Intn(5)
	} else {
		n = rand.Intn(4)
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
	var relations []string
	values := make([]interface{}, 0)
	for _, pk := range t.PartitionKeys {
		relations = append(relations, fmt.Sprintf("%s = ?", pk.Name))
		values = appendValue(pk.Type, p, values)
	}
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", s.Keyspace.Name, t.Name, strings.Join(relations, " AND "))
	return &Stmt{
		Query: query,
		Values: func() []interface{} {
			return values
		},
	}
}

func (s *Schema) genMultiplePartitionQuery(t Table, p *PartitionRange) *Stmt {
	var (
		relations []string
		values    []interface{}
	)
	pkNum := rand.Intn(len(t.PartitionKeys))
	if pkNum == 0 {
		pkNum = 1
	}
	for _, pk := range t.PartitionKeys {
		relations = append(relations, fmt.Sprintf("%s IN (%s)", pk.Name, strings.TrimRight(strings.Repeat("?,", pkNum), ",")))
		for i := 0; i < pkNum; i++ {
			values = appendValue(pk.Type, p, values)
		}
	}
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", s.Keyspace.Name, t.Name, strings.Join(relations, " AND "))
	return &Stmt{
		Query: query,
		Values: func() []interface{} {
			return values
		},
	}
}

func (s *Schema) genClusteringRangeQuery(t Table, p *PartitionRange) *Stmt {
	var (
		relations []string
		values    []interface{}
	)
	for _, pk := range t.PartitionKeys {
		relations = append(relations, fmt.Sprintf("%s = ?", pk.Name))
		values = appendValue(pk.Type, p, values)
	}
	maxClusteringRels := 0
	if len(t.ClusteringKeys) > 1 {
		maxClusteringRels = rand.Intn(len(t.ClusteringKeys) - 1)
		for i := 0; i < maxClusteringRels; i++ {
			relations = append(relations, fmt.Sprintf("%s = ?", t.ClusteringKeys[i].Name))
			values = appendValue(t.ClusteringKeys[i].Type, p, values)
		}
	}
	relations = append(relations, fmt.Sprintf("%s > ? AND %s < ?", t.ClusteringKeys[maxClusteringRels].Name, t.ClusteringKeys[maxClusteringRels].Name))
	values = appendValueRange(t.ClusteringKeys[maxClusteringRels].Type, p, values)
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", s.Keyspace.Name, t.Name, strings.Join(relations, " AND "))
	return &Stmt{
		Query: query,
		Values: func() []interface{} {
			return values
		},
	}
}

func (s *Schema) genMultiplePartitionClusteringRangeQuery(t Table, p *PartitionRange) *Stmt {
	var (
		relations []string
		values    []interface{}
	)
	pkNum := rand.Intn(len(t.PartitionKeys))
	if pkNum == 0 {
		pkNum = 1
	}
	for _, pk := range t.PartitionKeys {
		relations = append(relations, fmt.Sprintf("%s IN (%s)", pk.Name, strings.TrimRight(strings.Repeat("?,", pkNum), ",")))
		for i := 0; i < pkNum; i++ {
			values = appendValue(pk.Type, p, values)
		}
	}
	maxClusteringRels := 0
	if len(t.ClusteringKeys) > 1 {
		maxClusteringRels = rand.Intn(len(t.ClusteringKeys) - 1)
		for i := 0; i < maxClusteringRels; i++ {
			relations = append(relations, fmt.Sprintf("%s = ?", t.ClusteringKeys[i].Name))
			values = appendValue(t.ClusteringKeys[i].Type, p, values)
		}
	}
	relations = append(relations, fmt.Sprintf("%s > ? AND %s < ?", t.ClusteringKeys[maxClusteringRels].Name, t.ClusteringKeys[maxClusteringRels].Name))
	values = appendValueRange(t.ClusteringKeys[maxClusteringRels].Type, p, values)
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", s.Keyspace.Name, t.Name, strings.Join(relations, " AND "))
	return &Stmt{
		Query: query,
		Values: func() []interface{} {
			return values
		},
	}
}

func (s *Schema) genSingleIndexQuery(t Table, p *PartitionRange) *Stmt {
	if len(t.Indexes) == 0 {
		return nil
	}
	idx := rand.Intn(len(t.Indexes))
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s=?", s.Keyspace.Name, t.Name, t.Indexes[idx].Column.Name)
	values := appendValue(t.Indexes[idx].Column.Type, p, nil)
	return &Stmt{
		Query: query,
		Values: func() []interface{} {
			return values
		},
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
