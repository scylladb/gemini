package gemini

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/google/uuid"
)

type Keyspace struct {
	Name string `json:"name"`
}

type ColumnDef struct {
	Name string
	Type string
}

type Table struct {
	Name           string      `json:"name"`
	PartitionKeys  []ColumnDef `json:"partition_keys"`
	ClusteringKeys []ColumnDef `json:"clustering_keys"`
	Columns        []ColumnDef `json:"columns"`
}

type Schema interface {
	Tables() []Table
	GetDropSchema() []string
	GetCreateSchema() []string
	GenMutateStmt(Table, *PartitionRange) *Stmt
	GenCheckStmt(Table, *PartitionRange) *Stmt
}

type Stmt struct {
	Query  string
	Values func() []interface{}
}

type schema struct {
	keyspace Keyspace
	tables   []Table
}

type PartitionRange struct {
	Min int  `default:0`
	Max int  `default:100`
}

func randRange(min int, max int) int {
	return rand.Intn(max-min) + min
}

func (s *schema) Tables() []Table {
	return s.tables
}

func (s *schema) GetDropSchema() []string {
	return []string{
		fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", s.keyspace.Name),
	}
}

func generateValue(columnType string, p *PartitionRange, values []interface{}) []interface{} {
	switch columnType {
	case "int":
		values = append(values, randRange(p.Min, p.Max))
	case "int_range":
		start := randRange(p.Min, p.Max)
		end := start + randRange(p.Min, p.Max)
		values = append(values, start)
		values = append(values, end)
	case "blob":
		r, _ := uuid.NewRandom()
		values = append(values, r.String())
	default:
		fmt.Errorf("generate value: not supported type %s", columnType)
	}
	return values
}

func (s *schema) GetCreateSchema() []string {
	createKeyspace := fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}", s.keyspace.Name)

	stmts := []string{createKeyspace}

	for _, t := range s.tables {
		partitionKeys := []string{}
		clusteringKeys := []string{}
		columns := []string{}
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
			createTable = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY (%s))", s.keyspace.Name, t.Name, strings.Join(columns, ","),strings.Join(partitionKeys, ","))
		} else {
			createTable = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY ((%s), %s))", s.keyspace.Name, t.Name, strings.Join(columns, ","),
				strings.Join(partitionKeys, ","), strings.Join(clusteringKeys, ","))
		}
		stmts = append(stmts, createTable)
	}
	return stmts
}

func (s *schema) GenMutateStmt(t Table, p *PartitionRange) *Stmt {
	columns := []string{}
	placeholders := []string{}
	values := make([]interface{}, 0)
	for _, pk := range t.PartitionKeys {
		columns = append(columns, pk.Name)
		placeholders = append(placeholders, "?")
		values = generateValue(pk.Type, p, values)
	}
	for _, ck := range t.ClusteringKeys {
		columns = append(columns, ck.Name)
		placeholders = append(placeholders, "?")
		values = generateValue(ck.Type, p, values)
	}
	for _, cdef := range t.Columns {
		columns = append(columns, cdef.Name)
		placeholders = append(placeholders, "?")
		values = generateValue(cdef.Type, p, values)
	}
	query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", s.keyspace.Name, t.Name, strings.Join(columns, ","), strings.Join(placeholders, ","))
	return &Stmt{
		Query: query,
		Values: func() []interface{} {
			return values
		},
	}
}

func (s *schema) GenCheckStmt(t Table, p *PartitionRange) *Stmt {
	switch n := rand.Intn(4); n {
	case 0:
		return s.genSinglePartitionQuery(t, p)
	case 1:
		return s.genMultiplePartitionQuery(t, p)
	case 2:
		return s.genClusteringRangeQuery(t, p)
	case 3:
		return s.genMultiplePartitionClusteringRangeQuery(t, p)
	}
	return nil
}

func (s *schema) genSinglePartitionQuery(t Table, p *PartitionRange) *Stmt {
	relations := []string{}
	values := make([]interface{}, 0)
	for _, pk := range t.PartitionKeys {
		relations = append(relations, fmt.Sprintf("%s = ?", pk.Name))
		values = generateValue(pk.Type, p, values)
	}
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", s.keyspace.Name, t.Name, strings.Join(relations, " AND "))
	return &Stmt{
		Query:  query,
		Values: func() []interface{} {
			return values
		},
	}
}

func (s *schema) genMultiplePartitionQuery(t Table, p *PartitionRange) *Stmt {
	relations := []string{}
	values := make([]interface{}, 0)
	pkNum := rand.Intn(10)
	for _, pk := range t.PartitionKeys {
		relations = append(relations, fmt.Sprintf("%s IN (%s)", pk.Name, strings.TrimRight(strings.Repeat("?,", pkNum), ",")))
		for i := 0; i < pkNum; i++ {
			values = generateValue(pk.Type, p, values)
		}
	}
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", s.keyspace.Name, t.Name, strings.Join(relations, " AND "))
	return &Stmt{
		Query:  query,
		Values: func() []interface{} {
			return values
		},
	}
}

func (s *schema) genClusteringRangeQuery(t Table, p *PartitionRange) *Stmt {
	relations := []string{}
	values := make([]interface{}, 0)
	for _, pk := range t.PartitionKeys {
		relations = append(relations, fmt.Sprintf("%s = ?", pk.Name))
		values = generateValue(pk.Type, p, values)
	}
	for _, ck := range t.ClusteringKeys {
		relations = append(relations, fmt.Sprintf("%s > ? AND %s < ?", ck.Name, ck.Name))
		values = generateValue(ck.Type+"_range", p, values)
	}
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", s.keyspace.Name, t.Name, strings.Join(relations, " AND "))
	return &Stmt{
		Query:  query,
		Values: func() []interface{} {
			return values
		},
	}
}

func (s *schema) genMultiplePartitionClusteringRangeQuery(t Table, p *PartitionRange) *Stmt {
	relations := []string{}
	pkNum := rand.Intn(10)
	values := make([]interface{}, 0)
	for _, pk := range t.PartitionKeys {
		relations = append(relations, fmt.Sprintf("%s IN (%s)", pk.Name, strings.TrimRight(strings.Repeat("?,", pkNum), ",")))
		for i := 0; i < pkNum; i++ {
			values = generateValue(pk.Type, p, values)
		}
	}
	for _, ck := range t.ClusteringKeys {
		relations = append(relations, fmt.Sprintf("%s >= ? AND %s <= ?", ck.Name, ck.Name))
		values = generateValue(ck.Type+"_range", p, values)
	}
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", s.keyspace.Name, t.Name, strings.Join(relations, " AND "))
	return &Stmt{
		Query:  query,
		Values: func() []interface{} {
			return values
		},
	}
}

type SchemaBuilder interface {
	Keyspace(Keyspace) SchemaBuilder
	Table(Table) SchemaBuilder
	Build() Schema
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

func (s *schemaBuilder) Build() Schema {
	return &schema{keyspace: s.keyspace, tables: s.tables}
}

func NewSchemaBuilder() SchemaBuilder {
	return &schemaBuilder{}
}
