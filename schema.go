package gemini

import (
	"fmt"
	"math/rand"
)

type Keyspace struct {
	Name string
}

type ColumnDef struct {
	Name string
	Type string
}

type Table struct {
	Name       string
	PrimaryKey ColumnDef
	Columns    []ColumnDef
}

type Schema interface {
	GetDropSchema() []string
	GetCreateSchema() []string
	GenMutateOp() string
	GenCheckOp() string
}

type schema struct {
	keyspace Keyspace
	table    Table
}

func (s *schema) GetDropSchema() []string {
	return []string{
		fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", s.keyspace.Name),
	}
}

func (s *schema) GetCreateSchema() []string {
	createKeyspace := fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}", s.keyspace.Name)
	createTable := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (", s.keyspace.Name, s.table.Name)
	createTable += fmt.Sprintf("%s %s PRIMARY KEY", s.table.PrimaryKey.Name, s.table.PrimaryKey.Type)
	for _, cdef := range s.table.Columns {
		createTable += fmt.Sprintf(", %s %s", cdef.Name, cdef.Type)
	}
	createTable += ")"
	return []string{
		createKeyspace,
		createTable,
	}
}

func (s *schema) GenMutateOp() string {
	pk := rand.Intn(100)
	value := rand.Intn(100)
	return fmt.Sprintf("INSERT INTO %s.%s (%s, %s) VALUES (%d, %d)", s.keyspace.Name, s.table.Name, s.table.PrimaryKey.Name, s.table.Columns[0].Name, pk, value)
}

func (s *schema) GenCheckOp() string {
	query := fmt.Sprintf("SELECT * FROM %s.%s", s.keyspace.Name, s.table.Name)
	if rand.Intn(2) == 1 {
		query += fmt.Sprintf(" ORDER BY %s", s.table.Columns[0].Name)
		if rand.Intn(2) == 1 {
			query += " ASC"
		}
	}
	if rand.Intn(2) == 1 {
		query += fmt.Sprintf(" LIMIT %d", rand.Intn(100))
	}
	return query
}

type SchemaBuilder interface {
	Keyspace(Keyspace) SchemaBuilder
	Table(Table) SchemaBuilder
	Build() Schema
}

type schemaBuilder struct {
	keyspace Keyspace
	table    Table
}

func (s *schemaBuilder) Keyspace(keyspace Keyspace) SchemaBuilder {
	s.keyspace = keyspace
	return s
}

func (s *schemaBuilder) Table(table Table) SchemaBuilder {
	s.table = table
	return s
}

func (s *schemaBuilder) Build() Schema {
	return &schema{keyspace: s.keyspace, table: s.table}
}

func NewSchemaBuilder() SchemaBuilder {
	return &schemaBuilder{}
}
