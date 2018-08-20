package gemini

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/google/uuid"
)

type Keyspace struct {
	Name string
}

type ColumnDef struct {
	Name string
	Type string
}

type Table struct {
	Name           string
	PartitionKeys  []ColumnDef
	ClusteringKeys []ColumnDef
	Columns        []ColumnDef
}

type Schema interface {
	GetDropSchema() []string
	GetCreateSchema() []string
	GenMutateStmt(*PartitionRange) *Stmt
	GenCheckStmt(*PartitionRange) *Stmt
}

type Stmt struct {
	Query  string
	Values func() []interface{}
}

type schema struct {
	keyspace Keyspace
	table    Table
}

type PartitionRange struct {
	Min int  `default:0`
	Max int  `default:100`
}

func randRange(min int, max int) int {
	return rand.Intn(max-min) + min
}

func (s *schema) GetDropSchema() []string {
	return []string{
		fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", s.keyspace.Name),
	}
}

func (s *schema) GetCreateSchema() []string {
	createKeyspace := fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}", s.keyspace.Name)
	partitionKeys := []string{}
	clusteringKeys := []string{}
	columns := []string{}
	for _, pk := range s.table.PartitionKeys {
		partitionKeys = append(partitionKeys, pk.Name)
		columns = append(columns, fmt.Sprintf("%s %s", pk.Name, pk.Type))
	}
	for _, ck := range s.table.ClusteringKeys {
		clusteringKeys = append(clusteringKeys, ck.Name)
		columns = append(columns, fmt.Sprintf("%s %s", ck.Name, ck.Type))
	}
	for _, cdef := range s.table.Columns {
		columns = append(columns, fmt.Sprintf("%s %s", cdef.Name, cdef.Type))
	}
	createTable := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY ((%s), %s))", s.keyspace.Name, s.table.Name, strings.Join(columns, ","), strings.Join(partitionKeys, ","), strings.Join(clusteringKeys, ","))
	return []string{
		createKeyspace,
		createTable,
	}
}

func (s *schema) GenMutateStmt(p *PartitionRange) *Stmt {
	columns := []string{}
	values := []string{}
	for _, pk := range s.table.PartitionKeys {
		columns = append(columns, pk.Name)
		values = append(values, "?")
	}
	for _, pk := range s.table.ClusteringKeys {
		columns = append(columns, pk.Name)
		values = append(values, "?")
	}
	for _, cdef := range s.table.Columns {
		columns = append(columns, cdef.Name)
		values = append(values, "?")
	}
	query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", s.keyspace.Name, s.table.Name, strings.Join(columns, ","), strings.Join(values, ","))
	return &Stmt{
		Query: query,
		Values: func() []interface{} {
			values := make([]interface{}, 0)
			for _, _ = range s.table.PartitionKeys {
				values = append(values, randRange(p.Min, p.Max))
			}
			for _, _ = range s.table.ClusteringKeys {
				values = append(values, randRange(p.Min, p.Max))
			}
			for _, _ = range s.table.Columns {
				r, _ := uuid.NewRandom()
				values = append(values, r.String())
			}
			return values
		},
	}
}

func (s *schema) GenCheckStmt(p *PartitionRange) *Stmt {
	switch n := rand.Intn(4); n {
	case 0:
		return s.genSinglePartitionQuery(p)
	case 1:
		return s.genMultiplePartitionQuery(p)
	case 2:
		return s.genClusteringRangeQuery(p)
	case 3:
		return s.genClusteringRangeQueryComplex(p)
	}
	return nil
}

func (s *schema) genSinglePartitionQuery(p *PartitionRange) *Stmt {
	relations := []string{}
	for _, pk := range s.table.PartitionKeys {
		relations = append(relations, fmt.Sprintf("%s = ?", pk.Name))
	}
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", s.keyspace.Name, s.table.Name, strings.Join(relations, " AND "))
	values := func() []interface{} {
		values := make([]interface{}, 0)
		for _, _ = range s.table.PartitionKeys {
			values = append(values, randRange(p.Min, p.Max))
		}
		return values
	}
	return &Stmt{
		Query:  query,
		Values: values,
	}
}

func (s *schema) genMultiplePartitionQuery(p *PartitionRange) *Stmt {
	relations := []string{}
	for _, pk := range s.table.PartitionKeys {
		relations = append(relations, fmt.Sprintf("%s IN (?)", pk.Name))
	}
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", s.keyspace.Name, s.table.Name, strings.Join(relations, " AND "))
	values := func() []interface{} {
		values := make([]interface{}, 0)
		for _, _ = range s.table.PartitionKeys {
			keys := []int{}
			for i := 0; i < rand.Intn(10); i++ {
				keys = append(keys, randRange(p.Min, p.Max))
			}
			values = append(values, keys)
		}
		return values
	}
	return &Stmt{
		Query:  query,
		Values: values,
	}
}

func (s *schema) genClusteringRangeQuery(p *PartitionRange) *Stmt {
	relations := []string{}
	for _, pk := range s.table.PartitionKeys {
		relations = append(relations, fmt.Sprintf("%s = ?", pk.Name))
	}
	for _, ck := range s.table.ClusteringKeys {
		relations = append(relations, fmt.Sprintf("%s > ? AND %s < ?", ck.Name, ck.Name))
	}
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", s.keyspace.Name, s.table.Name, strings.Join(relations, " AND "))
	values := func() []interface{} {
		values := make([]interface{}, 0)
		for _, _ = range s.table.PartitionKeys {
			values = append(values, randRange(p.Min, p.Max))
		}
		for _, _ = range s.table.ClusteringKeys {
			start := randRange(p.Min, p.Max)
			end := start + randRange(p.Min, p.Max)
			values = append(values, start)
			values = append(values, end)
		}
		return values
	}
	return &Stmt{
		Query:  query,
		Values: values,
	}
}

func (s *schema) genClusteringRangeQueryComplex(p *PartitionRange) *Stmt {
	relations := []string{}
	for _, pk := range s.table.PartitionKeys {
		relations = append(relations, fmt.Sprintf("%s = ?", pk.Name))
	}
	for _, ck := range s.table.ClusteringKeys {
		relations = append(relations, fmt.Sprintf("%s > ? AND %s < ? OR %s > ? AND %s < ?", ck.Name, ck.Name, ck.Name, ck.Name))
	}
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", s.keyspace.Name, s.table.Name, strings.Join(relations, " AND "))
	values := func() []interface{} {
		values := make([]interface{}, 0)
		for _, _ = range s.table.PartitionKeys {
			values = append(values, randRange(p.Min, p.Max))
		}
		for _, _ = range s.table.ClusteringKeys {
			start := randRange(p.Min, p.Max)
			end := start + randRange(p.Min, p.Max)
			values = append(values, start)
			values = append(values, end)
			start = randRange(p.Min, p.Max)
			end = start + randRange(p.Min, p.Max)
			values = append(values, start)
			values = append(values, end)
		}
		return values
	}
	return &Stmt{
		Query:  query,
		Values: values,
	}
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
