package gemini

import (
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	"net"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"
)

const (
	TYPE_ASCII     = SimpleType("ascii")
	TYPE_BIGINT    = SimpleType("bigint")
	TYPE_BLOB      = SimpleType("blob")
	TYPE_BOOLEAN   = SimpleType("boolean")
	TYPE_DATE      = SimpleType("date")
	TYPE_DECIMAL   = SimpleType("decimal")
	TYPE_DOUBLE    = SimpleType("double")
	TYPE_DURATION  = SimpleType("duration")
	TYPE_FLOAT     = SimpleType("float")
	TYPE_INET      = SimpleType("inet")
	TYPE_INT       = SimpleType("int")
	TYPE_SMALLINT  = SimpleType("smallint")
	TYPE_TEXT      = SimpleType("text")
	TYPE_TIME      = SimpleType("time")
	TYPE_TIMESTAMP = SimpleType("timestamp")
	TYPE_TIMEUUID  = SimpleType("timeuuid")
	TYPE_TINYINT   = SimpleType("tinyint")
	TYPE_UUID      = SimpleType("uuid")
	TYPE_VARCHAR   = SimpleType("varchar")
	TYPE_VARINT    = SimpleType("varint")
)

// TODO: Add support for time when gocql bug is fixed.
var (
	pkTypes = []SimpleType{TYPE_ASCII, TYPE_BIGINT, TYPE_BLOB, TYPE_DATE, TYPE_DECIMAL, TYPE_DOUBLE, TYPE_FLOAT, TYPE_INET, TYPE_INT, TYPE_SMALLINT, TYPE_TEXT /*TYPE_TIME,*/, TYPE_TIMESTAMP, TYPE_TIMEUUID, TYPE_TINYINT, TYPE_UUID, TYPE_VARCHAR, TYPE_VARINT}
	types   = append(append([]SimpleType{}, pkTypes...), TYPE_BOOLEAN, TYPE_DURATION)
)

type Keyspace struct {
	Name string `json:"name"`
}

type ColumnDef struct {
	Name string
	Type Type
}

type Type interface {
	Name() string
	CQLDef() string
	CQLHolder() string
	GenValue(*PartitionRange) []interface{}
	GenValueRange(p *PartitionRange) ([]interface{}, []interface{})
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

type SimpleType string

func (st SimpleType) Name() string {
	return string(st)
}

func (st SimpleType) CQLDef() string {
	return string(st)
}

func (st SimpleType) CQLHolder() string {
	return "?"
}

func (st SimpleType) GenValue(p *PartitionRange) []interface{} {
	var val interface{}
	switch st {
	case TYPE_ASCII, TYPE_BLOB, TYPE_TEXT, TYPE_VARCHAR:
		val = randStringWithTime(nonEmptyRandIntRange(p.Max, p.Max, 10), randTime())
	case TYPE_BIGINT:
		val = rand.Int63()
	case TYPE_BOOLEAN:
		val = rand.Int()%2 == 0
	case TYPE_DATE:
		val = randDate()
	case TYPE_TIME, TYPE_TIMESTAMP:
		val = randTime()
	case TYPE_DECIMAL:
		val = inf.NewDec(randInt64Range(int64(p.Min), int64(p.Max)), 3)
	case TYPE_DOUBLE:
		val = randFloat64Range(float64(p.Min), float64(p.Max))
	case TYPE_DURATION:
		val = (time.Minute * time.Duration(randIntRange(p.Min, p.Max))).String()
	case TYPE_FLOAT:
		val = randFloat32Range(float32(p.Min), float32(p.Max))
	case TYPE_INET:
		val = net.ParseIP(randIpV4Address(rand.Intn(255), 2))
	case TYPE_INT:
		val = nonEmptyRandIntRange(p.Min, p.Max, 10)
	case TYPE_SMALLINT:
		val = int16(nonEmptyRandIntRange(p.Min, p.Max, 10))
	case TYPE_TIMEUUID, TYPE_UUID:
		r := gocql.UUIDFromTime(randTime())
		val = r.String()
	case TYPE_TINYINT:
		val = int8(nonEmptyRandIntRange(p.Min, p.Max, 10))
	case TYPE_VARINT:
		val = big.NewInt(randInt64Range(int64(p.Min), int64(p.Max)))
	default:
		panic(fmt.Sprintf("generate value: not supported type %s", st))
	}
	return []interface{}{
		val,
	}
}

func (st SimpleType) GenValueRange(p *PartitionRange) ([]interface{}, []interface{}) {
	var (
		left  interface{}
		right interface{}
	)
	switch st {
	case TYPE_ASCII, TYPE_BLOB, TYPE_TEXT, TYPE_VARCHAR:
		startTime := randTime()
		start := nonEmptyRandIntRange(p.Min, p.Max, 10)
		end := start + nonEmptyRandIntRange(p.Min, p.Max, 10)
		left = nonEmptyRandStringWithTime(start, startTime)
		right = nonEmptyRandStringWithTime(end, randTimeNewer(startTime))
	case TYPE_BIGINT:
		start := nonEmptyRandInt64Range(int64(p.Min), int64(p.Max), 10)
		end := start + nonEmptyRandInt64Range(int64(p.Min), int64(p.Max), 10)
		left = start
		right = end
	case TYPE_DATE, TYPE_TIME, TYPE_TIMESTAMP:
		start := randTime()
		end := randTimeNewer(start)
		left = start
		right = end
	case TYPE_DECIMAL:
		start := nonEmptyRandInt64Range(int64(p.Min), int64(p.Max), 10)
		end := start + nonEmptyRandInt64Range(int64(p.Min), int64(p.Max), 10)
		left = inf.NewDec(start, 3)
		right = inf.NewDec(end, 3)
	case TYPE_DOUBLE:
		start := nonEmptyRandFloat64Range(float64(p.Min), float64(p.Max), 10)
		end := start + nonEmptyRandFloat64Range(float64(p.Min), float64(p.Max), 10)
		left = start
		right = end
	case TYPE_DURATION:
		start := time.Minute * time.Duration(nonEmptyRandIntRange(p.Min, p.Max, 10))
		end := start + time.Minute*time.Duration(nonEmptyRandIntRange(p.Min, p.Max, 10))
		left = start
		right = end
	case TYPE_FLOAT:
		start := nonEmptyRandFloat32Range(float32(p.Min), float32(p.Max), 10)
		end := start + nonEmptyRandFloat32Range(float32(p.Min), float32(p.Max), 10)
		left = start
		right = end
	case TYPE_INET:
		start := randIpV4Address(0, 3)
		end := randIpV4Address(255, 3)
		left = net.ParseIP(start)
		right = net.ParseIP(end)
	case TYPE_INT:
		start := nonEmptyRandIntRange(p.Min, p.Max, 10)
		end := start + nonEmptyRandIntRange(p.Min, p.Max, 10)
		left = start
		right = end
	case TYPE_SMALLINT:
		start := int16(nonEmptyRandIntRange(p.Min, p.Max, 10))
		end := start + int16(nonEmptyRandIntRange(p.Min, p.Max, 10))
		left = start
		right = end
	case TYPE_TIMEUUID, TYPE_UUID:
		start := randTime()
		end := randTimeNewer(start)
		left = gocql.UUIDFromTime(start).String()
		right = gocql.UUIDFromTime(end).String()
	case TYPE_TINYINT:
		start := int8(nonEmptyRandIntRange(p.Min, p.Max, 10))
		end := start + int8(nonEmptyRandIntRange(p.Min, p.Max, 10))
		left = start
		right = end
	case TYPE_VARINT:
		end := &big.Int{}
		start := big.NewInt(randInt64Range(int64(p.Min), int64(p.Max)))
		end.Set(start)
		end = end.Add(start, big.NewInt(randInt64Range(int64(p.Min), int64(p.Max))))
		left = start
		right = end
	default:
		panic(fmt.Sprintf("generate value range: not supported type %s", st))
	}
	return []interface{}{left}, []interface{}{right}
}

type TupleType struct {
	Types []SimpleType
}

func (tt TupleType) Name() string {
	names := make([]string, len(tt.Types), len(tt.Types))
	for i, t := range tt.Types {
		names[i] = t.Name()
	}
	return "Type: " + strings.Join(names, ",")
}

func (tt TupleType) CQLDef() string {
	names := make([]string, len(tt.Types), len(tt.Types))
	for i, t := range tt.Types {
		names[i] = t.Name()
	}
	return "tuple<" + strings.Join(names, ",") + ">"
}

func (tt TupleType) CQLHolder() string {
	return "(" + strings.TrimRight(strings.Repeat("?,", len(tt.Types)), ",") + ")"
}

func (tt TupleType) GenValue(p *PartitionRange) []interface{} {
	vals := make([]interface{}, 0, len(tt.Types))
	for _, t := range tt.Types {
		vals = append(vals, t.GenValue(p)...)
	}
	return vals
}

func (tt TupleType) GenValueRange(p *PartitionRange) ([]interface{}, []interface{}) {
	left := make([]interface{}, 0, len(tt.Types))
	right := make([]interface{}, 0, len(tt.Types))
	for _, t := range tt.Types {
		ttLeft, ttRight := t.GenValueRange(p)
		left = append(left, ttLeft...)
		right = append(right, ttRight...)
	}
	return left, right
}

func genColumnName(prefix string, idx int) string {
	return fmt.Sprintf("%s%d", prefix, idx)
}

func genColumnType(numColumns int) Type {
	n := rand.Intn(numColumns + 1)
	switch n {
	case numColumns:
		return genTupleType()
	default:
		return genSimpleType()
	}
}

func genSimpleType() SimpleType {
	return types[rand.Intn(len(types))]
}

func genTupleType() Type {
	n := rand.Intn(5)
	if n == 0 {
		n = 1
	}
	typeList := make([]SimpleType, n, n)
	for i := 0; i < n; i++ {
		typeList[i] = genSimpleType()
	}
	return TupleType{
		Types: typeList,
	}
}

func genPrimaryKeyColumnType() Type {
	n := rand.Intn(len(pkTypes))
	return types[n]
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
			columns = append(columns, fmt.Sprintf("%s %s", pk.Name, pk.Type.CQLDef()))
		}
		for _, ck := range t.ClusteringKeys {
			clusteringKeys = append(clusteringKeys, ck.Name)
			columns = append(columns, fmt.Sprintf("%s %s", ck.Name, ck.Type.CQLDef()))
		}
		for _, cdef := range t.Columns {
			columns = append(columns, fmt.Sprintf("%s %s", cdef.Name, cdef.Type.CQLDef()))
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

func (s *Schema) GenInsertStmt(t Table, p *PartitionRange) (*Stmt, error) {
	var (
		columns      []string
		placeholders []string
	)
	values := make([]interface{}, 0)
	for _, pk := range t.PartitionKeys {
		columns = append(columns, pk.Name)
		placeholders = append(placeholders, pk.Type.CQLHolder())
		values = appendValue(pk.Type, p, values)
	}
	for _, ck := range t.ClusteringKeys {
		columns = append(columns, ck.Name)
		placeholders = append(placeholders, ck.Type.CQLHolder())
		values = appendValue(ck.Type, p, values)
	}
	for _, cdef := range t.Columns {
		columns = append(columns, cdef.Name)
		placeholders = append(placeholders, cdef.Type.CQLHolder())
		values = appendValue(cdef.Type, p, values)
	}
	query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", s.Keyspace.Name, t.Name, strings.Join(columns, ","), strings.Join(placeholders, ","))
	return &Stmt{
		Query: query,
		Values: func() []interface{} {
			return values
		},
	}, nil
}

func (s *Schema) GenInsertJsonStmt(t Table, p *PartitionRange) (*Stmt, error) {
	var (
		values map[string]interface{}
	)
	values = make(map[string]interface{})
	for _, pk := range t.PartitionKeys {
		values[pk.Name] = pk.Type.GenValue(p)
	}
	for _, ck := range t.ClusteringKeys {
		values[ck.Name] = ck.Type.GenValue(p)
	}
	for _, cdef := range t.Columns {
		values[cdef.Name] = cdef.Type.GenValue(p)
	}
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
	}, nil
}

func (s *Schema) GenDeleteRows(t Table, p *PartitionRange) (*Stmt, error) {
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
	}, nil
}

func (s *Schema) GenMutateStmt(t Table, p *PartitionRange) (*Stmt, error) {
	switch n := rand.Intn(1000); n {
	case 10, 100:
		return s.GenDeleteRows(t, p)
	default:
		switch n := rand.Intn(2); n {
		//case 0:
		//	return s.GenInsertJsonStmt(t, p)
		default:
			return s.GenInsertStmt(t, p)
		}
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
	var (
		relations []string
		values    []interface{}
	)

	if len(t.Indexes) == 0 {
		return nil
	}
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
	idx := rand.Intn(len(t.Indexes))
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s AND %s=?", s.Keyspace.Name, t.Name, strings.Join(relations, " AND "), t.Indexes[idx].Column.Name)
	values = appendValue(t.Indexes[idx].Column.Type, p, nil)
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
