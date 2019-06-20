package gemini

import (
	"encoding/json"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"gopkg.in/inf.v0"
)

var (
	millenium = time.Date(1999, 12, 31, 23, 59, 59, 0, time.UTC)
)

var prettytests = []struct {
	typ      Type
	query    string
	values   []interface{}
	expected string
}{
	{
		typ:      TYPE_ASCII,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{"a"},
		expected: "SELECT * FROM tbl WHERE pk0='a'",
	},
	{
		typ:      TYPE_BIGINT,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{big.NewInt(10)},
		expected: "SELECT * FROM tbl WHERE pk0=10",
	},
	{
		typ:      TYPE_BLOB,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{"a"},
		expected: "SELECT * FROM tbl WHERE pk0=textasblob('a')",
	},
	{
		typ:      TYPE_BOOLEAN,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{true},
		expected: "SELECT * FROM tbl WHERE pk0=true",
	},
	{
		typ:      TYPE_DATE,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{millenium.Format("2006-01-02")},
		expected: "SELECT * FROM tbl WHERE pk0='1999-12-31'",
	},
	{
		typ:      TYPE_DECIMAL,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{inf.NewDec(1000, 0)},
		expected: "SELECT * FROM tbl WHERE pk0=1000",
	},
	{
		typ:      TYPE_DOUBLE,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{10.0},
		expected: "SELECT * FROM tbl WHERE pk0=10.00",
	},
	{
		typ:      TYPE_DURATION,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{10 * time.Minute},
		expected: "SELECT * FROM tbl WHERE pk0=10m0s",
	},
	{
		typ:      TYPE_FLOAT,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{10.0},
		expected: "SELECT * FROM tbl WHERE pk0=10.00",
	},
	{
		typ:      TYPE_INET,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{net.ParseIP("192.168.0.1")},
		expected: "SELECT * FROM tbl WHERE pk0='192.168.0.1'",
	},
	{
		typ:      TYPE_INT,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{10},
		expected: "SELECT * FROM tbl WHERE pk0=10",
	},
	{
		typ:      TYPE_SMALLINT,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{2},
		expected: "SELECT * FROM tbl WHERE pk0=2",
	},
	{
		typ:      TYPE_TEXT,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{"a"},
		expected: "SELECT * FROM tbl WHERE pk0='a'",
	},
	{
		typ:      TYPE_TIME,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{millenium},
		expected: "SELECT * FROM tbl WHERE pk0='" + millenium.Format(time.RFC3339) + "'",
	},
	{
		typ:      TYPE_TIMESTAMP,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{millenium},
		expected: "SELECT * FROM tbl WHERE pk0='" + millenium.Format(time.RFC3339) + "'",
	},
	{
		typ:      TYPE_TIMEUUID,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{"63176980-bfde-11d3-bc37-1c4d704231dc"},
		expected: "SELECT * FROM tbl WHERE pk0=63176980-bfde-11d3-bc37-1c4d704231dc",
	},
	{
		typ:      TYPE_TINYINT,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{1},
		expected: "SELECT * FROM tbl WHERE pk0=1",
	},
	{
		typ:      TYPE_UUID,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{"63176980-bfde-11d3-bc37-1c4d704231dc"},
		expected: "SELECT * FROM tbl WHERE pk0=63176980-bfde-11d3-bc37-1c4d704231dc",
	},
	{
		typ:      TYPE_VARCHAR,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{"a"},
		expected: "SELECT * FROM tbl WHERE pk0='a'",
	},
	{
		typ:      TYPE_VARINT,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{big.NewInt(1001)},
		expected: "SELECT * FROM tbl WHERE pk0=1001",
	},
	{
		typ: BagType{
			Kind:   "set",
			Type:   TYPE_ASCII,
			Frozen: false,
		},
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{[]string{"a", "b"}},
		expected: "SELECT * FROM tbl WHERE pk0={'a','b'}",
	},
	{
		typ: BagType{
			Kind:   "list",
			Type:   TYPE_ASCII,
			Frozen: false,
		},
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{[]string{"a", "b"}},
		expected: "SELECT * FROM tbl WHERE pk0={'a','b'}",
	},
	{
		typ: MapType{
			KeyType:   TYPE_ASCII,
			ValueType: TYPE_ASCII,
			Frozen:    false,
		},
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{map[string]string{"a": "b"}},
		expected: "SELECT * FROM tbl WHERE pk0={a:'b'}",
	},
	{
		typ: MapType{
			KeyType:   TYPE_ASCII,
			ValueType: TYPE_BLOB,
			Frozen:    false,
		},
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{map[string]string{"a": "b"}},
		expected: "SELECT * FROM tbl WHERE pk0={a:textasblob('b')}",
	},
	{
		typ: TupleType{
			Types:  []SimpleType{TYPE_ASCII},
			Frozen: false,
		},
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{"a"},
		expected: "SELECT * FROM tbl WHERE pk0='a'",
	},
	{
		typ: TupleType{
			Types:  []SimpleType{TYPE_ASCII, TYPE_ASCII},
			Frozen: false,
		},
		query:    "SELECT * FROM tbl WHERE pk0={?,?}",
		values:   []interface{}{"a", "b"},
		expected: "SELECT * FROM tbl WHERE pk0={'a','b'}",
	},
}

func TestCQLPretty(t *testing.T) {
	for _, p := range prettytests {
		result, _ := p.typ.CQLPretty(p.query, p.values)
		if result != p.expected {
			t.Fatalf("expected '%s', got '%s' for values %v and type '%v'", p.expected, result, p.values, p.typ)
		}
	}
}

func TestMarshalUnmarshal(t *testing.T) {
	sc := &SchemaConfig{
		MaxTupleParts: 2,
		MaxUDTParts:   2,
	}
	columns := Columns{
		{
			Name: genColumnName("col", 0),
			Type: genMapType(sc),
		},
		{
			Name: genColumnName("col", 1),
			Type: genSetType(sc),
		},
		{
			Name: genColumnName("col", 2),
			Type: genListType(sc),
		},
		{
			Name: genColumnName("col", 3),
			Type: genTupleType(sc),
		},
		{
			Name: genColumnName("col", 4),
			Type: genUDTType(sc),
		},
	}
	s1 := &Schema{
		Tables: []*Table{
			{
				Name: "table",
				PartitionKeys: Columns{
					{
						Name: genColumnName("pk", 0),
						Type: genSimpleType(sc),
					},
				},
				ClusteringKeys: Columns{
					{
						Name: genColumnName("ck", 0),
						Type: genSimpleType(sc),
					},
				},
				Columns: columns,
				Indexes: []IndexDef{
					{
						Name:   genIndexName("col", 0),
						Column: columns[0].Name,
					},
					{
						Name:   genIndexName("col", 1),
						Column: columns[1].Name,
					},
				},
				MaterializedViews: []MaterializedView{
					{
						Name: "table1_mv_0",
						PartitionKeys: []ColumnDef{
							{
								Name: "pk_mv_0",
								Type: genListType(sc),
							},
							{
								Name: "pk_mv_1",
								Type: genTupleType(sc),
							},
						},
						ClusteringKeys: []ColumnDef{
							{
								Name: "ck_mv_0",
								Type: genSetType(sc),
							},
							{
								Name: "ck_mv_1",
								Type: genUDTType(sc),
							},
						},
					},
				},
			},
		},
	}

	b, err := json.MarshalIndent(s1, "  ", "  ")
	if err != nil {
		t.Fatalf("unable to marshal json, error=%s\n", err)
	}

	s2 := &Schema{}
	if err := json.Unmarshal(b, &s2); err != nil {
		t.Fatalf("unable to unmarshal json, error=%s\n", err)
	}

	if diff := cmp.Diff(s1, s2); diff != "" {
		t.Errorf("schema not the same after marshal/unmarshal, diff=%s", diff)
	}
}
