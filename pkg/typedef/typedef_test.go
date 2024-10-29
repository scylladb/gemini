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

package typedef

import (
	"fmt"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"gopkg.in/inf.v0"

	"github.com/google/go-cmp/cmp"
)

func TestValues(t *testing.T) {
	t.Parallel()

	tmp := make(Values, 0, 10)
	expected := Values{1, 2, 3, 4, 5, 6, 7}
	expected2 := Values{1, 2, 3, 4, 5, 6, 7, 8, 9}
	expected3 := Values{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	tmp = append(tmp, 1, 2, 3, 4, 5)
	tmp = tmp.CopyFrom(Values{6, 7})
	if !cmp.Equal(tmp, expected) {
		t.Error("%i != %i", tmp, expected)
	}
	tmp = tmp.CopyFrom(Values{8, 9})
	if !cmp.Equal(tmp, expected2) {
		t.Error("%i != %i", tmp, expected)
	}
	tmp = append(tmp, 10)
	if !cmp.Equal(tmp, expected3) {
		t.Error("%i != %i", tmp, expected)
	}
}

var stmt = &Stmt{
	StmtCache: &StmtCache{
		//nolint:lll
		Query:     SimpleQuery{`INSERT INTO tbl(col1, col2, col3, col4, col5, col6,col7,col8,col9,cold10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);`},
		QueryType: InsertStatementType,
		Types: Types{
			TYPE_ASCII,
			TYPE_BIGINT,
			TYPE_BLOB,
			TYPE_BOOLEAN,
			TYPE_DATE,
			TYPE_DECIMAL,
			TYPE_DOUBLE,
			TYPE_DURATION,
			TYPE_FLOAT,
			TYPE_INET,
			TYPE_INT,
			TYPE_SMALLINT,
			TYPE_TEXT,
			TYPE_TIME,
			TYPE_TIMESTAMP,
			TYPE_TIMEUUID,
			TYPE_UUID,
			TYPE_TINYINT,
			TYPE_VARCHAR,
			TYPE_VARINT,
		},
	},
	Values: Values{
		"a",
		big.NewInt(10),
		"a",
		true,
		millennium.Format("2006-01-02"),
		inf.NewDec(1000, 0),
		10.0,
		10 * time.Minute,
		10.0,
		net.ParseIP("192.168.0.1"),
		10,
		2,
		"a",
		millennium.UnixNano(),
		millennium.UnixMilli(),
		"63176980-bfde-11d3-bc37-1c4d704231dc",
		"63176980-bfde-11d3-bc37-1c4d704231dc",
		1,
		"a",
		big.NewInt(1001),
	},
}

func TestPrettyCQL(t *testing.T) {
	t.Parallel()

	query := stmt.PrettyCQL()

	//nolint:lll
	expected := fmt.Sprintf(
		`INSERT INTO tbl(col1, col2, col3, col4, col5, col6,col7,col8,col9,cold10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20) VALUES ('a',10,textasblob('a'),true,'1999-12-31',1000,10.00,10m0s,10.00,'192.168.0.1',10,2,'a','%s','%s',63176980-bfde-11d3-bc37-1c4d704231dc,63176980-bfde-11d3-bc37-1c4d704231dc,1,'a',1001);`,
		millennium.Format("15:04:05.999"),
		millennium.Format("2006-01-02T15:04:05.999-0700"),
	)

	if query != expected {
		t.Error("expected", expected, "got", query)
	}
}

func prettyCQLOld(query string, values Values, types Types) string {
	if len(values) == 0 {
		return query
	}

	k := 0
	out := make([]string, 0, len(values)*2)
	queryChunks := strings.Split(query, "?")
	out = append(out, queryChunks[0])
	qID := 1
	var builder strings.Builder
	for _, typ := range types {
		builder.Reset()
		tupleType, ok := typ.(*TupleType)
		if !ok {
			typ.CQLPretty(&builder, values[k])
			out = append(out, builder.String())
			out = append(out, queryChunks[qID])
			qID++
			k++
			continue
		}
		for _, t := range tupleType.ValueTypes {
			builder.Reset()
			t.CQLPretty(&builder, values[k])
			out = append(out, builder.String())
			out = append(out, queryChunks[qID])
			qID++
			k++
		}
	}
	out = append(out, queryChunks[qID:]...)
	return strings.Join(out, "")
}

func BenchmarkPrettyCQLOLD(b *testing.B) {
	b.Run("New", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			query, _ := stmt.Query.ToCql()
			values := stmt.Values.Copy()
			prettyCQL(query, values, stmt.Types)
		}
	})

	b.Run("Old", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			query, _ := stmt.Query.ToCql()
			values := stmt.Values.Copy()
			prettyCQLOld(query, values, stmt.Types)
		}
	})
}
