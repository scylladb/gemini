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
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"gopkg.in/inf.v0"
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
	StmtCache: StmtCache{
		//nolint:lll
		Query: SimpleQuery{
			`INSERT INTO tbl(col1, col2, col3, col4, col5, col6,col7,col8,col9,cold10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);`,
		},
		QueryType: InsertStatementType,
		Types: Types{
			TypeAscii,
			TypeBigint,
			TypeBlob,
			TypeBoolean,
			TypeDate,
			TypeDecimal,
			TypeDouble,
			TypeDuration,
			TypeFloat,
			TypeInet,
			TypeInt,
			TypeSmallint,
			TypeText,
			TypeTime,
			TypeTimestamp,
			TypeTimeuuid,
			TypeUuid,
			TypeTinyint,
			TypeVarchar,
			TypeVarint,
		},
	},
	Values: Values{
		"a",
		big.NewInt(10),
		"a",
		true,
		millennium.Format(time.DateOnly),
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
