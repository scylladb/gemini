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

package routingkey_test

import (
	"encoding/hex"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/routingkey"
	"github.com/scylladb/gemini/pkg/typedef"
)

func TestRoutingKey(t *testing.T) {
	t.Parallel()
	type data struct {
		values typedef.Values
		want   []byte
	}
	rkc := &routingkey.Creator{}
	tests := map[string]struct {
		table *typedef.Table
		data  []data
	}{
		"single_partition_key": {
			table: &typedef.Table{
				Name:          "tbl0",
				PartitionKeys: generators.CreatePkColumns(1, "pk"),
			},
			data: []data{
				{
					values: []any{1641072984},
					want:   decodeHex("61d0c958"),
				},
				{
					values: []any{1904972303},
					want:   decodeHex("718b920f"),
				},
				{
					values: []any{1236194666},
					want:   decodeHex("49aed56a"),
				},
				{
					values: []any{2095188122},
					want:   decodeHex("7ce2089a"),
				},
				{
					values: []any{45882928},
					want:   decodeHex("02bc1e30"),
				},
				{
					values: []any{1057932065},
					want:   decodeHex("3f0ec321"),
				},
				{
					values: []any{1236194666},
					want:   decodeHex("49aed56a"),
				},
				{
					values: []any{812457792},
					want:   decodeHex("306d1f40"),
				},
				{
					values: []any{1334454052},
					want:   decodeHex("4f8a2724"),
				},
				{
					values: []any{45882928},
					want:   decodeHex("02bc1e30"),
				},
				{
					values: []any{1904972303},
					want:   decodeHex("718b920f"),
				},
				{
					values: []any{368842197},
					want:   decodeHex("15fc15d5"),
				},
				{
					values: []any{2095188122},
					want:   decodeHex("7ce2089a"),
				},
				{
					values: []any{475379656},
					want:   decodeHex("1c55b7c8"),
				},
				{
					values: []any{1641072984},
					want:   decodeHex("61d0c958"),
				},
				{
					values: []any{904957324},
					want:   decodeHex("35f08d8c"),
				},
				{
					values: []any{262309475},
					want:   decodeHex("0fa28663"),
				},
				{
					values: []any{1227835653},
					want:   decodeHex("492f4905"),
				},
				{
					values: []any{1425448500},
					want:   decodeHex("54f69e34"),
				},
				{
					values: []any{597709428},
					want:   decodeHex("23a05274"),
				},
				{
					values: []any{1800248233},
					want:   decodeHex("6b4d9ba9"),
				},
				{
					values: []any{806697938},
					want:   decodeHex("30153bd2"),
				},
				{
					values: []any{2086829109},
					want:   decodeHex("7c627c35"),
				},
				{
					values: []any{1630944338},
					want:   decodeHex("61363c52"),
				},
				{
					values: []any{168212700},
					want:   decodeHex("0a06b8dc"),
				},
				{
					values: []any{168212700},
					want:   decodeHex("0a06b8dc"),
				},
				{
					values: []any{2086829109},
					want:   decodeHex("7c627c35"),
				},
				{
					values: []any{1800248233},
					want:   decodeHex("6b4d9ba9"),
				},
				{
					values: []any{904957324},
					want:   decodeHex("35f08d8c"),
				},
				{
					values: []any{806697938},
					want:   decodeHex("30153bd2"),
				},
				{
					values: []any{1425448500},
					want:   decodeHex("54f69e34"),
				},
				{
					values: []any{1227835653},
					want:   decodeHex("492f4905"),
				},
				{
					values: []any{597709428},
					want:   decodeHex("23a05274"),
				},
				{
					values: []any{1630944338},
					want:   decodeHex("61363c52"),
				},
				{
					values: []any{262309475},
					want:   decodeHex("0fa28663"),
				},
				{
					values: []any{1121302931},
					want:   decodeHex("42d5b993"),
				},
				{
					values: []any{1683526033},
					want:   decodeHex("64589191"),
				},
				{
					values: []any{413686973},
					want:   decodeHex("18a85cbd"),
				},
				{
					values: []any{1768299632},
					want:   decodeHex("69661c70"),
				},
				{
					values: []any{798338925},
					want:   decodeHex("2f95af6d"),
				},
				{
					values: []any{1499238155},
					want:   decodeHex("595c8f0b"),
				},
				{
					values: []any{162452846},
					want:   decodeHex("09aed56e"),
				},
				{
					values: []any{995951772},
					want:   decodeHex("3b5d049c"),
				},
				{
					values: []any{591949574},
					want:   decodeHex("23486f06"),
				},
				{
					values: []any{1980296387},
					want:   decodeHex("7608ecc3"),
				},
				{
					values: []any{1980296387},
					want:   decodeHex("7608ecc3"),
				},
				{
					values: []any{798338925},
					want:   decodeHex("2f95af6d"),
				},
				{
					values: []any{1121302931},
					want:   decodeHex("42d5b993"),
				},
				{
					values: []any{1499238155},
					want:   decodeHex("595c8f0b"),
				},
				{
					values: []any{162452846},
					want:   decodeHex("09aed56e"),
				},
				{
					values: []any{413686973},
					want:   decodeHex("18a85cbd"),
				},
				{
					values: []any{1683526033},
					want:   decodeHex("64589191"),
				},
				{
					values: []any{1768299632},
					want:   decodeHex("69661c70"),
				},
				{
					values: []any{995951772},
					want:   decodeHex("3b5d049c"),
				},
				{
					values: []any{591949574},
					want:   decodeHex("23486f06"),
				},
				{
					values: []any{1417141266},
					want:   decodeHex("5477dc12"),
				},
				{
					values: []any{1417141266},
					want:   decodeHex("5477dc12"),
				},
			},
		},
		"complex_partition_key": {
			table: &typedef.Table{
				Name:          "tbl0",
				PartitionKeys: generators.CreatePkColumns(2, "pk"),
			},
			data: []data{
				{
					values: []any{154109775, 10044141},
					want:   decodeHex("0004092f874f000004009942ed00"),
				},
				{
					values: []any{1313258788, 1466181868},
					want:   decodeHex("00044e46bd24000004576428ec00"),
				},
				{
					values: []any{287541659, 266079166},
					want:   decodeHex("00041123899b0000040fdc0bbe00"),
				},
				{
					values: []any{1555318302, 1661168631},
					want:   decodeHex("00045cb4461e00000463036bf700"),
				},
				{
					values: []any{441838458, 453773327},
					want:   decodeHex("00041a55eb7a0000041b0c080f00"),
				},
				{
					values: []any{876168687, 910346726},
					want:   decodeHex("0004343945ef0000043642c9e600"),
				},
				{
					values: []any{682056909, 682069788},
					want:   decodeHex("000428a75ccd00000428a78f1c00"),
				},
				{
					values: []any{1099404621, 1260277606},
					want:   decodeHex("00044187954d0000044b1e4f6600"),
				},
				{
					values: []any{48146003, 72800984},
					want:   decodeHex("000402dea6530000040456dad800"),
				},
				{
					values: []any{1045015705, 1017672610},
					want:   decodeHex("00043e49ac990000043ca873a200"),
				},
				{
					values: []any{1338382691, 1378061000},
					want:   decodeHex("00044fc6196300000452238ac800"),
				},
				{
					values: []any{441838458, 453773327},
					want:   decodeHex("00041a55eb7a0000041b0c080f00"),
				},
				{
					values: []any{1555318302, 1661168631},
					want:   decodeHex("00045cb4461e00000463036bf700"),
				},
				{
					values: []any{287541659, 266079166},
					want:   decodeHex("00041123899b0000040fdc0bbe00"),
				},
				{
					values: []any{1085821086, 1230891834},
					want:   decodeHex("000440b8509e000004495deb3a00"),
				},
				{
					values: []any{2118757525, 2091414430},
					want:   decodeHex("00047e49ac950000047ca8739e00"),
				},
				{
					values: []any{723092642, 797106325},
					want:   decodeHex("00042b1984a20000042f82e09500"),
				},
				{
					values: []any{1900826555, 1739648564},
					want:   decodeHex("0004714c4fbb00000467b0ee3400"),
				},
				{
					values: []any{48146003, 72800984},
					want:   decodeHex("000402dea6530000040456dad800"),
				},
				{
					values: []any{1338382691, 1378061000},
					want:   decodeHex("00044fc6196300000452238ac800"),
				},
				{
					values: []any{1045015705, 1017672610},
					want:   decodeHex("00043e49ac990000043ca873a200"),
				},
				{
					values: []any{290624561, 281368602},
					want:   decodeHex("00041152943100000410c5581a00"),
				},
				{
					values: []any{1628942696, 1544546792},
					want:   decodeHex("00046117b1680000045c0fe9e800"),
				},
				{
					values: []any{439094547, 457987688},
					want:   decodeHex("00041a2c0d130000041b4c566800"),
				},
				{
					values: []any{1085821086, 1230891834},
					want:   decodeHex("000440b8509e000004495deb3a00"),
				},
				{
					values: []any{2118757525, 2091414430},
					want:   decodeHex("00047e49ac950000047ca8739e00"),
				},
				{
					values: []any{55214822, 972879},
					want:   decodeHex("0004034a82e6000004000ed84f00"),
				},
				{
					values: []any{723092642, 797106325},
					want:   decodeHex("00042b1984a20000042f82e09500"),
				},
				{
					values: []any{1900826555, 1739648564},
					want:   decodeHex("0004714c4fbb00000467b0ee3400"),
				},
				{
					values: []any{1307654654, 1374426858},
					want:   decodeHex("00044df139fe00000451ec16ea00"),
				},
				{
					values: []any{958724071, 967981814},
					want:   decodeHex("00043924f7e700000439b23af600"),
				},
				{
					values: []any{290624561, 281368602},
					want:   decodeHex("00041152943100000410c5581a00"),
				},
				{
					values: []any{1628942696, 1544546792},
					want:   decodeHex("00046117b1680000045c0fe9e800"),
				},
				{
					values: []any{439094547, 457987688},
					want:   decodeHex("00041a2c0d130000041b4c566800"),
				},
				{
					values: []any{691470817, 838419659},
					want:   decodeHex("0004293701e100000431f944cb00"),
				},
				{
					values: []any{2032465891, 2041723634},
					want:   decodeHex("00047924f7e300000479b23af200"),
				},
				{
					values: []any{1724551686, 1929537609},
					want:   decodeHex("000466ca92060000047302684900"),
				},
				{
					values: []any{958724071, 967981814},
					want:   decodeHex("00043924f7e700000439b23af600"),
				},
				{
					values: []any{1307654654, 1374426858},
					want:   decodeHex("00044df139fe00000451ec16ea00"),
				},
				{
					values: []any{2032465891, 2041723634},
					want:   decodeHex("00047924f7e300000479b23af200"),
				},
				{
					values: []any{691470817, 838419659},
					want:   decodeHex("0004293701e100000431f944cb00"),
				},
				{
					values: []any{1724551686, 1929537609},
					want:   decodeHex("000466ca92060000047302684900"),
				},
				{
					values: []any{55214822, 972879},
					want:   decodeHex("0004034a82e6000004000ed84f00"),
				},
				{
					values: []any{1207341986, 1257554950},
					want:   decodeHex("000447f693a20000044af4c40600"),
				},
				{
					values: []any{1207341986, 1257554950},
					want:   decodeHex("000447f693a20000044af4c40600"),
				},
			},
		},
	}

	for name := range tests {
		test := tests[name]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for _, d := range test.data {
				result, err := rkc.CreateRoutingKey(test.table, d.values)
				if err != nil {
					t.Fatal(err)
				}
				if diff := cmp.Diff(d.want, result); diff != "" {
					t.Fatal(diff)
				}
			}
		})
	}
}

func decodeHex(v string) []byte {
	b, _ := hex.DecodeString(v)
	return b
}
