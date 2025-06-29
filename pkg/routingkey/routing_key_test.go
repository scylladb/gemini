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
		values map[string][]any
		want   []byte
	}
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
					values: map[string][]any{"pk0": {1641072984}},
					want:   decodeHex("61d0c958"),
				},
				{
					values: map[string][]any{"pk0": {1904972303}},
					want:   decodeHex("718b920f"),
				},
				{
					values: map[string][]any{"pk0": {1236194666}},
					want:   decodeHex("49aed56a"),
				},
				{
					values: map[string][]any{"pk0": {2095188122}},
					want:   decodeHex("7ce2089a"),
				},
				{
					values: map[string][]any{"pk0": {45882928}},
					want:   decodeHex("02bc1e30"),
				},
				{
					values: map[string][]any{"pk0": {1057932065}},
					want:   decodeHex("3f0ec321"),
				},
				{
					values: map[string][]any{"pk0": {1236194666}},
					want:   decodeHex("49aed56a"),
				},
				{
					values: map[string][]any{"pk0": {812457792}},
					want:   decodeHex("306d1f40"),
				},
				{
					values: map[string][]any{"pk0": {1334454052}},
					want:   decodeHex("4f8a2724"),
				},
				{
					values: map[string][]any{"pk0": {45882928}},
					want:   decodeHex("02bc1e30"),
				},
				{
					values: map[string][]any{"pk0": {1904972303}},
					want:   decodeHex("718b920f"),
				},
				{
					values: map[string][]any{"pk0": {368842197}},
					want:   decodeHex("15fc15d5"),
				},
				{
					values: map[string][]any{"pk0": {2095188122}},
					want:   decodeHex("7ce2089a"),
				},
				{
					values: map[string][]any{"pk0": {475379656}},
					want:   decodeHex("1c55b7c8"),
				},
				{
					values: map[string][]any{"pk0": {1641072984}},
					want:   decodeHex("61d0c958"),
				},
				{
					values: map[string][]any{"pk0": {904957324}},
					want:   decodeHex("35f08d8c"),
				},
				{
					values: map[string][]any{"pk0": {262309475}},
					want:   decodeHex("0fa28663"),
				},
				{
					values: map[string][]any{"pk0": {1227835653}},
					want:   decodeHex("492f4905"),
				},
				{
					values: map[string][]any{"pk0": {1425448500}},
					want:   decodeHex("54f69e34"),
				},
				{
					values: map[string][]any{"pk0": {597709428}},
					want:   decodeHex("23a05274"),
				},
				{
					values: map[string][]any{"pk0": {1800248233}},
					want:   decodeHex("6b4d9ba9"),
				},
				{
					values: map[string][]any{"pk0": {806697938}},
					want:   decodeHex("30153bd2"),
				},
				{
					values: map[string][]any{"pk0": {2086829109}},
					want:   decodeHex("7c627c35"),
				},
				{
					values: map[string][]any{"pk0": {1630944338}},
					want:   decodeHex("61363c52"),
				},
				{
					values: map[string][]any{"pk0": {168212700}},
					want:   decodeHex("0a06b8dc"),
				},
				{
					values: map[string][]any{"pk0": {168212700}},
					want:   decodeHex("0a06b8dc"),
				},
				{
					values: map[string][]any{"pk0": {2086829109}},
					want:   decodeHex("7c627c35"),
				},
				{
					values: map[string][]any{"pk0": {1800248233}},
					want:   decodeHex("6b4d9ba9"),
				},
				{
					values: map[string][]any{"pk0": {904957324}},
					want:   decodeHex("35f08d8c"),
				},
				{
					values: map[string][]any{"pk0": {806697938}},
					want:   decodeHex("30153bd2"),
				},
				{
					values: map[string][]any{"pk0": {1425448500}},
					want:   decodeHex("54f69e34"),
				},
				{
					values: map[string][]any{"pk0": {1227835653}},
					want:   decodeHex("492f4905"),
				},
				{
					values: map[string][]any{"pk0": {597709428}},
					want:   decodeHex("23a05274"),
				},
				{
					values: map[string][]any{"pk0": {1630944338}},
					want:   decodeHex("61363c52"),
				},
				{
					values: map[string][]any{"pk0": {262309475}},
					want:   decodeHex("0fa28663"),
				},
				{
					values: map[string][]any{"pk0": {1121302931}},
					want:   decodeHex("42d5b993"),
				},
				{
					values: map[string][]any{"pk0": {1683526033}},
					want:   decodeHex("64589191"),
				},
				{
					values: map[string][]any{"pk0": {413686973}},
					want:   decodeHex("18a85cbd"),
				},
				{
					values: map[string][]any{"pk0": {1768299632}},
					want:   decodeHex("69661c70"),
				},
				{
					values: map[string][]any{"pk0": {798338925}},
					want:   decodeHex("2f95af6d"),
				},
				{
					values: map[string][]any{"pk0": {1499238155}},
					want:   decodeHex("595c8f0b"),
				},
				{
					values: map[string][]any{"pk0": {162452846}},
					want:   decodeHex("09aed56e"),
				},
				{
					values: map[string][]any{"pk0": {995951772}},
					want:   decodeHex("3b5d049c"),
				},
				{
					values: map[string][]any{"pk0": {591949574}},
					want:   decodeHex("23486f06"),
				},
				{
					values: map[string][]any{"pk0": {1980296387}},
					want:   decodeHex("7608ecc3"),
				},
				{
					values: map[string][]any{"pk0": {1980296387}},
					want:   decodeHex("7608ecc3"),
				},
				{
					values: map[string][]any{"pk0": {798338925}},
					want:   decodeHex("2f95af6d"),
				},
				{
					values: map[string][]any{"pk0": {1121302931}},
					want:   decodeHex("42d5b993"),
				},
				{
					values: map[string][]any{"pk0": {1499238155}},
					want:   decodeHex("595c8f0b"),
				},
				{
					values: map[string][]any{"pk0": {162452846}},
					want:   decodeHex("09aed56e"),
				},
				{
					values: map[string][]any{"pk0": {413686973}},
					want:   decodeHex("18a85cbd"),
				},
				{
					values: map[string][]any{"pk0": {1683526033}},
					want:   decodeHex("64589191"),
				},
				{
					values: map[string][]any{"pk0": {1768299632}},
					want:   decodeHex("69661c70"),
				},
				{
					values: map[string][]any{"pk0": {995951772}},
					want:   decodeHex("3b5d049c"),
				},
				{
					values: map[string][]any{"pk0": {591949574}},
					want:   decodeHex("23486f06"),
				},
				{
					values: map[string][]any{"pk0": {1417141266}},
					want:   decodeHex("5477dc12"),
				},
				{
					values: map[string][]any{"pk0": {1417141266}},
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
					values: map[string][]any{"pk0": {154109775}, "pk1": {10044141}},
					want:   decodeHex("0004092f874f000004009942ed00"),
				},
				{
					values: map[string][]any{"pk0": {1313258788}, "pk1": {1466181868}},
					want:   decodeHex("00044e46bd24000004576428ec00"),
				},
				{
					values: map[string][]any{"pk0": {287541659}, "pk1": {266079166}},
					want:   decodeHex("00041123899b0000040fdc0bbe00"),
				},
				{
					values: map[string][]any{"pk0": {1555318302}, "pk1": {1661168631}},
					want:   decodeHex("00045cb4461e00000463036bf700"),
				},
				{
					values: map[string][]any{"pk0": {441838458}, "pk1": {453773327}},
					want:   decodeHex("00041a55eb7a0000041b0c080f00"),
				},
				{
					values: map[string][]any{"pk0": {876168687}, "pk1": {910346726}},
					want:   decodeHex("0004343945ef0000043642c9e600"),
				},
				{
					values: map[string][]any{"pk0": {682056909}, "pk1": {682069788}},
					want:   decodeHex("000428a75ccd00000428a78f1c00"),
				},
				{
					values: map[string][]any{"pk0": {1099404621}, "pk1": {1260277606}},
					want:   decodeHex("00044187954d0000044b1e4f6600"),
				},
				{
					values: map[string][]any{"pk0": {48146003}, "pk1": {72800984}},
					want:   decodeHex("000402dea6530000040456dad800"),
				},
				{
					values: map[string][]any{"pk0": {1045015705}, "pk1": {1017672610}},
					want:   decodeHex("00043e49ac990000043ca873a200"),
				},
				{
					values: map[string][]any{"pk0": {1338382691}, "pk1": {1378061000}},
					want:   decodeHex("00044fc6196300000452238ac800"),
				},
				{
					values: map[string][]any{"pk0": {441838458}, "pk1": {453773327}},
					want:   decodeHex("00041a55eb7a0000041b0c080f00"),
				},
				{
					values: map[string][]any{"pk0": {1555318302}, "pk1": {1661168631}},
					want:   decodeHex("00045cb4461e00000463036bf700"),
				},
				{
					values: map[string][]any{"pk0": {287541659}, "pk1": {266079166}},
					want:   decodeHex("00041123899b0000040fdc0bbe00"),
				},
				{
					values: map[string][]any{"pk0": {1085821086}, "pk1": {1230891834}},
					want:   decodeHex("000440b8509e000004495deb3a00"),
				},
				{
					values: map[string][]any{"pk0": {2118757525}, "pk1": {2091414430}},
					want:   decodeHex("00047e49ac950000047ca8739e00"),
				},
				{
					values: map[string][]any{"pk0": {723092642}, "pk1": {797106325}},
					want:   decodeHex("00042b1984a20000042f82e09500"),
				},
				{
					values: map[string][]any{"pk0": {1900826555}, "pk1": {1739648564}},
					want:   decodeHex("0004714c4fbb00000467b0ee3400"),
				},
				{
					values: map[string][]any{"pk0": {48146003}, "pk1": {72800984}},
					want:   decodeHex("000402dea6530000040456dad800"),
				},
				{
					values: map[string][]any{"pk0": {1338382691}, "pk1": {1378061000}},
					want:   decodeHex("00044fc6196300000452238ac800"),
				},
				{
					values: map[string][]any{"pk0": {1045015705}, "pk1": {1017672610}},
					want:   decodeHex("00043e49ac990000043ca873a200"),
				},
				{
					values: map[string][]any{"pk0": {290624561}, "pk1": {281368602}},
					want:   decodeHex("00041152943100000410c5581a00"),
				},
				{
					values: map[string][]any{"pk0": {1628942696}, "pk1": {1544546792}},
					want:   decodeHex("00046117b1680000045c0fe9e800"),
				},
				{
					values: map[string][]any{"pk0": {439094547}, "pk1": {457987688}},
					want:   decodeHex("00041a2c0d130000041b4c566800"),
				},
				{
					values: map[string][]any{"pk0": {1085821086}, "pk1": {1230891834}},
					want:   decodeHex("000440b8509e000004495deb3a00"),
				},
				{
					values: map[string][]any{"pk0": {2118757525}, "pk1": {2091414430}},
					want:   decodeHex("00047e49ac950000047ca8739e00"),
				},
				{
					values: map[string][]any{"pk0": {55214822}, "pk1": {972879}},
					want:   decodeHex("0004034a82e6000004000ed84f00"),
				},
				{
					values: map[string][]any{"pk0": {723092642}, "pk1": {797106325}},
					want:   decodeHex("00042b1984a20000042f82e09500"),
				},
				{
					values: map[string][]any{"pk0": {1900826555}, "pk1": {1739648564}},
					want:   decodeHex("0004714c4fbb00000467b0ee3400"),
				},
				{
					values: map[string][]any{"pk0": {1307654654}, "pk1": {1374426858}},
					want:   decodeHex("00044df139fe00000451ec16ea00"),
				},
				{
					values: map[string][]any{"pk0": {958724071}, "pk1": {967981814}},
					want:   decodeHex("00043924f7e700000439b23af600"),
				},
				{
					values: map[string][]any{"pk0": {290624561}, "pk1": {281368602}},
					want:   decodeHex("00041152943100000410c5581a00"),
				},
				{
					values: map[string][]any{"pk0": {1628942696}, "pk1": {1544546792}},
					want:   decodeHex("00046117b1680000045c0fe9e800"),
				},
				{
					values: map[string][]any{"pk0": {439094547}, "pk1": {457987688}},
					want:   decodeHex("00041a2c0d130000041b4c566800"),
				},
				{
					values: map[string][]any{"pk0": {691470817}, "pk1": {838419659}},
					want:   decodeHex("0004293701e100000431f944cb00"),
				},
				{
					values: map[string][]any{"pk0": {2032465891}, "pk1": {2041723634}},
					want:   decodeHex("00047924f7e300000479b23af200"),
				},
				{
					values: map[string][]any{"pk0": {1724551686}, "pk1": {1929537609}},
					want:   decodeHex("000466ca92060000047302684900"),
				},
				{
					values: map[string][]any{"pk0": {958724071}, "pk1": {967981814}},
					want:   decodeHex("00043924f7e700000439b23af600"),
				},
				{
					values: map[string][]any{"pk0": {1307654654}, "pk1": {1374426858}},
					want:   decodeHex("00044df139fe00000451ec16ea00"),
				},
				{
					values: map[string][]any{"pk0": {2032465891}, "pk1": {2041723634}},
					want:   decodeHex("00047924f7e300000479b23af200"),
				},
				{
					values: map[string][]any{"pk0": {691470817}, "pk1": {838419659}},
					want:   decodeHex("0004293701e100000431f944cb00"),
				},
				{
					values: map[string][]any{"pk0": {1724551686}, "pk1": {1929537609}},
					want:   decodeHex("000466ca92060000047302684900"),
				},
				{
					values: map[string][]any{"pk0": {55214822}, "pk1": {972879}},
					want:   decodeHex("0004034a82e6000004000ed84f00"),
				},
				{
					values: map[string][]any{"pk0": {1207341986}, "pk1": {1257554950}},
					want:   decodeHex("000447f693a20000044af4c40600"),
				},
				{
					values: map[string][]any{"pk0": {1207341986}, "pk1": {1257554950}},
					want:   decodeHex("000447f693a20000044af4c40600"),
				},
			},
		},
	}

	for name := range tests {
		test := tests[name]
		t.Run(name, func(t *testing.T) {
			rkc := routingkey.New(test.table)

			t.Parallel()
			for _, d := range test.data {
				result, err := rkc.CreateRoutingKey(d.values)
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
