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

package generators

import (
	"golang.org/x/exp/rand"

	"github.com/scylladb/gemini/pkg/typedef"
)

func CreatePartitionKeyValues(table *typedef.Table, r *rand.Rand, g *typedef.PartitionRangeConfig) []any {
	values := make([]any, 0, table.PartitionKeysLenValues())
	for _, pk := range table.PartitionKeys {
		values = append(values, pk.Type.GenValue(r, g)...)
	}
	return values
}

func CreatePkColumns(cnt int, prefix string) typedef.Columns {
	var cols typedef.Columns
	for i := 0; i < cnt; i++ {
		cols = append(cols, &typedef.ColumnDef{
			Name: GenColumnName(prefix, i),
			Type: typedef.TYPE_INT,
		})
	}
	return cols
}
