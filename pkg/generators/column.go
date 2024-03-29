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
	"github.com/scylladb/gemini/pkg/typedef"
)

func CreateIndexesForColumn(table *typedef.Table, maxIndexes int) []typedef.IndexDef {
	createdCount := 0
	indexes := make([]typedef.IndexDef, 0, maxIndexes)
	for i, col := range table.Columns {
		if col.Type.Indexable() && typedef.TypesForIndex.Contains(col.Type) {
			indexes = append(indexes, typedef.IndexDef{
				IndexName:  GenIndexName(table.Name+"_col", i),
				ColumnName: table.Columns[i].Name,
				Column:     table.Columns[i],
			})
			createdCount++
		}
		if createdCount == maxIndexes {
			break
		}
	}
	return indexes
}
