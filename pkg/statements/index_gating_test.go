// Copyright 2026 ScyllaDB
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

package statements_test

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"

	statements "github.com/scylladb/gemini/pkg/statements"
	"github.com/scylladb/gemini/pkg/typedef"
)

// TestIndexesAreCreatedWithoutMaterializedViews: the default feature set
// creates secondary indexes, so index SELECTs run in a default run. Basic
// creates none.
func TestIndexesAreCreatedWithoutMaterializedViews(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		feature     typedef.CQLFeature
		wantIndexes bool
	}{
		{name: "basic", feature: typedef.CQLFeatureBasic, wantIndexes: false},
		{name: "normal", feature: typedef.CQLFeatureNormal, wantIndexes: true},
		{name: "all", feature: typedef.CQLFeatureAll, wantIndexes: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sc := testSchemaConfig
			sc.CQLFeature = tc.feature
			sc.UseMaterializedViews = false
			sc.MinColumns = 5
			sc.MaxColumns = 10

			indexes := 0
			for seed := range uint64(20) {
				schema := statements.GenSchema(sc, rand.NewPCG(seed, seed))
				for _, table := range schema.Tables {
					indexes += len(table.Indexes)
					assert.Empty(t, table.MaterializedViews)
				}
			}

			if tc.wantIndexes {
				assert.Positive(t, indexes, "the default feature set must create indexes")
			} else {
				assert.Zero(t, indexes)
			}
		})
	}
}

// TestIndexesSkipTinyintColumns: an index query on a tinyint column reads
// about 1/256 of the table. No generated schema indexes a tinyint column.
func TestIndexesSkipTinyintColumns(t *testing.T) {
	t.Parallel()

	sc := testSchemaConfig
	sc.CQLFeature = typedef.CQLFeatureAll
	sc.MinColumns = 10
	sc.MaxColumns = 20

	for seed := range uint64(50) {
		schema := statements.GenSchema(sc, rand.NewPCG(seed, seed))
		for _, table := range schema.Tables {
			for _, idx := range table.Indexes {
				assert.NotEqual(t, typedef.TypeTinyint, idx.Column.Type, "seed %d indexes %s", seed, idx.ColumnName)
			}
		}
	}
}
