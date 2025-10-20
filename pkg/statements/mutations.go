// Copyright 2025 ScyllaDB
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

package statements

import (
	"context"

	"github.com/scylladb/gemini/pkg/typedef"
)

func (g *Generator) MutateStatement(ctx context.Context, generateDelete bool) (*typedef.Stmt, error) {
	g.table.RLock()
	defer g.table.RUnlock()

	var filterDeletes []StatementType

	if !generateDelete {
		filterDeletes = []StatementType{StatementTypeDelete}
	}

	switch g.ratioController.GetMutationStatementType(filterDeletes...) {
	case StatementTypeInsert:
		if g.table.IsCounterTable() {
			return g.Update(ctx)
		}

		if g.ratioController.GetInsertSubtype() == InsertJSONStatement {
			if g.table.KnownIssues[typedef.KnownIssuesJSONWithTuples] {
				return g.Insert(ctx)
			}

			return g.InsertJSON(ctx)
		}

		return g.Insert(ctx)
	case StatementTypeUpdate:
		if g.table.IsCounterTable() {
			return g.Update(ctx)
		}

		// TODO(CodeLieutenant): Update statement support expected in v2.1.0
		//       Falling back to Insert for now, until everything else is stable

		if g.ratioController.GetInsertSubtype() == InsertJSONStatement {
			return g.InsertJSON(ctx)
		}

		return g.Insert(ctx)
	case StatementTypeDelete:
		return g.Delete(ctx)
	default:
		panic("Invalid mutation statement type")
	}
}
