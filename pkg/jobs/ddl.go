package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/generators/statements"
	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/typedef"
)

func (m *mutation) DDL(ctx context.Context, table *typedef.Table) error {
	table.RLock()
	// Scylla does not allow changing the DDL of a table with materialized views.
	if len(table.MaterializedViews) > 0 {
		table.RUnlock()
		return nil
	}
	table.RUnlock()

	table.Lock()
	defer table.Unlock()
	ddlStmts, err := statements.GenDDLStmt(m.schema, table, m.random, m.partitionRangeConfig, m.schemaCfg)
	if err != nil {
		m.logger.Error("Failed! DDL Mutation statement generation failed", zap.Error(err))
		m.globalStatus.WriteErrors.Add(1)
		return err
	}
	if ddlStmts == nil {
		if w := m.logger.Check(zap.DebugLevel, "no statement generated"); w != nil {
			w.Write(zap.String("job", "ddl"))
		}
		return nil
	}
	for _, ddlStmt := range ddlStmts.List {
		if w := m.logger.Check(zap.DebugLevel, "ddl statement"); w != nil {
			prettyCQL, prettyCQLErr := ddlStmt.PrettyCQL()
			if prettyCQLErr != nil {
				return PrettyCQLError{
					PrettyCQL: prettyCQLErr,
					Stmt:      ddlStmt,
				}
			}

			w.Write(zap.String("pretty_cql", prettyCQL))
		}
		if err = m.store.Mutate(ctx, ddlStmt); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}

			prettyCQL, prettyCQLErr := ddlStmt.PrettyCQL()
			if prettyCQLErr != nil {
				return PrettyCQLError{
					PrettyCQL: prettyCQLErr,
					Stmt:      ddlStmt,
					Err:       err,
				}
			}

			m.globalStatus.AddWriteError(&joberror.JobError{
				Timestamp: time.Now(),
				StmtType:  ddlStmts.QueryType.String(),
				Message:   "DDL failed: " + err.Error(),
				Query:     prettyCQL,
			})

			return err
		}
		m.globalStatus.WriteOps.Add(1)
	}
	ddlStmts.PostStmtHook()

	jsonSchema, _ := json.MarshalIndent(m.schema, "", "    ")
	fmt.Printf("New schema: %v\n", string(jsonSchema))

	return nil
}
