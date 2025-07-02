// Copyright 2025 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jobs

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/pkg/errors"

	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/generators/statements"
	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
)

type Mutation struct {
	generator generators.Interface
	store     store.Store
	table     *typedef.Table
	statement *statements.Generator
	status    *status.GlobalStatus
	stopFlag  *stop.Flag
	schema    *typedef.Schema
	delete    bool
}

func NewMutation(
	schema *typedef.Schema,
	config typedef.SchemaConfig,
	table *typedef.Table,
	generator generators.Interface,
	status *status.GlobalStatus,
	stopFlag *stop.Flag,
	store store.Store,
	del bool,
	seed [32]byte,
) *Mutation {
	pc := config.GetPartitionRangeConfig()
	statementGenerator := statements.New(
		schema.Keyspace.Name,
		generator,
		table,
		rand.New(rand.NewChaCha8(seed)),
		&pc,
		config.UseLWT,
	)

	return &Mutation{
		schema:    schema,
		table:     table,
		statement: statementGenerator,
		generator: generator,
		status:    status,
		stopFlag:  stopFlag,
		store:     store,
		delete:    del,
	}
}

func (m *Mutation) Do(ctx context.Context) error {
	name := m.Name()
	executionTime := metrics.ExecutionTimeStart(name)

	for !m.stopFlag.IsHardOrSoft() {
		err := executionTime.RunFuncE(func() error {
			mutateStmt := m.statement.MutateStatement(ctx, m.delete)

			if mutateStmt == nil {
				return ErrNoStatement
			}

			defer m.generator.ReleaseToken(mutateStmt.PartitionKeys.Token)

			err := m.store.Mutate(ctx, mutateStmt)

			if err == nil {
				m.status.WriteOp()
				m.generator.GiveOlds(ctx, mutateStmt.PartitionKeys)
				return nil
			}

			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return nil
			}

			return joberror.JobError{
				Err:           err,
				Timestamp:     time.Now(),
				StmtType:      mutateStmt.QueryType,
				Message:       "Mutation failed: " + err.Error(),
				Query:         mutateStmt.Query,
				PartitionKeys: mutateStmt.PartitionKeys.Values,
			}
		})

		if errors.Is(err, ErrNoStatement) {
			time.Sleep(m.schema.Config.AsyncObjectStabilizationDelay)
		}

		var jobErr joberror.JobError
		if errors.As(err, &jobErr) {
			m.status.AddReadError(jobErr)
		}

		if m.status.HasErrors() {
			m.stopFlag.SetSoft(true)
			return errors.New("mutation job stopped due to errors")
		}
	}

	return nil
}

func (m *Mutation) Name() string {
	return "mutation_" + m.table.Name
}

// nolint
func (m *Mutation) ddl(_ context.Context) error {
	if len(m.table.MaterializedViews) > 0 {
		// Scylla does not allow changing the DDL of a table with materialized views.
		return nil
	}
	//w.table.Lock()
	//defer w.table.Unlock()
	////ddlStmts, err := GenDDLStmt(w.schema, w.table, w., p, sc)
	//if err != nil {
	//	w.status.WriteErrors.Add(1)
	//	return err
	//}
	//
	//if ddlStmts == nil {
	//	return nil
	//}
	//
	//for _, ddlStmt := range ddlStmts.List {
	//	if err = w.store.Mutate(ctx, ddlStmt); err != nil {
	//		w.status.AddWriteError(joberror.JobError{
	//			Timestamp: time.Now(),
	//			StmtType:  ddlStmts.QueryType,
	//			Message:   "DDL failed: " + err.Error(),
	//			Query:     ddlStmt.Query,
	//		})
	//
	//		return err
	//	}
	//
	//	w.status.WriteOps.Add(1)
	//}
	//ddlStmts.PostStmtHook()
	//jsonSchema, _ := json.MarshalIndent(w.schema, "", "    ")
	//fmt.Printf("New schema: %v\n", string(jsonSchema)) //nolint:forbidigo
	return nil
}
