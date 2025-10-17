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
	"errors"
	"math/rand/v2"
	"time"

	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/generators/statements"
	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
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
	table *typedef.Table,
	generator generators.Interface,
	status *status.GlobalStatus,
	statementRatioController *statements.RatioController,
	stopFlag *stop.Flag,
	store store.Store,
	del bool,
	seed [32]byte,
) *Mutation {
	pc := schema.Config.GetPartitionRangeConfig()
	statementGenerator := statements.New(
		schema.Keyspace.Name,
		generator,
		table,
		rand.New(rand.NewChaCha8(seed)),
		&pc,
		statementRatioController,
		schema.Config.UseLWT,
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

func (m *Mutation) run(ctx context.Context) error {
	mutateStmt, err := m.statement.MutateStatement(ctx, m.delete)
	if err != nil {
		return err
	}

	err = m.store.Mutate(ctx, mutateStmt)

	if err == nil {
		m.status.WriteOp()
		m.generator.GiveOlds(ctx, mutateStmt.PartitionKeys)
		return nil
	}

	// Treat context cancellation as expected termination
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}

	// If this is a comprehensive mutation error (all retries failed), surface it.
	// Note: MutationError implements Is and may match DeadlineExceeded; detect it explicitly.
	var mutErr *store.MutationError
	if errors.As(err, &mutErr) {
		return joberror.JobError{
			Err:           err,
			Timestamp:     time.Now(),
			StmtType:      mutateStmt.QueryType,
			Message:       "Mutation failed: " + err.Error(),
			Query:         mutateStmt.Query,
			PartitionKeys: mutateStmt.PartitionKeys.Values,
			Values:        mutateStmt.Values,
		}
	}

	// For pure context deadline expirations (e.g. job/context shutdown), don't count as errors
	if errors.Is(err, context.DeadlineExceeded) {
		return nil
	}

	return joberror.JobError{
		Err:           err,
		Timestamp:     time.Now(),
		StmtType:      mutateStmt.QueryType,
		Message:       "Mutation failed: " + err.Error(),
		Query:         mutateStmt.Query,
		PartitionKeys: mutateStmt.PartitionKeys.Values,
		Values:        mutateStmt.Values,
	}
}

func (m *Mutation) Do(ctx context.Context) error {
	name := m.Name()
	executionTime := metrics.ExecutionTimeStart(name)
	metrics.GeminiInformation.WithLabelValues("mutation_" + m.table.Name).Inc()
	defer metrics.GeminiInformation.WithLabelValues("mutation_" + m.table.Name).Dec()

	for !m.stopFlag.IsHardOrSoft() {
		// Check if context is cancelled before proceeding
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		err := executionTime.RunFuncE(func() error {
			return m.run(ctx)
		})

		if errors.Is(err, utils.ErrNoPartitionKeyValues) {
			// Add delay to prevent busy waiting when no partitions are available
			timer := utils.GetTimer(200 * time.Millisecond)
			select {
			case <-timer.C:
				utils.PutTimer(timer)
				continue
			case <-ctx.Done():
				utils.PutTimer(timer)
				return nil
			}
		}

		if errors.Is(err, context.Canceled) {
			return nil
		}

		if errors.Is(err, ErrNoStatement) {
			return nil
		}

		var jobErr joberror.JobError
		if errors.As(err, &jobErr) {
			// Record the write error, but only stop if we've exceeded the error budget
			m.status.AddWriteError(jobErr)
			if m.status.HasReachedErrorCount() {
				m.stopFlag.SetSoft(true)
				return errors.New("mutation job stopped due to errors")
			}
			// Continue processing; transient errors should not halt immediately
			continue
		}

		if m.status.HasReachedErrorCount() {
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
	//for _, ddlStmt := range ddlStmts.Jobs {
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
