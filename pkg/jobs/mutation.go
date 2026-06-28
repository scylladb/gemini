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

	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/partitions"
	"github.com/scylladb/gemini/pkg/statements"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

type Mutation struct {
	generator partitions.Interface
	store     store.Store
	table     *typedef.Table
	statement *statements.Generator
	status    *status.GlobalStatus
	stopFlag  *stop.Flag
	schema    *typedef.Schema
	logger    *zap.Logger
	delete    bool
}

func NewMutation(
	schema *typedef.Schema,
	table *typedef.Table,
	generator partitions.Interface,
	status *status.GlobalStatus,
	statementRatioController *statements.RatioController,
	stopFlag *stop.Flag,
	store store.Store,
	del bool,
	seed [32]byte,
	logger *zap.Logger,
) *Mutation {
	vc := schema.Config.GetValueRangeConfig()
	statementGenerator := statements.New(
		schema.Keyspace.Name,
		generator,
		table,
		rand.New(rand.NewChaCha8(seed)),
		&vc,
		statementRatioController,
		schema.Config.UseLWT,
	)

	if logger == nil {
		logger = zap.NewNop()
	}

	return &Mutation{
		schema:    schema,
		table:     table,
		statement: statementGenerator,
		generator: generator,
		status:    status,
		stopFlag:  stopFlag,
		store:     store,
		delete:    del,
		logger:    logger,
	}
}

func (m *Mutation) run(ctx context.Context) error {
	mutateStmt, err := m.statement.MutateStatement(ctx, m.delete)
	// Drain whatever tracked-row fallbacks the generator recorded into the metric.
	m.recordTrackedMisses()
	if err != nil {
		return err
	}

	// Ensure partition keys are released when we're done with the statement
	defer func() {
		for i := range mutateStmt.PartitionKeys {
			if mutateStmt.PartitionKeys[i].Release != nil {
				mutateStmt.PartitionKeys[i].Release()
			}
		}
	}()

	err = m.store.Mutate(ctx, mutateStmt)

	if err == nil {
		m.status.WriteOp()
		return nil
	}

	// Treat context cancellation as expected termination
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}

	// For context deadline expirations (CQL RequestTimeout or job shutdown), don't
	// count as data errors. This covers both the raw error and MutationError whose
	// FinalError is DeadlineExceeded (all retries timed out on a slow CI runner).
	// Exception: if the stores ended up asymmetric (one committed, the other timed
	// out on all retries), mark the affected partitions invalid so the validation
	// phase skips them rather than reporting a false divergence.
	// OracleStoreSuccess is only set true when an oracle is configured (see
	// delegatingStore.Mutate), so this check is safe when oracleStore == nil.
	if errors.Is(err, context.DeadlineExceeded) {
		var mutErr *store.MutationError
		if errors.As(err, &mutErr) && mutErr.OracleStoreSuccess != mutErr.TestStoreSuccess {
			for i := range mutateStmt.PartitionKeys {
				m.logger.Debug("marking partition invalid due to asymmetric write timeout",
					zap.String("partition_id", mutateStmt.PartitionKeys[i].ID.String()),
					zap.Bool("test_store_success", mutErr.TestStoreSuccess),
					zap.Bool("oracle_store_success", mutErr.OracleStoreSuccess),
				)
				m.statement.MarkInvalid(&mutateStmt.PartitionKeys[i])
			}
		}
		return nil
	}

	// If this is a comprehensive mutation error (all retries failed for a non-timeout
	// reason), surface it as a write error.
	var mutationFailErr *store.MutationError
	if errors.As(err, &mutationFailErr) {
		je := &joberror.JobError{
			Err:       err,
			Timestamp: time.Now(),
			StmtType:  mutateStmt.QueryType,
			Message:   "Mutation failed: " + err.Error(),
			Query:     mutateStmt.Query,
			PartitionKeys: func() *typedef.Values {
				if len(mutateStmt.PartitionKeys) > 0 {
					return mutateStmt.PartitionKeys[0].Values
				}
				return nil
			}(),
			PartitionIDs: collectPartitionIDs(mutateStmt.PartitionKeys),
			Values:       mutateStmt.Values,
		}
		return je
	}

	je2 := &joberror.JobError{
		Err:       err,
		Timestamp: time.Now(),
		StmtType:  mutateStmt.QueryType,
		Message:   "Mutation failed: " + err.Error(),
		Query:     mutateStmt.Query,
		PartitionKeys: func() *typedef.Values {
			if len(mutateStmt.PartitionKeys) > 0 {
				return mutateStmt.PartitionKeys[0].Values
			}
			return nil
		}(),
		PartitionIDs: collectPartitionIDs(mutateStmt.PartitionKeys),
		Values:       mutateStmt.Values,
	}
	return je2
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

		if errors.Is(err, ErrNoStatement) || errors.Is(err, statements.ErrNoTrackedRows) {
			// No statement generated at this moment, back off briefly and retry
			timer := utils.GetTimer(100 * time.Millisecond)
			select {
			case <-timer.C:
				utils.PutTimer(timer)
				continue
			case <-ctx.Done():
				utils.PutTimer(timer)
				return nil
			}
		}

		var jobErr *joberror.JobError
		if errors.As(err, &jobErr) {
			// Record the write error, but only stop if we've exceeded the error budget
			m.status.AddWriteError(*jobErr)
			if m.status.HasReachedErrorCount() {
				m.stopFlag.SetSoft(true)
				return ErrMutationJobStopped
			}
			// Continue processing; transient errors should not halt immediately
			continue
		}

		if m.status.HasReachedErrorCount() {
			m.stopFlag.SetSoft(true)
			return ErrMutationJobStopped
		}
	}

	return nil
}

func (m *Mutation) Name() string {
	return "mutation_" + m.table.Name
}

// recordTrackedMisses drains the statement generator's tracked-row
// schema-mismatch fallback counts and adds them to the
// tracked_row_schema_mismatch_total metric, labelled by mutation kind. Kept at
// the jobs layer so pkg/statements has no metrics dependency.
func (m *Mutation) recordTrackedMisses() {
	c := m.statement.DrainTrackedMisses()
	if c == (statements.TrackedMissCounts{}) {
		return
	}

	keyspace := m.schema.Keyspace.Name
	if c.Update > 0 {
		metrics.TrackedRowSchemaMismatch.WithLabelValues(keyspace, m.table.Name, "update").Add(float64(c.Update))
	}
	if c.DeleteSingleRow > 0 {
		metrics.TrackedRowSchemaMismatch.WithLabelValues(keyspace, m.table.Name, "delete_single_row").Add(float64(c.DeleteSingleRow))
	}
	if c.DeleteClusteringSubset > 0 {
		metrics.TrackedRowSchemaMismatch.WithLabelValues(keyspace, m.table.Name, "delete_clustering_subset").Add(float64(c.DeleteClusteringSubset))
	}
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
