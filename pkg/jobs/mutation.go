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

	var mutErr *store.MutationError
	isMutationErr := errors.As(err, &mutErr)

	// Whether the partition is still trustworthy is decided FIRST, independent of
	// how the write failed. Nesting this under the deadline check would let every
	// non-timeout asymmetric failure through: if the test store commits and the
	// oracle returns, say, Unavailable, the write is just as asymmetric as a
	// timeout, but the error is not DeadlineExceeded, so the partition would stay
	// in validation coverage and gemini would later report its own divergence as a
	// product bug.
	//
	// Two independent reasons to distrust the partition:
	//
	//   1. Asymmetric acknowledgement — one cluster took the write and the other
	//      did not, so their contents may differ. OracleStoreSuccess is only set
	//      true when an oracle is configured (see delegatingStore.Mutate), so this
	//      is safe when oracleStore == nil.
	//   2. Failed compensation — the compensating DELETE did not fully succeed.
	//      This one is NOT implied by the flags: when both original writes time
	//      out the flags are equal (both false) even though each server may
	//      independently have committed. Compensation is what collapses that
	//      ambiguity, so if it half-succeeds the clusters can genuinely differ
	//      while the flags still look symmetric.
	if isMutationErr &&
		(mutErr.OracleStoreSuccess != mutErr.TestStoreSuccess || mutErr.CompensationFailed) {
		// Whether the clusters actually diverged is UNKNOWN — a timed-out server
		// may have applied the write and lost the response — so this is reported as
		// a possible divergence, not a confirmed one. Claiming otherwise would give
		// operators a false corruption signal. Surfaced at Warn rather than
		// accumulating silently, and the affected partitions are marked invalid so
		// validation skips them instead of reporting a mismatch gemini caused.
		m.logger.Warn("write left partitions in an unknown state, marking them invalid",
			zap.Int("partition_count", len(mutateStmt.PartitionKeys)),
			zap.String("query_type", mutateStmt.QueryType.String()),
			zap.Bool("asymmetric_ack", mutErr.TestStoreSuccess != mutErr.OracleStoreSuccess),
			zap.Bool("compensation_failed", mutErr.CompensationFailed),
			zap.Bool("test_store_success", mutErr.TestStoreSuccess),
			zap.Bool("oracle_store_success", mutErr.OracleStoreSuccess),
			zap.Bool("timed_out", errors.Is(err, context.DeadlineExceeded)),
		)

		for i := range mutateStmt.PartitionKeys {
			m.statement.MarkInvalid(&mutateStmt.PartitionKeys[i])
		}
	}

	// For context deadline expirations (CQL RequestTimeout or job shutdown), don't
	// count as data errors. This covers both the raw error and MutationError whose
	// FinalError is DeadlineExceeded (all retries timed out on a slow CI runner).
	// Any partition invalidation the timeout warranted has already happened above.
	if errors.Is(err, context.DeadlineExceeded) {
		return nil
	}

	// If this is a comprehensive mutation error (all retries failed for a non-timeout
	// reason), surface it as a write error. Invalidation above and error accounting
	// here are independent: an asymmetric non-timeout failure needs BOTH.
	if isMutationErr {
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

		if errors.Is(err, statements.ErrNoMutationCandidates) {
			// Permanent misconfiguration: the configured ratios leave nothing
			// this worker is allowed to generate (e.g. a no-delete worker on a
			// table whose DeleteRatio is 1.0). Retrying can never recover, so
			// stop loudly instead of spinning on the error forever.
			m.logger.Error("mutation worker cannot generate any statement under its filter",
				zap.String("table", m.table.Name),
				zap.Bool("deletes_enabled", m.delete),
				zap.Error(err),
			)
			m.stopFlag.SetSoft(true)

			return err
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
