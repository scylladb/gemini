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

	"github.com/scylladb/gemini/pkg/concurrency"
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
	// limiter parks this worker while workerIdx is at or above its limit.
	// Nil runs the worker without a limit.
	limiter   *concurrency.Limiter
	workerIdx int
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

	statementsGenerated[mutateStmt.QueryType].Inc()

	// Ensure partition keys are released when we're done with the statement
	defer func() {
		for i := range mutateStmt.PartitionKeys {
			if mutateStmt.PartitionKeys[i].Release != nil {
				mutateStmt.PartitionKeys[i].Release()
			}
		}
	}()

	start := time.Now()
	err = m.store.Mutate(ctx, mutateStmt)
	if m.limiter != nil {
		m.limiter.Observe(time.Since(start))
	}

	if err == nil {
		m.status.WriteOp()
		return nil
	}

	mutErr, isMutationErr := errors.AsType[*store.MutationError](err)

	// Whether the partition is still trustworthy is decided FIRST, independent of
	// how the write failed and of who is charged for it. When the clusters can
	// hold different data, the partitions are marked invalid so validation skips
	// them instead of reporting a mismatch gemini caused. The check runs before
	// the cancellation return below: a write the shutdown interrupted can leave
	// the clusters asymmetric too.
	if isMutationErr && writeOutcomeUnknown(mutErr) {
		// Whether the clusters actually diverged is UNKNOWN — a timed-out server
		// may have applied the write and lost the response — so this is reported as
		// a possible divergence, not a confirmed one.
		m.logger.Warn("write left partitions in an unknown state, marking them invalid",
			zap.Int("partition_count", len(mutateStmt.PartitionKeys)),
			zap.String("query_type", mutateStmt.QueryType.String()),
			zap.Bool("asymmetric_ack", mutErr.TestStoreSuccess != mutErr.OracleStoreSuccess),
			zap.Bool("compensation_failed", mutErr.CompensationFailed),
			zap.Bool("outcome_unknown", mutErr.OutcomeUnknown),
			zap.Bool("test_store_success", mutErr.TestStoreSuccess),
			zap.Bool("oracle_store_success", mutErr.OracleStoreSuccess),
			zap.NamedError("test_error", mutErr.TestErr),
			zap.NamedError("oracle_error", mutErr.OracleErr),
		)

		for i := range mutateStmt.PartitionKeys {
			m.statement.MarkInvalid(&mutateStmt.PartitionKeys[i])
		}
	}

	// Treat context cancellation as expected termination.
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}

	// The job context ended while the write was retried. The run is stopping,
	// and the invalidation above already covers the partitions.
	if ctx.Err() != nil {
		return nil
	}

	if !isMutationErr {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil
		}
		return newWriteJobError(err, mutateStmt)
	}

	// The test cluster's error alone decides whether the system under test
	// failed. The oracle error never hides it, and is never charged to it.
	switch {
	case mutErr.TestErr == nil:
		// The test cluster applied the write. The oracle cluster failed, so
		// the fault is in the reference cluster, not in the system under test.
		metrics.MutationOracleFailures.Inc()
		m.logger.Warn("oracle cluster failed a write that the test cluster applied",
			zap.String("query_type", mutateStmt.QueryType.String()),
			zap.NamedError("oracle_error", mutErr.OracleErr),
		)
		return nil
	case errors.Is(mutErr.TestErr, context.DeadlineExceeded):
		// A CQL request timeout of the test cluster is an infrastructure
		// signal. A slow runner would use up the error budget otherwise.
		if mutErr.OracleErr != nil && !errors.Is(mutErr.OracleErr, context.DeadlineExceeded) {
			metrics.MutationOracleFailures.Inc()
		}
		return nil
	default:
		return newWriteJobError(err, mutateStmt)
	}
}

// writeOutcomeUnknown reports whether a failed write can leave the clusters
// with different data. Four conditions cause it:
//
//  1. Asymmetric acknowledgement. One cluster took the write and the other did
//     not. OracleStoreSuccess is true only when an oracle is configured, so the
//     check is safe when there is no oracle.
//  2. Failed compensation. When both original writes time out, the flags are
//     equal (both false), but each server can have committed. Compensation
//     collapses that ambiguity. When it half-succeeds, the clusters can differ
//     while the flags look symmetric.
//  3. A timeout on one cluster and a hard error on the other. The cluster that
//     timed out can have applied the write, and the other did not. A
//     successful compensation (Compensated) made the partition equal again.
//  4. Unknown outcome. A cluster can have applied a write that a DELETE cannot
//     undo, such as a counter increment whose reply did not arrive.
func writeOutcomeUnknown(me *store.MutationError) bool {
	if me.OracleStoreSuccess != me.TestStoreSuccess || me.CompensationFailed || me.OutcomeUnknown {
		return true
	}

	testTimedOut := errors.Is(me.TestErr, context.DeadlineExceeded)
	oracleTimedOut := errors.Is(me.OracleErr, context.DeadlineExceeded)

	return !me.Compensated && testTimedOut != oracleTimedOut && me.TestErr != nil && me.OracleErr != nil
}

// newWriteJobError returns the write error that is charged to the system under
// test.
func newWriteJobError(err error, stmt *typedef.Stmt) *joberror.JobError {
	var pkValues *typedef.Values
	if len(stmt.PartitionKeys) > 0 {
		pkValues = stmt.PartitionKeys[0].Values
	}

	return &joberror.JobError{
		Err:           err,
		Timestamp:     time.Now(),
		StmtType:      stmt.QueryType,
		Message:       "Mutation failed: " + err.Error(),
		Query:         stmt.Query,
		PartitionKeys: pkValues,
		PartitionIDs:  collectPartitionIDs(stmt.PartitionKeys),
		Values:        stmt.Values,
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

		if m.limiter != nil {
			if err := m.limiter.Wait(ctx, m.workerIdx, m.stopFlag.IsHardOrSoft); err != nil {
				return nil
			}
			if m.stopFlag.IsHardOrSoft() {
				break
			}
		}

		executionTime.Start()
		err := m.run(ctx)
		executionTime.Record()

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

		if jobErr, ok := errors.AsType[*joberror.JobError](err); ok {
			// Record the write error, but only stop if we've exceeded the error budget
			m.status.AddWriteError(*jobErr)
			if m.status.HasReachedErrorCount() {
				m.stopFlag.SetSoft(true)
				metrics.WorkerStopEvents.WithLabelValues("mutation", "error_budget").Inc()
				return ErrMutationJobStopped
			}
			// Continue processing; transient errors should not halt immediately
			continue
		}

		if err != nil && ctx.Err() == nil {
			m.logger.Error("mutation worker could not generate or run a statement",
				zap.String("table", m.table.Name),
				zap.Error(err),
			)
		}

		if m.status.HasReachedErrorCount() {
			m.stopFlag.SetSoft(true)
			metrics.WorkerStopEvents.WithLabelValues("mutation", "error_budget").Inc()
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

//nolint:unused
func (m *Mutation) ddl(_ context.Context) error {
	return nil
}
