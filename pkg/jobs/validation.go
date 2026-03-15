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
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/gocqlx/v3/qb"

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

type Validation struct {
	generator     partitions.Interface
	store         store.Store
	table         *typedef.Table
	statement     *statements.Generator
	status        *status.GlobalStatus
	stopFlag      *stop.Flag
	keyspaceName  string
	selectColumns []string
	maxAttempts   int
	delay         time.Duration
}

func NewValidation(
	schema *typedef.Schema,
	table *typedef.Table,
	generator partitions.Interface,
	status *status.GlobalStatus,
	statementRatioController *statements.RatioController,
	stopFlag *stop.Flag,
	store store.Store,
	seed [32]byte,
) *Validation {
	maxAttempts := schema.Config.AsyncObjectStabilizationAttempts
	delay := schema.Config.AsyncObjectStabilizationDelay
	vc := schema.Config.GetValueRangeConfig()

	if maxAttempts <= 1 {
		maxAttempts = 10
	}

	if delay <= 10*time.Millisecond {
		delay = 200 * time.Millisecond
	}

	statementGenerator := statements.New(
		schema.Keyspace.Name,
		generator,
		table,
		rand.New(rand.NewChaCha8(seed)),
		&vc,
		statementRatioController,
		schema.Config.UseLWT,
	)

	return &Validation{
		table:         table,
		keyspaceName:  schema.Keyspace.Name,
		statement:     statementGenerator,
		generator:     generator,
		status:        status,
		stopFlag:      stopFlag,
		store:         store,
		maxAttempts:   maxAttempts,
		selectColumns: table.SelectColumnNames(),
		delay:         delay,
	}
}

// releaseKeys releases all partition key references held by stmt.
func releaseKeys(stmt *typedef.Stmt) {
	for i := range stmt.PartitionKeys {
		if stmt.PartitionKeys[i].Release != nil {
			stmt.PartitionKeys[i].Release()
		}
	}
}

func (v *Validation) run(ctx context.Context, metric prometheus.Counter) error {
	stmt, stmtErr := v.statement.Select(ctx)
	if errors.Is(stmtErr, utils.ErrNoPartitionKeyValues) {
		return utils.ErrNoPartitionKeyValues
	}

	if stmt == nil {
		if v.status.HasReachedErrorCount() {
			v.stopFlag.SetSoft(true)
		}
		return ErrNoStatement
	}

	// Delegate retries to the store implementation.
	validatedRows, err := v.store.Check(ctx, v.table, stmt, 0)
	if err == nil {
		metric.Add(float64(validatedRows))
		v.status.AddValidatedRows(validatedRows)
		v.status.ReadOp()
		if v.generator != nil {
			for i := range stmt.PartitionKeys {
				v.generator.ValidationSuccess(&stmt.PartitionKeys[i])
			}
		}
		releaseKeys(stmt)
		return nil
	}

	// Context cancellation/deadline — not a data discrepancy.
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		releaseKeys(stmt)
		return nil
	}

	return v.handleCheckFailure(err, stmt)
}

// handleCheckFailure processes a non-context store.Check error: it records the
// failure, releases all keys, and returns a JobError only for the first goroutine
// to mark each partition invalid (deduplication via MarkInvalid).
func (v *Validation) handleCheckFailure(err error, stmt *typedef.Stmt) error {
	if v.generator == nil {
		releaseKeys(stmt)
		return v.buildJobError(err, stmt, "Validation failed: "+err.Error())
	}

	// Step 1: Record validation failure timestamps (UUID still live in uuidToIdx).
	for i := range stmt.PartitionKeys {
		v.generator.ValidationFailure(&stmt.PartitionKeys[i])
	}

	// Step 2: Attempt to mark invalid BEFORE releasing keys.
	// releaseKeys may trigger deleteValidation which removes the UUID from
	// uuidToIdx, so MarkInvalid must be called while the UUID is still registered.
	// MarkInvalid is non-blocking and idempotent — only the first goroutine for
	// a given partition returns true. Emit a JobError only for newly-marked
	// partitions; silently drop the rest to ensure unique reporting.
	marked := false
	for i := range stmt.PartitionKeys {
		if v.generator.MarkInvalid(&stmt.PartitionKeys[i]) {
			marked = true
			// Do not break: every key in the statement must be marked so that
			// future Next()/ReplaceNext() calls skip all of them, not just the first.
		}
	}

	// Step 3: Release keys (may trigger deleteValidation — safe now).
	releaseKeys(stmt)

	if marked {
		return v.buildJobError(err, stmt, "Validation failed: "+err.Error())
	}
	return nil
}

// extractComparisonResults extracts comparison results from a store.ValidationError
// and converts store.Rows to json.RawMessage for the joberror format.
func extractComparisonResults(err error) *joberror.ComparisonResults {
	var valErr *store.ValidationError
	if !errors.As(err, &valErr) || valErr.ComparisonResults == nil {
		return nil
	}

	cr := valErr.ComparisonResults

	return &joberror.ComparisonResults{
		TestRows:       marshalRows(cr.TestRows),
		OracleRows:     marshalRows(cr.OracleRows),
		TestOnlyRows:   marshalRows(cr.TestOnlyRows),
		OracleOnlyRows: marshalRows(cr.OracleOnlyRows),
		DifferentRows:  marshalDifferentRows(cr.DifferentRows),
	}
}

func marshalRows(rows store.Rows) []json.RawMessage {
	if len(rows) == 0 {
		return nil
	}
	result := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		data, err := json.Marshal(row)
		if err != nil {
			continue
		}
		result = append(result, data)
	}
	return result
}

func marshalDifferentRows(diffs []store.RowDifference) []joberror.RowDiff {
	if len(diffs) == 0 {
		return nil
	}
	result := make([]joberror.RowDiff, 0, len(diffs))
	for _, d := range diffs {
		testData, _ := json.Marshal(d.TestRow)
		oracleData, _ := json.Marshal(d.OracleRow)
		result = append(result, joberror.RowDiff{
			TestRow:   testData,
			OracleRow: oracleData,
			Diff:      d.Diff,
		})
	}
	return result
}

// collectLastValidations gathers validation timing data for all partition keys in the statement.
func (v *Validation) collectLastValidations(stmt *typedef.Stmt) map[string]joberror.PartitionValidation {
	if v.generator == nil || len(stmt.PartitionKeys) == 0 {
		return nil
	}

	result := make(map[string]joberror.PartitionValidation, len(stmt.PartitionKeys))
	for i := range stmt.PartitionKeys {
		pk := &stmt.PartitionKeys[i]
		first, last, failure, recent, successCount := v.generator.ValidationStats(pk.ID)
		if first == 0 && last == 0 && failure == 0 {
			continue
		}
		result[pk.ID.String()] = joberror.PartitionValidation{
			FirstSuccessNS: first,
			LastSuccessNS:  last,
			LastFailureNS:  failure,
			Recent:         recent,
			SuccessCount:   successCount,
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// buildJobError creates a JobError with ComparisonResults and LastValidations populated.
func (v *Validation) buildJobError(err error, stmt *typedef.Stmt, message string) *joberror.JobError {
	je := &joberror.JobError{
		Timestamp: time.Now(),
		Err:       err,
		StmtType:  stmt.QueryType,
		Message:   message,
		Query:     stmt.Query,
		PartitionKeys: func() *typedef.Values {
			if len(stmt.PartitionKeys) > 0 {
				return stmt.PartitionKeys[0].Values
			}
			return nil
		}(),
		PartitionIDs:    collectPartitionIDs(stmt.PartitionKeys),
		Values:          stmt.Values,
		Results:         extractComparisonResults(err),
		LastValidations: v.collectLastValidations(stmt),
	}
	return je
}

// createSelectStmtForPartitionKeys creates a SELECT statement for specific partition key values
func (v *Validation) createSelectStmtForPartitionKeys(keys typedef.PartitionKeys) *typedef.Stmt {
	keyspaceAndTable := v.keyspaceName + "." + v.table.Name
	builder := qb.Select(keyspaceAndTable).Columns(v.selectColumns...)

	for _, pk := range v.table.PartitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
	}

	query, _ := builder.ToCql()

	return &typedef.Stmt{
		QueryType:     typedef.SelectStatementType,
		Query:         query,
		PartitionKeys: []typedef.PartitionKeys{keys},
		Values:        keys.Values.ToCQLValues(v.table.PartitionKeys),
	}
}

// validateDeletedPartition validates a deleted partition by verifying it no longer exists.
// The partition should have been deleted from both test and oracle clusters, so we expect
// zero rows to be returned.
func (v *Validation) validateDeletedPartition(ctx context.Context, keys typedef.PartitionKeys, deletionTimeNS uint64) error {
	stmt := v.createSelectStmtForPartitionKeys(keys)

	// Ensure partition keys are released when we're done
	defer func() {
		if keys.Release != nil {
			keys.Release()
		}
	}()

	if _, err := v.store.Check(ctx, v.table, stmt, 0); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return err
		}
		// final failure after retries for deleted partition
		if v.generator != nil {
			v.generator.ValidationFailure(&stmt.PartitionKeys[0])
		}

		// Mark the partition invalid and only report if this is the first time.
		if v.generator != nil && !v.generator.MarkInvalid(&stmt.PartitionKeys[0]) {
			// Already reported by another goroutine.
			return nil
		}

		jobErr := v.buildJobError(err, stmt, "Deleted partition validation failed (difference found): "+err.Error())
		jobErr.DeletionTimeNS = deletionTimeNS
		return jobErr
	}

	// success
	if v.generator != nil {
		v.generator.ValidationSuccess(&stmt.PartitionKeys[0])
	}
	return nil
}

func (v *Validation) Do(ctx context.Context) error {
	name := v.Name()

	executionTime := metrics.ExecutionTimeStart(name)
	metrics.GeminiInformation.WithLabelValues("validation_" + v.table.Name).Inc()
	defer metrics.GeminiInformation.WithLabelValues("validation_" + v.table.Name).Dec()

	validatedRowsMetric := metrics.ValidatedRows.WithLabelValues(v.table.Name)
	deletedPartitionsCh := v.generator.Deleted()
	defer func() {
		// no-op
	}()

	for !v.stopFlag.IsHardOrSoft() {
		// Check for deleted partitions to validate or perform regular validation
		select {
		case <-ctx.Done():
			return nil
		case deletedKeys, ok := <-deletedPartitionsCh:
			if !ok {
				// Channel closed, stop processing deleted partitions
				deletedPartitionsCh = nil
				continue
			}

			// Validate that the deleted partition is actually gone
			deletionNS := deletedKeys.DeletedAtNS
			err := executionTime.RunFuncE(func() error {
				return v.validateDeletedPartition(ctx, deletedKeys, deletionNS)
			})

			if err == nil {
				// Deleted partition validation succeeded
				v.status.ReadOp()
				continue
			}

			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}

			// Handle validation error
			var jobErr *joberror.JobError
			if errors.As(err, &jobErr) {
				v.status.AddReadError(*jobErr)
			} else {
				panic(fmt.Sprintf("invalid type err %T: %v", err, err))
			}

			if v.status.HasReachedErrorCount() {
				v.stopFlag.SetSoft(true)
				return ErrValidationJobStopped
			}

		default:
			// Perform regular validation
			err := executionTime.RunFuncE(func() error {
				return v.run(ctx, validatedRowsMetric)
			})

			if err == nil {
				continue
			}

			if errors.Is(err, utils.ErrNoPartitionKeyValues) {
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

			if errors.Is(err, context.Canceled) {
				return context.Canceled
			}

			if errors.Is(err, ErrNoStatement) {
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

			// Handle different error types that can be returned from the store
			var jobErr *joberror.JobError
			if errors.As(err, &jobErr) {
				// It's already a joberror.JobError, use it directly
				v.status.AddReadError(*jobErr)
			} else {
				panic(fmt.Sprintf("invalid type err %T: %v", err, err))
			}

			if v.status.HasReachedErrorCount() {
				v.stopFlag.SetSoft(true)
				return ErrValidationJobStopped
			}
		}
	}

	return nil
}

func (v *Validation) Name() string {
	return "validation_" + v.table.Name
}
