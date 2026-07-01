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

	"github.com/google/uuid"
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
	generator        partitions.Interface
	store            store.Store
	random           *rand.Rand
	statement        *statements.Generator
	status           *status.GlobalStatus
	stopFlag         *stop.Flag
	table            *typedef.Table
	keyspaceName     string
	selectColumns    []string
	permScratch      []int
	maxAttempts      int
	delay            time.Duration
	sampleRate       float64
	maxSamplesPerRun int
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
	targetedConsumeRatio float64,
) *Validation {
	maxAttempts := schema.Config.AsyncObjectStabilizationAttempts
	delay := schema.Config.AsyncObjectStabilizationDelay
	vc := schema.Config.GetValueRangeConfig()

	if maxAttempts <= 1 {
		maxAttempts = 10
	}

	if delay <= 100*time.Millisecond {
		delay = 2 * time.Second
	}

	rng := rand.New(rand.NewChaCha8(seed))

	statementGenerator := statements.New(
		schema.Keyspace.Name,
		generator,
		table,
		rng,
		&vc,
		statementRatioController,
		schema.Config.UseLWT,
	)

	// Compute sample rate based on the effective tracked-row consume ratio
	// (targeted deletes + single-row updates). When nothing consumes tracked
	// rows, we don't sample at all. Scales linearly, capped at 50% for heavy
	// delete/update workloads.
	sampleRate := min(0.50, targetedConsumeRatio*3.5)
	maxSamplesPerRun := max(1, min(10, int(30*sampleRate)))

	return &Validation{
		table:            table,
		keyspaceName:     schema.Keyspace.Name,
		statement:        statementGenerator,
		generator:        generator,
		status:           status,
		stopFlag:         stopFlag,
		store:            store,
		random:           rng,
		maxAttempts:      maxAttempts,
		selectColumns:    table.SelectColumnNames(),
		delay:            delay,
		sampleRate:       sampleRate,
		maxSamplesPerRun: maxSamplesPerRun,
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

// runResult represents the outcome of a single validation attempt.
// When needsRetry is true, the stmt is NOT released and should be
// scheduled for retry. When needsRetry is false, the stmt has been
// released (or handed to handleCheckFailure which releases it).
type runResult struct {
	err        error
	stmt       *typedef.Stmt
	attempt    int
	needsRetry bool
}

func (v *Validation) run(ctx context.Context, metric prometheus.Counter) runResult {
	stmt, stmtErr := v.statement.Select(ctx)
	if errors.Is(stmtErr, utils.ErrNoPartitionKeyValues) {
		return runResult{err: utils.ErrNoPartitionKeyValues}
	}

	if stmt == nil {
		if v.status.HasReachedErrorCount() {
			v.stopFlag.SetSoft(true)
		}
		return runResult{err: ErrNoStatement}
	}

	return v.checkOnce(ctx, stmt, 0, metric)
}

// checkOnce performs a single validation attempt on a statement. Used both
// for fresh queries and for retries. On success it releases keys and records
// metrics. On a retryable failure it returns needsRetry=true with the stmt
// still held. On a final failure it calls handleCheckFailure which releases keys.
//
// testRows are returned by CheckOnce for free — they were already fetched for
// comparison, so we reuse them for row-tracker sampling without a second SELECT.
func (v *Validation) checkOnce(ctx context.Context, stmt *typedef.Stmt, attempt int, metric prometheus.Counter) runResult {
	validatedRows, testRows, err := v.store.CheckOnce(ctx, v.table, stmt, attempt)
	if err == nil {
		metric.Add(float64(validatedRows))
		v.status.AddValidatedRows(validatedRows)
		v.status.ReadOp()
		if v.generator != nil {
			for i := range stmt.PartitionKeys {
				v.generator.ValidationSuccess(&stmt.PartitionKeys[i])
			}
		}

		// Sample rows for the row tracker (used by targeted deletions).
		// We reuse the testRows already fetched by Check — no extra SELECT.
		//
		// Fill-zone behaviour:
		//   < 30%  (lean)    — always push; bypass probabilistic sample-rate gate.
		//   30–70% (healthy) — no extra sampling; queue is in a good state.
		//   70–90% (filling) — probabilistic gate: only push when random draw < sampleRate.
		//   ≥ 90%  (full)   — skip entirely; queue is nearly full.
		if v.generator != nil && len(v.table.ClusteringKeys) > 0 && len(testRows) > 0 {
			fill := v.generator.RowTrackerFillRatio()
			switch {
			case fill >= partitions.FillZoneSkip:
				// Queue is ≥90% full — skip sampling to avoid wasted work.
			case fill >= partitions.FillZoneSampled:
				// Queue is filling (70–90%) — apply the probabilistic gate.
				if v.random.Float64() < v.sampleRate {
					v.sampleRowsForTracker(stmt, testRows, false)
				}
			case fill < partitions.FillZoneAlwaysPush:
				// Queue is lean (<30%) — always push regardless of sample rate.
				v.sampleRowsForTracker(stmt, testRows, true)
			default:
				// Queue is healthy (30–70%) — no extra sampling needed.
			}
		}

		releaseKeys(stmt)
		return runResult{}
	}

	// Context cancellation/deadline — not a data discrepancy.
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		releaseKeys(stmt)
		return runResult{}
	}

	// Signal that this can be retried (caller decides based on attempt count)
	return runResult{err: err, stmt: stmt, attempt: attempt, needsRetry: true}
}

// sampleRowsForTracker pushes a random subset of already-fetched test rows
// into the row tracker for later use by targeted delete operations.
// It reuses the rows returned by Check — no additional SELECT is issued.
//
// n is drawn uniformly from [1, len(rows)], so between one row and all rows
// may be pushed per call. When forceAll is true (queue is lean, fill <
// FillZoneAlwaysPush) the maxSamplesPerRun cap is bypassed.
func (v *Validation) sampleRowsForTracker(stmt *typedef.Stmt, rows store.Rows, forceAll bool) {
	if len(rows) == 0 || len(stmt.PartitionKeys) == 0 {
		return
	}

	type candidate struct {
		values []any
		id     uuid.UUID
	}
	candidates := make([]candidate, len(stmt.PartitionKeys))
	for i := range stmt.PartitionKeys {
		pk := &stmt.PartitionKeys[i]
		values := make([]any, 0, v.table.PartitionKeysLenValues())
		for _, col := range v.table.PartitionKeys {
			values = append(values, pk.Values.Get(col.Name)...)
		}
		candidates[i] = candidate{id: pk.ID, values: values}
	}
	single := len(candidates) == 1

	matchPartition := func(row store.Row) int {
		if single {
			return 0
		}
		for ci := range candidates {
			matched := true
			for ki, col := range v.table.PartitionKeys {
				if !store.ValuesEqual(row.Get(col.Name), candidates[ci].values[ki]) {
					matched = false
					break
				}
			}
			if matched {
				return ci
			}
		}
		return -1
	}

	// Pick a random number of rows to push: between 1 and len(rows).
	limit := 1 + v.random.IntN(len(rows))
	if !forceAll && limit > v.maxSamplesPerRun {
		limit = v.maxSamplesPerRun
	}

	perm := v.samplingPerm(len(rows))

	sampled := 0
	for i := 0; i < len(perm) && sampled < limit; i++ {
		j := i + v.random.IntN(len(perm)-i)
		perm[i], perm[j] = perm[j], perm[i]

		row := rows[perm[i]]

		ci := matchPartition(row)
		if ci < 0 {
			continue
		}

		ckValues := make([]any, 0, v.table.ClusteringKeysLenValues())

		// Extract clustering key values from the row.
		validRow := true
		for _, ck := range v.table.ClusteringKeys {
			val := row.Get(ck.Name)
			if val == nil {
				validRow = false
				break
			}
			ckValues = append(ckValues, val)
		}

		if !validRow {
			continue
		}

		v.generator.TrackRow(partitions.TrackedRow{
			PartitionID:      candidates[ci].id,
			PartitionValues:  candidates[ci].values, // shared slice; callers treat as read-only
			ClusteringValues: ckValues,
		})
		sampled++
	}
}

func (v *Validation) samplingPerm(n int) []int {
	if len(v.permScratch) == n {
		return v.permScratch
	}
	if cap(v.permScratch) >= n {
		v.permScratch = v.permScratch[:n]
	} else {
		v.permScratch = make([]int, n)
	}
	for i := range v.permScratch {
		v.permScratch[i] = i
	}
	return v.permScratch
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

	if _, _, err := v.store.Check(ctx, v.table, stmt, 0); err != nil {
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

	rq := newRetryQueue(v.maxAttempts, v.delay, 100*time.Millisecond)
	defer rq.Drain(releaseKeys)

	for !v.stopFlag.IsHardOrSoft() {
		// First, check if any retries are ready (non-blocking).
		if idx := rq.Ready(); idx >= 0 {
			retry := rq.Take(idx)
			result := v.checkOnce(ctx, retry.stmt, retry.attempt, validatedRowsMetric)
			if err := v.handleRunResult(result, rq); err != nil {
				return err
			}
			continue
		}

		// Main select: context, deleted partitions, retry timer, or new work.
		retryTimerCh := rq.EarliestTimer()

		select {
		case <-ctx.Done():
			return nil

		case deletedKeys, ok := <-deletedPartitionsCh:
			if !ok {
				deletedPartitionsCh = nil
				continue
			}

			if err := v.handleDeletedPartition(ctx, &executionTime, deletedKeys); err != nil {
				return err
			}

		case <-retryTimerCh:
			// A retry timer fired — pick it up.
			retry := rq.TakeFirst()
			result := v.checkOnce(ctx, retry.stmt, retry.attempt, validatedRowsMetric)
			if err := v.handleRunResult(result, rq); err != nil {
				return err
			}

		default:
			if err := v.handleNewWork(ctx, &executionTime, validatedRowsMetric, rq); err != nil {
				return err
			}
		}
	}

	return nil
}

// handleDeletedPartition validates a single deleted-partition event received
// from the generator's Deleted channel. Returns a non-nil error only when the
// worker must stop.
func (v *Validation) handleDeletedPartition(ctx context.Context, executionTime *metrics.RunningTime, deletedKeys typedef.PartitionKeys) error {
	deletionNS := deletedKeys.DeletedAtNS
	err := executionTime.RunFuncE(func() error {
		return v.validateDeletedPartition(ctx, deletedKeys, deletionNS)
	})

	if err == nil {
		v.status.ReadOp()
		return nil
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}

	var jobErr *joberror.JobError
	if errors.As(err, &jobErr) {
		v.status.AddReadError(*jobErr)
	} else {
		panic(fmt.Sprintf("invalid type err %T: %v", err, err))
	}

	if v.status.HasReachedErrorCount() {
		v.stopFlag.SetSoft(true)
		metrics.WorkerStopEvents.WithLabelValues("validation", "error_budget").Inc()
		return ErrValidationJobStopped
	}

	return nil
}

// handleNewWork performs new validation work and processes the result,
// including short sleeps on transient no-data conditions. Returns a non-nil
// error only when the worker must stop.
func (v *Validation) handleNewWork(ctx context.Context, executionTime *metrics.RunningTime, metric prometheus.Counter, rq *retryQueue) error {
	executionTime.Start()
	result := v.run(ctx, metric)
	executionTime.Record()

	if result.err == nil && !result.needsRetry {
		return nil
	}

	if waitOnTransientError(ctx, result.err) {
		return nil
	}

	if errors.Is(result.err, context.Canceled) {
		return context.Canceled
	}

	return v.handleRunResult(result, rq)
}

// waitOnTransientError checks whether err is a transient "no data yet"
// condition (ErrNoPartitionKeyValues or ErrNoStatement) and, if so, sleeps
// briefly before returning true. Returns false for all other errors.
func waitOnTransientError(ctx context.Context, err error) bool {
	if !errors.Is(err, utils.ErrNoPartitionKeyValues) && !errors.Is(err, ErrNoStatement) {
		return false
	}

	timer := utils.GetTimer(100 * time.Millisecond)
	select {
	case <-timer.C:
		utils.PutTimer(timer)
	case <-ctx.Done():
		utils.PutTimer(timer)
	}

	return true
}

// handleRunResult processes a runResult: if the stmt can be retried it is
// scheduled on the retry queue; otherwise the failure is recorded and error
// budget checked. Returns a non-nil error only when the worker must stop.
func (v *Validation) handleRunResult(result runResult, rq *retryQueue) error {
	if !result.needsRetry {
		return nil
	}

	// Try to schedule a retry.
	if rq.Schedule(result.stmt, result.attempt) {
		// Stmt ownership transferred to the retry queue — do NOT release keys.
		return nil
	}

	// All retries exhausted — treat as final failure.
	metrics.ValidationRetriesExhausted.Inc()
	failErr := v.handleCheckFailure(result.err, result.stmt)
	if failErr == nil {
		return nil
	}

	var jobErr *joberror.JobError
	if errors.As(failErr, &jobErr) {
		v.status.AddReadError(*jobErr)
	} else {
		panic(fmt.Sprintf("invalid type err %T: %v", failErr, failErr))
	}

	if v.status.HasReachedErrorCount() {
		v.stopFlag.SetSoft(true)
		return ErrValidationJobStopped
	}

	return nil
}

func (v *Validation) Name() string {
	return "validation_" + v.table.Name
}
