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
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
	"uuid"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/gocqlx/v3/qb"

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

// retryQueueSaturationBackoff bounds how long Do waits before re-checking a
// saturated retry queue, so soft-stop is still noticed promptly.
const retryQueueSaturationBackoff = 50 * time.Millisecond

type Validation struct {
	// limiter parks this worker while workerIdx is at or above its limit.
	// Nil runs the worker without a limit.
	limiter      *concurrency.Limiter
	generator    partitions.Interface
	store        store.Store
	random       *rand.Rand
	statement    *statements.Generator
	status       *status.GlobalStatus
	stopFlag     *stop.Flag
	table        *typedef.Table
	keyspaceName string
	// deletedQuery is the SELECT of one whole partition, which re-validates
	// a deleted partition. It is the same for every partition of the table,
	// so it is built once. See deletedPartitionQuery.
	deletedQuery     string
	selectColumns    []string
	permScratch      []int
	maxAttempts      int
	delay            time.Duration
	sampleRate       float64
	maxSamplesPerRun int
	workerIdx        int
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

	if maxAttempts <= 0 {
		maxAttempts = 10
	}

	// Use the default only when the delay is unset (<= 0). Any positive value,
	// including a sub-100ms one such as 50ms, is the caller's tuning.
	if delay <= 0 {
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

	v := &Validation{
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
	v.deletedPartitionQuery()

	return v
}

// attemptHistory holds what the attempts of one validation saw, so that the
// last attempt alone does not decide the result. A timeout on the last
// attempt must not hide a divergence that an earlier attempt found.
type attemptHistory struct {
	// divergence is the latest attempt error that found different rows.
	divergence error
	// testReadErr is the latest attempt error in which the test cluster did
	// not return its rows. An attempt in which only the oracle cluster did
	// not return its rows is not recorded: it is not charged to the system
	// under test.
	testReadErr error
}

func (h *attemptHistory) record(err error) {
	switch {
	case store.IsDivergence(err):
		h.divergence = err
	case !store.IsOracleLoadError(err):
		h.testReadErr = err
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
	history    attemptHistory
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

	statementsGenerated[stmt.QueryType].Inc()

	return v.checkOnce(ctx, stmt, 0, attemptHistory{}, metric)
}

// checkOnce performs a single validation attempt on a statement. Used both
// for fresh queries and for retries. On success it releases keys and records
// metrics. On a retryable failure it returns needsRetry=true with the stmt
// still held. On a final failure it calls handleCheckFailure which releases keys.
//
// testRows are returned by CheckOnce for free — they were already fetched for
// comparison, so we reuse them for row-tracker sampling without a second SELECT.
//
// history holds what the earlier attempts of stmt saw. A failed attempt adds
// to it, and the result carries it to the next attempt.
func (v *Validation) checkOnce(ctx context.Context, stmt *typedef.Stmt, attempt int, history attemptHistory, metric prometheus.Counter) runResult {
	start := time.Now()
	validatedRows, testRows, err := v.store.CheckOnce(ctx, v.table, stmt, attempt)
	if v.limiter != nil {
		v.limiter.Observe(time.Since(start))
	}
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

	// The worker's context ended: not a data discrepancy. An attempt that
	// failed with a deadline of its own while ctx is alive is retried.
	if ctx.Err() != nil {
		releaseKeys(stmt)
		return runResult{}
	}

	// Signal that this can be retried (caller decides based on attempt count)
	history.record(err)
	return runResult{err: err, stmt: stmt, attempt: attempt, history: history, needsRetry: true}
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

// handleCheckFailure decides the result of a validation that used all its
// attempts. It releases the keys of stmt, and returns a JobError when the
// failure is charged to the system under test.
//
//   - A divergence that any attempt saw is reported. See reportDivergence.
//   - Else, when the test cluster failed to read, the failure is a read error
//     of the run. The message says that no data mismatch was seen.
//   - Else only the oracle cluster failed to read. That is a fault of the
//     reference cluster, so it is counted in a metric and not charged.
func (v *Validation) handleCheckFailure(history attemptHistory, stmt *typedef.Stmt) error {
	switch {
	case history.divergence != nil:
		return v.reportDivergence(history.divergence, stmt, "Validation failed: ")
	case history.testReadErr != nil:
		metrics.ValidationReadFailures.WithLabelValues(string(store.TypeTest)).Inc()
		je := v.buildJobError(history.testReadErr, stmt,
			"Validation could not read rows from the test cluster, no data mismatch was seen: "+history.testReadErr.Error())
		releaseKeys(stmt)
		return je
	default:
		metrics.ValidationReadFailures.WithLabelValues(string(store.TypeOracle)).Inc()
		releaseKeys(stmt)
		return nil
	}
}

// reportDivergence records a divergence that err holds for stmt, releases the
// keys of stmt, and returns a JobError when the divergence is new.
//
// Only the partitions whose rows differ are blamed and marked invalid. A
// multi-partition SELECT can read a partition that gemini already made
// diverge, for example with an asymmetric write. Its difference must not be
// reported against a healthy partition of the same statement. MarkInvalid
// reports false for a partition that is already invalid or that was replaced
// and already reported. Such a partition is dropped from the report. When no
// differing partition is new, nothing is reported.
//
// MarkInvalid must run while the keys are held: releaseKeys can remove the
// UUID that MarkInvalid looks up.
func (v *Validation) reportDivergence(err error, stmt *typedef.Stmt, prefix string) error {
	if v.generator == nil {
		je := v.buildJobError(err, stmt, prefix+err.Error())
		releaseKeys(stmt)
		return je
	}

	diverged, attributed := v.divergedKeys(err, stmt)
	newly := make([]bool, len(stmt.PartitionKeys))
	newCount := 0
	for _, i := range diverged {
		v.generator.ValidationFailure(&stmt.PartitionKeys[i])
		// Do not stop at the first result: every differing partition must be
		// marked, so that Next skips all of them.
		if v.generator.MarkInvalid(&stmt.PartitionKeys[i]) {
			newly[i] = true
			newCount++
		}
	}

	// An unattributed difference with a known partition among the suspects
	// is dropped. It cannot be told apart from the difference that gemini
	// already knows about.
	var je *joberror.JobError
	switch {
	case len(stmt.PartitionKeys) == 0:
		je = v.indexDivergence(err, stmt, prefix)
	case newCount == 0:
	case newCount == len(stmt.PartitionKeys):
		je = v.buildJobError(err, stmt, prefix+err.Error())
	case attributed:
		je = v.buildPartialJobError(err, stmt, newly, prefix)
	}

	releaseKeys(stmt)
	if je == nil {
		return nil
	}

	return je
}

// divergedKeys returns the indexes of the partitions of stmt whose rows differ
// between the clusters. attributed is false when a differing row matches no
// partition of stmt. Then every partition of stmt is returned as a suspect.
func (v *Validation) divergedKeys(err error, stmt *typedef.Stmt) (diverged []int, attributed bool) {
	keys := stmt.PartitionKeys
	all := make([]int, len(keys))
	for i := range keys {
		all[i] = i
	}
	if len(keys) <= 1 {
		return all, true
	}

	ve, ok := errors.AsType[*store.ValidationError](err)
	if !ok || ve.ComparisonResults == nil {
		return all, false
	}

	hit := make([]bool, len(keys))
	mark := func(row store.Row) bool {
		ci := v.partitionOf(row, keys)
		if ci < 0 {
			return false
		}
		hit[ci] = true
		return true
	}

	cr := ve.ComparisonResults
	for _, row := range cr.TestOnlyRows {
		if !mark(row) {
			return all, false
		}
	}
	for _, row := range cr.OracleOnlyRows {
		if !mark(row) {
			return all, false
		}
	}
	for _, d := range cr.DifferentRows {
		if !mark(d.TestRow) && !mark(d.OracleRow) {
			return all, false
		}
	}

	for i, h := range hit {
		if h {
			diverged = append(diverged, i)
		}
	}
	if len(diverged) == 0 {
		return all, false
	}

	return diverged, true
}

// partitionOf returns the index of the partition in keys that row belongs to,
// or -1.
func (v *Validation) partitionOf(row store.Row, keys []typedef.PartitionKeys) int {
	for ci := range keys {
		if v.rowInPartition(row, &keys[ci]) {
			return ci
		}
	}

	return -1
}

func (v *Validation) rowInPartition(row store.Row, pk *typedef.PartitionKeys) bool {
	if pk.Values == nil {
		return false
	}
	for _, col := range v.table.PartitionKeys {
		values := pk.Values.Get(col.Name)
		if len(values) != 1 || !store.ValuesEqual(row.Get(col.Name), values[0]) {
			return false
		}
	}

	return true
}

// buildPartialJobError reports the partitions of stmt at the indexes where
// newly is true, with only their rows in the comparison results. The other differing partitions
// were already reported or are already known to differ because of gemini.
func (v *Validation) buildPartialJobError(err error, stmt *typedef.Stmt, newly []bool, prefix string) *joberror.JobError {
	keep := make([]typedef.PartitionKeys, 0, len(stmt.PartitionKeys))
	for i := range stmt.PartitionKeys {
		if newly[i] {
			keep = append(keep, stmt.PartitionKeys[i])
		}
	}
	view := &typedef.Stmt{
		PartitionKeys: keep,
		Query:         stmt.Query,
		Values:        stmt.Values,
		QueryType:     stmt.QueryType,
	}

	je := v.buildJobError(err, view, prefix+err.Error())
	if ve, ok := errors.AsType[*store.ValidationError](err); ok && ve.ComparisonResults != nil {
		inKeep := func(row store.Row) bool { return v.partitionOf(row, keep) >= 0 }
		cr := ve.ComparisonResults
		filtered := &store.ComparisonResults{
			TestRows:       filterRows(cr.TestRows, inKeep),
			OracleRows:     filterRows(cr.OracleRows, inKeep),
			TestOnlyRows:   filterRows(cr.TestOnlyRows, inKeep),
			OracleOnlyRows: filterRows(cr.OracleOnlyRows, inKeep),
		}
		for _, d := range cr.DifferentRows {
			if inKeep(d.TestRow) || inKeep(d.OracleRow) {
				filtered.DifferentRows = append(filtered.DifferentRows, d)
			}
		}
		je.Results = comparisonResults(filtered)
	}

	return je
}

// indexDivergence returns the JobError of an index query, or nil. An index
// query names no partition: its rows come from any partition that the index
// matches. Each differing row is attributed to its partition by
// partitions.KeyFingerprint. A row is reported only when ClaimDivergedKey
// reports its partition as new. A partition that gemini made diverge, for
// example with an asymmetric write, was named by MarkInvalid, and a partition
// that an earlier report named was claimed. Neither is reported again. A row
// without a fingerprint is always reported.
func (v *Validation) indexDivergence(err error, stmt *typedef.Stmt, prefix string) *joberror.JobError {
	ve, ok := errors.AsType[*store.ValidationError](err)
	if !ok || ve.ComparisonResults == nil {
		return v.buildJobError(err, stmt, prefix+err.Error())
	}

	claims := make(map[string]bool)
	isNew := func(row store.Row) bool {
		fp, hasFP := partitions.KeyFingerprint(v.table.PartitionKeys, row.Get)
		if !hasFP {
			return true
		}
		claimed, seen := claims[fp]
		if !seen {
			claimed = v.generator.ClaimDivergedKey(fp)
			claims[fp] = claimed
		}
		return claimed
	}

	cr := ve.ComparisonResults
	filtered := &store.ComparisonResults{
		TestOnlyRows:   filterRows(cr.TestOnlyRows, isNew),
		OracleOnlyRows: filterRows(cr.OracleOnlyRows, isNew),
	}
	for _, d := range cr.DifferentRows {
		if isNew(d.TestRow) {
			filtered.DifferentRows = append(filtered.DifferentRows, d)
		}
	}
	if len(filtered.TestOnlyRows)+len(filtered.OracleOnlyRows)+len(filtered.DifferentRows) == 0 {
		return nil
	}
	if len(claims) > 0 {
		reported := func(row store.Row) bool {
			fp, hasFP := partitions.KeyFingerprint(v.table.PartitionKeys, row.Get)
			return !hasFP || claims[fp]
		}
		filtered.TestRows = filterRows(cr.TestRows, reported)
		filtered.OracleRows = filterRows(cr.OracleRows, reported)
	}

	je := v.buildJobError(err, stmt, prefix+err.Error())
	je.Results = comparisonResults(filtered)

	return je
}

func filterRows(rows store.Rows, keep func(store.Row) bool) store.Rows {
	var out store.Rows
	for _, row := range rows {
		if keep(row) {
			out = append(out, row)
		}
	}

	return out
}

// extractComparisonResults extracts comparison results from a store.ValidationError
// and converts store.Rows to jsontext.Value for the joberror format.
func extractComparisonResults(err error) *joberror.ComparisonResults {
	var valErr *store.ValidationError
	if !errors.As(err, &valErr) || valErr.ComparisonResults == nil {
		return nil
	}

	return comparisonResults(valErr.ComparisonResults)
}

func comparisonResults(cr *store.ComparisonResults) *joberror.ComparisonResults {
	return &joberror.ComparisonResults{
		TestRows:       marshalRows(cr.TestRows),
		OracleRows:     marshalRows(cr.OracleRows),
		TestOnlyRows:   marshalRows(cr.TestOnlyRows),
		OracleOnlyRows: marshalRows(cr.OracleOnlyRows),
		DifferentRows:  marshalDifferentRows(cr.DifferentRows),
	}
}

func marshalRows(rows store.Rows) []jsontext.Value {
	if len(rows) == 0 {
		return nil
	}
	result := make([]jsontext.Value, 0, len(rows))
	for _, row := range rows {
		data, err := json.Marshal(row, json.Deterministic(true))
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
		testData := utils.MarshalJSONKey(d.TestRow)
		oracleData := utils.MarshalJSONKey(d.OracleRow)
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
		first, last, failure, recent, successCount := v.generator.ValidationStats(pk)
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

// deletedPartitionQuery returns the SELECT of one whole partition of the
// table. It builds the query on the first call and keeps it.
func (v *Validation) deletedPartitionQuery() string {
	if v.deletedQuery != "" {
		return v.deletedQuery
	}

	builder := qb.Select(v.keyspaceName + "." + v.table.Name).Columns(v.selectColumns...)
	for _, pk := range v.table.PartitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
	}
	v.deletedQuery, _ = builder.ToCql()

	return v.deletedQuery
}

// createSelectStmtForPartitionKeys creates a SELECT statement for specific partition key values
func (v *Validation) createSelectStmtForPartitionKeys(keys typedef.PartitionKeys) *typedef.Stmt {
	return &typedef.Stmt{
		QueryType:     typedef.SelectStatementType,
		Query:         v.deletedPartitionQuery(),
		PartitionKeys: []typedef.PartitionKeys{keys},
		Values:        keys.Values.ToCQLValues(v.table.PartitionKeys),
	}
}

// validateDeletedPartition validates a deleted partition by verifying it no longer exists.
// The partition should have been deleted from both test and oracle clusters, so we expect
// zero rows to be returned.
//
// store.Check decides the outcome from all its attempts. A divergence is
// reported. A read failure of the test cluster is a read error, and a read
// failure of the oracle cluster alone is not charged to the system under test.
func (v *Validation) validateDeletedPartition(ctx context.Context, keys typedef.PartitionKeys, deletionTimeNS uint64) error {
	stmt := v.createSelectStmtForPartitionKeys(keys)

	// Ensure partition keys are released when we're done
	defer func() {
		if keys.Release != nil {
			keys.Release()
		}
	}()

	_, _, err := v.store.Check(ctx, v.table, stmt, 0)
	if err == nil {
		if v.generator != nil {
			v.generator.ValidationSuccess(&stmt.PartitionKeys[0])
		}
		return nil
	}

	// A context error that is not a validation result means that the
	// worker's context ended.
	_, isValidationErr := errors.AsType[*store.ValidationError](err)
	if ctx.Err() != nil || (!isValidationErr && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled))) {
		return err
	}

	switch {
	case store.IsDivergence(err):
		if v.generator != nil {
			v.generator.ValidationFailure(&stmt.PartitionKeys[0])
			// Mark the partition invalid and only report if this is the first time.
			if !v.generator.MarkInvalid(&stmt.PartitionKeys[0]) {
				return nil
			}
		}
		jobErr := v.buildJobError(err, stmt, "Deleted partition validation failed (difference found): "+err.Error())
		jobErr.DeletionTimeNS = deletionTimeNS
		return jobErr
	case store.IsOracleLoadError(err):
		metrics.ValidationReadFailures.WithLabelValues(string(store.TypeOracle)).Inc()
		return nil
	default:
		metrics.ValidationReadFailures.WithLabelValues(string(store.TypeTest)).Inc()
		jobErr := v.buildJobError(err, stmt,
			"Deleted partition validation failed (test cluster read error, no data mismatch was seen): "+err.Error())
		jobErr.DeletionTimeNS = deletionTimeNS
		return jobErr
	}
}

func (v *Validation) Do(ctx context.Context) error {
	name := v.Name()

	executionTime := metrics.ExecutionTimeStart(name)
	metrics.GeminiInformation.WithLabelValues("validation_" + v.table.Name).Inc()
	defer metrics.GeminiInformation.WithLabelValues("validation_" + v.table.Name).Dec()

	validatedRowsMetric := metrics.ValidatedRows.WithLabelValues(v.table.Name)
	deletedPartitionsCh := v.generator.Deleted()

	rq := newRetryQueue(v.maxAttempts, v.delay, store.ValidationRetryMinDelay)
	defer func() {
		metrics.ValidationRetriesAbandoned.Add(float64(rq.Len()))
		rq.Drain(releaseKeys)
	}()

	for !v.stopFlag.IsHardOrSoft() {
		if v.limiter != nil {
			if err := v.parkUntilAllowed(ctx, rq, validatedRowsMetric); err != nil {
				if errors.Is(err, ErrValidationJobStopped) {
					return err
				}
				return nil
			}
			if v.stopFlag.IsHardOrSoft() {
				break
			}
		}

		// First, check if any retries are ready (non-blocking).
		if idx := rq.Ready(); idx >= 0 {
			if err := v.runRetry(ctx, rq, idx, validatedRowsMetric); err != nil {
				return err
			}
			continue
		}

		// Main select: context, deleted partitions, retry timer, or new work.
		earliestIdx, retryTimerCh := rq.Earliest()

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
			// The earliest retry's timer fired — pick up that retry.
			if err := v.runRetry(ctx, rq, earliestIdx, validatedRowsMetric); err != nil {
				return err
			}

		default:
			if rq.Saturated() {
				// Backpressure: stop generating new work while the retry
				// queue is saturated so a sustained cluster fault can't pin
				// an unbounded number of partition keys or delay soft-stop
				// behind a backlog of doomed retries. Wait briefly for the
				// queue to drain instead of busy-looping.
				timer := utils.GetTimer(retryQueueSaturationBackoff)
				select {
				case <-ctx.Done():
					utils.PutTimer(timer)
					return nil
				case <-timer.C:
					utils.PutTimer(timer)
				}
				continue
			}

			if err := v.handleNewWork(ctx, &executionTime, validatedRowsMetric, rq); err != nil {
				return err
			}
		}
	}

	return v.finishPendingRetries(ctx, rq, validatedRowsMetric)
}

// parkUntilAllowed waits while the limiter parks this worker. A parked worker
// starts no new validation, which is the purpose of the limiter. It still
// runs the retries that come due, because a retry is a validation that
// already started: left in the queue, its keys stay pinned and its result
// comes late. It returns ctx.Err() when ctx ends, and ErrValidationJobStopped
// when a retry uses up the error budget.
func (v *Validation) parkUntilAllowed(ctx context.Context, rq *retryQueue, metric prometheus.Counter) error {
	for {
		earliestIdx, retryTimerCh := rq.Earliest()
		woke, err := v.limiter.WaitOrWake(ctx, v.workerIdx, v.stopFlag.IsHardOrSoft, retryTimerCh)
		if err != nil || !woke {
			return err
		}

		if err = v.runRetry(ctx, rq, earliestIdx, metric); err != nil {
			return err
		}
	}
}

// runRetry takes the retry at idx, whose timer fired, and runs its next
// attempt. Returns a non-nil error only when the worker must stop.
func (v *Validation) runRetry(ctx context.Context, rq *retryQueue, idx int, metric prometheus.Counter) error {
	retry := rq.Take(idx)
	result := v.checkOnce(ctx, retry.stmt, retry.attempt, retry.history, metric)

	return v.handleRunResult(result, rq)
}

// finishPendingRetries runs the queued retries to completion after a soft
// stop. A retry is a validation that already started, so dropping it would
// leave a possible difference unreported. Each retry is bounded by
// maxAttempts. A hard stop or the end of ctx leaves the rest to the drain in
// Do, which counts them in validation_retries_abandoned_total.
func (v *Validation) finishPendingRetries(ctx context.Context, rq *retryQueue, metric prometheus.Counter) error {
	for rq.Len() > 0 && !v.stopFlag.IsHard() {
		// Ready and the select below both read the fired timer, so the retry
		// they name is taken by its index.
		var retry pendingRetry
		if idx := rq.Ready(); idx >= 0 {
			retry = rq.Take(idx)
		} else {
			earliestIdx, timerCh := rq.Earliest()
			select {
			case <-ctx.Done():
				return nil
			case <-timerCh:
				retry = rq.Take(earliestIdx)
			}
		}

		result := v.checkOnce(ctx, retry.stmt, retry.attempt, retry.history, metric)
		if err := v.handleRunResult(result, rq); err != nil {
			return err
		}
	}

	return nil
}

// handleDeletedPartition validates a single deleted-partition event received
// from the generator's Deleted channel. Returns a non-nil error only when the
// worker must stop.
func (v *Validation) handleDeletedPartition(ctx context.Context, executionTime *metrics.RunningTime, deletedKeys typedef.PartitionKeys) error {
	executionTime.Start()
	err := v.validateDeletedPartition(ctx, deletedKeys, deletedKeys.DeletedAtNS)
	executionTime.Record()

	if err == nil {
		v.status.ReadOp()
		return nil
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}

	if jobErr, isJobErr := errors.AsType[*joberror.JobError](err); isJobErr {
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
	switch rq.Schedule(result.stmt, result.attempt, result.history) {
	case scheduleAccepted:
		// Stmt ownership transferred to the retry queue — do NOT release keys.
		return nil
	case scheduleSaturated:
		// Backpressure dropped this retry: release the keys but do NOT report a
		// validation failure — a full queue is not a data discrepancy. (Do()
		// gates new work on Saturated(), so this is normally unreachable; kept
		// safe for any future caller that schedules without that pre-check.)
		metrics.ValidationRetriesDropped.Inc()
		releaseKeys(result.stmt)
		return nil
	case scheduleExhausted:
		// Fall through to final-failure handling below.
	}

	// All retries exhausted — treat as final failure.
	metrics.ValidationRetriesExhausted.Inc()
	failErr := v.handleCheckFailure(result.history, result.stmt)
	if failErr == nil {
		return nil
	}

	if jobErr, isJobErr := errors.AsType[*joberror.JobError](failErr); isJobErr {
		v.status.AddReadError(*jobErr)
	} else {
		panic(fmt.Sprintf("invalid type err %T: %v", failErr, failErr))
	}

	if v.status.HasReachedErrorCount() {
		v.stopFlag.SetSoft(true)
		metrics.WorkerStopEvents.WithLabelValues("validation", "error_budget").Inc()
		return ErrValidationJobStopped
	}

	return nil
}

func (v *Validation) Name() string {
	return "validation_" + v.table.Name
}
