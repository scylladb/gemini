// Copyright 2019 ScyllaDB
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

package store

import (
	"context"
	"errors"
	"io"
	"math/big"
	"slices"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	pkgerrors "github.com/pkg/errors"
	"github.com/samber/mo"
	"github.com/scylladb/go-set/strset"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"gopkg.in/inf.v0"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
	"github.com/scylladb/gemini/pkg/workpool"
)

var comparers = []cmp.Option{
	cmp.AllowUnexported(),
	cmpopts.SortMaps(func(x, y *inf.Dec) bool {
		return x.Cmp(y) < 0
	}),
	cmp.Comparer(func(x, y *inf.Dec) bool {
		return x.Cmp(y) == 0
	}), cmp.Comparer(func(x, y *big.Int) bool {
		return x.Cmp(y) == 0
	}),
}

type loader interface {
	load(context.Context, *typedef.Stmt) (Rows, error)
}

type storer interface {
	mutate(context.Context, *typedef.Stmt, mo.Option[time.Time]) error
}

type storeLoader interface {
	storer
	loader
	Close() error
	name() string
}

type Store interface {
	io.Closer

	Create(context.Context, *typedef.Stmt, *typedef.Stmt) error
	Mutate(context.Context, *typedef.Stmt) error
	Check(context.Context, *typedef.Table, *typedef.Stmt, int) (int, error)
}

type (
	ScyllaClusterConfig struct {
		Name                    stmtlogger.Type
		HostSelectionPolicy     HostSelectionPolicy
		Consistency             string
		Username                string
		Password                string
		TracingDir              string
		Hosts                   []string
		RequestTimeout          time.Duration
		ConnectTimeout          time.Duration
		UseServerSideTimestamps bool
	}
	Config struct {
		OracleClusterConfig              *ScyllaClusterConfig
		OracleStatementFile              string
		TestStatementFile                string
		TestClusterConfig                ScyllaClusterConfig
		MaxRetriesMutate                 int
		MaxRetriesMutateSleep            time.Duration
		AsyncObjectStabilizationAttempts int
		AsyncObjectStabilizationDelay    time.Duration
		Compression                      stmtlogger.Compression
		UseServerSideTimestamps          bool
	}
)

func New(
	schemaChangesValues typedef.PartitionKeys,
	workers *workpool.Pool,
	schema *typedef.Schema,
	cfg Config,
	logger *zap.Logger,
	e *joberror.ErrorList,
) (Store, error) {
	logger.Debug("creating store",
		zap.Bool("has_oracle", cfg.OracleClusterConfig != nil),
		zap.Int("max_retries_mutate", cfg.MaxRetriesMutate),
		zap.Duration("retry_sleep_mutate", cfg.MaxRetriesMutateSleep),
		zap.Int("async_stabilization_attempts", cfg.AsyncObjectStabilizationAttempts),
		zap.Duration("async_stabilization_delay", cfg.AsyncObjectStabilizationDelay),
		zap.Bool("server_side_timestamps", cfg.UseServerSideTimestamps),
	)

	var statementLogger *stmtlogger.Logger
	if cfg.OracleClusterConfig != nil {
		logger.Debug("creating statement logger with oracle cluster")
		var err error
		statementLogger, err = stmtlogger.NewLogger(
			stmtlogger.WithScyllaLogger(
				schemaChangesValues,
				schema,
				cfg.OracleStatementFile,
				cfg.TestStatementFile,
				cfg.OracleClusterConfig.Hosts,
				cfg.OracleClusterConfig.Username,
				cfg.OracleClusterConfig.Password,
				cfg.Compression,
				e,
				workers,
				logger.Named("stmt_logger"),
			))
		if err != nil {
			return nil, pkgerrors.Wrap(err, "failed to create statement logger")
		}
		logger.Debug("statement logger created successfully")
	}

	if cfg.MaxRetriesMutate < 0 {
		cfg.MaxRetriesMutate = 10
	}

	if cfg.MaxRetriesMutateSleep <= 50*time.Millisecond {
		cfg.MaxRetriesMutateSleep = 50 * time.Millisecond
	}

	if cfg.AsyncObjectStabilizationAttempts <= 0 {
		cfg.AsyncObjectStabilizationAttempts = 10
	}

	if cfg.AsyncObjectStabilizationDelay <= 0 {
		cfg.AsyncObjectStabilizationDelay = 10 * time.Millisecond
	}

	logger.Debug("creating test store", zap.Strings("hosts", cfg.TestClusterConfig.Hosts))
	testStore, err := newCQLStore(
		cfg.TestClusterConfig,
		statementLogger,
		schema,
		logger.Named("test_store"),
		"test",
	)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "failed to create test cluster")
	}
	logger.Debug("test store created successfully")

	var oracleStore storeLoader

	if cfg.OracleClusterConfig != nil {
		logger.Debug("creating oracle store", zap.Strings("hosts", cfg.OracleClusterConfig.Hosts))
		oracleStore, err = newCQLStore(
			*cfg.OracleClusterConfig,
			statementLogger,
			schema,
			logger.Named("oracle_store"),
			"oracle",
		)
		if err != nil {
			return nil, pkgerrors.Wrap(err, "failed to create oracle cluster")
		}
		logger.Debug("oracle store created successfully")
	}

	ds := &delegatingStore{
		workers:              workers,
		testStore:            testStore,
		oracleStore:          oracleStore,
		logger:               logger.Named("delegating_store"),
		statementLogger:      statementLogger,
		serverSideTimestamps: cfg.UseServerSideTimestamps,
		mutationRetries:      cfg.MaxRetriesMutate,
		mutationRetrySleep:   cfg.MaxRetriesMutateSleep,
		validationRetries:    cfg.AsyncObjectStabilizationAttempts,
		validationRetrySleep: cfg.AsyncObjectStabilizationDelay,
	}

	logger.Debug("store created successfully")
	return ds, nil
}

type delegatingStore struct {
	workers              *workpool.Pool
	oracleStore          storeLoader
	testStore            storeLoader
	logger               *zap.Logger
	statementLogger      *stmtlogger.Logger
	mutationRetrySleep   time.Duration
	mutationRetries      int
	validationRetrySleep time.Duration
	validationRetries    int
	serverSideTimestamps bool
}

func (ds delegatingStore) Create(ctx context.Context, testBuilder, stmt *typedef.Stmt) error {
	ctx = WithContextData(ctx, &ContextData{
		GeminiAttempt: 0,
		Statement:     stmt,
	})

	ts := mo.None[time.Time]()

	if !ds.serverSideTimestamps {
		ts = ts.MapNone(func() (time.Time, bool) {
			return time.Now(), true
		})
	}

	if err := ds.testStore.mutate(ctx, testBuilder, ts); err != nil {
		return pkgerrors.Wrapf(
			err,
			"unable to apply mutations to the %s store",
			ds.testStore.name(),
		)
	}

	if ds.oracleStore != nil {
		if err := ds.oracleStore.mutate(ctx, stmt, ts); err != nil {
			return pkgerrors.Wrapf(
				err,
				"unable to apply mutations to the %s store",
				ds.testStore.name(),
			)
		}
		ds.logger.Debug("oracle store created successfully")
	}

	return nil
}

// mutationResult represents the result of a single mutation attempt
type mutationResult struct {
	testErr    error
	oracleErr  error
	testDone   bool
	oracleDone bool
}

// calculateExponentialBackoff returns an exponential backoff delay where mutationRetrySleep is the maximum cap.
func (ds delegatingStore) calculateExponentialBackoff(attempt int) time.Duration {
	// Use a small minimum delay and double each attempt, capped by ds.mutationRetrySleep.
	return utils.ExponentialBackoffCapped(attempt, ds.mutationRetrySleep, 10*time.Millisecond)
}

// executeParallelMutations executes mutations on stores that need to be retried
func (ds delegatingStore) executeParallelMutations(
	ctx context.Context,
	stmt *typedef.Stmt,
	timestamp mo.Option[time.Time],
	attempt int,
	retryTest, retryOracle bool,
) mutationResult {
	result := mutationResult{}

	// Update context with attempt number
	ctxData := &ContextData{
		GeminiAttempt: attempt,
		Statement:     stmt,
		Timestamp:     time.Now().UTC(),
	}
	doCtx := WithContextData(ctx, ctxData)

	var testCh, oracleCh chan mo.Result[any]

	// Start test mutation only if we need to retry it
	if retryTest {
		testCh = ds.workers.Send(doCtx, func(ctx context.Context) (any, error) {
			return nil, ds.testStore.mutate(ctx, stmt, timestamp)
		})
	}

	// Start oracle mutation only if we need to retry it and it exists
	if retryOracle && ds.oracleStore != nil {
		oracleCh = ds.workers.Send(doCtx, func(ctx context.Context) (any, error) {
			return nil, ds.oracleStore.mutate(ctx, stmt, timestamp)
		})
	}

	// Wait for results
	ds.waitForMutationResults(doCtx, testCh, oracleCh, &result, retryTest, retryOracle)

	return result
}

// waitForMutationResults waits for mutation results and updates the result struct
//
//nolint:gocyclo
func (ds delegatingStore) waitForMutationResults(ctx context.Context, testCh, oracleCh chan mo.Result[any], result *mutationResult, expectTest, expectOracle bool) {
	testPending := testCh != nil
	oraclePending := oracleCh != nil

	for testPending || oraclePending {
		switch {
		case testPending && oraclePending:
			select {
			case testRes := <-testCh:
				ds.workers.Release(testCh)
				result.testDone = true
				testPending = false
				if testRes.IsError() {
					result.testErr = testRes.Error()
				}

			case oracleRes := <-oracleCh:
				ds.workers.Release(oracleCh)
				result.oracleDone = true
				oraclePending = false
				if oracleRes.IsError() {
					result.oracleErr = oracleRes.Error()
				}

			case <-ctx.Done():
				if testPending {
					ds.workers.Release(testCh)
					result.testErr = ctx.Err()
				}
				if oraclePending {
					ds.workers.Release(oracleCh)
					result.oracleErr = ctx.Err()
				}
				return
			}
		case testPending:
			select {
			case testRes := <-testCh:
				ds.workers.Release(testCh)
				result.testDone = true
				testPending = false
				if testRes.IsError() {
					result.testErr = testRes.Error()
				}

			case <-ctx.Done():
				ds.workers.Release(testCh)
				result.testErr = ctx.Err()
				return
			}
		case oraclePending:
			select {
			case oracleRes := <-oracleCh:
				ds.workers.Release(oracleCh)
				result.oracleDone = true
				oraclePending = false
				if oracleRes.IsError() {
					result.oracleErr = oracleRes.Error()
				}

			case <-ctx.Done():
				ds.workers.Release(oracleCh)
				result.oracleErr = ctx.Err()
				return
			}
		}
	}

	// Mark stores as done if they weren't retried (meaning they were already successful)
	if !expectTest {
		result.testDone = true
	}
	if !expectOracle {
		result.oracleDone = true
	}
}

// shouldRetryMutations determines if we should retry based on the mutation results
func (ds delegatingStore) shouldRetryMutations(result mutationResult) bool {
	return result.testErr != nil || (ds.oracleStore != nil && result.oracleErr != nil)
}

// logMutationRetry logs retry attempts with appropriate details
func (ds delegatingStore) logMutationRetry(attempt, maxAttempts int, result mutationResult, retryTest, retryOracle bool) {
	fields := []zap.Field{
		zap.Int("attempt", attempt),
		zap.Int("max_attempts", maxAttempts),
	}

	var failedStores, successfulStores, retryingStores []string

	// Track test store status
	if result.testErr != nil {
		fields = append(fields, zap.Error(result.testErr))
		failedStores = append(failedStores, "test")
		if retryTest {
			retryingStores = append(retryingStores, "test")
		}
	} else if result.testDone {
		successfulStores = append(successfulStores, "test")
	}

	// Track oracle store status
	if ds.oracleStore != nil {
		if result.oracleErr != nil {
			fields = append(fields, zap.NamedError("oracle_error", result.oracleErr))
			failedStores = append(failedStores, "oracle")
			if retryOracle {
				retryingStores = append(retryingStores, "oracle")
			}
		} else if result.oracleDone {
			successfulStores = append(successfulStores, "oracle")
		}
	}

	if len(failedStores) > 0 {
		fields = append(fields,
			zap.Strings("failed_stores", failedStores),
			zap.Strings("successful_stores", successfulStores),
			zap.Strings("retrying_stores", retryingStores))

		ds.logger.Warn("mutation failed, retrying with exponential backoff", fields...)
	}
}

//nolint:gocyclo
func (ds delegatingStore) Mutate(ctx context.Context, stmt *typedef.Stmt) error {
	// Prepare timestamp
	ts := mo.None[time.Time]()
	if !ds.serverSideTimestamps {
		ts = mo.Some(time.Now().UTC())
	}

	mutationErr := NewStoreMutationError(stmt, nil)
	var cumulativeResult mutationResult
	maxAttempts := ds.mutationRetries + 1 // +1 for the initial attempt

	for attempt := range maxAttempts {
		attemptStart := time.Now()

		// Determine which stores need to be tried/retried
		retryTest := attempt == 0 || cumulativeResult.testErr != nil
		retryOracle := (attempt == 0 || cumulativeResult.oracleErr != nil) && ds.oracleStore != nil

		// Execute mutations only on stores that need to be retried
		attemptResult := ds.executeParallelMutations(ctx, stmt, ts, attempt, retryTest, retryOracle)
		duration := time.Since(attemptStart)

		// Record any errors that occurred during this attempt
		if attemptResult.testErr != nil {
			mutationErr.AddAttempt(attempt, TypeTest, attemptResult.testErr, duration)
		}
		if attemptResult.oracleErr != nil {
			mutationErr.AddAttempt(attempt, TypeOracle, attemptResult.oracleErr, duration)
		}

		// Update cumulative result with new attempts
		if attemptResult.testDone {
			cumulativeResult.testDone = true
			cumulativeResult.testErr = attemptResult.testErr
			mutationErr.SetStoreSuccess(TypeTest, attemptResult.testErr == nil)
		}
		if attemptResult.oracleDone {
			cumulativeResult.oracleDone = true
			cumulativeResult.oracleErr = attemptResult.oracleErr
			mutationErr.SetStoreSuccess(TypeOracle, attemptResult.oracleErr == nil)
		}

		// Check if we succeeded
		if !ds.shouldRetryMutations(cumulativeResult) {
			if attempt > 0 {
				// Log which systems succeeded after retries
				successfulStores := make([]string, 0, 2)
				if cumulativeResult.testDone && cumulativeResult.testErr == nil {
					successfulStores = append(successfulStores, "test")
				}
				if ds.oracleStore != nil && cumulativeResult.oracleDone && cumulativeResult.oracleErr == nil {
					successfulStores = append(successfulStores, "oracle")
				}

				ds.logger.Info("mutation succeeded after retries",
					zap.Int("attempts", attempt+1),
					zap.Strings("successful_stores", successfulStores))
			}
			return nil
		}

		// Log retry attempt (except for the last attempt which will fail)
		if attempt < maxAttempts-1 {
			// Determine what will be retried in the next attempt
			nextRetryTest := cumulativeResult.testErr != nil
			nextRetryOracle := cumulativeResult.oracleErr != nil && ds.oracleStore != nil
			ds.logMutationRetry(attempt+1, maxAttempts, cumulativeResult, nextRetryTest, nextRetryOracle)

			// Apply exponential backoff delay
			delay := ds.calculateExponentialBackoff(attempt)
			timer := utils.GetTimer(delay)
			select {
			case <-timer.C:
				utils.PutTimer(timer)
				continue
			case <-ctx.Done():
				utils.PutTimer(timer)
				if errors.Is(ctx.Err(), context.Canceled) {
					return nil
				}

				return pkgerrors.Wrap(ctx.Err(), "mutation cancelled during retry delay")
			}
		}
	}

	// All attempts failed - finalize the mutation error with comprehensive details
	var finalErr error

	switch {
	case cumulativeResult.testErr != nil && cumulativeResult.oracleErr != nil:
		finalErr = errors.Join(cumulativeResult.testErr, cumulativeResult.oracleErr)
	case cumulativeResult.testErr != nil:
		finalErr = cumulativeResult.testErr
	case cumulativeResult.oracleErr != nil:
		finalErr = cumulativeResult.oracleErr
	}

	mutationErr.Finalize(finalErr)

	ds.logger.Error("mutation failed after all retry attempts",
		zap.Int("total_attempts", maxAttempts),
		zap.Bool("test_store_success", mutationErr.TestStoreSuccess),
		zap.Bool("oracle_store_success", mutationErr.OracleStoreSuccess),
		zap.Int("total_attempt_errors", len(mutationErr.Attempts)),
		zap.Error(mutationErr))

	return mutationErr
}

// helper to safely extract Rows from a mo.Result without risking MustGet on error
func rowsFromResult(res mo.Result[any]) (rows Rows, isNil bool, err error) {
	if res.IsError() {
		return nil, false, res.Error()
	}
	v := res.MustGet()
	if v == nil {
		return nil, true, nil
	}
	return v.(Rows), false, nil
}

// compareRows compares two sets of rows and returns either the matching row count or a descriptive error
func compareRows(table *typedef.Table, testRows, oracleRows Rows) (int, error) {
	if len(testRows) == 0 && len(oracleRows) == 0 {
		return 0, nil
	}

	if len(testRows) != len(oracleRows) {
		testSet := pks(table, testRows)
		oracleSet := pks(table, oracleRows)
		missingInTest := strset.Difference(oracleSet, testSet).List()
		missingInOracle := strset.Difference(testSet, oracleSet).List()

		return 0, ErrorRowDifference{
			MissingInTest:   missingInTest,
			MissingInOracle: missingInOracle,
			TestRows:        len(testRows),
			OracleRows:      len(oracleRows),
		}
	}

	if len(testRows) > 1 {
		slices.SortStableFunc(testRows, rowsCmp)
	}
	if len(oracleRows) > 1 {
		slices.SortStableFunc(oracleRows, rowsCmp)
	}
	var diffErr error
	for i, oracleRow := range oracleRows {
		if diff := cmp.Diff(oracleRow, testRows[i], comparers...); diff != "" {
			diffErr = multierr.Append(diffErr, ErrorRowDifference{
				Diff:      diff,
				OracleRow: oracleRow,
				TestRow:   testRows[i],
			})
		}
	}
	if diffErr == nil {
		return len(testRows), nil
	}
	return 0, diffErr
}

//nolint:gocyclo
func (ds delegatingStore) Check(
	ctx context.Context,
	table *typedef.Table,
	stmt *typedef.Stmt,
	_ int,
) (int, error) {
	validationErr := NewValidationError("validation", stmt, table)
	maxAttempts := ds.validationRetries
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	for attempt := range maxAttempts {
		attemptStart := time.Now()
		doCtx := WithContextData(ctx, &ContextData{
			GeminiAttempt: attempt,
			Statement:     stmt,
			Timestamp:     time.Now().UTC(),
		})

		// Launch loads
		var oracleCh chan mo.Result[any]
		if ds.oracleStore != nil {
			oracleCh = ds.workers.Send(doCtx, func(ctx context.Context) (any, error) {
				return ds.oracleStore.load(ctx, stmt)
			})
		}
		testCh := ds.workers.Send(doCtx, func(ctx context.Context) (any, error) {
			return ds.testStore.load(ctx, stmt)
		})

		// Wait for test result with context cancellation support
		var testResult mo.Result[any]
		select {
		case testResult = <-testCh:
		case <-ctx.Done():
			ds.workers.Release(testCh)
			if oracleCh != nil {
				ds.workers.Release(oracleCh)
			}
			return 0, ctx.Err()
		}
		ds.workers.Release(testCh)

		// If there is no oracle, handle only the test result safely
		if oracleCh == nil {
			testRows, testNil, testErr := rowsFromResult(testResult)
			duration := time.Since(attemptStart)

			if testErr != nil {
				validationErr.AddAttempt(attempt, TypeTest, testErr, duration)
			} else {
				if testNil {
					return 0, nil
				}
				return len(testRows), nil
			}
		} else {
			// We have oracle: wait for oracle result
			var oracleResult mo.Result[any]
			select {
			case oracleResult = <-oracleCh:
			case <-ctx.Done():
				ds.workers.Release(oracleCh)
				return 0, ctx.Err()
			}
			ds.workers.Release(oracleCh)

			// Safely extract both results
			testRows, testNil, testErr := rowsFromResult(testResult)
			oracleRows, oracleNil, oracleErr := rowsFromResult(oracleResult)
			duration := time.Since(attemptStart)

			// Record any errors that occurred
			if testErr != nil {
				validationErr.AddAttempt(attempt, TypeTest, testErr, duration)
			}
			if oracleErr != nil {
				validationErr.AddAttempt(attempt, TypeOracle, oracleErr, duration)
			}

			// If any side errored, continue to retry logic below
			if testErr == nil && oracleErr == nil {
				// Both sides OK; normalize nils and compare
				if testNil && oracleNil {
					return 0, nil
				}
				if testNil {
					testRows = Rows{}
				}
				if oracleNil {
					oracleRows = Rows{}
				}

				count, diffErr := compareRows(table, testRows, oracleRows)
				if diffErr == nil {
					return count, nil
				}

				validationErr.AddAttempt(attempt, TypeTest, diffErr, duration)

			}
		}

		// If not last attempt, wait with backoff before retrying
		if attempt < maxAttempts-1 {
			delay := utils.ExponentialBackoffCapped(attempt, ds.validationRetrySleep, 10*time.Millisecond)
			timer := utils.GetTimer(delay)
			select {
			case <-timer.C:
				utils.PutTimer(timer)
				// continue to next attempt
			case <-ctx.Done():
				utils.PutTimer(timer)
				if errors.Is(ctx.Err(), context.Canceled) {
					return 0, context.Canceled
				}
				return 0, pkgerrors.Wrap(ctx.Err(), "validation cancelled during retry delay")
			}
		}
	}

	// All attempts failed
	if len(validationErr.Attempts) == 0 {
		return 0, nil
	}

	if errors.Is(ctx.Err(), context.Canceled) {
		return 0, context.Canceled
	}

	// Finalize the validation error with summary
	lastAttempt := validationErr.GetLastAttempt()
	var finalErr error
	if lastAttempt != nil {
		finalErr = lastAttempt.Error
	}
	validationErr.Finalize(finalErr)

	return 0, validationErr
}

func (ds delegatingStore) Close() error {
	ds.logger.Info("closing store")

	ds.logger.Debug("closing test store")
	err := multierr.Append(nil, ds.testStore.Close())

	if ds.oracleStore != nil {
		ds.logger.Debug("closing oracle store")
		err = multierr.Append(err, ds.oracleStore.Close())
	}

	if ds.statementLogger != nil {
		ds.logger.Debug("closing statement logger")
		err = multierr.Append(err, ds.statementLogger.Close())
	}

	if err != nil {
		ds.logger.Error("store closed with errors", zap.Error(err))
	} else {
		ds.logger.Debug("store closed successfully")
	}

	return err
}
