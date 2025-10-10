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

// calculateExponentialBackoff calculates the delay for exponential backoff
func (ds delegatingStore) calculateExponentialBackoff(attempt int) time.Duration {
	// Base delay is the configured mutationRetrySleep
	// Exponential backoff: baseDelay * 2^attempt with some jitter
	baseDelay := ds.mutationRetrySleep
	if attempt == 0 {
		return baseDelay
	}

	// Cap the exponential growth to avoid extremely long delays
	multiplier := 1 << uint(attempt) // 2^attempt
	if multiplier > 16 {             // Cap at 16x base delay
		multiplier = 16
	}

	delay := baseDelay * time.Duration(multiplier)
	// Add up to 10% jitter to avoid thundering herd
	jitterMultiplier := 0.5 + 0.5*float64(time.Now().UnixNano()%2)
	jitter := time.Duration(float64(delay) * 0.1 * jitterMultiplier)
	return delay + jitter
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

	var cumulativeResult mutationResult
	maxAttempts := ds.mutationRetries + 1 // +1 for the initial attempt

	for attempt := range maxAttempts {
		// Determine which stores need to be tried/retried
		retryTest := attempt == 0 || cumulativeResult.testErr != nil
		retryOracle := (attempt == 0 || cumulativeResult.oracleErr != nil) && ds.oracleStore != nil

		// Execute mutations only on stores that need to be retried
		attemptResult := ds.executeParallelMutations(ctx, stmt, ts, attempt, retryTest, retryOracle)

		// Update cumulative result with new attempts
		if attemptResult.testDone {
			cumulativeResult.testDone = true
			cumulativeResult.testErr = attemptResult.testErr
		}
		if attemptResult.oracleDone {
			cumulativeResult.oracleDone = true
			cumulativeResult.oracleErr = attemptResult.oracleErr
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
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return pkgerrors.Wrap(ctx.Err(), "mutation cancelled during retry delay")
			}
		}
	}

	// All attempts failed, return the combined errors
	if cumulativeResult.testErr != nil || cumulativeResult.oracleErr != nil {
		// Build detailed failure information
		failedStores := make([]string, 0, 2)
		successfulStores := make([]string, 0, 2)

		if cumulativeResult.testErr != nil {
			failedStores = append(failedStores, "test")
		} else if cumulativeResult.testDone {
			successfulStores = append(successfulStores, "test")
		}

		if ds.oracleStore != nil {
			if cumulativeResult.oracleErr != nil {
				failedStores = append(failedStores, "oracle")
			} else if cumulativeResult.oracleDone {
				successfulStores = append(successfulStores, "oracle")
			}
		}

		ds.logger.Error("mutation failed after all retry attempts",
			zap.Int("total_attempts", maxAttempts),
			zap.Strings("failed_stores", failedStores),
			zap.Strings("successful_stores", successfulStores),
			zap.Error(cumulativeResult.testErr),
			zap.NamedError("oracle_error", cumulativeResult.oracleErr))
		return errors.Join(cumulativeResult.testErr, cumulativeResult.oracleErr)
	}

	return nil
}

//nolint:gocyclo
func (ds delegatingStore) Check(
	ctx context.Context,
	table *typedef.Table,
	stmt *typedef.Stmt,
	attempt int,
) (int, error) {
	var oracleCh chan mo.Result[any]

	doCtx := WithContextData(ctx, &ContextData{
		GeminiAttempt: attempt,
		Statement:     stmt,
		Timestamp:     time.Now().UTC(),
	})

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

	if testResult.IsError() {
		return 0, pkgerrors.Wrap(testResult.Error(), "unable to load check data from the test store")
	}

	if oracleCh == nil {
		rows := testResult.MustGet()
		if rows == nil {
			return 0, nil
		}

		return len(rows.(Rows)), nil
	}

	// Wait for oracle result with context cancellation support
	var oracleResult mo.Result[any]
	select {
	case oracleResult = <-oracleCh:
	case <-ctx.Done():
		ds.workers.Release(oracleCh)
		return 0, ctx.Err()
	}
	ds.workers.Release(oracleCh)

	if oracleResult.IsError() {
		return 0, pkgerrors.Wrap(oracleResult.Error(), "unable to load check data from the oracle store")
	}

	oracle := oracleResult.MustGet()
	test := testResult.MustGet()

	var (
		testRows   Rows
		oracleRows Rows
	)

	if oracle == nil && test == nil {
		return 0, nil
	}

	if oracle == nil {
		oracleRows = Rows{}
	} else {
		oracleRows = oracle.(Rows)
	}

	if test == nil {
		testRows = Rows{}
	} else {
		testRows = test.(Rows)
	}

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

	for i, oracleRow := range oracleRows {
		if diff := cmp.Diff(oracleRow, testRows[i], comparers...); diff != "" {
			return 0, ErrorRowDifference{
				Diff:      diff,
				OracleRow: oracleRow,
				TestRow:   testRows[i],
			}
		}
	}

	return len(testRows), nil
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
