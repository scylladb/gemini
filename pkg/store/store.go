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
	"time"

	pkgerrors "github.com/pkg/errors"
	"github.com/samber/mo"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/stmtlogger/scylla"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

type loader interface {
	load(context.Context, *typedef.Stmt) (Rows, error)
	loadIter(context.Context, *typedef.Stmt) RowIterator
}

type storer interface {
	mutate(context.Context, *typedef.Stmt, mo.Option[time.Time]) error
}

type storeLoader interface {
	storer
	loader
	Init() error
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
		Replication             replication.Replication
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
		UseServerSideTimestamps          bool
		MinimumDelay                     time.Duration
	}
)

func New(
	keyspace, table string,
	partitionKeyColumns typedef.Columns,
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

	if cfg.MaxRetriesMutate < 0 {
		cfg.MaxRetriesMutate = 10
	}

	if cfg.MaxRetriesMutateSleep <= 0 {
		cfg.MaxRetriesMutateSleep = 10 * time.Millisecond
	}

	if cfg.AsyncObjectStabilizationAttempts <= 0 {
		cfg.AsyncObjectStabilizationAttempts = 10
	}

	if cfg.AsyncObjectStabilizationDelay <= 0 {
		cfg.AsyncObjectStabilizationDelay = 25 * time.Millisecond
	}

	if cfg.MinimumDelay <= 0 {
		cfg.MinimumDelay = 25 * time.Millisecond
	}

	logger.Debug("creating test store", zap.Strings("hosts", cfg.TestClusterConfig.Hosts))
	testStore, err := newCQLStore(
		cfg.TestClusterConfig,
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
		//nolint:govet
		oracleStoreImpl, err := newCQLStore(
			*cfg.OracleClusterConfig,
			schema,
			logger.Named("oracle_store"),
			"oracle",
		)
		if err != nil {
			return nil, err
		}
		oracleStore = oracleStoreImpl
		logger.Debug("oracle store created successfully")

		if cfg.OracleStatementFile != "" && cfg.TestStatementFile != "" {
			logger.Debug("creating statement logger with oracle cluster")

			ch := make(chan stmtlogger.Item, 50_000)

			statementLogger, err = stmtlogger.NewLogger(
				stmtlogger.WithChannel(ch),
				stmtlogger.WithLogger(scylla.New(
					keyspace,
					table,
					oracleStoreImpl.getSession,
					testStore.getSession,
					cfg.OracleClusterConfig.Hosts,
					cfg.OracleClusterConfig.Username,
					cfg.OracleClusterConfig.Password,
					partitionKeyColumns,
					cfg.OracleClusterConfig.Replication,
					ch,
					cfg.OracleStatementFile,
					cfg.TestStatementFile,
					e.GetChannel(),
					logger.With(zap.String("component", "statement_logger")),
				)),
			)

			oracleStoreImpl.SetLogger(statementLogger, true)
			testStore.SetLogger(statementLogger, true)

			if err != nil {
				return nil, errors.Join(err, errors.New("failed to create statement logger"))
			}
			logger.Debug("statement logger created successfully")
		} else {
			logger.Warn("both oracle and test statement files must be provided to enable statement logging; statement logging disabled")
		}
	}

	if err = testStore.Init(); err != nil {
		return nil, errors.Join(err, errors.New("failed to initialize test store"))
	}

	if oracleStore != nil {
		if err = oracleStore.Init(); err != nil {
			return nil, errors.Join(err, errors.New("failed to initialize oracle store"))
		}
	}

	ds := &delegatingStore{
		testStore:            testStore,
		oracleStore:          oracleStore,
		logger:               logger.Named("delegating_store"),
		statementLogger:      statementLogger,
		serverSideTimestamps: cfg.UseServerSideTimestamps,
		mutationRetries:      cfg.MaxRetriesMutate,
		mutationRetrySleep:   cfg.MaxRetriesMutateSleep,
		validationRetries:    cfg.AsyncObjectStabilizationAttempts,
		validationRetrySleep: cfg.AsyncObjectStabilizationDelay,
		minimumDelay:         cfg.MinimumDelay,
	}

	logger.Debug("store created successfully")
	return ds, nil
}

type delegatingStore struct {
	oracleStore          storeLoader
	testStore            storeLoader
	logger               *zap.Logger
	statementLogger      *stmtlogger.Logger
	mutationRetrySleep   time.Duration
	mutationRetries      int
	validationRetrySleep time.Duration
	minimumDelay         time.Duration
	validationRetries    int
	serverSideTimestamps bool
}

// getLogger returns a non-nil logger. If delegatingStore.logger is nil (e.g.,
// in unit tests where delegatingStore is constructed directly), it returns
// zap.NewNop() to avoid nil pointer dereferences during logging.
func (ds delegatingStore) getLogger() *zap.Logger {
	if ds.logger != nil {
		return ds.logger
	}
	return zap.NewNop()
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
		ds.getLogger().Debug("oracle mutation applied successfully")
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

// mutationChanRes is a tagged result for mutation goroutines
type mutationChanRes struct {
	err    error
	oracle bool
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

	// Use a single channel for both mutations
	ch := make(chan mutationChanRes, 2)

	expected := 0
	if retryTest {
		expected++
		go func() {
			err := ds.testStore.mutate(doCtx, stmt, timestamp)
			select {
			case ch <- mutationChanRes{oracle: false, err: err}:
			case <-doCtx.Done():
			}
		}()
	}
	if retryOracle && ds.oracleStore != nil {
		expected++
		go func() {
			err := ds.oracleStore.mutate(doCtx, stmt, timestamp)
			select {
			case ch <- mutationChanRes{oracle: true, err: err}:
			case <-doCtx.Done():
			}
		}()
	}

	// Wait for results
	ds.waitForMutationResults(doCtx, ch, &result, retryTest, retryOracle, expected)

	return result
}

// waitForMutationResults waits for mutation results and updates the result struct
//
//nolint:gocyclo
func (ds delegatingStore) waitForMutationResults(ctx context.Context, ch chan mutationChanRes, result *mutationResult, expectTest, expectOracle bool, expected int) {
	received := 0
	for received < expected {
		select {
		case r := <-ch:
			if r.oracle {
				result.oracleDone = true
				result.oracleErr = r.err
			} else {
				result.testDone = true
				result.testErr = r.err
			}
			received++
		case <-ctx.Done():
			if expectTest && !result.testDone {
				result.testErr = ctx.Err()
			}
			if expectOracle && !result.oracleDone {
				result.oracleErr = ctx.Err()
			}
			return
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

		ds.getLogger().Warn("mutation failed, retrying with exponential backoff", fields...)
	}
}

//nolint:gocyclo
func (ds delegatingStore) Mutate(ctx context.Context, stmt *typedef.Stmt) error {
	// Prepare timestamp
	ts := mo.None[time.Time]()
	if !ds.serverSideTimestamps {
		ts = mo.Some(time.Now().UTC())
	}

	mutationErr := NewStoreMutationError(stmt)
	var cumulativeResult mutationResult
	maxAttempts := ds.mutationRetries + 1 // +1 for the initial attempt

	for attempt := range maxAttempts {

		// Determine which stores need to be tried/retried
		retryTest := attempt == 0 || cumulativeResult.testErr != nil
		retryOracle := (attempt == 0 || cumulativeResult.oracleErr != nil) && ds.oracleStore != nil

		// Execute mutations only on stores that need to be retried
		attemptResult := ds.executeParallelMutations(ctx, stmt, ts, attempt, retryTest, retryOracle)
		// Record any errors that occurred during this attempt
		if attemptResult.testErr != nil {
			mutationErr.AddAttempt(attemptResult.testErr)
		}
		if attemptResult.oracleErr != nil {
			mutationErr.AddAttempt(attemptResult.oracleErr)
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

				ds.getLogger().Info("mutation succeeded after retries",
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
			delay := utils.Backoff(utils.ExponentialBackoffStrategy, attempt, ds.mutationRetrySleep, ds.minimumDelay)
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

	ds.getLogger().Error("mutation failed after all retry attempts",
		zap.Int("total_attempts", maxAttempts),
		zap.Bool("test_store_success", mutationErr.TestStoreSuccess),
		zap.Bool("oracle_store_success", mutationErr.OracleStoreSuccess),
		zap.Uint64("total_attempt_errors", mutationErr.TotalAttempts.Load()),
		zap.Error(mutationErr))

	return mutationErr
}

//nolint:gocyclo
func (ds delegatingStore) Check(
	ctx context.Context,
	table *typedef.Table,
	stmt *typedef.Stmt,
	_ int,
) (int, error) {
	validationErr := NewValidationError("validation", stmt)
	maxAttempts := ds.validationRetries
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	var lastErr error

	for attempt := range maxAttempts {
		doCtx := WithContextData(ctx, &ContextData{
			GeminiAttempt: attempt,
			Statement:     stmt,
			Timestamp:     time.Now().UTC(),
		})

		// If there is no oracle, just count test rows
		if ds.oracleStore == nil {
			testRows, testErr := ds.testStore.load(doCtx, stmt)

			if testErr != nil {
				validationErr.AddAttempt(testErr)
				lastErr = testErr
			} else {
				return len(testRows), nil
			}
		} else {
			// Run iterators in parallel using a single channel (size 2)
			type rowsTagged struct {
				err    error
				rows   Rows
				oracle bool
			}

			ch := make(chan rowsTagged, 2)

			go func() {
				rows, err := ds.testStore.load(doCtx, stmt)
				select {
				case ch <- rowsTagged{oracle: false, rows: rows, err: err}:
				case <-doCtx.Done():
				}
			}()

			go func() {
				rows, err := ds.oracleStore.load(doCtx, stmt)
				select {
				case ch <- rowsTagged{oracle: true, rows: rows, err: err}:
				case <-doCtx.Done():
				}
			}()

			var tRes, oRes rowsTagged
			// Wait for both results or context cancellation
			received := 0
			for received < 2 {
				select {
				case r := <-ch:
					if r.oracle {
						oRes = r
					} else {
						tRes = r
					}
					received++
				case <-doCtx.Done():
					return 0, doCtx.Err()
				}
			}

			// Extract results
			testRows, oracleRows := tRes.rows, oRes.rows
			testErr, oracleErr := tRes.err, oRes.err

			// Record any errors that occurred
			if testErr != nil {
				validationErr.AddAttempt(testErr)
				lastErr = testErr
			}
			if oracleErr != nil {
				validationErr.AddAttempt(oracleErr)
				lastErr = oracleErr
			}

			// Check if comparison succeeded
			if testErr == nil && oracleErr == nil {
				// Compare results normally; if both sides are empty this will succeed with count 0.
				result := CompareCollectedRows(table, testRows, oracleRows)
				compErr := result.ToError()
				if compErr == nil {
					// No differences found
					return result.MatchCount, nil
				}

				// Found differences, record them
				validationErr.AddAttempt(compErr)
				lastErr = compErr
			}
		}

		if attempt < maxAttempts-1 {
			delay := utils.Backoff(utils.LinearBackoffStrategy, attempt, ds.validationRetrySleep, ds.minimumDelay)
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
	if validationErr.TotalAttempts.Load() == 0 {
		return 0, nil
	}

	if errors.Is(ctx.Err(), context.Canceled) {
		return 0, context.Canceled
	}

	// Finalize the validation error with summary
	validationErr.Finalize(lastErr)

	return 0, validationErr
}

func (ds delegatingStore) Close() error {
	ds.getLogger().Info("closing store")

	var err error
	if ds.statementLogger != nil {
		ds.getLogger().Debug("closing statement logger")
		err = multierr.Append(err, ds.statementLogger.Close())
	}

	if err != nil {
		ds.getLogger().Error("store closed with errors", zap.Error(err))
	} else {
		ds.getLogger().Debug("store closed successfully")
	}

	return err
}
