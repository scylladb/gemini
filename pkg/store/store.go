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
	"strings"
	"sync"
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
	// Check validates a statement against both stores, retrying internally. It
	// returns the number of matched rows, the raw test-store rows (used by the
	// validation layer to sample rows for the row tracker without issuing a
	// second SELECT), and any error.
	Check(context.Context, *typedef.Table, *typedef.Stmt, int) (int, Rows, error)
	// CheckOnce performs a single validation attempt without retries. It returns
	// the number of matched rows, the raw test-store rows (for row-tracker
	// sampling), and a ValidationError on mismatch (nil on success). Callers
	// manage retry scheduling externally (e.g. the validation retry queue).
	CheckOnce(ctx context.Context, table *typedef.Table, stmt *typedef.Stmt, attempt int) (int, Rows, error)
}

type (
	ScyllaClusterConfig struct {
		Replication             replication.Replication
		TracingDir              string
		HostSelectionPolicy     HostSelectionPolicy
		Consistency             string
		Username                string
		Password                string
		Name                    stmtlogger.Type
		Hosts                   []string
		Port                    int
		RequestTimeout          time.Duration
		ConnectTimeout          time.Duration
		DockerMode              bool
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
					schema,
					oracleStoreImpl.getSession,
					testStore.getSession,
					cfg.OracleClusterConfig.Hosts,
					cfg.OracleClusterConfig.Port,
					cfg.OracleClusterConfig.DockerMode,
					cfg.OracleClusterConfig.Username,
					cfg.OracleClusterConfig.Password,
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
		inflight:             new(sync.WaitGroup),
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
		partitionKeyColumns:  partitionKeyColumns,
		keyspaceAndTable:     keyspace + "." + table,
	}

	logger.Debug("store created successfully")
	return ds, nil
}

type delegatingStore struct {
	oracleStore          storeLoader
	testStore            storeLoader
	logger               *zap.Logger
	statementLogger      *stmtlogger.Logger
	inflight             *sync.WaitGroup
	keyspaceAndTable     string
	partitionKeyColumns  typedef.Columns
	mutationRetries      int
	minimumDelay         time.Duration
	validationRetries    int
	validationRetrySleep time.Duration
	mutationRetrySleep   time.Duration
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

// buildPartitionDeleteStmt constructs a DELETE that removes the entire partition
// identified by keys from keyspaceAndTable.
//
// Both the WHERE columns and the bind values come from keys.Values (the
// statement's own partition-key map), so the bind arity always matches —
// regardless of how many partition-key columns the table has. Earlier this used
// the store's fixed table[0] schema, which mis-bound (and targeted the wrong
// table) whenever a mutation on any other table timed out. Returns nil when the
// keyspace.table or partition-key values are unavailable.
func (ds delegatingStore) buildPartitionDeleteStmt(keys *typedef.PartitionKeys, keyspaceAndTable string) *typedef.Stmt {
	if keys == nil || keys.Values == nil || keyspaceAndTable == "" {
		return nil
	}

	pkNames := keys.Values.Keys() // sorted → stable query string (prepared once)
	if len(pkNames) == 0 {
		return nil
	}

	// Build: DELETE FROM keyspace.table WHERE pk1=? AND pk2=? ...
	var sb strings.Builder
	sb.WriteString("DELETE FROM ")
	sb.WriteString(keyspaceAndTable)
	sb.WriteString(" WHERE ")

	values := make([]any, 0, len(pkNames))
	for i, name := range pkNames {
		if i > 0 {
			sb.WriteString(" AND ")
		}
		sb.WriteString(name)
		sb.WriteString("=?")
		values = append(values, keys.Values.Get(name)...)
	}

	return &typedef.Stmt{
		PartitionKeys: []typedef.PartitionKeys{*keys},
		Values:        values,
		QueryType:     typedef.DeleteWholePartitionType,
		Query:         sb.String(),
	}
}

// compensateAsymmetricWrite performs best-effort compensating DELETEs on both
// stores to restore symmetry after a timeout-induced asymmetric commit.
//
// When a CQL write times out (gocql returns context.DeadlineExceeded), the
// server may have committed the write even though the client did not receive an
// acknowledgment.  If one store committed and the other did not, the two clusters
// diverge.  Deleting from both stores makes them deterministically empty for the
// affected partition, eliminating the divergence regardless of which (if any)
// store actually committed.
//
// A fresh background context with a 15 s deadline is used because the original
// request context has already expired — we intentionally do NOT propagate a
// cancelled/timed-out context here.  This means the compensation is not
// sensitive to global shutdown signals; callers that need cancellation should
// ensure the process exits promptly (e.g. via the watchdog).
// Errors are logged but not returned; this is a best-effort operation.
// Returns true if all deletes completed without error.
func (ds delegatingStore) compensateAsymmetricWrite(stmt *typedef.Stmt) bool {
	if len(stmt.PartitionKeys) == 0 || len(ds.partitionKeyColumns) == 0 {
		return true
	}

	// Target the table the timed-out mutation actually hit, not the store's
	// fixed table[0]. Best-effort: if it can't be determined, skip.
	keyspaceAndTable := utils.KeyspaceTableFromQuery(stmt.Query)
	if keyspaceAndTable == "" {
		return true
	}

	compCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	ts := mo.None[time.Time]()
	ok := true

	for i := range stmt.PartitionKeys {
		deleteStmt := ds.buildPartitionDeleteStmt(&stmt.PartitionKeys[i], keyspaceAndTable)
		if deleteStmt == nil {
			continue
		}

		if err := ds.testStore.mutate(compCtx, deleteStmt, ts); err != nil {
			ds.getLogger().Error("compensating delete on test store failed — partition may be asymmetric",
				zap.String("partition", stmt.PartitionKeys[i].ID.String()),
				zap.Error(err))
			ok = false
		}

		if ds.oracleStore != nil {
			if err := ds.oracleStore.mutate(compCtx, deleteStmt, ts); err != nil {
				ds.getLogger().Error("compensating delete on oracle store failed — partition may be asymmetric",
					zap.String("partition", stmt.PartitionKeys[i].ID.String()),
					zap.Error(err))
				ok = false
			}
		}
	}

	return ok
}

// isOnlyTimeoutFailure reports whether all non-nil errors in result are
// context.DeadlineExceeded (CQL request timeouts), and at least one store had
// such a timeout.  This distinguishes transient timeouts from hard failures
// (e.g. serialisation errors, network errors) that should propagate to the
// caller as write errors.
//
// Assumption: individual store errors are plain values, not errors.Join
// composites.  delegatingStore stores one error per store; the only joined
// errors appear in the finalErr that is passed to MutationError.Finalize,
// which is never inspected by this function.
func isOnlyTimeoutFailure(result mutationResult) bool {
	testTimeout := result.testErr == nil || errors.Is(result.testErr, context.DeadlineExceeded)
	oracleTimeout := result.oracleErr == nil || errors.Is(result.oracleErr, context.DeadlineExceeded)
	atLeastOne := errors.Is(result.testErr, context.DeadlineExceeded) || errors.Is(result.oracleErr, context.DeadlineExceeded)
	return testTimeout && oracleTimeout && atLeastOne
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
				ds.oracleStore.name(),
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

	// Use a single channel for both mutations.
	// Capacity = 2 guarantees that goroutine sends are always non-blocking,
	// even when the receiver has already returned due to context cancellation.
	// This is the key invariant that allows waitForMutationResults to drain
	// all goroutines after ctx fires without the risk of a blocked send.
	ch := make(chan mutationChanRes, 2)

	expected := 0
	if retryTest {
		expected++
		ds.inflight.Add(1)
		go func() {
			defer ds.inflight.Done()
			err := ds.testStore.mutate(doCtx, stmt, timestamp)
			// Unconditional send: the channel always has capacity so this
			// never blocks.  Dropping the "case <-doCtx.Done()" branch
			// means the goroutine always delivers its result, which lets
			// waitForMutationResults drain it even after ctx is cancelled.
			ch <- mutationChanRes{oracle: false, err: err}
		}()
	}
	if retryOracle && ds.oracleStore != nil {
		expected++
		ds.inflight.Add(1)
		go func() {
			defer ds.inflight.Done()
			err := ds.oracleStore.mutate(doCtx, stmt, timestamp)
			ch <- mutationChanRes{oracle: true, err: err}
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
			// Drain the remaining goroutines before returning.
			//
			// executeParallelMutations guarantees that every spawned goroutine
			// sends exactly one result to ch (unconditional send, channel
			// capacity == number of goroutines).  Draining here ensures that
			// goroutines are never left running after this function returns —
			// which would otherwise cause them to use a CQL session that has
			// already been closed by delegatingStore.Close(), producing
			// "use of closed network connection (potentially executed: true)"
			// errors and silent data divergence between oracle and test.
			//
			// Capture the actual goroutine result instead of discarding it:
			// the CQL response may have arrived just after ctx.Done() fired
			// (e.g. the server committed the write but the ACK raced with the
			// context deadline).  Using the real error lets the caller detect
			// asymmetric commits (one store succeeded, the other timed out)
			// rather than treating both as symmetric failures.
			for received < expected {
				r := <-ch
				received++
				if r.oracle {
					if expectOracle && !result.oracleDone {
						result.oracleDone = true
						result.oracleErr = r.err
					}
				} else {
					if expectTest && !result.testDone {
						result.testDone = true
						result.testErr = r.err
					}
				}
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
			// Only record oracle success when oracle is actually configured.
			// When oracleStore is nil the "done" signal is synthetic (expectOracle=false),
			// so OracleStoreSuccess would be a meaningless true and would mask the
			// asymmetric-timeout check in mutation.run().
			if ds.oracleStore != nil {
				mutationErr.SetStoreSuccess(TypeOracle, attemptResult.oracleErr == nil)
			}
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

				// Context expired during the backoff delay between retries.
				// The stores may be asymmetric at this point (e.g. the previous
				// attempt committed on test but timed out on oracle).
				mutationErr.Finalize(pkgerrors.Wrap(ctx.Err(), "mutation cancelled during retry delay"))

				// If the only failures are CQL timeouts, attempt a compensating
				// DELETE on both stores so the partition is deterministically
				// empty for the upcoming validation phase.  On success, return
				// nil so the partition is NOT marked invalid.
				if isOnlyTimeoutFailure(cumulativeResult) && ds.compensateAsymmetricWrite(stmt) {
					return nil
				}

				// Compensation not applicable or failed — return the
				// MutationError so mutation.run() can inspect
				// TestStoreSuccess / OracleStoreSuccess and mark the affected
				// partitions invalid, preventing a false divergence report.
				return mutationErr
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

	// If all remaining failures are CQL timeouts, the servers may have
	// committed on one side but not the other.  Issue a compensating DELETE
	// on both stores so the partition is deterministically empty before the
	// validation phase starts.  On success return nil so the partition is not
	// marked invalid.
	if isOnlyTimeoutFailure(cumulativeResult) && ds.compensateAsymmetricWrite(stmt) {
		return nil
	}

	ds.getLogger().Error("mutation failed after all retry attempts",
		zap.Int("total_attempts", maxAttempts),
		zap.Bool("test_store_success", mutationErr.TestStoreSuccess),
		zap.Bool("oracle_store_success", mutationErr.OracleStoreSuccess),
		zap.Uint64("total_attempt_errors", mutationErr.TotalAttempts.Load()),
		zap.Error(mutationErr))

	return mutationErr
}

// checkOnceInternal performs a single validation attempt. On success it returns
// the matched row count, the raw test-store rows (used by the validation layer
// to sample rows for the row tracker without a second SELECT), and a nil error.
// On failure it returns the validation error with comparison results attached
// (when applicable).
//
//nolint:gocyclo
//nolint:gocyclo
func (ds delegatingStore) checkOnceInternal(
	ctx context.Context,
	table *typedef.Table,
	stmt *typedef.Stmt,
	attempt int,
) (int, Rows, error) {
	doCtx := WithContextData(ctx, &ContextData{
		GeminiAttempt: attempt,
		Statement:     stmt,
		Timestamp:     time.Now().UTC(),
	})

	// If there is no oracle, just count test rows
	if ds.oracleStore == nil {
		testRows, testErr := ds.testStore.load(doCtx, stmt)
		if testErr != nil {
			validationErr := NewValidationError("validation", stmt)
			validationErr.AddAttempt(testErr)
			validationErr.Finalize(testErr)
			return 0, nil, validationErr
		}
		return len(testRows), testRows, nil
	}

	// Run iterators in parallel using a single channel (size 2).
	// Goroutines are tracked by ds.inflight so Close() can wait for them.
	type rowsTagged struct {
		err    error
		rows   Rows
		oracle bool
	}

	ch := make(chan rowsTagged, 2)

	ds.inflight.Add(2)
	go func() {
		defer ds.inflight.Done()
		rows, err := ds.testStore.load(doCtx, stmt)
		select {
		case ch <- rowsTagged{oracle: false, rows: rows, err: err}:
		case <-doCtx.Done():
		}
	}()

	go func() {
		defer ds.inflight.Done()
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
			return 0, nil, doCtx.Err()
		}
	}

	// Extract results
	testRows, oracleRows := tRes.rows, oRes.rows
	testErr, oracleErr := tRes.err, oRes.err

	validationErr := NewValidationError("validation", stmt)

	// Record any errors that occurred
	if testErr != nil {
		validationErr.AddAttempt(testErr)
	}
	if oracleErr != nil {
		validationErr.AddAttempt(oracleErr)
	}

	// If either side had a load error, finalize and return
	if testErr != nil || oracleErr != nil {
		lastErr := testErr
		if oracleErr != nil {
			lastErr = oracleErr
		}
		validationErr.Finalize(lastErr)
		return 0, nil, validationErr
	}

	// Compare results normally; if both sides are empty this will succeed with count 0.
	result := CompareCollectedRows(table, testRows, oracleRows)
	compErr := result.ToError()
	if compErr == nil {
		// No differences found
		return result.MatchCount, testRows, nil
	}

	// Found differences, record them and store the comparison results
	validationErr.AddAttempt(compErr)
	validationErr.ComparisonResults = &ComparisonResults{
		TestRows:       testRows,
		OracleRows:     oracleRows,
		TestOnlyRows:   result.TestOnlyRows,
		OracleOnlyRows: result.OracleOnlyRows,
		DifferentRows:  result.DifferentRows,
	}
	validationErr.Finalize(compErr)

	return 0, nil, validationErr
}

func (ds delegatingStore) CheckOnce(
	ctx context.Context,
	table *typedef.Table,
	stmt *typedef.Stmt,
	attempt int,
) (int, Rows, error) {
	return ds.checkOnceInternal(ctx, table, stmt, attempt)
}

func (ds delegatingStore) Check(
	ctx context.Context,
	table *typedef.Table,
	stmt *typedef.Stmt,
	_ int,
) (int, Rows, error) {
	maxAttempts := ds.validationRetries
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := range maxAttempts {
		count, testRows, err := ds.checkOnceInternal(ctx, table, stmt, attempt)
		if err == nil {
			return count, testRows, nil
		}

		// Context cancellation/deadline is terminal — do not retry.
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return 0, nil, err
		}
		lastErr = err

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
					return 0, nil, context.Canceled
				}
				return 0, nil, pkgerrors.Wrap(ctx.Err(), "validation cancelled during retry delay")
			}
		}
	}

	// All attempts failed — return the error from the final attempt
	// (already finalized by checkOnceInternal).
	if errors.Is(ctx.Err(), context.Canceled) {
		return 0, nil, context.Canceled
	}

	return 0, nil, lastErr
}

func (ds delegatingStore) Close() error {
	ds.getLogger().Info("closing store")

	// Drain all goroutines spawned by executeParallelMutations before closing
	// the CQL sessions. If waitForMutationResults returned early (context
	// cancelled), those goroutines are still in-flight. Closing sessions while
	// they are running causes "use of closed network connection (potentially
	// executed: true)" errors and undetected data divergence between oracle
	// and test.
	if ds.inflight != nil {
		ds.inflight.Wait()
	}

	var err error
	if ds.statementLogger != nil {
		ds.getLogger().Debug("closing statement logger")
		err = multierr.Append(err, ds.statementLogger.Close())
	}

	ds.getLogger().Debug("closing test store")
	err = multierr.Append(err, ds.testStore.Close())

	if ds.oracleStore != nil {
		ds.getLogger().Debug("closing oracle store")
		err = multierr.Append(err, ds.oracleStore.Close())
	}

	if err != nil {
		ds.getLogger().Error("store closed with errors", zap.Error(err))
	} else {
		ds.getLogger().Debug("store closed successfully")
	}

	return err
}
