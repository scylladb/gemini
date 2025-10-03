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
		OracleClusterConfig     *ScyllaClusterConfig
		OracleStatementFile     string
		TestStatementFile       string
		TestClusterConfig       ScyllaClusterConfig
		MaxRetriesMutate        int
		MaxRetriesMutateSleep   time.Duration
		Compression             stmtlogger.Compression
		UseServerSideTimestamps bool
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
	var statementLogger *stmtlogger.Logger
	if cfg.OracleClusterConfig != nil {
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
	}

	if cfg.MaxRetriesMutate < 0 {
		cfg.MaxRetriesMutate = 10
	}

	if cfg.MaxRetriesMutateSleep <= 5*time.Millisecond {
		cfg.MaxRetriesMutateSleep = 5 * time.Millisecond
	}

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

	var oracleStore storeLoader

	if cfg.OracleClusterConfig != nil {
		oracleStore, err = newCQLStore(
			*cfg.OracleClusterConfig,
			statementLogger,
			schema,
			logger.Named("oracle_store"),
			"oracle",
		)
		if err != nil {
			return nil, pkgerrors.Wrap(err, "failed to create test cluster")
		}
	}

	return &delegatingStore{
		workers:              workers,
		testStore:            testStore,
		oracleStore:          oracleStore,
		logger:               logger.Named("delegating_store"),
		statementLogger:      statementLogger,
		serverSideTimestamps: cfg.UseServerSideTimestamps,
		mutationRetries:      cfg.MaxRetriesMutate,
		mutationRetrySleep:   cfg.MaxRetriesMutateSleep,
	}, nil
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
	}

	return nil
}

// mutationContext holds the context and parameters for a mutation operation
type mutationContext struct {
	ctx            context.Context
	stmt           *typedef.Stmt
	timestamp      mo.Option[time.Time]
	hasOracleStore bool
}

// mutationState tracks the state of test and oracle mutations
type mutationState struct {
	testErr       error
	oracleErr     error
	testSuccess   bool
	oracleSuccess bool
}

// prepareMutationContext sets up the context and timestamp for mutation operations
func (ds delegatingStore) prepareMutationContext(ctx context.Context, stmt *typedef.Stmt) mutationContext {
	ctxData := &ContextData{
		Statement: stmt,
		Timestamp: time.Now().UTC(),
	}

	doCtx := WithContextData(ctx, ctxData)

	ts := mo.None[time.Time]()
	if !ds.serverSideTimestamps {
		ts = mo.Some(ctxData.Timestamp)
	}

	return mutationContext{
		ctx:            doCtx,
		stmt:           stmt,
		timestamp:      ts,
		hasOracleStore: ds.oracleStore != nil,
	}
}

// executeMutationAttempt performs a single mutation attempt on test and oracle stores
func (ds delegatingStore) executeMutationAttempt(mutCtx mutationContext, state *mutationState) {
	var testCh, oracleCh chan mo.Result[any]

	// Start test mutation if not already successful
	if !state.testSuccess {
		testCh = ds.workers.Send(mutCtx.ctx, func(ctx context.Context) (any, error) {
			return nil, ds.testStore.mutate(ctx, mutCtx.stmt, mutCtx.timestamp)
		})
	}

	// Start oracle mutation if available and not already successful
	if mutCtx.hasOracleStore && !state.oracleSuccess {
		oracleCh = ds.workers.Send(mutCtx.ctx, func(ctx context.Context) (any, error) {
			return nil, ds.oracleStore.mutate(ctx, mutCtx.stmt, mutCtx.timestamp)
		})
	}

	// Wait for results and update state
	ds.waitForMutationResults(mutCtx.ctx, testCh, oracleCh, state)
}

// waitForMutationResults handles parallel result collection and updates mutation state
func (ds delegatingStore) waitForMutationResults(ctx context.Context, testCh, oracleCh chan mo.Result[any], state *mutationState) {
	testPending := testCh != nil
	oraclePending := oracleCh != nil

	for testPending || oraclePending {
		select {
		case testResult := <-testCh:
			ds.workers.Release(testCh)
			if testPending {
				ds.handleMutationResult(testResult, &state.testErr, &state.testSuccess)
				testPending = false
			}

		case oracleResult := <-oracleCh:
			ds.workers.Release(oracleCh)
			if oraclePending {
				ds.handleMutationResult(oracleResult, &state.oracleErr, &state.oracleSuccess)
				oraclePending = false
			}

		case <-ctx.Done():
			// Context was cancelled, set cancellation error and return immediately
			ctxErr := ctx.Err()
			if testPending {
				state.testErr = ctxErr
				ds.workers.Release(testCh)
			}
			if oraclePending {
				state.oracleErr = ctxErr
				ds.workers.Release(oracleCh)
			}
			return
		}
	}
}

// handleMutationResult processes a single mutation result and updates the corresponding state
func (ds delegatingStore) handleMutationResult(result mo.Result[any], err *error, success *bool) {
	if result.IsError() {
		*err = result.Error()
	} else {
		*success = true
		*err = nil
	}
}

// shouldRetryMutation determines if mutation should be retried based on current state
func (ds delegatingStore) shouldRetryMutation(state mutationState, hasOracle bool) bool {
	return !state.testSuccess || (hasOracle && !state.oracleSuccess)
}

// shouldDelayRetry determines if we should sleep before the next retry
func (ds delegatingStore) shouldDelayRetry(state mutationState, hasOracle bool) bool {
	// Don't delay if oracle succeeded - oracle failures are rare and waiting wastes time
	return hasOracle && state.oracleSuccess
}

func (ds delegatingStore) Mutate(ctx context.Context, stmt *typedef.Stmt) error {
	mutCtx := ds.prepareMutationContext(ctx, stmt)
	state := mutationState{}

	for range ds.mutationRetries {
		ds.executeMutationAttempt(mutCtx, &state)

		// Break early if both operations succeeded
		if !ds.shouldRetryMutation(state, mutCtx.hasOracleStore) {
			break
		}

		// Apply retry delay if needed
		if ds.shouldDelayRetry(state, mutCtx.hasOracleStore) {
			time.Sleep(ds.mutationRetrySleep)
		}
	}

	if state.testErr != nil || state.oracleErr != nil {
		return errors.Join(state.testErr, state.oracleErr)
	}

	return nil
}

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

	testResult := <-testCh
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

	oracleResult := <-oracleCh
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
	err := multierr.Append(nil, ds.testStore.Close())

	if ds.oracleStore != nil {
		err = multierr.Append(err, ds.oracleStore.Close())
	}

	if ds.statementLogger != nil {
		return multierr.Append(err, ds.statementLogger.Close())
	}

	return nil
}
