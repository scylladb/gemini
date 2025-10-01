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
	"io"
	"math/big"
	"reflect"
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

	if cfg.MaxRetriesMutate <= 1 {
		cfg.MaxRetriesMutate = 10
	}

	if cfg.MaxRetriesMutateSleep <= 10*time.Millisecond {
		cfg.MaxRetriesMutateSleep = 200 * time.Millisecond
	}

	testStore, err := newCQLStore(
		cfg.TestClusterConfig,
		statementLogger,
		schema,
		logger.Named("test_store"),
		"test",
		cfg.MaxRetriesMutate,
		cfg.MaxRetriesMutateSleep,
		cfg.UseServerSideTimestamps,
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
			cfg.MaxRetriesMutate,
			cfg.MaxRetriesMutateSleep,
			cfg.UseServerSideTimestamps,
		)
		if err != nil {
			return nil, pkgerrors.Wrap(err, "failed to create test cluster")
		}
	}

	return &delegatingStore{
		workers:         workers,
		testStore:       testStore,
		oracleStore:     oracleStore,
		logger:          logger.Named("delegating_store"),
		statementLogger: statementLogger,
	}, nil
}

type delegatingStore struct {
	workers         *workpool.Pool
	oracleStore     storeLoader
	testStore       storeLoader
	logger          *zap.Logger
	statementLogger *stmtlogger.Logger
}

func (ds delegatingStore) Create(ctx context.Context, testBuilder, stmt *typedef.Stmt) error {
	ctx = WithContextData(ctx, &ContextData{
		GeminiAttempt: 0,
		Statement:     stmt,
	})
	if err := ds.testStore.mutate(ctx, testBuilder, mo.None[time.Time]()); err != nil {
		return pkgerrors.Wrapf(
			err,
			"unable to apply mutations to the %s store",
			ds.testStore.name(),
		)
	}

	if ds.oracleStore != nil {
		if err := ds.oracleStore.mutate(ctx, stmt, mo.None[time.Time]()); err != nil {
			return pkgerrors.Wrapf(
				err,
				"unable to apply mutations to the %s store",
				ds.testStore.name(),
			)
		}
	}

	return nil
}

func (ds delegatingStore) Mutate(ctx context.Context, stmt *typedef.Stmt) error {
	var oracleCh chan mo.Result[any]
	doCtx := WithContextData(ctx, &ContextData{
		Statement: stmt,
	})

	if ds.oracleStore != nil {
		oracleCh = ds.workers.Send(doCtx, func(ctx context.Context) (any, error) {
			return nil, ds.oracleStore.mutate(ctx, stmt, mo.None[time.Time]())
		})
	}

	testCh := ds.workers.Send(doCtx, func(ctx context.Context) (any, error) {
		return nil, ds.testStore.mutate(ctx, stmt, mo.None[time.Time]())
	})

	result := <-testCh
	ds.workers.Release(testCh)
	if result.IsError() {
		return result.Error()
	}

	if oracleCh != nil {
		result = <-oracleCh
		ds.workers.Release(oracleCh)
		if result.IsError() {
			return result.Error()
		}
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

	if ds.oracleStore != nil {
		doCtx := WithContextData(ctx, &ContextData{
			GeminiAttempt: attempt,
			Statement:     stmt,
		})

		oracleCh = ds.workers.Send(doCtx, func(ctx context.Context) (any, error) {
			return ds.oracleStore.load(ctx, stmt)
		})
	}

	doCtx := WithContextData(ctx, &ContextData{
		GeminiAttempt: attempt,
		Statement:     stmt,
	})

	testCh := ds.workers.Send(doCtx, func(ctx context.Context) (any, error) {
		return ds.testStore.load(ctx, stmt)
	})

	testResult := <-testCh
	ds.workers.Release(testCh)

	if testResult.IsError() {
		return 0, pkgerrors.Wrap(testResult.Error(), "unable to load check data from the test store")
	}

	if oracleCh == nil {
		return len(testResult.MustGet().(Rows)), nil
	}

	oracleResult := <-oracleCh
	ds.workers.Release(oracleCh)

	if oracleResult.IsError() {
		return 0, pkgerrors.Wrap(oracleResult.Error(), "unable to load check data from the oracle store")
	}

	oracleRows := oracleResult.MustGet().(Rows)
	testRows := testResult.MustGet().(Rows)

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

	if reflect.DeepEqual(testRows, oracleRows) {
		return len(testRows), nil
	}

	slices.SortStableFunc(testRows, rowsCmp)
	slices.SortStableFunc(oracleRows, rowsCmp)

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
