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
	"reflect"
	"slices"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	pkgerrors "github.com/pkg/errors"
	"github.com/samber/mo"
	"github.com/scylladb/go-set/strset"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"gopkg.in/inf.v0"

	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
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
	mutate(context.Context, *typedef.Stmt) error
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
	Check(context.Context, *typedef.Table, *typedef.Stmt, bool) error
}

type Config struct {
	TestLogStatementsFile       string
	OracleLogStatementsFile     string
	LogStatementFileCompression stmtlogger.Compression
	MaxRetriesMutate            int
	MaxRetriesMutateSleep       time.Duration
	UseServerSideTimestamps     bool
}

func New(
	_ context.Context,
	schema *typedef.Schema,
	testCluster, oracleCluster *gocql.ClusterConfig,
	cfg Config,
	logger *zap.Logger,
) (Store, error) {
	if testCluster == nil {
		return nil, errors.New("test cluster is empty")
	}

	oracleStore, err := getStore(
		"oracle",
		schema,
		oracleCluster,
		cfg,
		cfg.OracleLogStatementsFile,
		cfg.LogStatementFileCompression,
		logger,
	)
	if err != nil {
		return nil, err
	}

	testStore, err := getStore(
		"test",
		schema,
		testCluster,
		cfg,
		cfg.TestLogStatementsFile,
		cfg.LogStatementFileCompression,
		logger,
	)
	if err != nil {
		return nil, err
	}

	return &delegatingStore{
		workers:     newWorkers(1024),
		testStore:   testStore,
		oracleStore: oracleStore,
		logger:      logger.Named("delegating_store"),
	}, nil
}

type delegatingStore struct {
	workers     *workers
	oracleStore storeLoader
	testStore   storeLoader
	logger      *zap.Logger
}

func (ds delegatingStore) Create(ctx context.Context, testBuilder, oracleBuilder *typedef.Stmt) error {
	if err := ds.testStore.mutate(ctx, testBuilder); err != nil {
		return pkgerrors.Wrapf(
			err,
			"unable to apply mutations to the %s store",
			ds.testStore.name(),
		)
	}

	if ds.oracleStore != nil {
		if err := ds.oracleStore.mutate(ctx, oracleBuilder); err != nil {
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
	var oracleCh chan mo.Result[Rows]

	if ds.oracleStore != nil {
		oracleCh = ds.workers.Send(ctx, func(ctx context.Context) (Rows, error) {
			return nil, ds.oracleStore.mutate(ctx, stmt)
		})
	}

	testCh := ds.workers.Send(ctx, func(ctx context.Context) (Rows, error) {
		return nil, ds.testStore.mutate(ctx, stmt)
	})

	result := <-testCh
	ds.workers.Release(testCh)
	if result.IsError() {
		// Test store failed, transition cannot take place
		ds.logger.Error(
			"test store failed mutation, transition to next state impossible so continuing with next mutation",
			zap.Error(result.Error()),
		)

		return result.Error()
	}

	if oracleCh != nil {
		result = <-oracleCh
		ds.workers.Release(oracleCh)
		if result.IsError() {
			// Test store failed, transition cannot take place
			ds.logger.Error(
				"oracle store failed mutation, transition to next state impossible so continuing with next mutation",
				zap.Error(result.Error()),
			)

			return result.Error()
		}
	}

	return nil
}

func (ds delegatingStore) Check(
	ctx context.Context,
	table *typedef.Table,
	stmt *typedef.Stmt,
	detailedDiff bool,
) error {
	doCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var ch chan mo.Result[Rows]

	if ds.oracleStore != nil {
		ch = ds.workers.Send(doCtx, func(ctx context.Context) (Rows, error) {
			return ds.oracleStore.load(ctx, stmt)
		})
	}

	testRows, testErr := ds.testStore.load(doCtx, stmt)

	if testErr != nil {
		ds.workers.Release(ch)
		return pkgerrors.Wrap(testErr, "unable to load check data from the test store")
	}

	if ch == nil {
		return nil
	}

	result := <-ch
	ds.workers.Release(ch)

	if result.IsError() {
		return pkgerrors.Wrap(result.Error(), "unable to load check data from the oracle store")
	}

	oracleRows := result.MustGet()

	if len(testRows) == 0 && len(oracleRows) == 0 {
		return nil
	}

	if len(testRows) != len(oracleRows) {
		if !detailedDiff {
			return ErrorRowDifference{
				TestRows:   len(testRows),
				OracleRows: len(oracleRows),
			}
		}
		testSet := pks(table, testRows)
		oracleSet := pks(table, oracleRows)
		missingInTest := strset.Difference(oracleSet, testSet).List()
		missingInOracle := strset.Difference(testSet, oracleSet).List()

		return ErrorRowDifference{
			TestRows:        len(testRows),
			OracleRows:      len(oracleRows),
			MissingInTest:   missingInTest,
			MissingInOracle: missingInOracle,
		}
	}

	if reflect.DeepEqual(testRows, oracleRows) {
		return nil
	}

	if !detailedDiff {
		return ErrorRowDifference{
			TestRows:   len(testRows),
			OracleRows: len(oracleRows),
		}
	}

	slices.SortStableFunc(testRows, rowsCmp)
	slices.SortStableFunc(oracleRows, rowsCmp)

	for i, oracleRow := range oracleRows {
		if diff := cmp.Diff(oracleRow, testRows[i], comparers...); diff != "" {
			return ErrorRowDifference{
				Diff:      diff,
				OracleRow: oracleRow,
				TestRow:   testRows[i],
			}
		}
	}

	return nil
}

func (ds delegatingStore) Close() error {
	err := multierr.Append(ds.workers.Close(), ds.testStore.Close())

	if ds.oracleStore != nil {
		err = multierr.Append(err, ds.oracleStore.Close())
	}

	return err
}

func getStore(
	name string,
	schema *typedef.Schema,
	clusterConfig *gocql.ClusterConfig,
	cfg Config,
	stmtLogFile string,
	compression stmtlogger.Compression,
	logger *zap.Logger,
) (storeLoader, error) {
	if clusterConfig == nil {
		return nil, nil
	}

	session, err := clusterConfig.CreateSession()
	if err != nil {
		return nil, pkgerrors.Wrapf(err, "failed to connect to %s cluster", name)
	}

	stmtLogger, err := stmtlogger.NewFileLogger(stmtLogFile, compression)
	if err != nil {
		return nil, err
	}

	return &cqlStore{
		session:                 session,
		schema:                  schema,
		system:                  name,
		maxRetriesMutate:        cfg.MaxRetriesMutate + 10,
		maxRetriesMutateSleep:   cfg.MaxRetriesMutateSleep,
		useServerSideTimestamps: cfg.UseServerSideTimestamps,
		logger:                  logger.Named(name),
		stmtLogger:              stmtLogger,
	}, nil
}
