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
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	pkgerrors "github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"gopkg.in/inf.v0"

	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
)

type loader interface {
	load(context.Context, *typedef.Stmt) ([]Row, error)
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
	Create(context.Context, *typedef.Stmt, *typedef.Stmt) error
	Mutate(context.Context, *typedef.Stmt) error
	Check(context.Context, *typedef.Table, *typedef.Stmt, bool) error
	Close() error
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
	schema *typedef.Schema,
	testCluster, oracleCluster *gocql.ClusterConfig,
	cfg Config,
	logger *zap.Logger,
) (Store, error) {
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

	if testCluster == nil {
		return nil, errors.New("test cluster is empty")
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
		testStore:   testStore,
		oracleStore: oracleStore,
		logger:      logger.Named("delegating_store"),
	}, nil
}

type delegatingStore struct {
	oracleStore     storeLoader
	testStore       storeLoader
	statementLogger stmtlogger.StmtToFile
	logger          *zap.Logger
}

func (ds delegatingStore) Create(
	ctx context.Context,
	testBuilder, oracleBuilder *typedef.Stmt,
) error {
	if ds.statementLogger != nil {
		if err := ds.statementLogger.LogStmt(testBuilder); err != nil {
			return err
		}
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

	if err := ds.testStore.mutate(ctx, testBuilder); err != nil {
		return pkgerrors.Wrapf(
			err,
			"unable to apply mutations to the %s store",
			ds.testStore.name(),
		)
	}

	return nil
}

func (ds delegatingStore) Mutate(ctx context.Context, stmt *typedef.Stmt) error {
	var wg sync.WaitGroup

	doCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var oracleErr error

	if ds.oracleStore != nil {
		wg.Add(1)

		go func() {
			defer wg.Done()
			oracleErr = ds.oracleStore.mutate(doCtx, stmt)
		}()
	}

	if testErr := ds.testStore.mutate(doCtx, stmt); testErr != nil {
		// Test store failed, transition cannot take place
		ds.logger.Info(
			"test store failed mutation, transition to next state impossible so continuing with next mutation",
			zap.Error(testErr),
		)
		return testErr
	}

	wg.Wait()

	if oracleErr != nil {
		// Test store failed, transition cannot take place
		ds.logger.Info(
			"oracle store failed mutation, transition to next state impossible so continuing with next mutation",
			zap.Error(oracleErr),
		)
		return oracleErr
	}

	return nil
}

func (ds delegatingStore) Check(
	ctx context.Context,
	table *typedef.Table,
	stmt *typedef.Stmt,
	detailedDiff bool,
) error {
	var (
		oracleRows []Row
		oracleErr  error
		wg         sync.WaitGroup
	)

	doCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if ds.oracleStore != nil {
		wg.Add(1)

		go func() {
			defer wg.Done()
			oracleRows, oracleErr = ds.oracleStore.load(doCtx, stmt)
		}()
	}

	testRows, testErr := ds.testStore.load(doCtx, stmt)

	if testErr != nil {
		cancel()
		return pkgerrors.Wrapf(testErr, "unable to load check data from the test store")
	}

	wg.Wait()

	if oracleErr != nil {
		return pkgerrors.Wrapf(oracleErr, "unable to load check data from the oracle store")
	}

	if ds.oracleStore == nil {
		return nil
	}

	if len(testRows) == 0 && len(oracleRows) == 0 {
		return nil
	}

	if len(testRows) != len(oracleRows) {
		if !detailedDiff {
			return fmt.Errorf(
				"rows count differ (test store rows %d, oracle store rows %d, detailed information will be at last attempt)",
				len(testRows),
				len(oracleRows),
			)
		}
		testSet := strset.New(pks(table, testRows)...)
		oracleSet := strset.New(pks(table, oracleRows)...)
		missingInTest := strset.Difference(oracleSet, testSet).List()
		missingInOracle := strset.Difference(testSet, oracleSet).List()
		return fmt.Errorf(
			"row count differ (test has %d rows, oracle has %d rows, test is missing rows: %s, oracle is missing rows: %s)",
			len(testRows),
			len(oracleRows),
			missingInTest,
			missingInOracle,
		)
	}
	if reflect.DeepEqual(testRows, oracleRows) {
		return nil
	}
	if !detailedDiff {
		return fmt.Errorf(
			"test and oracle store have difference, detailed information will be at last attempt",
		)
	}
	sort.SliceStable(testRows, func(i, j int) bool {
		return lt(testRows[i], testRows[j])
	})
	sort.SliceStable(oracleRows, func(i, j int) bool {
		return lt(oracleRows[i], oracleRows[j])
	})
	for i, oracleRow := range oracleRows {
		testRow := testRows[i]
		cmp.AllowUnexported()
		diff := cmp.Diff(oracleRow, testRow,
			cmpopts.SortMaps(func(x, y *inf.Dec) bool {
				return x.Cmp(y) < 0
			}),
			cmp.Comparer(func(x, y *inf.Dec) bool {
				return x.Cmp(y) == 0
			}), cmp.Comparer(func(x, y *big.Int) bool {
				return x.Cmp(y) == 0
			}))
		if diff != "" {
			return fmt.Errorf("rows differ (-%v +%v): %v", oracleRow, testRow, diff)
		}
	}
	return nil
}

func (ds delegatingStore) Close() error {
	var err error

	err = multierr.Append(err, ds.testStore.Close())

	if ds.oracleStore != nil {
		err = multierr.Append(err, ds.oracleStore.Close())
	}

	if ds.statementLogger != nil {
		err = multierr.Append(err, ds.statementLogger.Close())
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
) (out storeLoader, err error) {
	if clusterConfig == nil {
		return nil, nil
	}

	session, err := clusterConfig.CreateSession()
	if err != nil {
		return nil, pkgerrors.Wrapf(err, "failed to connect to %s cluster", name)
	}

	oracleFileLogger, err := stmtlogger.NewFileLogger(stmtLogFile, compression)
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
		stmtLogger:              oracleFileLogger,
	}, nil
}
