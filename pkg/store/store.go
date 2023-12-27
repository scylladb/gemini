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
	"fmt"
	"math/big"
	"os"
	"reflect"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/multierr"
	"gopkg.in/inf.v0"

	"github.com/scylladb/go-set/strset"

	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
)

type loader interface {
	load(context.Context, *typedef.Stmt) ([]map[string]interface{}, error)
}

type storer interface {
	mutate(context.Context, *typedef.Stmt) error
}

type storeLoader interface {
	storer
	loader
	close() error
	name() string
}

type Store interface {
	Create(context.Context, *typedef.Stmt, *typedef.Stmt) error
	Mutate(context.Context, *typedef.Stmt) error
	Check(context.Context, *typedef.Table, *typedef.Stmt, bool) error
	Close() error
}

type Config struct {
	TestLogStatementsFile   string
	OracleLogStatementsFile string
	MaxRetriesMutate        int
	MaxRetriesMutateSleep   time.Duration
	UseServerSideTimestamps bool
}

func New(schema *typedef.Schema, testCluster, oracleCluster *gocql.ClusterConfig, cfg Config, traceOut *os.File, logger *zap.Logger) (Store, error) {
	ops := promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gemini_cql_requests",
		Help: "How many CQL requests processed, partitioned by system and CQL query type aka 'method' (batch, delete, insert, update).",
	}, []string{"system", "method"},
	)

	oracleStore, err := getStore("oracle", schema, oracleCluster, cfg, cfg.OracleLogStatementsFile, traceOut, logger, ops)
	if err != nil {
		return nil, err
	}

	if testCluster == nil {
		return nil, errors.New("test cluster is empty")
	}
	testStore, err := getStore("test", schema, testCluster, cfg, cfg.TestLogStatementsFile, traceOut, logger, ops)
	if err != nil {
		return nil, err
	}

	return &delegatingStore{
		testStore:   testStore,
		oracleStore: oracleStore,
		validations: oracleStore != nil,
		logger:      logger.Named("delegating_store"),
	}, nil
}

type noOpStore struct {
	system string
}

func (n *noOpStore) mutate(context.Context, *typedef.Stmt) error {
	return nil
}

func (n *noOpStore) load(context.Context, *typedef.Stmt) ([]map[string]interface{}, error) {
	return nil, nil
}

func (n *noOpStore) Close() error {
	return nil
}

func (n *noOpStore) name() string {
	return n.system
}

func (n *noOpStore) close() error {
	return nil
}

type delegatingStore struct {
	oracleStore     storeLoader
	testStore       storeLoader
	statementLogger stmtlogger.StmtToFile
	logger          *zap.Logger
	validations     bool
}

func (ds delegatingStore) Create(ctx context.Context, testBuilder, oracleBuilder *typedef.Stmt) error {
	if ds.statementLogger != nil {
		ds.statementLogger.LogStmt(testBuilder)
	}
	if err := mutate(ctx, ds.oracleStore, oracleBuilder); err != nil {
		return errors.Wrap(err, "oracle failed store creation")
	}
	if err := mutate(ctx, ds.testStore, testBuilder); err != nil {
		return errors.Wrap(err, "test failed store creation")
	}
	return nil
}

func (ds delegatingStore) Mutate(ctx context.Context, stmt *typedef.Stmt) error {
	var testErr error
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		testErr = errors.Wrapf(
			ds.testStore.mutate(ctx, stmt),
			"unable to apply mutations to the %s store", ds.testStore.name())
	}()

	if oracleErr := ds.oracleStore.mutate(ctx, stmt); oracleErr != nil {
		// Oracle failed, transition cannot take place
		ds.logger.Info("oracle store failed mutation, transition to next state impossible so continuing with next mutation", zap.Error(oracleErr))
		return oracleErr
	}
	wg.Wait()
	if testErr != nil {
		// Test store failed, transition cannot take place
		ds.logger.Info("test store failed mutation, transition to next state impossible so continuing with next mutation", zap.Error(testErr))
		return testErr
	}
	return nil
}

func mutate(ctx context.Context, s storeLoader, stmt *typedef.Stmt) error {
	if err := s.mutate(ctx, stmt); err != nil {
		return errors.Wrapf(err, "unable to apply mutations to the %s store", s.name())
	}
	return nil
}

func (ds delegatingStore) Check(ctx context.Context, table *typedef.Table, stmt *typedef.Stmt, detailedDiff bool) error {
	var testRows, oracleRows []map[string]interface{}
	var testErr, oracleErr error
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		testRows, testErr = ds.testStore.load(ctx, stmt)
		wg.Done()
	}()
	oracleRows, oracleErr = ds.oracleStore.load(ctx, stmt)
	if oracleErr != nil {
		return errors.Wrapf(oracleErr, "unable to load check data from the oracle store")
	}
	wg.Wait()
	if testErr != nil {
		return errors.Wrapf(testErr, "unable to load check data from the test store")
	}
	if !ds.validations {
		return nil
	}
	if len(testRows) == 0 && len(oracleRows) == 0 {
		return nil
	}
	if len(testRows) != len(oracleRows) {
		if !detailedDiff {
			return fmt.Errorf("rows count differ (test store rows %d, oracle store rows %d, detailed information will be at last attempt)", len(testRows), len(oracleRows))
		}
		testSet := strset.New(pks(table, testRows)...)
		oracleSet := strset.New(pks(table, oracleRows)...)
		missingInTest := strset.Difference(oracleSet, testSet).List()
		missingInOracle := strset.Difference(testSet, oracleSet).List()
		return fmt.Errorf("row count differ (test has %d rows, oracle has %d rows, test is missing rows: %s, oracle is missing rows: %s)",
			len(testRows), len(oracleRows), missingInTest, missingInOracle)
	}
	if reflect.DeepEqual(testRows, oracleRows) {
		return nil
	}
	if !detailedDiff {
		return fmt.Errorf("test and oracle store have difference, detailed information will be at last attempt")
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

func (ds delegatingStore) Close() (err error) {
	if ds.statementLogger != nil {
		err = multierr.Append(err, ds.statementLogger.Close())
	}
	err = multierr.Append(err, ds.testStore.close())
	err = multierr.Append(err, ds.oracleStore.close())
	return
}

func getStore(
	name string,
	schema *typedef.Schema,
	clusterConfig *gocql.ClusterConfig,
	cfg Config,
	stmtLogFile string,
	traceOut *os.File,
	logger *zap.Logger,
	ops *prometheus.CounterVec,
) (out storeLoader, err error) {
	if clusterConfig == nil {
		return &noOpStore{
			system: name,
		}, nil
	}
	oracleSession, err := newSession(clusterConfig, traceOut)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to connect to %s cluster", name)
	}
	oracleFileLogger, err := stmtlogger.NewFileLogger(stmtLogFile)
	if err != nil {
		return nil, err
	}

	return &cqlStore{
		session:                 oracleSession,
		schema:                  schema,
		system:                  name,
		ops:                     ops,
		maxRetriesMutate:        cfg.MaxRetriesMutate + 10,
		maxRetriesMutateSleep:   cfg.MaxRetriesMutateSleep,
		useServerSideTimestamps: cfg.UseServerSideTimestamps,
		logger:                  logger.Named(name),
		stmtLogger:              oracleFileLogger,
	}, nil
}
