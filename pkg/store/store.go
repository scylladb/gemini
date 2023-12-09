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
	"os"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/store/comp"
	mv "github.com/scylladb/gemini/pkg/store/mv"
	sv "github.com/scylladb/gemini/pkg/store/sv"
	"github.com/scylladb/gemini/pkg/store/ver"
	"github.com/scylladb/gemini/pkg/typedef"
)

var errorResponseDiffer = errors.New("response from test and oracle store have difference")

type loader interface {
	loadSV(context.Context, *typedef.Stmt) (sv.Result, error)
	loadMV(context.Context, *typedef.Stmt) (mv.Result, error)
	loadVerCheck(context.Context, *typedef.Stmt) (mv.Result, error)
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

type stmtLogger interface {
	LogStmt(*typedef.Stmt)
	LogStmtWithTimeStamp(stmt *typedef.Stmt, ts time.Time)
	Close() error
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

func (n *noOpStore) loadSV(context.Context, *typedef.Stmt) (sv.Result, error) {
	return sv.Result{}, nil
}

func (n *noOpStore) loadMV(context.Context, *typedef.Stmt) (mv.Result, error) {
	return mv.Result{}, nil
}

func (n *noOpStore) loadVerCheck(context.Context, *typedef.Stmt) (mv.Result, error) {
	return mv.Result{}, nil
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
	statementLogger stmtLogger
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

func (ds delegatingStore) Check(ctx context.Context, _ *typedef.Table, stmt *typedef.Stmt, detailedDiff bool) error {
	var testErr, oracleErr error
	var wg sync.WaitGroup
	wg.Add(1)
	var results comp.Results
	switch {
	case ver.Check.ModeSV():
		resultsSV := sv.Results{}
		go func() {
			resultsSV.Test, testErr = ds.testStore.loadSV(ctx, stmt)
			wg.Done()
		}()
		resultsSV.Oracle, oracleErr = ds.oracleStore.loadSV(ctx, stmt)
		results = &resultsSV
	case ver.Check.Done():
		resultsMV := mv.Results{}
		go func() {
			resultsMV.Test, testErr = ds.testStore.loadMV(ctx, stmt)
			wg.Done()
		}()
		resultsMV.Oracle, oracleErr = ds.oracleStore.loadMV(ctx, stmt)
		results = &resultsMV
	default:
		resultsMV := mv.Results{}
		go func() {
			resultsMV.Test, testErr = ds.testStore.loadVerCheck(ctx, stmt)
			wg.Done()
		}()
		resultsMV.Oracle, oracleErr = ds.oracleStore.loadVerCheck(ctx, stmt)
		results = &resultsMV
	}
	if oracleErr != nil {
		return errors.Wrapf(oracleErr, "unable to load check data from the oracle store")
	}
	wg.Wait()
	if testErr != nil {
		return errors.Wrapf(testErr, "unable to load check data from the test store")
	}
	if !ds.validations || !results.HaveRows() {
		return nil
	}
	var diff comp.Info
	if detailedDiff {
		diff = comp.GetCompareInfoDetailed(results)
	} else {
		diff = comp.GetCompareInfoSimple(results)
	}
	if diff.Len() > 0 {
		return errors.Wrap(errorResponseDiffer, diff.String())
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
