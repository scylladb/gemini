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
	"io"
	"math/big"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/hostpolicy"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/hailocab/go-hostpool"
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
	Check(context.Context, *typedef.Table, *typedef.Stmt, bool) error
}

type (
	ScyllaClusterConfig struct {
		Name                    stmtlogger.Type
		HostSelectionPolicy     string
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
	schemaChangesValues typedef.Values,
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

	testSession, err := createCluster(cfg.TestClusterConfig, logger, statementLogger, true)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "failed to create test cluster")
	}

	testStore := &cqlStore{
		session:                 testSession,
		schema:                  schema,
		system:                  "test",
		maxRetriesMutate:        cfg.MaxRetriesMutate,
		maxRetriesMutateSleep:   cfg.MaxRetriesMutateSleep,
		useServerSideTimestamps: cfg.UseServerSideTimestamps,
		logger:                  logger.Named("test_store"),
	}

	var oracleStore storeLoader

	if cfg.OracleClusterConfig != nil {
		var oracleSession *gocql.Session
		oracleSession, err = createCluster(*cfg.OracleClusterConfig, logger, statementLogger, true)
		if err != nil {
			return nil, pkgerrors.Wrap(err, "failed to create test cluster")
		}
		oracleStore = &cqlStore{
			session:                 oracleSession,
			schema:                  schema,
			system:                  "oracle",
			maxRetriesMutate:        cfg.MaxRetriesMutate,
			maxRetriesMutateSleep:   cfg.MaxRetriesMutateSleep,
			useServerSideTimestamps: cfg.UseServerSideTimestamps,
			logger:                  logger.Named("oracle_store"),
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

func getHostSelectionPolicy(policy string, hosts []string) (gocql.HostSelectionPolicy, error) {
	switch policy {
	case "round-robin":
		return gocql.RoundRobinHostPolicy(), nil
	case "host-pool":
		return hostpolicy.HostPool(hostpool.New(hosts)), nil
	case "token-aware":
		return gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy()), nil
	default:
		return nil, fmt.Errorf("unknown host selection policy \"%s\"", policy)
	}
}

func createCluster(
	config ScyllaClusterConfig,
	logger *zap.Logger,
	statementLogger *stmtlogger.Logger,
	enableObserver bool,
) (*gocql.Session, error) {
	for i := range len(config.Hosts) {
		config.Hosts[i] = strings.TrimSpace(config.Hosts[i])
	}

	c, err := gocql.ParseConsistencyWrapper(config.Consistency)
	if err != nil {
		return nil, err
	}

	hp, err := getHostSelectionPolicy(config.HostSelectionPolicy, config.Hosts)
	if err != nil {
		return nil, err
	}

	cluster := gocql.NewCluster(config.Hosts...)

	if enableObserver {
		observer := ClusterObserver{
			ClusterName: config.Name,
			AppLogger:   logger,
			Logger:      statementLogger,
		}

		cluster.ConnectObserver = observer
		cluster.QueryObserver = observer
		cluster.BatchObserver = observer
	}

	cluster.Timeout = config.RequestTimeout
	cluster.ConnectTimeout = config.ConnectTimeout
	cluster.MaxRoutingKeyInfo = 50_000
	cluster.MaxPreparedStmts = 50_000
	cluster.ReconnectInterval = config.ConnectTimeout
	cluster.Events.DisableTopologyEvents = false
	cluster.Events.DisableSchemaEvents = false
	cluster.Events.DisableNodeStatusEvents = false
	cluster.Logger = zap.NewStdLog(logger.Named("gocql").With(zap.String("cluster", string(config.Name))))
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		Min:        time.Second,
		Max:        60 * time.Second,
		NumRetries: 5,
	}
	cluster.Consistency = c
	cluster.DefaultTimestamp = !config.UseServerSideTimestamps
	cluster.PoolConfig.HostSelectionPolicy = hp

	if config.Username != "" && config.Password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: config.Username,
			Password: config.Password,
		}
	}

	return cluster.CreateSession()
}

type delegatingStore struct {
	workers         *workpool.Pool
	oracleStore     storeLoader
	testStore       storeLoader
	logger          *zap.Logger
	statementLogger *stmtlogger.Logger
}

func (ds delegatingStore) Create(ctx context.Context, testBuilder, oracleBuilder *typedef.Stmt) error {
	doCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	doCtx = context.WithValue(doCtx, utils.QueryID, gocql.UUIDFromTime(time.Now()))

	if err := ds.testStore.mutate(doCtx, testBuilder, mo.None[time.Time]()); err != nil {
		return pkgerrors.Wrapf(
			err,
			"unable to apply mutations to the %s store",
			ds.testStore.name(),
		)
	}

	if ds.oracleStore != nil {
		if err := ds.oracleStore.mutate(doCtx, oracleBuilder, mo.None[time.Time]()); err != nil {
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

	doCtx := context.WithValue(ctx, utils.QueryID, gocql.TimeUUID())

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
	doCtx := context.WithValue(ctx, utils.QueryID, gocql.TimeUUID())
	var oracleCh chan mo.Result[any]

	if ds.oracleStore != nil {
		oracleCh = ds.workers.Send(doCtx, func(ctx context.Context) (any, error) {
			return ds.oracleStore.load(ctx, stmt)
		})
	}

	testCh := ds.workers.Send(doCtx, func(ctx context.Context) (any, error) {
		return ds.testStore.load(ctx, stmt)
	})

	testResult := <-testCh
	if testResult.IsError() {
		ds.workers.Release(oracleCh)
		return pkgerrors.Wrap(testResult.Error(), "unable to load check data from the test store")
	}

	if oracleCh == nil {
		return nil
	}

	oracleResult := <-oracleCh
	ds.workers.Release(oracleCh)

	if oracleResult.IsError() {
		return pkgerrors.Wrap(oracleResult.Error(), "unable to load check data from the oracle store")
	}

	oracleRows := oracleResult.MustGet().(Rows)
	testRows := testResult.MustGet().(Rows)

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
	err := multierr.Append(nil, ds.testStore.Close())

	if ds.oracleStore != nil {
		err = multierr.Append(err, ds.oracleStore.Close())
	}

	if ds.statementLogger != nil {
		return multierr.Append(err, ds.statementLogger.Close())
	}

	return nil
}
