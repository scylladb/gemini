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
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/hostpolicy"
	"github.com/hailocab/go-hostpool"
	pkgerrors "github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/mo"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

type cqlStore struct {
	cqlRequestsMetric         [typedef.StatementTypeCount]prometheus.Counter
	cqlErrorRequestsMetric    [typedef.StatementTypeCount]prometheus.Counter
	cqlTimeoutsRequestsMetric [typedef.StatementTypeCount]prometheus.Counter
	session                   *gocql.Session
	schema                    *typedef.Schema
	logger                    *zap.Logger
	system                    string
	maxRetriesMutate          int
	maxRetriesMutateSleep     time.Duration
	useServerSideTimestamps   bool
}

func newCQLStoreWithSession(
	session *gocql.Session,
	schema *typedef.Schema,
	logger *zap.Logger,
	system string,
	maxRetriesMutate int,
	maxRetriesMutateSleep time.Duration,
	useServerSideTimestamps bool,
) *cqlStore {
	store := &cqlStore{
		session:                 session,
		schema:                  schema,
		logger:                  logger,
		system:                  system,
		maxRetriesMutate:        maxRetriesMutate,
		maxRetriesMutateSleep:   maxRetriesMutateSleep,
		useServerSideTimestamps: useServerSideTimestamps,
	}

	for i := range typedef.StatementTypeCount {
		store.cqlRequestsMetric[i] = metrics.CQLRequests.WithLabelValues(system, i.String())
		store.cqlErrorRequestsMetric[i] = metrics.CQLRequests.WithLabelValues(system, i.String())
		store.cqlTimeoutsRequestsMetric[i] = metrics.CQLRequests.WithLabelValues(system, i.String())
	}

	return store
}

func newCQLStore(
	cfg ScyllaClusterConfig,
	statementLogger *stmtlogger.Logger,
	schema *typedef.Schema,
	logger *zap.Logger,
	system string,
	maxRetriesMutate int,
	maxRetriesMutateSleep time.Duration,
	useServerSideTimestamps bool,
) (*cqlStore, error) {
	testSession, err := createCluster(cfg, logger, statementLogger, true)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "failed to create test cluster")
	}

	return newCQLStoreWithSession(
		testSession,
		schema,
		logger,
		system,
		maxRetriesMutate,
		maxRetriesMutateSleep,
		useServerSideTimestamps,
	), nil
}

func (c *cqlStore) name() string {
	return c.system
}

type MutationError struct {
	Inner         error
	PartitionKeys *typedef.Values
}

func (e MutationError) Error() string {
	data, _ := json.Marshal(e.PartitionKeys)

	return "mutation error: " + e.Inner.Error() + ", partition keys: " + utils.UnsafeString(data)
}

func (c *cqlStore) mutate(ctx context.Context, stmt *typedef.Stmt, ts mo.Option[time.Time]) error {
	var err error

	query := c.session.Query(stmt.Query, stmt.Values...).WithContext(ctx)
	defer query.Release()

	if !c.useServerSideTimestamps {
		ts = ts.MapNone(func() (time.Time, bool) {
			return time.Now(), true
		})
		query.WithTimestamp(ts.MustGet().UnixMicro())
	}

	for i := range c.maxRetriesMutate {
		mutateErr := query.Exec()
		c.cqlRequestsMetric[stmt.QueryType].Inc()

		if mutateErr == nil || errors.Is(mutateErr, gocql.ErrNotFound) {
			return nil
		}

		if errors.Is(mutateErr, context.Canceled) {
			return context.Canceled
		}

		if errors.Is(mutateErr, context.DeadlineExceeded) {
			c.cqlTimeoutsRequestsMetric[stmt.QueryType].Inc()
			return nil
		}

		err = multierr.Append(err, MutationError{
			Inner:         mutateErr,
			PartitionKeys: stmt.PartitionKeys.Values,
		})

		data, _ := GetContextData(ctx)
		data.GeminiAttempt = i
		c.cqlErrorRequestsMetric[stmt.QueryType].Inc()
		time.Sleep(c.maxRetriesMutateSleep)
	}

	return err
}

func (c *cqlStore) load(ctx context.Context, stmt *typedef.Stmt) (Rows, error) {
	query := c.session.Query(stmt.Query, stmt.Values...).WithContext(ctx)
	iter := query.Iter()

	defer func() {
		query.Release()
		c.cqlRequestsMetric[stmt.QueryType].Inc()
	}()

	rows := make(Rows, iter.NumRows())

	for i := range iter.NumRows() {
		row := make(Row, len(iter.Columns()))
		if !iter.MapScan(row) {
			return nil, iter.Close()
		}

		rows[i] = row
	}

	return rows, iter.Close()
}

func (c *cqlStore) Close() error {
	c.session.Close()
	return nil
}

func getHostSelectionPolicy(policy string, hosts []string) (gocql.HostSelectionPolicy, error) {
	switch policy {
	case "round-robin":
		return gocql.RoundRobinHostPolicy(), nil
	case "host-pool":
		return hostpolicy.HostPool(hostpool.New(slices.Clone(hosts))), nil
	case "token-aware":
		p := gocql.TokenAwareHostPolicy(
			gocql.RoundRobinHostPolicy(),
			gocql.ShuffleReplicas(),
		)
		return p, nil
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
	c, err := gocql.ParseConsistencyWrapper(config.Consistency)
	if err != nil {
		return nil, err
	}

	hp, err := getHostSelectionPolicy(config.HostSelectionPolicy, config.Hosts)
	if err != nil {
		return nil, err
	}

	cluster := gocql.NewCluster(slices.Clone(config.Hosts)...)

	if enableObserver {
		o := NewClusterObserver(statementLogger, logger, config.Name)

		cluster.ConnectObserver = o
		cluster.QueryObserver = o
		cluster.BatchObserver = o
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
	cluster.DefaultIdempotence = false
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		Min:        10 * time.Millisecond,
		Max:        10 * time.Second,
		NumRetries: 10,
	}
	cluster.ReconnectionPolicy = &gocql.ExponentialReconnectionPolicy{
		MaxRetries:      10,
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     config.ConnectTimeout,
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
