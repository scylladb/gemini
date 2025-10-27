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
	"io"
	"math/big"
	"os"
	"path/filepath"
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
	"gopkg.in/inf.v0"

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
}

func newCQLStoreWithSession(
	session *gocql.Session,
	schema *typedef.Schema,
	logger *zap.Logger,
	system string,
) *cqlStore {
	logger.Info("creating cql store with session", zap.String("system", system))

	store := &cqlStore{
		session: session,
		schema:  schema,
		logger:  logger,
		system:  system,
	}

	for i := range typedef.StatementTypeCount {
		store.cqlRequestsMetric[i] = metrics.CQLRequests.WithLabelValues(system, i.String())
		store.cqlErrorRequestsMetric[i] = metrics.CQLErrorRequests.WithLabelValues(system, i.String())
		store.cqlTimeoutsRequestsMetric[i] = metrics.CQLQueryTimeouts.WithLabelValues(system, i.String())
	}

	logger.Info("cql store created", zap.String("system", system))
	return store
}

func newCQLStore(
	cfg ScyllaClusterConfig,
	statementLogger *stmtlogger.Logger,
	schema *typedef.Schema,
	logger *zap.Logger,
	system string,
) (*cqlStore, error) {
	logger.Info("creating cql store",
		zap.String("system", system),
		zap.Strings("hosts", cfg.Hosts),
		zap.String("consistency", cfg.Consistency),
	)

	testSession, err := createCluster(cfg, logger, statementLogger, true)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "failed to create test cluster")
	}

	logger.Info("cluster session created", zap.String("system", system))
	return newCQLStoreWithSession(
		testSession,
		schema,
		logger,
		system,
	), nil
}

func (c *cqlStore) name() string {
	return c.system
}

type MutationStoreError struct {
	Inner         error
	PartitionKeys *typedef.Values
}

func (e MutationStoreError) Error() string {
	data, _ := json.Marshal(e.PartitionKeys)

	return "mutation error: " + e.Inner.Error() + ", partition keys: " + utils.UnsafeString(data)
}

func (c *cqlStore) mutate(ctx context.Context, stmt *typedef.Stmt, ts mo.Option[time.Time]) error {
	query := c.session.QueryWithContext(ctx, stmt.Query, stmt.Values...)
	defer query.Release()

	var acc error

	if ts.IsSome() {
		query = query.WithTimestamp(ts.MustGet().UnixMicro())
	} else {
		query = query.DefaultTimestamp(false)
	}

	mutateErr := query.Exec()
	c.cqlRequestsMetric[stmt.QueryType].Inc()

	if mutateErr == nil || errors.Is(mutateErr, gocql.ErrNotFound) {
		return nil
	}

	if errors.Is(mutateErr, context.Canceled) {
		c.logger.Debug("mutation cancelled", zap.String("system", c.system))
		return context.Canceled
	}

	if errors.Is(mutateErr, context.DeadlineExceeded) {
		c.logger.Warn("mutation timed out",
			zap.String("system", c.system),
			zap.String("query_type", stmt.QueryType.String()),
		)
		c.cqlTimeoutsRequestsMetric[stmt.QueryType].Inc()
		return mutateErr // Return timeout error so delegating store can retry
	}

	c.logger.Error("mutation failed",
		zap.String("system", c.system),
		zap.String("query_type", stmt.QueryType.String()),
		zap.Error(mutateErr),
	)

	acc = multierr.Append(acc, MutationStoreError{
		Inner:         mutateErr,
		PartitionKeys: stmt.PartitionKeys.Values,
	})

	c.cqlErrorRequestsMetric[stmt.QueryType].Inc()

	return acc
}

func (c *cqlStore) load(ctx context.Context, stmt *typedef.Stmt) (Rows, error) {
	query := c.session.QueryWithContext(ctx, stmt.Query, stmt.Values...)
	defer func() {
		query.Release()
		c.cqlRequestsMetric[stmt.QueryType].Inc()
	}()

	iter := query.Iter()

	if iter.NumRows() == 0 {
		return nil, iter.Close()
	}

	// Pre-allocate rows slice
	rows := make(Rows, 0, iter.NumRows())

	rowData, err := iter.RowData()
	if err != nil {
		return nil, err
	}

	for iter.Scan(rowData.Values...) {
		row := NewRow(rowData.Columns, rowData.Values)
		rows = append(rows, row)
	}

	if err = iter.Close(); err != nil && !errors.Is(err, context.Canceled) {
		c.logger.Error("error closing iterator",
			zap.String("system", c.system),
			zap.Error(err),
		)
	}

	return rows, err
}

// loadIter returns an iterator that yields rows one by one without loading all into memory
func (c *cqlStore) loadIter(ctx context.Context, stmt *typedef.Stmt) RowIterator {
	return func(yield func(Row, error) bool) {
		query := c.session.QueryWithContext(ctx, stmt.Query, stmt.Values...)
		defer query.Release()
		defer c.cqlRequestsMetric[stmt.QueryType].Inc()

		iter := query.Iter()
		defer func() {
			if err := iter.Close(); err != nil && !errors.Is(err, context.Canceled) {
				c.logger.Error("error closing iterator",
					zap.String("system", c.system),
					zap.Error(err),
				)
			}
		}()

		// Check if query returned any rows
		if iter.NumRows() == 0 {
			return
		}

		// Get column info once - order is guaranteed by the driver
		rowData, err := iter.RowData()
		if err != nil {
			yield(Row{}, err)
			return
		}

		for {
			// Check for context cancellation
			select {
			case <-ctx.Done():
				yield(Row{}, ctx.Err())
				return
			default:
			}

			// Scan into slice - this is faster than MapScan
			if !iter.Scan(rowData.Values...) {
				// No more rows
				break
			}

			row := NewRow(rowData.Columns, rowData.Values)

			// Yield the row
			if !yield(row, nil) {
				// Consumer stopped iteration
				return
			}
		}
	}
}

// deepCopyValue creates a deep copy of a value to avoid pointer and slice reuse issues
// This is necessary because gocql reuses internal buffers for efficiency
func deepCopyValue(v any) any {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case []byte:
		// Critical: Must copy byte slices as gocql reuses the buffer
		if val == nil {
			return nil
		}
		copied := make([]byte, len(val))
		copy(copied, val)
		return copied
	case string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		// Primitive types are safe to return directly
		return v
	case *big.Int:
		if val == nil {
			return nil
		}
		return new(big.Int).Set(val)
	case *inf.Dec:
		if val == nil {
			return nil
		}
		// Create a new Dec from the string representation to ensure deep copy
		copied := new(inf.Dec)
		copied.SetString(val.String())
		return copied
	case time.Time:
		// time.Time is a struct, so it's copied by value
		return val
	default:
		// For other types (UUID, custom types), return as-is
		// They should be safe as gocql creates new instances for them
		return v
	}
}

func (c *cqlStore) Close() error {
	c.logger.Debug("closing cql store", zap.String("system", c.system))
	c.session.Close()
	c.logger.Debug("cql store closed", zap.String("system", c.system))
	return nil
}

type HostSelectionPolicy string

const (
	HostSelectionTokenAware HostSelectionPolicy = "token-aware"
	HostSelectionRoundRobin HostSelectionPolicy = "round-robin"
	HostSelectionHostPool   HostSelectionPolicy = "host-pool"
	HostSelectionDefault    HostSelectionPolicy = ""
)

func getHostSelectionPolicy(policy HostSelectionPolicy, hosts []string) gocql.HostSelectionPolicy {
	switch policy {
	case HostSelectionRoundRobin:
		return gocql.RoundRobinHostPolicy()
	case HostSelectionHostPool:
		return hostpolicy.HostPool(hostpool.New(slices.Clone(hosts)))
	case HostSelectionDefault, HostSelectionTokenAware:
		p := gocql.TokenAwareHostPolicy(
			gocql.RoundRobinHostPolicy(),
			gocql.ShuffleReplicas(),
		)
		return p
	default:
		panic("unknown host selection policy: " + policy)
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
	cluster.PoolConfig.HostSelectionPolicy = getHostSelectionPolicy(config.HostSelectionPolicy, config.Hosts)

	if config.Username != "" && config.Password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: config.Username,
			Password: config.Password,
		}
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	if config.TracingDir != "" {
		var file io.Writer

		switch config.TracingDir {
		case "stdout":
			file = os.Stdout
		case "stderr":
			file = os.Stderr
		default:
			file, err = utils.CreateFile(filepath.Join(config.TracingDir, string(config.Name)+"-driver.log"), true)
			if err != nil {
				session.Close()
				return nil, err
			}
		}

		tracer := gocql.NewTraceWriter(session, file)
		tracer.SetSleepInterval(100 * time.Millisecond)
		tracer.SetMaxAttempts(10)

		session.SetTrace(tracer)
	}

	return session, nil
}
