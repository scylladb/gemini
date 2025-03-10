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

package drivers

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/gocql/gocql"
	"github.com/hailocab/go-hostpool"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2/qb"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/auth"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

type (
	ScyllaDB struct {
		statementLog            stmtlogger.StmtToFile
		session                 *gocql.Session
		schema                  *typedef.Schema
		logger                  *zap.Logger
		system                  string
		maxRetriesMutateSleep   time.Duration
		maxRetriesMutate        int
		useServerSideTimestamps bool
	}

	CQLConfig struct {
		Password                string
		Trace                   string
		Consistency             string
		DC                      string
		HostSelectionPolicy     string
		Username                string
		StatementLog            string
		Hosts                   []string
		RequestTimeout          time.Duration
		ConnectTimeout          time.Duration
		MaxRetriesMutateSleep   time.Duration
		MaxRetriesMutate        int
		UseServerSideTimestamps bool
	}
)

func NewCQL(
	ctx context.Context,
	name string,
	schema *typedef.Schema,
	cfg CQLConfig,
	logger *zap.Logger,
	logCompression string,
) (*ScyllaDB, error) {
	clusterConfig, err := createClusters(cfg, logger)
	if err != nil {
		return nil, err
	}

	session, err := clusterConfig.CreateSession()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to connect to %s cluster", name)
	}

	if cfg.Trace != "" {
		var trace io.Writer
		trace, err = cfg.getTraceLogFile()
		if err != nil {
			return nil, errors.Wrap(err, "failed to create trace file")
		}

		session.SetTrace(gocql.NewTraceWriter(session, trace))
	}

	if err = session.AwaitSchemaAgreement(ctx); err != nil {
		return nil, errors.Wrapf(err, "failed to await schema agreement for %s cluster", name)
	}

	compression, err := stmtlogger.ParseCompression(logCompression)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse compression")
	}

	statementLog, err := cfg.getStatementLogFile(compression)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create statement logger")
	}

	return &ScyllaDB{
		session:                 session,
		schema:                  schema,
		logger:                  logger.Named(name),
		system:                  name,
		statementLog:            statementLog,
		maxRetriesMutateSleep:   cfg.MaxRetriesMutateSleep,
		maxRetriesMutate:        cfg.MaxRetriesMutate,
		useServerSideTimestamps: cfg.UseServerSideTimestamps,
	}, nil
}

func (cs *ScyllaDB) Execute(ctx context.Context, stmt *typedef.Stmt) error {
	for range cs.maxRetriesMutate {
		if err := cs.doMutate(ctx, stmt); err == nil {
			metrics.CQLRequests.WithLabelValues(cs.system, opType(stmt)).Inc()
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(cs.maxRetriesMutateSleep):
		}
	}

	return errors.Errorf("failed to Mutate after %d retries", cs.maxRetriesMutate)
}

func (cs *ScyllaDB) doMutate(ctx context.Context, stmt *typedef.Stmt) error {
	queryBody, _ := stmt.Query.ToCql()
	query := cs.session.Query(queryBody, stmt.Values...).WithContext(ctx).DefaultTimestamp(false)
	defer query.Release()

	var ts time.Time

	if !cs.useServerSideTimestamps {
		ts = time.Now()
		query = query.WithTimestamp(ts.UnixMicro())
	}

	if err := query.Exec(); err != nil {
		return errors.Wrapf(err, "[cluster = %s, query = %s]", cs.system, queryBody)
	}

	if err := cs.statementLog.LogStmt(stmt, ts); err != nil {
		return err
	}

	return nil
}

func (cs *ScyllaDB) Fetch(ctx context.Context, stmt *typedef.Stmt) ([]map[string]any, error) {
	cql, _ := stmt.Query.ToCql()

	query := cs.session.
		Query(cql, stmt.Values...).
		WithContext(ctx).
		DefaultTimestamp(false)

	defer query.Release()

	metrics.CQLRequests.WithLabelValues(cs.system, opType(stmt)).Inc()

	result, err := loadSet(query.Iter())
	if err != nil {
		return nil, errors.Wrapf(err, "[cluster = %s, query = %s]", cs.system, cql)
	}

	if err = cs.statementLog.LogStmt(stmt); err != nil {
		return nil, err
	}

	return result, nil
}

func (cs *ScyllaDB) Close() error {
	cs.session.Close()
	return nil
}

func opType(stmt *typedef.Stmt) string {
	switch stmt.Query.(type) {
	case *qb.InsertBuilder:
		return "insert"
	case *qb.DeleteBuilder:
		return "delete"
	case *qb.UpdateBuilder:
		return "update"
	case *qb.SelectBuilder:
		return "select"
	case *qb.BatchBuilder:
		return "batch"
	default:
		return "unknown"
	}
}

func createClusters(cfg CQLConfig, logger *zap.Logger) (*gocql.ClusterConfig, error) {
	authenticator, err := auth.BuildAuthenticator(cfg.Username, cfg.Password)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create authenticator")
	}

	cons, err := gocql.ParseConsistencyWrapper(cfg.Consistency)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse consistency: %s", cfg.Consistency)
	}

	selectionPolicy, err := parseHostSelectionPolicy(cfg.HostSelectionPolicy, cfg.DC, cfg.Hosts)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse host selection policy: %s", cfg.HostSelectionPolicy)
	}

	cluster := gocql.NewCluster(cfg.Hosts...)
	cluster.Timeout = cfg.RequestTimeout
	cluster.ProtoVersion = 4
	cluster.ConnectTimeout = cfg.ConnectTimeout
	cluster.Consistency = cons
	cluster.PoolConfig.HostSelectionPolicy = selectionPolicy
	cluster.DefaultTimestamp = false
	cluster.Logger = zap.NewStdLog(logger)
	cluster.MaxRoutingKeyInfo = 10_000
	cluster.MaxPreparedStmts = 10_000
	cluster.Authenticator = authenticator
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		Min:        500 * time.Millisecond,
		Max:        cfg.RequestTimeout,
		NumRetries: cfg.MaxRetriesMutate,
	}
	cluster.ReconnectionPolicy = &gocql.ExponentialReconnectionPolicy{
		InitialInterval: 500 * time.Millisecond,
		MaxRetries:      10,
		MaxInterval:     cfg.ConnectTimeout,
	}

	return cluster, nil
}

func parseHostSelectionPolicy(policy, dc string, hosts []string) (gocql.HostSelectionPolicy, error) {
	switch policy {
	case "round-robin":
		if dc != "" {
			return gocql.DCAwareRoundRobinPolicy(dc), nil
		}

		return gocql.RoundRobinHostPolicy(), nil
	case "host-pool":
		return gocql.HostPoolHostPolicy(hostpool.New(hosts)), nil
	case "token-aware":
		if dc != "" {
			return gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(dc)), nil
		}

		return gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy()), nil
	default:
		return nil, fmt.Errorf("unknown host selection policy \"%s\"", policy)
	}
}

func loadSet(iter *gocql.Iter) ([]map[string]any, error) {
	rows := make([]map[string]any, 0, iter.NumRows())

	for {
		row := make(map[string]any, len(iter.Columns()))

		if !iter.MapScan(row) {
			break
		}

		rows = append(rows, row)
	}

	return rows, iter.Close()
}

func (c CQLConfig) getStatementLogFile(compression stmtlogger.Compression) (stmtlogger.StmtToFile, error) {
	writer, err := utils.CreateFile(c.StatementLog)
	if err != nil {
		return nil, err
	}

	return stmtlogger.NewLogger(writer, compression)
}

func (c CQLConfig) getTraceLogFile() (io.Writer, error) {
	writer, err := utils.CreateFile(c.StatementLog)
	if err != nil {
		return nil, err
	}

	return bufio.NewWriterSize(writer, 8192), nil
}
