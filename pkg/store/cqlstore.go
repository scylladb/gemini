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
	errs "errors"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/samber/mo"
	"github.com/scylladb/gocqlx/v3/qb"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
)

type cqlStore struct {
	stmtLogger              stmtlogger.StmtToFile
	session                 *gocql.Session
	schema                  *typedef.Schema
	logger                  *zap.Logger
	system                  string
	maxRetriesMutate        int
	maxRetriesMutateSleep   time.Duration
	useServerSideTimestamps bool
}

func (c *cqlStore) name() string {
	return c.system
}

func (c *cqlStore) mutate(ctx context.Context, stmt *typedef.Stmt, ts mo.Option[time.Time]) error {
	var err error

	for i := range c.maxRetriesMutate {
		err = c.doMutate(ctx, stmt, ts)

		if err == nil {
			metrics.CQLRequests.WithLabelValues(c.system, opType(stmt)).Inc()
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			c.logger.Error(
				"failed to apply mutation",
				zap.Int("attempt", i),
				zap.Error(err),
			)
			time.Sleep(c.maxRetriesMutateSleep)
		}
	}

	return err
}

func (c *cqlStore) doMutate(_ context.Context, stmt *typedef.Stmt, timestamp mo.Option[time.Time]) error {
	queryBody, _ := stmt.Query.ToCql()
	query := c.session.Query(queryBody, stmt.Values...)
	defer query.Release()

	if !c.useServerSideTimestamps {
		timestamp = timestamp.MapNone(func() (time.Time, bool) {
			return time.Now(), true
		})
		query.WithTimestamp(timestamp.MustGet().UnixMicro())
	}

	if err := query.Exec(); err != nil {
		if errs.Is(err, context.DeadlineExceeded) || errs.Is(err, context.Canceled) {
			c.logger.Debug("deadline exceeded for mutation query",
				zap.String("system", c.system),
				zap.String("query", queryBody),
				zap.Any("values", stmt.Values),
			)
		}

		if !c.ignore(err, opType(stmt)) {
			return errors.Wrapf(err, "[cluster = %s, query = '%s']", c.system, queryBody)
		}
	}

	if c.stmtLogger != nil {
		_ = c.stmtLogger.LogStmt(stmt, timestamp)
	}
	return nil
}

func (c *cqlStore) load(_ context.Context, stmt *typedef.Stmt) (Rows, error) {
	cql, _ := stmt.Query.ToCql()

	query := c.session.Query(cql, stmt.Values...)
	iter := query.Iter()

	defer func() {
		query.Release()
		metrics.CQLRequests.WithLabelValues(c.system, opType(stmt)).Inc()
		_ = iter.Close()
	}()

	rows := make(Rows, iter.NumRows())

	for i := range iter.NumRows() {
		row := make(Row, len(iter.Columns()))
		if !iter.MapScan(row) {
			return nil, iter.Close()
		}

		rows[i] = row
	}

	if c.stmtLogger != nil {
		_ = c.stmtLogger.LogStmt(stmt, mo.None[time.Time]())
	}

	return rows, nil
}

func (c *cqlStore) Close() error {
	c.session.Close()
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

func (c *cqlStore) ignore(err error, ty string) bool {
	if err == nil {
		return true
	}
	//nolint:errorlint
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		metrics.CQLQueryTimeouts.WithLabelValues(c.system, ty).Inc()
		return true
	default:
		return false
	}
}
