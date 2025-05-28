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

func (c *cqlStore) mutate(ctx context.Context, stmt *typedef.Stmt) (err error) {
	var i int
	for i = 0; i < c.maxRetriesMutate; i++ {
		// retry with new timestamp as list modification with the same ts
		// will produce duplicated values, see https://github.com/scylladb/scylladb/issues/7937
		err = c.doMutate(ctx, stmt, time.Now())
		if err == nil {
			metrics.CQLRequests.WithLabelValues(c.system, opType(stmt)).Inc()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.maxRetriesMutateSleep):
		}
	}
	if w := c.logger.Check(zap.ErrorLevel, "failed to apply mutation"); w != nil {
		w.Write(zap.Int("attempts", i), zap.Error(err))
	}
	return err
}

func (c *cqlStore) doMutate(ctx context.Context, stmt *typedef.Stmt, ts time.Time) error {
	queryBody, _ := stmt.Query.ToCql()
	query := c.session.Query(queryBody, stmt.Values...)
	defer query.Release()

	if err := query.Exec(); err != nil {
		if errs.Is(err, context.DeadlineExceeded) {
			if w := c.logger.Check(zap.DebugLevel, "deadline exceeded for mutation query"); w != nil {
				w.Write(
					zap.String("system", c.system),
					zap.String("query", queryBody),
					zap.Error(err),
				)
			}
		}

		if !c.ignore(err, opType(stmt)) {
			return errors.Wrapf(err, "[cluster = %s, query = '%s']", c.system, queryBody)
		}
	}

	if c.useServerSideTimestamps {
		query.DefaultTimestamp(false)
		_ = c.stmtLogger.LogStmt(stmt)
	} else {
		query.WithTimestamp(ts.UnixNano() / 1000)
		_ = c.stmtLogger.LogStmt(stmt, ts)
	}

	return nil
}

func (c *cqlStore) load(ctx context.Context, stmt *typedef.Stmt) ([]Row, error) {
	cql, _ := stmt.Query.ToCql()

	query := c.session.Query(cql, stmt.Values...)
	defer query.Release()

	iter := query.Iter()

	defer func() {
		metrics.CQLRequests.WithLabelValues(c.system, opType(stmt)).Inc()
		_ = iter.Close()
	}()

	rows := make([]Row, 0, iter.NumRows())

	for range iter.NumRows() {
		row := make(Row, len(iter.Columns()))
		if !iter.MapScan(row) {
			return nil, iter.Close()
		}

		rows = append(rows, row)
	}

	if err := c.stmtLogger.LogStmt(stmt); err != nil {
		return nil, err
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
