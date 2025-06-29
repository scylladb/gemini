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
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/samber/mo"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

type cqlStore struct {
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

type MutationError struct {
	Inner         error
	PartitionKeys typedef.Values
}

func (e MutationError) Error() string {
	data, _ := json.Marshal(e.PartitionKeys)

	return "mutation error: " + e.Inner.Error() + ", partition keys: " + utils.UnsafeString(data)
}

func (c *cqlStore) mutate(ctx context.Context, stmt *typedef.Stmt, ts mo.Option[time.Time]) error {
	var err error

	stType := stmt.QueryType.String()

	cqlRequestsMetric := metrics.CQLRequests.WithLabelValues(c.system, stType)
	cqlErrorRequestsMetric := metrics.CQLErrorRequests.WithLabelValues(c.system, stType)
	cqlTimeoutRequestsMetric := metrics.CQLQueryTimeouts.WithLabelValues(c.system, stType)

	query := c.session.Query(stmt.Query, stmt.Values...).WithContext(ctx)
	defer query.Release()

	if !c.useServerSideTimestamps {
		ts = ts.MapNone(func() (time.Time, bool) {
			return time.Now(), true
		})
		query.WithTimestamp(ts.MustGet().UnixMicro())
	}

	for range c.maxRetriesMutate {
		mutateErr := query.Exec()
		cqlRequestsMetric.Inc()

		if mutateErr == nil || errors.Is(mutateErr, gocql.ErrNotFound) || errors.Is(mutateErr, context.Canceled) {
			return nil
		}

		if errors.Is(mutateErr, context.DeadlineExceeded) {
			cqlTimeoutRequestsMetric.Inc()
			return nil
		}

		err = multierr.Append(err, MutationError{
			Inner:         mutateErr,
			PartitionKeys: stmt.PartitionKeys.Values,
		})
		cqlErrorRequestsMetric.Inc()
		time.Sleep(c.maxRetriesMutateSleep)
	}

	return err
}

func (c *cqlStore) load(ctx context.Context, stmt *typedef.Stmt) (Rows, error) {
	query := c.session.Query(stmt.Query, stmt.Values...).WithContext(ctx)
	iter := query.Iter()

	defer func() {
		query.Release()
		metrics.CQLRequests.WithLabelValues(c.system, stmt.QueryType.String()).Inc()
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
