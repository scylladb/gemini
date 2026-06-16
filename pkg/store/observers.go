// Copyright 2025 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"context"
	"net"
	"slices"
	"strconv"
	"sync"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/mo"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
)

type observerInitializer[T any] func(host string, ty typedef.StatementType) T

type observer[T any] struct {
	data        map[string][typedef.StatementTypeCount]T
	initializer observerInitializer[T]
	mu          sync.RWMutex
}

func newObserver[T any](initializer observerInitializer[T]) *observer[T] {
	o := &observer[T]{
		data:        make(map[string][typedef.StatementTypeCount]T),
		initializer: initializer,
	}
	return o
}

func (o *observer[T]) initialize(host string) [typedef.StatementTypeCount]T {
	var arr [typedef.StatementTypeCount]T
	for t := range typedef.StatementTypeCount {
		arr[t] = o.initializer(host, t)
	}

	o.mu.Lock()
	o.data[host] = arr
	o.mu.Unlock()

	return arr
}

func (o *observer[T]) Get(host string, ty typedef.StatementType) T {
	o.mu.RLock()

	if data, ok := o.data[host]; ok {
		o.mu.RUnlock()
		return data[ty]
	}

	o.mu.RUnlock()

	return o.initialize(host)[ty]
}

type ClusterObserver struct {
	logger            *stmtlogger.Logger
	appLogger         *zap.Logger
	goCQLBatchQueries *observer[prometheus.Counter]
	goCQLBatches      *observer[prometheus.Counter]
	goCQLQueryErrors  *observer[prometheus.Counter]
	goCQLQueries      *observer[prometheus.Counter]
	goCQLQueryTime    *observer[prometheus.Observer]
	goCQLConnections  *observer[prometheus.Gauge]
	clusterName       stmtlogger.Type
}

func NewClusterObserver(
	logger *stmtlogger.Logger,
	appLogger *zap.Logger,
	clusterName stmtlogger.Type,
) *ClusterObserver {
	c := &ClusterObserver{
		logger:      logger,
		appLogger:   appLogger,
		clusterName: clusterName,
		goCQLBatchQueries: newObserver[prometheus.Counter](
			func(host string, ty typedef.StatementType) prometheus.Counter {
				return metrics.GoCQLBatchQueries.WithLabelValues(string(clusterName), host, ty.String())
			},
		),
		goCQLBatches: newObserver[prometheus.Counter](
			func(host string, _ typedef.StatementType) prometheus.Counter {
				return metrics.GoCQLBatches.WithLabelValues(string(clusterName), host)
			}),
		goCQLQueryErrors: newObserver[prometheus.Counter](
			func(host string, _ typedef.StatementType) prometheus.Counter {
				return metrics.GoCQLQueryErrors.WithLabelValues(string(clusterName), host)
			},
		),
		goCQLQueries: newObserver[prometheus.Counter](
			func(host string, ty typedef.StatementType) prometheus.Counter {
				return metrics.GoCQLQueries.WithLabelValues(string(clusterName), host, ty.String())
			}),
		goCQLQueryTime: newObserver[prometheus.Observer](
			func(host string, ty typedef.StatementType) prometheus.Observer {
				return metrics.GoCQLQueryTime.WithLabelValues(string(clusterName), host, ty.String())
			}),
		goCQLConnections: newObserver[prometheus.Gauge](
			func(host string, _ typedef.StatementType) prometheus.Gauge {
				return metrics.GoCQLConnections.WithLabelValues(string(clusterName), host)
			}),
	}

	return c
}

func (c *ClusterObserver) incGoCQLQueryError(instance string, err error) {
	if err == nil {
		return
	}
	metrics.GoCQLQueryErrors.
		WithLabelValues(string(c.clusterName), instance, err.Error()).
		Inc()
}

// logStatement emits one statement-log Item per partition key the statement
// touches. Each emitted Item carries a single, fully-populated
// typedef.PartitionKeys, so the committer always binds the exact number of
// partition-key values the _logs INSERT expects. Multi-partition statements
// therefore produce one _logs row per partition; statements with no partition
// keys are not logged at all.
//
// Previously the observers attached data.Statement.PartitionKeys[0] only when
// the statement touched exactly one partition and otherwise attached an empty
// typedef.PartitionKeys{} (nil Values). That empty item bound zero partition-key
// values against the N-column _logs INSERT, so gocql rejected every affected
// batch client-side with "expected N values got M". Under a multi-partition
// delete workload this recurred thousands of times, collapsing committer
// throughput while statements kept enqueuing — and ultimately OOM-killed the
// loader. Emitting one well-formed Item per partition removes that failure mode
// at the source.
//
// NOTE: base.Values carries the full statement's bind values. For a genuine
// multi-partition statement every emitted row would therefore repeat the whole
// value list rather than that partition's slice. This is currently inert: the
// only multi-partition statement types are SELECTs (never logged) and
// DeleteMultiplePartitions (disabled — see pkg/statements/delete.go, which falls
// back to a single-partition delete), so in practice every logged statement
// touches exactly one partition. Revisit the per-row Values slicing here if
// multi-partition mutations are ever re-enabled.
func (c *ClusterObserver) logStatement(stmt *typedef.Stmt, base stmtlogger.Item) {
	for i := range stmt.PartitionKeys {
		item := base
		item.PartitionKeys = stmt.PartitionKeys[i]
		if err := c.logger.LogStmt(item); err != nil {
			c.appLogger.Error("failed to log statement", zap.Error(err))
		}
	}
}

func (c *ClusterObserver) ObserveBatch(ctx context.Context, batch gocql.ObservedBatch) {
	data := MustGetContextData(ctx)
	if data == nil {
		return
	}
	instance := net.JoinHostPort(batch.Host.ConnectAddress().String(), strconv.Itoa(batch.Host.Port()))

	var errStr string

	if batch.Err != nil {
		errStr = batch.Err.Error()
		c.incGoCQLQueryError(instance, batch.Err)

		switch {
		case errors.Is(batch.Err, gocql.ErrConnectionClosed) || errors.Is(batch.Err, gocql.ErrHostDown):
			c.goCQLConnections.Get(instance, 0).Dec()
		case errors.Is(batch.Err, gocql.ErrNoConnections):
			c.goCQLConnections.Get(instance, 0).Set(0)
		default:
		}
	}

	// NOTE: this loops over batch.Statements AND logStatement loops over the
	// statement's partition keys. gemini never builds multi-statement CQL
	// batches (all mutations are single Query().Exec(), so this path sees
	// len(batch.Statements)==1), which keeps the product to one row per
	// partition. If multi-statement batches are ever introduced for a multi-PK
	// data.Statement, this would emit len(batch.Statements)×len(PartitionKeys)
	// rows — revisit the pairing of batch.Statements[i] with the per-PK loop.
	for i, query := range batch.Statements {
		if c.logger != nil && !data.Statement.QueryType.IsSelect() {
			c.logStatement(data.Statement, stmtlogger.Item{
				Error:         mo.Right[error, string](errStr),
				Statement:     query,
				Values:        mo.Left[[]any, []byte](slices.Clone(batch.Values[i])),
				Start:         stmtlogger.Time{Time: batch.Start},
				Duration:      stmtlogger.Duration{Duration: batch.End.Sub(batch.Start)},
				Host:          instance,
				Attempt:       batch.Attempt,
				GeminiAttempt: data.GeminiAttempt,
				Type:          c.clusterName,
				StatementType: data.Statement.QueryType,
			})
		}

		c.goCQLBatchQueries.Get(instance, data.Statement.QueryType).Inc()
	}
}

func (c *ClusterObserver) ObserveQuery(ctx context.Context, query gocql.ObservedQuery) {
	data := MustGetContextData(ctx)
	if data == nil {
		return
	}

	instance := net.JoinHostPort(query.Host.ConnectAddress().String(), strconv.Itoa(query.Host.Port()))

	var errStr string
	if query.Err != nil {
		metrics.GoCQLQueryErrors.WithLabelValues(string(c.clusterName), instance, query.Err.Error()).Inc()
		errStr = query.Err.Error()

		switch {
		case errors.Is(query.Err, gocql.ErrConnectionClosed) || errors.Is(query.Err, gocql.ErrHostDown):
			c.goCQLConnections.Get(instance, 0).Dec()
		case errors.Is(query.Err, gocql.ErrNoConnections):
			c.goCQLConnections.Get(instance, 0).Set(0)
		default:
		}
	}

	duration := query.End.Sub(query.Start)

	if c.logger != nil && !data.Statement.QueryType.IsSelect() {
		attempts := 0
		// Protect against possible nil metrics in tests or certain driver paths
		if query.Metrics != nil {
			attempts = query.Metrics.Attempts
		}

		c.logStatement(data.Statement, stmtlogger.Item{
			Error:         mo.Right[error, string](errStr),
			Statement:     query.Statement,
			Values:        mo.Left[[]any, []byte](slices.Clone(query.Values)),
			Start:         stmtlogger.Time{Time: query.Start},
			Duration:      stmtlogger.Duration{Duration: duration},
			Host:          instance,
			Attempt:       attempts,
			GeminiAttempt: data.GeminiAttempt,
			Type:          c.clusterName,
			StatementType: data.Statement.QueryType,
		})
	}

	c.goCQLQueries.Get(instance, data.Statement.QueryType).Inc()
	c.goCQLQueryTime.Get(instance, data.Statement.QueryType).Observe(float64(duration) / 1e3)
}

func (c *ClusterObserver) ObserveConnect(connect gocql.ObservedConnect) {
	instance := connect.Host.ConnectAddressAndPort()

	if connect.Err != nil {
		metrics.GoCQLConnectionsErrors.WithLabelValues(
			string(c.clusterName),
			instance,
			connect.Err.Error(),
		).Inc()
		metrics.GoCQLConnections.WithLabelValues(string(c.clusterName), instance).Dec()
		return
	}

	c.goCQLConnections.Get(instance, 0).Inc()
	metrics.GoCQLConnectTime.
		WithLabelValues(string(c.clusterName), instance).
		Observe(float64(connect.End.Sub(connect.Start) / 1e3))
}
