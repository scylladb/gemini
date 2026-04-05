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
	"errors"
	"net"
	"slices"
	"strconv"
	"sync"

	"github.com/gocql/gocql"
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

// classifyError maps a gocql error to a short, low-cardinality category.
func classifyError(err error) string {
	if err == nil {
		return ""
	}

	switch {
	case errors.Is(err, gocql.ErrTimeoutNoResponse):
		return "timeout_no_response"
	case errors.Is(err, gocql.ErrTooManyTimeouts):
		return "too_many_timeouts"
	case errors.Is(err, gocql.ErrConnectionClosed):
		return "connection_closed"
	case errors.Is(err, gocql.ErrHostDown):
		return "host_down"
	case errors.Is(err, gocql.ErrNoConnections):
		return "no_connections"
	case errors.Is(err, gocql.ErrSessionClosed):
		return "session_closed"
	case errors.Is(err, gocql.ErrNoStreams):
		return "no_streams"
	}

	// Check CQL protocol-level request errors
	var reqErr gocql.RequestError
	if errors.As(err, &reqErr) {
		switch reqErr.Code() {
		case gocql.ErrCodeReadTimeout:
			return "read_timeout"
		case gocql.ErrCodeWriteTimeout:
			return "write_timeout"
		case gocql.ErrCodeReadFailure:
			return "read_failure"
		case gocql.ErrCodeWriteFailure:
			return "write_failure"
		case gocql.ErrCodeUnavailable:
			return "unavailable"
		case gocql.ErrCodeOverloaded:
			return "overloaded"
		case gocql.ErrCodeSyntax:
			return "syntax_error"
		case gocql.ErrCodeInvalid:
			return "invalid_query"
		case gocql.ErrCodeUnprepared:
			return "unprepared"
		case gocql.ErrCodeServer:
			return "server_error"
		default:
			return "request_error"
		}
	}

	return "other"
}

func (c *ClusterObserver) ObserveBatch(ctx context.Context, batch gocql.ObservedBatch) {
	data := MustGetContextData(ctx)
	if data == nil {
		return
	}
	instance := net.JoinHostPort(batch.Host.ConnectAddress().String(), strconv.Itoa(batch.Host.Port()))

	var errStr string

	cluster := string(c.clusterName)
	if batch.Err != nil {
		errStr = batch.Err.Error()
		c.incGoCQLQueryError(instance, batch.Err)
		metrics.GoCQLErrorsByType.WithLabelValues(cluster, classifyError(batch.Err)).Inc()

		switch {
		case errors.Is(batch.Err, gocql.ErrConnectionClosed) || errors.Is(batch.Err, gocql.ErrHostDown):
			c.goCQLConnections.Get(instance, 0).Dec()
			metrics.GoCQLHostState.WithLabelValues(cluster, instance).Set(0)
		case errors.Is(batch.Err, gocql.ErrNoConnections):
			c.goCQLConnections.Get(instance, 0).Set(0)
			metrics.GoCQLHostState.WithLabelValues(cluster, instance).Set(0)
		default:
		}
	}

	for i, query := range batch.Statements {
		if c.logger != nil && !data.Statement.QueryType.IsSelect() {
			var pk typedef.PartitionKeys
			if len(data.Statement.PartitionKeys) == 1 {
				pk = data.Statement.PartitionKeys[0]
			}

			err := c.logger.LogStmt(stmtlogger.Item{
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
				PartitionKeys: pk,
			})
			if err != nil {
				c.appLogger.Error("failed to log batch statement", zap.Error(err), zap.Any("batch", batch))
			}
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
	cluster := string(c.clusterName)

	var errStr string
	if query.Err != nil {
		metrics.GoCQLQueryErrors.WithLabelValues(cluster, instance, query.Err.Error()).Inc()
		errStr = query.Err.Error()

		// Classified error tracking (low cardinality)
		metrics.GoCQLErrorsByType.WithLabelValues(cluster, classifyError(query.Err)).Inc()

		switch {
		case errors.Is(query.Err, gocql.ErrConnectionClosed) || errors.Is(query.Err, gocql.ErrHostDown):
			c.goCQLConnections.Get(instance, 0).Dec()
			metrics.GoCQLHostState.WithLabelValues(cluster, instance).Set(0)
		case errors.Is(query.Err, gocql.ErrNoConnections):
			c.goCQLConnections.Get(instance, 0).Set(0)
			metrics.GoCQLHostState.WithLabelValues(cluster, instance).Set(0)
		default:
		}
	}

	duration := query.End.Sub(query.Start)

	// Track driver-level retry attempts
	metrics.GoCQLRetryAttempts.WithLabelValues(cluster, data.Statement.QueryType.String()).Observe(float64(query.Attempt))

	// Track latency by attempt number (first attempt vs retries)
	attemptLabel := "0"
	if query.Attempt > 0 {
		attemptLabel = strconv.Itoa(query.Attempt)
		if query.Attempt > 3 {
			attemptLabel = "4+"
		}
	}
	metrics.GoCQLQueryLatencyByAttempt.WithLabelValues(cluster, attemptLabel).Observe(float64(duration) / 1e3)

	// Track rows observed at driver level (per page)
	if data.Statement.QueryType.IsSelect() {
		metrics.GoCQLQueryRowsObserved.WithLabelValues(cluster).Observe(float64(query.Rows))
	}

	if c.logger != nil && !data.Statement.QueryType.IsSelect() {
		attempts := 0
		// Protect against possible nil metrics in tests or certain driver paths
		if query.Metrics != nil {
			attempts = query.Metrics.Attempts
		}

		var pk typedef.PartitionKeys
		if len(data.Statement.PartitionKeys) == 1 {
			pk = data.Statement.PartitionKeys[0]
		}

		err := c.logger.LogStmt(stmtlogger.Item{
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
			PartitionKeys: pk,
		})
		if err != nil {
			c.appLogger.Error("failed to log query statement", zap.Error(err), zap.Any("query", query))
		}
	}

	c.goCQLQueries.Get(instance, data.Statement.QueryType).Inc()
	c.goCQLQueryTime.Get(instance, data.Statement.QueryType).Observe(float64(duration) / 1e3)
}

func (c *ClusterObserver) ObserveConnect(connect gocql.ObservedConnect) {
	instance := connect.Host.ConnectAddressAndPort()
	cluster := string(c.clusterName)

	if connect.Err != nil {
		metrics.GoCQLConnectionsErrors.WithLabelValues(cluster, instance, connect.Err.Error()).Inc()
		metrics.GoCQLErrorsByType.WithLabelValues(cluster, "connect_"+classifyError(connect.Err)).Inc()
		metrics.GoCQLConnections.WithLabelValues(cluster, instance).Dec()
		metrics.GoCQLHostState.WithLabelValues(cluster, instance).Set(0)
		return
	}

	c.goCQLConnections.Get(instance, 0).Inc()
	metrics.GoCQLHostState.WithLabelValues(cluster, instance).Set(1)
	metrics.GoCQLConnectTime.
		WithLabelValues(cluster, instance).
		Observe(float64(connect.End.Sub(connect.Start) / 1e3))
}
