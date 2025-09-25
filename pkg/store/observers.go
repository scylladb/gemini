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
	"slices"
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
	logger           *stmtlogger.Logger
	appLogger        *zap.Logger
	goCQLQueryErrors *observer[prometheus.Counter]
	goCQLQueries     *observer[prometheus.Counter]
	goCQLQueryTime   *observer[prometheus.Observer]
	goCQLConnections *observer[prometheus.Gauge]
	clusterName      stmtlogger.Type
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

func (c *ClusterObserver) ObserveQuery(ctx context.Context, query gocql.ObservedQuery) {
	instance := query.Host.ConnectAddressAndPort()
	data := MustGetContextData(ctx)
	if query.Err != nil {
		metrics.GoCQLQueryErrors.WithLabelValues(string(c.clusterName), instance, query.Err.Error()).Inc()

		switch {
		case errors.Is(query.Err, gocql.ErrConnectionClosed) || errors.Is(query.Err, gocql.ErrHostDown):
			c.goCQLConnections.Get(instance, data.Statement.QueryType).Dec()
		case errors.Is(query.Err, gocql.ErrNoConnections):
			c.goCQLConnections.Get(instance, 0).Set(0)
		default:
		}
	}

	duration := query.End.Sub(query.Start)

	if c.logger != nil && !data.Statement.QueryType.IsSelect() {
		err := c.logger.LogStmt(stmtlogger.Item{
			Error:           mo.Left[error, string](query.Err),
			Statement:       query.Statement,
			GeneratedValues: mo.Left[[]any, []byte](data.Statement.Values),
			DriverValues:    slices.Clone(query.Values),
			Start:           stmtlogger.Time{Time: query.Start},
			Duration:        stmtlogger.Duration{Duration: duration},
			Host:            instance,
			Attempt:         query.Metrics.Attempts,
			GeminiAttempt:   data.GeminiAttempt,
			Type:            c.clusterName,
			StatementType:   data.Statement.QueryType,
			PartitionKeys:   data.Statement.PartitionKeys.Values,
		})
		if err != nil {
			c.appLogger.Error("failed to log batch statement", zap.Error(err), zap.Any("query", query))
		}
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
