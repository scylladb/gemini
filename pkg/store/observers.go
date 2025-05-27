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

	"github.com/gocql/gocql"

	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/typedef"
)

type ClusterObserver struct {
	ClusterName string
}

func (c ClusterObserver) ObserveBatch(_ context.Context, batch gocql.ObservedBatch) {
	instance := batch.Host.ConnectAddressAndPort()

	for _, query := range batch.Statements {
		stType := typedef.ParseStatementTypeFromQuery(query).String()
		metrics.GoCQLBatchQueries.
			WithLabelValues(c.ClusterName, instance, stType).
			Inc()
	}

	metrics.GoCQLBatches.WithLabelValues(c.ClusterName, instance).Inc()
}

func (c ClusterObserver) ObserveQuery(_ context.Context, query gocql.ObservedQuery) {
	instance := query.Host.ConnectAddressAndPort()

	if query.Err != nil {
		metrics.GoCQLQueryErrors.
			WithLabelValues(c.ClusterName, instance, query.Statement, query.Err.Error()).
			Inc()
		return
	}

	stType := typedef.ParseStatementTypeFromQuery(query.Statement).String()

	metrics.GoCQLQueries.
		WithLabelValues(c.ClusterName, instance, stType).
		Inc()

	metrics.GoCQLQueryTime.
		WithLabelValues(c.ClusterName, instance, stType).
		Observe(float64(query.End.Sub(query.Start) / 1e3))
}

func (c ClusterObserver) ObserveConnect(connect gocql.ObservedConnect) {
	instance := connect.Host.ConnectAddressAndPort()

	if connect.Err != nil {
		metrics.GoCQLConnectionsErrors.WithLabelValues(
			c.ClusterName,
			instance,
			connect.Err.Error(),
		).Inc()
		metrics.GoCQLConnections.WithLabelValues(c.ClusterName, instance).Dec()
		return
	}

	metrics.GoCQLConnections.WithLabelValues(c.ClusterName, instance).
		Inc()

	metrics.GoCQLConnectTime.
		WithLabelValues(c.ClusterName, instance).
		Observe(float64(connect.End.Sub(connect.Start) / 1e3))
}
