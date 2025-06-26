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
	"github.com/samber/mo"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

type ClusterObserver struct {
	Logger      *stmtlogger.Logger
	AppLogger   *zap.Logger
	ClusterName stmtlogger.Type
}

func (c ClusterObserver) ObserveBatch(ctx context.Context, batch gocql.ObservedBatch) {
	instance := batch.Host.ConnectAddressAndPort()

	for i, query := range batch.Statements {
		stType := typedef.ParseStatementTypeFromQuery(query)
		op := stType.OpType()

		if c.Logger != nil && op != typedef.OpSelect {
			geminiAttempt, ok := ctx.Value(utils.GeminiAttempt).(int)
			if !ok {
				geminiAttempt = 1
			}

			queryID, ok := ctx.Value(utils.QueryID).(gocql.UUID)
			if !ok {
				queryID = gocql.UUIDFromTime(batch.Start)
			}

			err := c.Logger.LogStmt(stmtlogger.Item{
				ID:            queryID,
				Error:         mo.Left[error, string](batch.Err),
				Statement:     query,
				Values:        mo.Left[typedef.Values, string](batch.Values[i]),
				Start:         stmtlogger.Time{Time: batch.Start},
				Duration:      stmtlogger.Duration{Duration: batch.End.Sub(batch.Start)},
				Host:          instance,
				Attempt:       batch.Attempt,
				GeminiAttempt: geminiAttempt,
				Type:          c.ClusterName,
				StatementType: op,
			})
			if err != nil {
				c.AppLogger.Error("failed to log batch statement", zap.Error(err), zap.Any("batch", batch))
			}
		}

		metrics.GoCQLBatchQueries.
			WithLabelValues(string(c.ClusterName), instance, stType.String()).
			Inc()
	}

	metrics.GoCQLBatches.WithLabelValues(string(c.ClusterName), instance).Inc()
}

func (c ClusterObserver) ObserveQuery(ctx context.Context, query gocql.ObservedQuery) {
	instance := query.Host.ConnectAddressAndPort()

	if query.Err != nil {
		metrics.GoCQLQueryErrors.
			WithLabelValues(string(c.ClusterName), instance, query.Statement, query.Err.Error()).
			Inc()
	}

	stType := typedef.ParseStatementTypeFromQuery(query.Statement)
	op := stType.OpType()

	if c.Logger != nil && op != typedef.OpSelect {
		geminiAttempt, ok := ctx.Value(utils.GeminiAttempt).(int)
		if !ok {
			geminiAttempt = 1
		}

		queryID, ok := ctx.Value(utils.QueryID).(gocql.UUID)
		if !ok {
			queryID = gocql.UUIDFromTime(query.Start)
		}

		err := c.Logger.LogStmt(stmtlogger.Item{
			ID:            queryID,
			Error:         mo.Left[error, string](query.Err),
			Statement:     query.Statement,
			Values:        mo.Left[typedef.Values, string](query.Values),
			Start:         stmtlogger.Time{Time: query.Start},
			Duration:      stmtlogger.Duration{Duration: query.End.Sub(query.Start)},
			Host:          instance,
			Attempt:       query.Attempt,
			GeminiAttempt: geminiAttempt,
			Type:          c.ClusterName,
			StatementType: op,
		})
		if err != nil {
			c.AppLogger.Error("failed to log batch statement", zap.Error(err), zap.Any("query", query))
		}
	}

	metrics.GoCQLQueries.
		WithLabelValues(string(c.ClusterName), instance, stType.String()).
		Inc()

	metrics.GoCQLQueryTime.
		WithLabelValues(string(c.ClusterName), instance, stType.String()).
		Observe(float64(query.End.Sub(query.Start) / 1e3))
}

func (c ClusterObserver) ObserveConnect(connect gocql.ObservedConnect) {
	instance := connect.Host.ConnectAddressAndPort()

	if connect.Err != nil {
		metrics.GoCQLConnectionsErrors.WithLabelValues(
			string(c.ClusterName),
			instance,
			connect.Err.Error(),
		).Inc()
		metrics.GoCQLConnections.WithLabelValues(string(c.ClusterName), instance).Dec()
		return
	}

	metrics.GoCQLConnections.WithLabelValues(string(c.ClusterName), instance).
		Inc()

	metrics.GoCQLConnectTime.
		WithLabelValues(string(c.ClusterName), instance).
		Observe(float64(connect.End.Sub(connect.Start) / 1e3))
}
