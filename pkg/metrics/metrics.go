// Copyright 2025 ScyllaDB
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
//
//nolint:revive
package metrics

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"runtime"
	"runtime/metrics"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var registerer = prometheus.NewRegistry()

var (
	ExecutionTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "execution_time",
			Help:    "Time taken to execute a task.",
			Buckets: []float64{0.001, 0.01, 0.1, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 30000},
		},
		[]string{"task"},
	)

	CQLRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cql_requests",
		},
		[]string{"system", "method"},
	)
	CQLErrorRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cql_error_requests",
		},
		[]string{"system", "method"},
	)

	CQLQueryTimeouts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cql_query_timeouts",
		},
		[]string{"cluster", "query_type"},
	)

	GoCQLConnections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cql_connections",
		},
		[]string{"cluster", "host"},
	)

	GoCQLConnectionsErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cql_connections_errors",
		},
		[]string{"cluster", "host", "error"},
	)

	GoCQLConnectTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "cql_connect_time",
			Help:    "Time taken to establish a connection to the CQL server.",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10},
		},
		[]string{"cluster", "host"},
	)

	GoCQLQueries = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cql_queries",
		},
		[]string{"cluster", "host", "query_type"},
	)

	GoCQLQueryTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "cql_query_time",
			Help:    "Time taken to execute a CQL query.",
			Buckets: []float64{0.001, 0.01, 0.1, 0.5, 1, 2, 5, 10, 20, 50, 100},
		},
		[]string{"cluster", "host", "query"},
	)

	GoCQLQueryErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cql_query_errors",
			Help: "Number of CQL query errors.",
		},
		[]string{"cluster", "host", "error"},
	)

	GoCQLBatchQueries = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cql_batched_queries",
		},
		[]string{"cluster", "host", "query_type"},
	)

	GoCQLBatches = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cql_batches",
		},
		[]string{"cluster", "host"},
	)

	ExecutionErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "execution_errors",
		},
		[]string{"ty"},
	)

	GeminiInformation = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "information",
	}, []string{"ty"})

	ValidatedRows = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "validated_rows",
		},
		[]string{"table"},
	)

	CQLRowsReturned = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cql_rows_returned_total",
			Help: "Total number of rows returned by read queries.",
		},
		[]string{"system"},
	)

	CQLRowsPerQuery = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "cql_rows_per_query",
			Help:    "Distribution of row counts returned per read query.",
			Buckets: []float64{0, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000, 50000},
		},
		[]string{"system"},
	)

	StatementLoggerEnqueuedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "statement_logger_enqueued_total",
			Help: "Total number of items enqueued into the statement logger.",
		},
	)

	StatementLoggerDequeuedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "statement_logger_dequeued_total",
			Help: "Total number of items dequeued from the statement logger.",
		},
	)

	StatementLoggerItems = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "statement_logger_items",
			Help: "Total number of statement log items successfully.",
		},
	)

	StatementLoggerFlushes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "statement_logger_flushes_total",
			Help: "Number of flush operations performed by the statement logger per sink.",
		},
		[]string{"sink"},
	)

	StatementErrorLastTS = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "stmt_error_last_timestamp_seconds",
			Help: "Unix timestamp of the last error seen for this label set.",
		},
		[]string{
			"keyspace",
			"table",
			"stmt_type",
			"stmt_storage",
			"error",
			"stmt_logger",
			"partition_hash",
		},
	)

	WorkersCurrent = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "workers_current",
			Help: "Current number of active workers per job.",
		},
		[]string{"job"},
	)

	CQLPreparedStmtsUnique = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cql_prepared_stmts_unique",
			Help: "Number of unique prepared statement query strings seen.",
		},
		[]string{"system"},
	)

	CQLPreparedStmtsRatio = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cql_prepared_stmts_ratio",
			Help: "Ratio of unique prepared statements to the max prepared statements cache size.",
		},
		[]string{"system"},
	)

	CQLPreparedStmtsNew = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cql_prepared_stmts_new_total",
			Help: "Total number of new (never-before-seen) prepared statement query strings.",
		},
		[]string{"system"},
	)

	// --- Partition lifecycle ---

	PartitionsInvalid = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "partitions_invalid",
			Help: "Current number of partitions marked as permanently invalid.",
		},
	)

	PartitionsReplaced = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "partitions_replaced_total",
			Help: "Total number of partition slot replacements (triggered by deletes).",
		},
	)

	PartitionsExtended = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "partitions_extended_total",
			Help: "Total number of new partition slots added.",
		},
	)

	DeletedPartitionsPending = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "deleted_partitions_pending",
			Help: "Number of deleted partitions waiting for validation.",
		},
	)

	DeletedPartitionsEmitted = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "deleted_partitions_emitted_total",
			Help: "Total deleted partitions emitted for validation.",
		},
	)

	// --- Validation comparison results ---

	ValidationRowsMatched = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "validation_rows_matched_total",
			Help: "Total rows that matched between test and oracle.",
		},
	)

	ValidationRowsMissingInTest = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "validation_rows_missing_in_test_total",
			Help: "Total rows present in oracle but missing in test.",
		},
	)

	ValidationRowsMissingInOracle = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "validation_rows_missing_in_oracle_total",
			Help: "Total rows present in test but missing in oracle.",
		},
	)

	ValidationRowsDifferent = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "validation_rows_different_total",
			Help: "Total rows present in both but with value differences.",
		},
	)

	ValidationRowsDeduplicated = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "validation_rows_deduplicated_total",
			Help: "Total duplicate rows removed during comparison (eventual consistency).",
		},
	)

	// --- Retry tracking ---

	MutationRetriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mutation_retries_total",
			Help: "Total mutation retry attempts by outcome.",
		},
		[]string{"outcome"}, // "success", "exhausted"
	)

	ValidationRetriesScheduled = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "validation_retries_scheduled_total",
			Help: "Total validation statements scheduled for retry.",
		},
	)

	ValidationRetriesExhausted = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "validation_retries_exhausted_total",
			Help: "Total validation statements that exhausted all retry attempts.",
		},
	)

	ValidationRetriesPending = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "validation_retries_pending",
			Help: "Current number of validation retries waiting in the queue.",
		},
	)

	// --- Context cancellation / timeout tracking ---

	ContextCancellations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "context_cancellations_total",
			Help: "Total operations cancelled or timed out.",
		},
		[]string{"operation", "reason"}, // operation: "mutation"/"validation"/"load", reason: "cancelled"/"deadline"
	)

	// --- Worker lifecycle ---

	WorkerStopEvents = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "worker_stop_events_total",
			Help: "Total worker stop events by cause.",
		},
		[]string{"job", "reason"}, // reason: "error_budget", "context_done", "stop_flag"
	)

	// --- Statement generation ---

	StatementsGenerated = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "statements_generated_total",
			Help: "Total statements generated by type and subtype.",
		},
		[]string{"type", "subtype"},
	)

	// --- Driver-level diagnostics ---

	GoCQLRetryAttempts = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "cql_driver_retry_attempts",
			Help:    "Distribution of driver-level retry attempt numbers per query. Attempt 0 = no retry.",
			Buckets: []float64{0, 1, 2, 3, 4, 5},
		},
		[]string{"cluster", "query_type"},
	)

	GoCQLQueryRowsObserved = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "cql_driver_query_rows",
			Help:    "Number of rows returned per driver query observation (per page).",
			Buckets: []float64{0, 1, 5, 10, 50, 100, 500, 1000, 5000},
		},
		[]string{"cluster"},
	)

	GoCQLErrorsByType = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cql_driver_errors_by_type_total",
			Help: "Driver-level errors classified by type.",
		},
		[]string{"cluster", "error_type"},
	)

	GoCQLQueryLatencyByAttempt = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "cql_driver_query_latency_by_attempt",
			Help:    "Query latency in microseconds broken down by attempt number.",
			Buckets: []float64{100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000, 5000000, 10000000},
		},
		[]string{"cluster", "attempt"},
	)

	GoCQLHostState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cql_driver_host_up",
			Help: "Whether a host is considered up by the driver (1=up, 0=down).",
		},
		[]string{"cluster", "host"},
	)

	// --- Statement ratio configuration ---

	StatementRatioConfigured = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "statement_ratio_configured",
			Help: "Configured statement generation ratio (set at startup).",
		},
		[]string{"type", "subtype"},
	)

	mutexWaitSeconds = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "go_sync_mutex_wait_total_seconds",
			Help: "Total time goroutines spent waiting on sync.Mutex and RWMutex",
		},
	)
)

func init() {
	r := prometheus.WrapRegistererWithPrefix("gemini_", registerer)

	r.MustRegister(ExecutionTime, mutexWaitSeconds)

	r.MustRegister(
		CQLRequests,
		CQLQueryTimeouts,
		GoCQLConnections,
		GoCQLConnectionsErrors,
		GoCQLConnectTime,
		GoCQLQueries,
		GoCQLQueryTime,
		GoCQLQueryErrors,
		GoCQLBatchQueries,
		GoCQLBatches,
		ExecutionErrors,
		CQLErrorRequests,
		GeminiInformation,
		ValidatedRows,
		StatementLoggerEnqueuedTotal,
		StatementLoggerDequeuedTotal,
		StatementLoggerItems,
		StatementLoggerFlushes,
		StatementErrorLastTS,
		WorkersCurrent,
		CQLPreparedStmtsUnique,
		CQLPreparedStmtsRatio,
		CQLPreparedStmtsNew,
		CQLRowsReturned,
		CQLRowsPerQuery,
		PartitionsInvalid,
		PartitionsReplaced,
		PartitionsExtended,
		DeletedPartitionsPending,
		DeletedPartitionsEmitted,
		ValidationRowsMatched,
		ValidationRowsMissingInTest,
		ValidationRowsMissingInOracle,
		ValidationRowsDifferent,
		ValidationRowsDeduplicated,
		MutationRetriesTotal,
		ValidationRetriesScheduled,
		ValidationRetriesExhausted,
		ValidationRetriesPending,
		ContextCancellations,
		WorkerStopEvents,
		StatementsGenerated,
		StatementRatioConfigured,
		GoCQLRetryAttempts,
		GoCQLQueryRowsObserved,
		GoCQLErrorsByType,
		GoCQLQueryLatencyByAttempt,
		GoCQLHostState,
	)

	r.MustRegister(
		collectors.NewGoCollector(
			collectors.WithGoCollectorRuntimeMetrics(collectors.MetricsAll),
		),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{
			ReportErrors: true,
			PidFn: func() (int, error) {
				return os.Getpid(), nil
			},
		}),
		collectors.NewBuildInfoCollector(),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "go_goroutines_count",
			Help: "Number of goroutines currently active.",
		}, func() float64 {
			return float64(runtime.NumGoroutine())
		}),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "go_gc_total_count",
			Help: "Total number of garbage collections.",
		}, func() float64 {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			return float64(m.NumGC)
		}),
	)
}

func StartMetricsServer(ctx context.Context, bind string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.InstrumentMetricHandler(
		registerer, promhttp.HandlerFor(registerer, promhttp.HandlerOpts{
			EnableOpenMetrics: true,
			Registry:          registerer,
			OfferedCompressions: []promhttp.Compression{
				promhttp.Zstd,
				promhttp.Gzip,
				promhttp.Identity,
			},
		}),
	))

	server := &http.Server{
		BaseContext: func(_ net.Listener) context.Context {
			return ctx
		},
		WriteTimeout: 1 * time.Minute,
		Handler:      mux,
		Addr:         bind,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(errors.Wrapf(err, "failed to start metrics server on %s", bind))
		}
	}()

	go func() {
		<-ctx.Done()
		if err := server.Shutdown(context.Background()); err != nil {
			log.Println(err)
		}
	}()

	go func() {
		samples := []metrics.Sample{
			{Name: "/sync/mutex/wait/total:seconds"},
		}
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		var lastMutexWait float64

		for {
			select {
			case <-ticker.C:
				metrics.Read(samples)
				v := samples[0].Value
				// Value is a float64 for :seconds
				seconds := v.Float64()

				delta := seconds - lastMutexWait
				if delta < 0 {
					delta = 0
				}
				mutexWaitSeconds.Add(delta)
				lastMutexWait = seconds
			case <-ctx.Done():
				return
			}
		}
	}()
}

type RunningTime struct {
	start    time.Time
	observer prometheus.Observer
	task     string
}

func ExecutionTimeStart(task string) RunningTime {
	return RunningTime{
		task:     task,
		observer: ExecutionTime.WithLabelValues(task),
	}
}

func (r *RunningTime) Start() {
	r.start = time.Now()
}

func (r *RunningTime) Record() {
	r.observer.Observe(float64(time.Since(r.start).Nanoseconds()))
}

func (r *RunningTime) RunFuncE(f func() error) error {
	r.Start()
	err := f()
	r.Record()

	return err
}
