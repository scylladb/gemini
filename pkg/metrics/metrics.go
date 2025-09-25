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

package metrics

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"runtime"
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

	GeneratorEmittedValues = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "generated_emitted_values",
		},
		[]string{"table"},
	)

	GeneratorDroppedValues = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "generated_dropped_values",
		},
		[]string{"table", "type"},
	)

	MemoryMetrics = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "memory_footprint",
		},
		[]string{"type", "context"},
	)

	FileSizeMetrics = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "file_size_bytes",
		},
		[]string{"file"},
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
)

func init() {
	r := prometheus.WrapRegistererWithPrefix("gemini_", registerer)

	r.MustRegister(channelMetrics, ExecutionTime)

	r.MustRegister(
		CQLRequests,
		CQLQueryTimeouts,
		GoCQLConnections,
		GoCQLConnectionsErrors,
		GoCQLConnectTime,
		GoCQLQueries,
		GoCQLQueryTime,
		GoCQLQueryErrors,
		GeneratorEmittedValues,
		GeneratorDroppedValues,
		MemoryMetrics,
		FileSizeMetrics,
		ExecutionErrors,
		CQLErrorRequests,
		GeminiInformation,
		ValidatedRows,
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
