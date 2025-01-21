package metrics

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var CQLRequests *prometheus.CounterVec

func init() {
	CQLRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gemini_cql_requests",
			Help: "How many CQL requests processed, partitioned by system and CQL query type aka 'method' (batch, delete, insert, update).",
		},
		[]string{"system", "method"},
	)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe("0.0.0.0:2121", nil); err != nil {
			log.Fatalf("Failed to start metrics server: %v\n", err)
		}
	}()
}
