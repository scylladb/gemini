package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var CQLRequests = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gemini_cql_requests",
	Help: "How many CQL requests processed, partitioned by system and CQL query type aka 'method' (batch, delete, insert, update).",
}, []string{"system", "method"},
)
