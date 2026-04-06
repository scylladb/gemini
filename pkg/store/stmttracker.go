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

package store

import (
	"hash/maphash"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/scylladb/gemini/pkg/metrics"
)

const stmtTrackerShards = 64

// stmtTracker tracks unique prepared statement strings using sharded maps
// for minimal contention under high concurrency. It only stores hashes,
// not full query strings, to keep memory usage constant.
type stmtTracker struct {
	uniqueGauge  prometheus.Gauge
	ratioGauge   prometheus.Gauge
	newStmtCount prometheus.Counter
	shards       [stmtTrackerShards]stmtShard
	seed         maphash.Seed
	uniqueCount  atomic.Int64
	maxPrepared  int
}

type stmtShard struct {
	seen map[uint64]struct{}
	mu   sync.Mutex
}

func newStmtTracker(system string, maxPrepared int) *stmtTracker {
	t := &stmtTracker{
		seed:         maphash.MakeSeed(),
		maxPrepared:  maxPrepared,
		uniqueGauge:  metrics.CQLPreparedStmtsUnique.WithLabelValues(system),
		ratioGauge:   metrics.CQLPreparedStmtsRatio.WithLabelValues(system),
		newStmtCount: metrics.CQLPreparedStmtsNew.WithLabelValues(system),
	}

	for i := range t.shards {
		t.shards[i].seen = make(map[uint64]struct{}, 64)
	}

	return t
}

// Track records a query string. Returns true if this is the first time
// this query has been seen. Uses maphash for fast, allocation-free hashing.
func (t *stmtTracker) Track(query string) bool {
	h := maphash.String(t.seed, query)
	shard := &t.shards[h%stmtTrackerShards]

	shard.mu.Lock()
	_, exists := shard.seen[h]
	if !exists {
		shard.seen[h] = struct{}{}
	}
	shard.mu.Unlock()

	if !exists {
		count := t.uniqueCount.Add(1)
		t.uniqueGauge.Set(float64(count))
		t.ratioGauge.Set(float64(count) / float64(t.maxPrepared))
		t.newStmtCount.Inc()
	}

	return !exists
}
