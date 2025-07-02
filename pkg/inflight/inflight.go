// Copyright 2019 ScyllaDB
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

package inflight

import (
	"sync"
	"time"

	"go.uber.org/atomic"
	"golang.org/x/exp/maps"
)

// We track inflights in the map, maps in golang are not shrinking
// Therefore we track how many inflights were deleted and when it reaches the limit
// we forcefully recreate the map to shrink it
const shrinkInflightsLimit = 1000

type InFlight interface {
	AddIfNotPresent(uint32) bool
	Delete(uint32)
	Has(uint32) bool
}

// New creates a instance of a simple InFlight set.
// It's internal data is protected by a simple sync.RWMutex.
func New() InFlight {
	return newSyncU64set(shrinkInflightsLimit)
}

func newSyncU64set(limit int64) *syncU64set {
	s := &syncU64set{
		values: make(map[uint32]struct{}),
		lock:   sync.Mutex{},
	}

	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			if s.deleted.Load() >= limit {
				s.lock.Lock()
				s.values = maps.Clone(s.values)
				s.lock.Unlock()
			}
		}
	}()

	return s
}

// NewConcurrent creates a instance of a sharded InFlight set.
// It shards the values over 256 buckets which should afford a
// decent increase in concurrency support.
func NewConcurrent() InFlight {
	return newShardedSyncU64set()
}

func newShardedSyncU64set() *shardedSyncU64set {
	s := &shardedSyncU64set{}
	for i := range s.shards {
		s.shards[i] = newSyncU64set(shrinkInflightsLimit)
	}
	return s
}

// shardedSyncU64set is a sharded InFlight implementation protected by a sync.RWLock
// which should support greater concurrency.
type shardedSyncU64set struct {
	shards [256]*syncU64set
}

func (s *shardedSyncU64set) Delete(v uint32) {
	ss := s.shards[v%256]
	ss.Delete(v)
}

func (s *shardedSyncU64set) AddIfNotPresent(v uint32) bool {
	ss := s.shards[v%256]
	return ss.AddIfNotPresent(v)
}

func (s *shardedSyncU64set) Has(v uint32) bool {
	ss := s.shards[v%256]
	return ss.Has(v)
}

type syncU64set struct {
	values  map[uint32]struct{}
	deleted atomic.Int64
	lock    sync.Mutex
}

func (s *syncU64set) AddIfNotPresent(u uint32) bool {
	s.lock.Lock()
	_, ok := s.values[u]
	if ok {
		s.lock.Unlock()
		return false
	}
	s.values[u] = struct{}{}
	s.lock.Unlock()
	return true
}

func (s *syncU64set) Has(u uint32) bool {
	s.lock.Lock()
	_, ok := s.values[u]
	s.lock.Unlock()
	return ok
}

func (s *syncU64set) Delete(u uint32) {
	s.lock.Lock()
	_, ok := s.values[u]
	if !ok {
		s.lock.Unlock()
		return
	}
	delete(s.values, u)
	s.lock.Unlock()

	s.deleted.Dec()
}
