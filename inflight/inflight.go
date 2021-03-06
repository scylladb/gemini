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

	"github.com/scylladb/go-set/u64set"
)

type InFlight interface {
	AddIfNotPresent(uint64) bool
	Delete(uint64)
}

// New creates a instance of a simple InFlight set.
// It's internal data is protected by a simple sync.RWMutex.
func New() InFlight {
	return newSyncU64set()
}

func newSyncU64set() *syncU64set {
	return &syncU64set{
		pks: u64set.New(),
		mu:  &sync.RWMutex{},
	}
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
		s.shards[i] = newSyncU64set()
	}
	return s
}

// shardedSyncU64set is a sharded InFlight implementation protected by a sync.RWLock
// which should support greater concurrency.
type shardedSyncU64set struct {
	shards [256]*syncU64set
}

func (s *shardedSyncU64set) Delete(v uint64) {
	ss := s.shards[v%256]
	ss.Delete(v)
}

func (s *shardedSyncU64set) AddIfNotPresent(v uint64) bool {
	ss := s.shards[v%256]
	return ss.AddIfNotPresent(v)
}

// syncU64set is an InFlight implementation protected by a sync.RWLock
type syncU64set struct {
	pks *u64set.Set
	mu  *sync.RWMutex
}

func (s *syncU64set) Delete(v uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pks.Remove(v)
}

func (s *syncU64set) AddIfNotPresent(v uint64) bool {
	s.mu.RLock()
	if s.pks.Has(v) {
		s.mu.RUnlock()
		return false
	}
	s.mu.RUnlock()
	return s.addIfNotPresent(v)
}

func (s *syncU64set) addIfNotPresent(v uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pks.Has(v) {
		// double check
		return false
	}
	s.pks.Add(v)
	return true
}
