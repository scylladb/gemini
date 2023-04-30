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
)

// We track inflights in the map, maps in golang are not shrinking
// Therefore we track how many inflights were deleted and when it reaches the limit
// we forcefully recreate the map to shrink it
const shrinkInflightsLimit = 1000000

type InFlight interface {
	AddIfNotPresent(uint64) bool
	Delete(uint64)
}

// New creates a instance of a simple InFlight set.
// It's internal data is protected by a simple sync.RWMutex.
func New() InFlight {
	return newSyncU64set(shrinkInflightsLimit)
}

func newSyncU64set(limit uint64) *syncU64set {
	return &syncU64set{
		values:  make(map[uint64]struct{}),
		limit:   limit,
		deleted: 0,
		lock:    sync.RWMutex{},
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
		s.shards[i] = newSyncU64set(shrinkInflightsLimit)
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

type syncU64set struct {
	values  map[uint64]struct{}
	deleted uint64
	limit   uint64
	lock    sync.RWMutex
}

func (s *syncU64set) AddIfNotPresent(u uint64) bool {
	s.lock.RLock()
	_, ok := s.values[u]
	if ok {
		s.lock.RUnlock()
		return false
	}
	s.lock.RUnlock()
	s.lock.Lock()
	defer s.lock.Unlock()
	_, ok = s.values[u]
	if ok {
		return false
	}
	s.values[u] = struct{}{}
	return true
}

func (s *syncU64set) Has(u uint64) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	_, ok := s.values[u]
	return ok
}

func (s *syncU64set) Delete(u uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	_, ok := s.values[u]
	if !ok {
		return
	}
	delete(s.values, u)
	s.addDeleted(1)
}

func (s *syncU64set) addDeleted(n uint64) {
	s.deleted += n
	if s.limit != 0 && s.deleted > s.limit {
		go s.shrink()
	}
}

func (s *syncU64set) shrink() {
	s.lock.Lock()
	defer s.lock.Unlock()
	mapLen := uint64(0)
	if uint64(len(s.values)) >= s.deleted {
		mapLen = uint64(len(s.values)) - s.deleted
	}
	newValues := make(map[uint64]struct{}, mapLen)

	for key, val := range s.values {
		newValues[key] = val
	}
	s.values = newValues
	s.deleted = 0
}
