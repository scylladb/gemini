package gemini

import (
	"sync"

	"github.com/scylladb/go-set/u64set"
)

//syncU64set is a u64set protected by a sync.RWLock
//It could potentially become contended and then we
//should replace it with a sharded version.
type syncU64set struct {
	pks *u64set.Set
	mu  *sync.RWMutex
}

func (s *syncU64set) delete(v uint64) bool {
	s.mu.Lock()
	_, found := s.pks.Pop2()
	s.mu.Unlock()
	return found
}

func (s *syncU64set) addIfNotPresent(v uint64) bool {
	s.mu.RLock()
	if s.pks.Has(v) {
		s.mu.RUnlock()
		return false
	}
	s.mu.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pks.Has(v) {
		// double check
		return false
	}
	s.pks.Add(v)
	return true
}
