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
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
)

func TestAddIfNotPresent(t *testing.T) {
	t.Parallel()
	flight := newSyncU64set(shrinkInflightsLimit)
	if !flight.AddIfNotPresent(10) {
		t.Error("could not add the first value")
	}
	if flight.AddIfNotPresent(10) {
		t.Error("value added twice")
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()
	flight := newSyncU64set(shrinkInflightsLimit)
	flight.AddIfNotPresent(10)

	flight.Delete(10)
	if flight.Has(10) {
		t.Error("did not delete the value")
	}
}

func TestAddIfNotPresentSharded(t *testing.T) {
	t.Parallel()
	flight := newShardedSyncU64set()
	if !flight.AddIfNotPresent(10) {
		t.Error("could not add the first value")
	}
	if flight.AddIfNotPresent(10) {
		t.Error("value added twice")
	}
}

func TestDeleteSharded(t *testing.T) {
	t.Parallel()
	flight := newShardedSyncU64set()
	flight.AddIfNotPresent(10)

	flight.Delete(10)
	if flight.shards[10%256].Has(10) {
		t.Error("did not delete the value")
	}
}

func TestInflight(t *testing.T) {
	t.Parallel()
	flight := newSyncU64set(shrinkInflightsLimit)
	f := func(v uint32) any {
		return flight.AddIfNotPresent(v)
	}
	g := func(v uint32) any {
		flight.Delete(v)
		return !flight.Has(v)
	}

	cfg := createQuickConfig()
	if err := quick.CheckEqual(f, g, cfg); err != nil {
		t.Error(err)
	}
}

func TestInflightSharded(t *testing.T) {
	t.Parallel()
	flight := newShardedSyncU64set()
	f := func(v uint32) any {
		return flight.AddIfNotPresent(v)
	}
	g := func(v uint32) any {
		flight.Delete(v)
		return !flight.shards[v%256].Has(v)
	}

	cfg := createQuickConfig()
	if err := quick.CheckEqual(f, g, cfg); err != nil {
		t.Error(err)
	}
}

func TestNew_ImplementsInFlight(t *testing.T) {
	t.Parallel()
	f := New()
	if f == nil {
		t.Fatal("New() returned nil")
	}
	// Verify basic add/has/delete via the interface
	if !f.AddIfNotPresent(42) {
		t.Error("New(): first AddIfNotPresent(42) should return true")
	}
	if !f.Has(42) {
		t.Error("New(): Has(42) should return true after adding")
	}
	f.Delete(42)
	if f.Has(42) {
		t.Error("New(): Has(42) should return false after Delete")
	}
}

func TestNewConcurrent_ImplementsInFlight(t *testing.T) {
	t.Parallel()
	f := NewConcurrent()
	if f == nil {
		t.Fatal("NewConcurrent() returned nil")
	}
	if !f.AddIfNotPresent(99) {
		t.Error("NewConcurrent(): first AddIfNotPresent(99) should return true")
	}
	if !f.Has(99) {
		t.Error("NewConcurrent(): Has(99) should return true after adding")
	}
	f.Delete(99)
	if f.Has(99) {
		t.Error("NewConcurrent(): Has(99) should return false after Delete")
	}
}

func TestSyncU64set_Has(t *testing.T) {
	t.Parallel()
	s := newSyncU64set(shrinkInflightsLimit)

	// Has on absent key
	if s.Has(7) {
		t.Error("Has(7) should be false on empty set")
	}
	// Add then Has
	s.AddIfNotPresent(7)
	if !s.Has(7) {
		t.Error("Has(7) should be true after AddIfNotPresent")
	}
}

func TestShardedSyncU64set_Has(t *testing.T) {
	t.Parallel()
	s := newShardedSyncU64set()

	if s.Has(123) {
		t.Error("Has(123) should be false on empty sharded set")
	}
	s.AddIfNotPresent(123)
	if !s.Has(123) {
		t.Error("Has(123) should be true after AddIfNotPresent")
	}
	s.Delete(123)
	if s.Has(123) {
		t.Error("Has(123) should be false after Delete")
	}
}

func createQuickConfig() *quick.Config {
	return &quick.Config{
		MaxCount: 200000,
		Values: func(vs []reflect.Value, r *rand.Rand) {
			for i := 0; i < len(vs); i++ {
				uv := r.Uint32()
				v := reflect.New(reflect.TypeOf(uv)).Elem()
				v.SetUint(uint64(uv))
				vs[i] = v
			}
		},
	}
}
