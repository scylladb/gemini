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
	flight := newSyncU64set()
	if !flight.AddIfNotPresent(10) {
		t.Error("could not add the first value")
	}
	if flight.AddIfNotPresent(10) {
		t.Error("value added twice")
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()
	flight := newSyncU64set()
	flight.AddIfNotPresent(10)

	flight.Delete(10)
	if flight.pks.Has(10) {
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
	if flight.shards[10%256].pks.Has(10) {
		t.Error("did not delete the value")
	}
}

func TestInflight(t *testing.T) {
	t.Parallel()
	flight := newSyncU64set()
	f := func(v uint64) interface{} {
		return flight.AddIfNotPresent(v)
	}
	g := func(v uint64) interface{} {
		flight.Delete(v)
		return !flight.pks.Has(v)
	}

	cfg := createQuickConfig()
	if err := quick.CheckEqual(f, g, cfg); err != nil {
		t.Error(err)
	}
}

func TestInflightSharded(t *testing.T) {
	t.Parallel()
	flight := newShardedSyncU64set()
	f := func(v uint64) interface{} {
		return flight.AddIfNotPresent(v)
	}
	g := func(v uint64) interface{} {
		flight.Delete(v)
		return !flight.shards[v%256].pks.Has(v)
	}

	cfg := createQuickConfig()
	if err := quick.CheckEqual(f, g, cfg); err != nil {
		t.Error(err)
	}
}

func createQuickConfig() *quick.Config {
	return &quick.Config{
		MaxCount: 2000000,
		Values: func(vs []reflect.Value, r *rand.Rand) {
			for i := 0; i < len(vs); i++ {
				uv := r.Uint64()
				v := reflect.New(reflect.TypeOf(uv)).Elem()
				v.SetUint(uv)
				vs[i] = v
			}
		},
	}
}
