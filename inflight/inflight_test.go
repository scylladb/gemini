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

	if !flight.Delete(10) {
		t.Error("did not delete the value")
	}
	if flight.Delete(10) {
		t.Error("deleted the value twice")
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

	if !flight.Delete(10) {
		t.Error("did not delete the value")
	}
	if flight.Delete(10) {
		t.Error("deleted the value twice")
	}
}

func TestInflight(t *testing.T) {
	t.Parallel()
	flight := newSyncU64set()
	f := func(v uint64) interface{} {
		return flight.AddIfNotPresent(v)
	}
	g := func(v uint64) interface{} {
		return flight.Delete(v)
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
		return flight.Delete(v)
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
