package gemini

import (
	"math/rand"
	"testing"
	"testing/quick"
	"time"
)

var rnd = rand.New(rand.NewSource(time.Now().UnixNano()))

func TestNonEmptyRandFloat32Range(t *testing.T) {
	f := func(x, y float32) bool {
		r := nonEmptyRandFloat32Range(rnd, x, y, 10)
		return r > 0
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestNonEmptyRandFloat64Range(t *testing.T) {
	f := func(x, y float64) bool {
		r := nonEmptyRandFloat64Range(rnd, x, y, 10)
		return r > 0
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

var bench_r string

func BenchmarkNonEmptyRandStringWithTime(b *testing.B) {
	tt := time.Now()
	for i := 0; i < b.N; i++ {
		bench_r = nonEmptyRandStringWithTime(rnd, 30, tt)
	}
}

func BenchmarkNonEmptyRandStringWithTimeParallel(b *testing.B) {
	tt := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bench_r = nonEmptyRandStringWithTime(rnd, 30, tt)
		}
	})
}
