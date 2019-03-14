package gemini

import (
	"testing"
	"testing/quick"
	"time"
)

func TestNonEmptyRandRange(t *testing.T) {
	f := func(x, y int) bool {
		r := nonEmptyRandRange(x, y, 10)
		return r > 0
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestNonEmptyRandRange64(t *testing.T) {
	f := func(x, y int) bool {
		r := nonEmptyRandRange(x, y, 10)
		return r > 0
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestNonEmptyRandString(t *testing.T) {
	// TODO: Figure out why this is so horribly slow...
	tt := time.Now()
	f := func(len int32) bool {
		r := nonEmptyRandStringWithTime(int(len), tt)
		return r != ""
	}
	cfg := &quick.Config{MaxCount: 10}
	if err := quick.Check(f, cfg); err != nil {
		t.Error(err)
	}
}

var bench_r string

func BenchmarkNonEmptyRandStringWithTime(b *testing.B) {
	tt := time.Now()
	for i := 0; i < b.N; i++ {
		bench_r = nonEmptyRandStringWithTime(30, tt)
	}
}

func BenchmarkNonEmptyRandStringWithTimeParallel(b *testing.B) {
	tt := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bench_r = nonEmptyRandStringWithTime(30, tt)
		}
	})
}

var bench_rr int

func BenchmarkNonEmptyRandRange(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bench_rr = nonEmptyRandRange(0, 50, 30)
	}
}

var bench_rr64 int64

func BenchmarkNonEmptyRandRange64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bench_rr64 = nonEmptyRandRange64(0, 50, 30)
	}
}
