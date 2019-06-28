package gemini

import (
	"testing"
	"time"

	"golang.org/x/exp/rand"
)

var rnd = rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

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
