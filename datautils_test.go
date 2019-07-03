package gemini

import (
	"time"

	"golang.org/x/exp/rand"
)

var rnd = rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

var bench_r string
