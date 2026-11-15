// Copyright 2019 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package distributions

import (
	"crypto/sha256"
	"math"
	"math/rand/v2"
	"strconv"
	"sync"

	"gonum.org/v1/gonum/stat/distuv"
)

type (
	Distribution string

	// DistributionFunc returns the index of the next partition, in
	// [0, partition count). Every worker of every table calls it, so it is
	// safe for concurrent use.
	DistributionFunc func() uint64
)

const (
	Zipf      Distribution = "zipf"
	LogNormal Distribution = "lognormal"
	Normal    Distribution = "normal"
	Uniform   Distribution = "uniform"
)

// maxRedraws bounds how often a distribution draws again when a draw falls
// outside the partition pool, before it folds the draw into the pool.
const maxRedraws = 64

// maxLogSpaceMean is the largest lognormal mean that can give a finite index:
// exp of anything larger overflows a uint64.
var maxLogSpaceMean = math.Log(math.MaxUint64)

// LockedSource is a rand.Source that is safe for concurrent use. The
// distribution function, the ratio controller and the job setup all draw from
// the one source of a run, from many goroutines.
type LockedSource struct {
	src *rand.ChaCha8
	mu  sync.Mutex
}

func newLockedSource(seed [32]byte) *LockedSource {
	return &LockedSource{src: rand.NewChaCha8(seed)}
}

// Uint64 returns the next value of the source.
func (l *LockedSource) Uint64() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.src.Uint64()
}

// New returns the random source of a run and the function that picks the
// partition index of each statement. Both are safe for concurrent use.
//
// Normal draws a position on the uint64 range, with mu and sigma on that
// range: the defaults center it on the middle of the pool. LogNormal draws an
// index, with mu and sigma in log space. A log-space mean above ln(2^64) can
// give no finite index, so it is taken as unset: the distribution then uses
// mu 0 and sigma ln(partition count)/3, a hot head of the pool that still
// reaches its end.
func New(distribution Distribution, partitionCount, seed uint64, mu, sigma float64) (*LockedSource, DistributionFunc) {
	if partitionCount == 0 {
		panic("distributions: partition count must be positive")
	}

	toHash := string(distribution) + strconv.FormatInt(int64(partitionCount), 10) + strconv.FormatUint(seed, 10)
	withParams := toHash + strconv.FormatFloat(mu, 'f', -1, 64) + strconv.FormatFloat(sigma, 'f', -1, 64)
	count := float64(partitionCount)

	switch distribution {
	case Zipf:
		src := newLockedSource(sha256.Sum256([]byte(toHash)))
		zipf := rand.NewZipf(rand.New(src), 1.001, count, partitionCount-1)

		var zipfMu sync.Mutex

		return src, func() uint64 {
			// rand.Zipf keeps state between draws.
			zipfMu.Lock()
			defer zipfMu.Unlock()

			return zipf.Uint64()
		}
	case LogNormal:
		src := newLockedSource(sha256.Sum256([]byte(withParams)))

		if mu > maxLogSpaceMean || !(sigma > 0) || math.IsInf(sigma, 0) || sigma > maxLogSpaceMean {
			mu, sigma = 0, math.Max(math.Log(count)/3, 1)
		}

		d := distuv.LogNormal{Src: src, Mu: mu, Sigma: sigma}

		return src, func() uint64 {
			return drawIndex(d.Rand, func(v float64) float64 { return v }, partitionCount)
		}
	case Normal:
		src := newLockedSource(sha256.Sum256([]byte(withParams)))
		d := distuv.Normal{Src: src, Mu: mu, Sigma: sigma}

		// The position on the uint64 range, scaled into the pool.
		scale := count / math.Exp2(64)

		return src, func() uint64 {
			return drawIndex(d.Rand, func(v float64) float64 { return v * scale }, partitionCount)
		}
	case Uniform:
		src := newLockedSource(sha256.Sum256([]byte(toHash)))
		d := distuv.Uniform{Min: 0, Max: count, Src: src}

		return src, func() uint64 {
			return drawIndex(d.Rand, func(v float64) float64 { return v }, partitionCount)
		}
	default:
		panic("unsupported distribution: " + string(distribution))
	}
}

// drawIndex draws until toIndex maps a draw into [0, count), up to
// maxRedraws times, then folds the last draw into the pool. The result is
// always a valid index.
func drawIndex(draw func() float64, toIndex func(float64) float64, count uint64) uint64 {
	var idx float64

	for range maxRedraws {
		idx = toIndex(draw())
		if idx >= 0 && idx < float64(count) {
			return min(uint64(idx), count-1)
		}
	}

	if math.IsNaN(idx) || math.IsInf(idx, 0) {
		return 0
	}

	return min(uint64(math.Mod(math.Abs(idx), float64(count))), count-1)
}
