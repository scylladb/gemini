// Copyright 2026 ScyllaDB
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
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The defaults of --normal-dist-mean and --normal-dist-sigma.
const (
	defaultMean  = math.MaxUint64 / 2
	defaultSigma = 0.341 * math.MaxUint64
)

// TestEveryDistributionStaysInThePool: the partition pool indexes a slice with
// the result, so a value outside [0, count) crashes the run on the first pick.
func TestEveryDistributionStaysInThePool(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		dist      Distribution
		mu, sigma float64
	}{
		{dist: Uniform},
		{dist: Zipf},
		{dist: Normal, mu: defaultMean, sigma: defaultSigma},
		{dist: LogNormal, mu: defaultMean, sigma: defaultSigma},
		{dist: LogNormal, mu: 0, sigma: 1},
		{dist: LogNormal, mu: 30, sigma: 5},
		{dist: Normal, mu: 0, sigma: 1e30},
	} {
		for _, count := range []uint64{1, 2, 1000, 5_000_000} {
			_, next := New(tc.dist, count, 1, tc.mu, tc.sigma)

			seen := map[uint64]bool{}
			for range 20_000 {
				idx := next()
				require.Less(t, idx, count, "%s mu=%g sigma=%g count=%d", tc.dist, tc.mu, tc.sigma, count)
				seen[idx] = true
			}

			if count >= 1000 {
				assert.Greater(t, len(seen), 10, "%s count=%d must spread over the pool", tc.dist, count)
			}
		}
	}
}

// TestNormalDefaultsCenterOnThePool: the default mean is the middle of the
// uint64 range, so the draws must center on the middle of the pool.
func TestNormalDefaultsCenterOnThePool(t *testing.T) {
	t.Parallel()

	const count = 1_000_000

	_, next := New(Normal, count, 1, defaultMean, defaultSigma)

	var sum float64
	for range 50_000 {
		sum += float64(next())
	}

	assert.InDelta(t, count/2, sum/50_000, count/20)
}

// TestDistributionsAreSafeForConcurrentUse: every worker of every table draws
// from one function and one source. Run with -race.
func TestDistributionsAreSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	for _, dist := range []Distribution{Uniform, Zipf, Normal, LogNormal} {
		src, next := New(dist, 1000, 1, defaultMean, defaultSigma)

		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				for range 2000 {
					_ = next()
					_ = src.Uint64()
				}
			})
		}
		wg.Wait()
	}
}
