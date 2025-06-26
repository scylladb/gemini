// Copyright 2025 ScyllaDB
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

package distributions

import (
	"math"
	"testing"
	"time"
)

const (
	stdDistMean = math.MaxUint64 / 2
	oneStdDev   = 0.341 * math.MaxUint64
)

func TestNew(t *testing.T) {
	t.Parallel()

	data := []struct {
		dist          string
		size          uint64
		seed          uint64
		mu, sigma     float64
		maxSameValues int
	}{
		{dist: "zipf", seed: uint64(time.Now().UnixNano()), size: 10000, maxSameValues: 10},
		{
			dist:          "normal",
			seed:          uint64(time.Now().UnixNano()),
			size:          10000,
			mu:            stdDistMean,
			sigma:         oneStdDev,
			maxSameValues: 100,
		},
		{
			dist:          "uniform",
			seed:          uint64(time.Now().UnixNano()),
			size:          10000,
			mu:            stdDistMean,
			sigma:         oneStdDev,
			maxSameValues: 100,
		},
	}

	for _, item := range data {
		t.Run("test-"+item.dist, func(t *testing.T) {
			t.Parallel()

			_, distFunc, err := New(item.dist, item.size, item.seed, stdDistMean, oneStdDev)
			if err != nil {
				t.Errorf("failed to create distribution function: %s", item.dist)
			}

			same := 0

			for range item.size {
				m := distFunc()
				n := distFunc()

				if m == n {
					same++
				}
			}

			if same > item.maxSameValues {
				t.Errorf(
					"too many calls to the distribution function returned the same value: %d",
					same,
				)
			}
		})
	}
}
