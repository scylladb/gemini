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

package generators

import (
	"math"
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/exp/rand"
	"gonum.org/v1/gonum/stat/distuv"
)

const (
	StdDistMean = math.MaxUint64 / 2
	OneStdDev   = 0.341 * math.MaxUint64
)

func ParseDistributionDefault(distribution string, size, seed uint64) (DistributionFunc, error) {
	return ParseDistribution(distribution, size, seed, StdDistMean, OneStdDev)
}

func ParseDistribution(distribution string, size, seed uint64, mu, sigma float64) (DistributionFunc, error) {
	switch strings.ToLower(distribution) {
	case "zipf":
		dist := rand.NewZipf(rand.New(rand.NewSource(seed)), 1.1, 1.1, size)
		return func() TokenIndex {
			return TokenIndex(dist.Uint64())
		}, nil
	case "normal":
		dist := distuv.Normal{
			Src:   rand.NewSource(seed),
			Mu:    mu,
			Sigma: sigma,
		}
		return func() TokenIndex {
			return TokenIndex(dist.Rand())
		}, nil
	case "uniform":
		rnd := rand.New(rand.NewSource(seed))
		return func() TokenIndex {
			return TokenIndex(rnd.Uint64n(size))
		}, nil
	default:
		return nil, errors.Errorf("unsupported distribution: %s", distribution)
	}
}
