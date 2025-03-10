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
	"math"
	"math/rand/v2"
	"strings"

	"github.com/pkg/errors"
)

type (
	TokenIndex       uint64
	DistributionFunc func() TokenIndex

	generator interface {
		Uint64() uint64
	}
)

func New(distribution string, size, seed uint64, mu, sigma float64) (DistributionFunc, error) {
	var rnd generator

	src := rand.NewPCG(seed, seed)

	switch strings.ToLower(distribution) {
	case "zipf":
		rnd = rand.NewZipf(rand.New(src), 1.1, 1.1, size)
	case "normal":
		rnd = Normal{
			Src:   src,
			Mu:    mu,
			Sigma: sigma,
		}
	case "uniform":
		rnd = Uniform{
			Src: src,
			Min: 1,
			Max: math.MaxUint64,
		}
	default:
		return nil, errors.Errorf("unsupported distribution: %s", distribution)
	}

	return func() TokenIndex {
		return TokenIndex(rnd.Uint64())
	}, nil
}
