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
	"math/rand/v2"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"gonum.org/v1/gonum/stat/distuv"
)

type (
	TokenIndex       uint64
	DistributionFunc func() TokenIndex

	generator interface {
		Uint64() uint64
	}
)

func New(distribution string, partitionCount, seed uint64, mu, sigma float64) (*rand.ChaCha8, DistributionFunc, error) {
	var rnd generator

	hash := sha256.Sum256(
		[]byte(
			distribution + strconv.FormatUint(partitionCount, 10) + strconv.FormatUint(seed, 10) + strconv.FormatFloat(mu, 'f', -1, 64) + strconv.FormatFloat(sigma, 'f', -1, 64),
		),
	)

	src := rand.NewChaCha8(hash)

	switch strings.ToLower(distribution) {
	case "zipf":
		rnd = rand.NewZipf(rand.New(src), 1.001, float64(partitionCount), partitionCount)
	case "lognormal":
		d := distuv.LogNormal{
			Src:   src,
			Mu:    mu,
			Sigma: sigma,
		}

		return src, func() TokenIndex {
			return TokenIndex(d.Rand())
		}, nil
	case "normal":
		d := distuv.Normal{
			Src:   src,
			Mu:    mu,
			Sigma: sigma,
		}

		return src, func() TokenIndex {
			return TokenIndex(d.Rand())
		}, nil
	case "uniform":
		d := distuv.Uniform{
			Min: 0,
			Max: float64(partitionCount),
			Src: src,
		}

		return src, func() TokenIndex {
			return TokenIndex(d.Rand())
		}, nil
	default:
		return nil, nil, errors.Errorf("unsupported distribution: %s", distribution)
	}

	return src, func() TokenIndex {
		return TokenIndex(rnd.Uint64())
	}, nil
}
