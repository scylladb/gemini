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
	"fmt"
	"math/rand/v2"
	"strconv"

	"gonum.org/v1/gonum/stat/distuv"
)

type (
	Distribution string

	DistributionFunc func() uint32
)

const (
	DistributionZipf      Distribution = "zipf"
	DistributionLogNormal Distribution = "lognormal"
	DistributionNormal    Distribution = "normal"
	DistributionUniform   Distribution = "uniform"
)

func New(distribution Distribution, partitionCount int, seed uint64, mu, sigma float64) (*rand.ChaCha8, DistributionFunc) {
	hash := sha256.Sum256(
		[]byte(
			string(distribution) + strconv.FormatInt(
				int64(partitionCount),
				10,
			) + strconv.FormatUint(
				seed,
				10,
			) + strconv.FormatFloat(
				mu,
				'f',
				-1,
				64,
			) + strconv.FormatFloat(
				sigma,
				'f',
				-1,
				64,
			),
		),
	)

	src := rand.NewChaCha8(hash)

	switch distribution {
	case DistributionZipf:
		zipf := rand.NewZipf(rand.New(src), 1.001, float64(partitionCount), uint64(partitionCount))

		return src, func() uint32 {
			return uint32(zipf.Uint64())
		}
	case DistributionLogNormal:
		d := distuv.LogNormal{
			Src:   src,
			Mu:    mu,
			Sigma: sigma,
		}

		return src, func() uint32 {
			return uint32(d.Rand())
		}
	case DistributionNormal:
		d := distuv.Normal{
			Src:   src,
			Mu:    mu,
			Sigma: sigma,
		}

		return src, func() uint32 {
			return uint32(d.Rand())
		}
	case DistributionUniform:
		d := distuv.Uniform{
			Min: 0,
			Max: float64(partitionCount),
			Src: src,
		}

		return src, func() uint32 {
			return uint32(d.Rand())
		}
	default:
		panic(fmt.Sprintf("unsupported distribution: %s", string(distribution)))
	}
}
