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

package distributions

import (
	"math"
	"math/rand/v2"
	"testing"
)

const (
	stdDistMean = math.MaxUint64 / 2
	oneStdDev   = 0.341 * math.MaxUint64
)

func TestFullRandom(t *testing.T) {
	t.Parallel()

	rnd := Normal{
		Src:   rand.New(rand.NewPCG(100, 100)),
		Mu:    stdDistMean,
		Sigma: oneStdDev,
	}

	if rnd.Uint64() == rnd.Uint64() { //nolint:staticcheck
		t.Error("Expected different values")
	}
}
