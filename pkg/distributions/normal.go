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
	"math/rand/v2"
)

type Normal struct {
	Src   rand.Source
	Mu    float64
	Sigma float64
}

func (n Normal) Uint64() uint64 {
	return uint64(n.Rand())
}

func (n Normal) Rand() float64 {
	var rnd float64
	if n.Src == nil {
		rnd = rand.NormFloat64()
	} else {
		rnd = rand.New(n.Src).NormFloat64()
	}
	return rnd*n.Sigma + n.Mu
}
