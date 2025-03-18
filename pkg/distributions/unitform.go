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

type Uniform struct {
	Src rand.Source
	Min float64
	Max float64
}

func (u Uniform) Rand() float64 {
	var rnd float64
	if u.Src == nil {
		rnd = rand.Float64()
	} else {
		rnd = rand.New(u.Src).Float64()
	}

	return rnd*(u.Max-u.Min) + u.Min
}

func (u Uniform) Uint64() uint64 {
	return uint64(u.Rand())
}
