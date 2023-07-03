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

package utils_test

import (
	"testing"
	"time"

	"golang.org/x/exp/rand"

	"github.com/scylladb/gemini/pkg/utils"
)

var rnd = rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

func BenchmarkUtilsRandString100(t *testing.B) {
	for x := 0; x < t.N; x++ {
		utils.RandString(rnd, 100)
	}
}

func BenchmarkUtilsRandString1000(t *testing.B) {
	for x := 0; x < t.N; x++ {
		utils.RandString(rnd, 1000)
	}
}

func TestRandString(t *testing.T) {
	t.Parallel()

	for _, ln := range []int{1, 3, 5, 16, 45, 100, 1000} {
		out := utils.RandString(rnd, ln)
		if len(out) != ln {
			t.Fatalf("%d != %d", ln, len(out))
		}
	}
}
