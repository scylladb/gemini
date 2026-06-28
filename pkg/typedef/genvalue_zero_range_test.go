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

package typedef_test

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/scylladb/gemini/pkg/typedef"
)

// TestSimpleType_GenValue_ZeroRangeDoesNotPanic guards the partition-fill crash:
// a RangeConfig whose max string/blob length is zero (what the --dataset-size=small
// preset produced for partition keys before the fix) made string/blob value
// generation call rand.IntN(0), which panics ("invalid argument to IntN") and
// crashes the whole process at partition fill. The randSpan guard must turn that
// into a harmless (empty) value instead.
func TestSimpleType_GenValue_ZeroRangeDoesNotPanic(t *testing.T) {
	t.Parallel()

	r := rand.New(rand.NewPCG(1, 2))

	// All-zero ranges — the degenerate config that used to panic.
	var prc typedef.PartitionRangeConfig

	for _, st := range []typedef.SimpleType{
		typedef.TypeText, typedef.TypeAscii, typedef.TypeVarchar, typedef.TypeBlob,
	} {
		assert.NotPanicsf(t, func() { _ = st.GenValue(r, prc) },
			"genValue for %v with a zero-length range must not panic", st)
	}
}
