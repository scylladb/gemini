// Copyright 2025 ScyllaDB
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

package partitions

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/typedef"
)

func TestNew(t *testing.T) {
	t.Parallel()

	assert := require.New(t)

	src, fn := distributions.New(distributions.Uniform, 10_000, 1, 0, 0)

	table := &typedef.Table{}

	parts := New(rand.New(src), fn, table, typedef.PartitionRangeConfig{}, 10_000)

	assert.NotNil(parts)
}
