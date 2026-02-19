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

package partitions

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidationTracking_SuccessAndFailure(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 2)

	// Single-partition success updates timestamps and ring
	v0 := parts.Get(0)
	require.NotNil(t, v0)

	before := uint64(time.Now().UTC().UnixNano())
	parts.ValidationSuccess(&v0)

	fs, ls, lf, recent := parts.ValidationStats(v0.ID)
	require.NotZero(t, fs)
	require.GreaterOrEqual(t, ls, fs)
	require.Zero(t, lf)

	// Ring buffer should have at least one non-zero entry
	require.GreaterOrEqual(t, len(recent), 1)

	// Second success keeps firstSuccessNS and updates lastSuccessNS
	parts.ValidationSuccess(&v0)
	fs2, ls2, _, _ := parts.ValidationStats(v0.ID)
	require.Equal(t, fs, fs2)
	require.GreaterOrEqual(t, ls2, ls)

	// Multi-partition failure updates failure timestamp for both
	v1 := parts.Get(1)
	require.NotNil(t, v1)
	parts.ValidationFailure(&v0)
	parts.ValidationFailure(&v1)

	_, _, lf0, _ := parts.ValidationStats(v0.ID)
	_, _, lf1, _ := parts.ValidationStats(v1.ID)
	require.GreaterOrEqual(t, lf0, before)
	require.GreaterOrEqual(t, lf1, before)
}

func TestValidationTracking_RecentRingWrap(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 1)
	key := parts.Get(0)
	require.NotNil(t, key)

	for range 7 {
		parts.ValidationSuccess(&key)
	}

	first, last, failure, recent := parts.ValidationStats(key.ID)
	require.NotZero(t, first)
	require.GreaterOrEqual(t, last, first)
	require.Zero(t, failure)
	require.Len(t, recent, 5)
}

func TestValidationTracking_MissingAndRelease(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 1)

	require.NotPanics(t, func() {
		parts.ValidationSuccess(nil)
	})
	require.NotPanics(t, func() {
		parts.ValidationFailure(nil)
	})

	key := parts.Get(0)
	require.NotNil(t, key)
	parts.ValidationSuccess(&key)
	key.Release()

	parts.ValidationFailure(&key)
	first, last, failure, recent := parts.ValidationStats(key.ID)
	require.Zero(t, first)
	require.Zero(t, last)
	require.Zero(t, failure)
	require.Empty(t, recent)
}
