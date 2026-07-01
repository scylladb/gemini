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

package main

import (
	"testing"

	"go.uber.org/zap"
)

// TestCreateSchemaConfig_SmallSetsPartitionKeyLengths guards the regression
// where --dataset-size=small built a SchemaConfig that left MaxPKStringLength /
// MaxPKBlobLength (and their minimums) at zero. Partition-key string/blob value
// generation then called rand.IntN(0), which panics ("invalid argument to
// IntN") at partition fill and crashed every small-dataset run before it did any
// work. The partition range config must carry positive lengths.
func TestCreateSchemaConfig_SmallSetsPartitionKeyLengths(t *testing.T) {
	t.Parallel()

	cfg := createSchemaConfig("small", zap.NewNop())
	prc := cfg.GetPartitionRangeConfig()

	checks := map[string]int{
		"MaxPKStringLength": prc.GetMaxStringLength(),
		"MinPKStringLength": prc.GetMinStringLength(),
		"MaxPKBlobLength":   prc.GetMaxBlobLength(),
		"MinPKBlobLength":   prc.GetMinBlobLength(),
	}
	for name, got := range checks {
		if got < 1 {
			t.Errorf("small preset %s = %d, want >= 1 (zero panics rand.IntN at partition fill)", name, got)
		}
	}
}
