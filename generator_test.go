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
package gemini

import (
	"context"
	"sync/atomic"
	"testing"

	"go.uber.org/zap"
)

func TestGenerator(t *testing.T) {
	table := &Table{
		Name:          "tbl",
		PartitionKeys: createPkColumns(1, "pk"),
	}
	var current uint64
	cfg := &GeneratorConfig{
		PartitionsRangeConfig: PartitionRangeConfig{
			MaxStringLength: 10,
			MinStringLength: 0,
			MaxBlobLength:   10,
			MinBlobLength:   0,
		},
		PkUsedBufferSize: 10000,
		PartitionsCount:  1000,
		PartitionsDistributionFunc: func() TokenIndex {
			return TokenIndex(atomic.LoadUint64(&current))
		},
	}
	logger, _ := zap.NewDevelopment()
	generators := NewGenerator(context.Background(), table, cfg, logger)
	for i := uint64(0); i < cfg.PartitionsCount; i++ {
		atomic.StoreUint64(&current, i)
		v, _ := generators.Get()
		n, _ := generators.Get()
		if v.Token%generators.partitionCount != n.Token%generators.partitionCount {
			t.Errorf("expected %v, got %v", v, n)
		}
	}
}
