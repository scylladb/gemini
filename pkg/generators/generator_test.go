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

package generators_test

import (
	"sync/atomic"
	"testing"

	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/typedef"
)

func TestGenerator(t *testing.T) {
	t.Parallel()
	table := &typedef.Table{
		Name:          "tbl",
		PartitionKeys: generators.CreatePkColumns(1, "pk"),
	}
	var current uint64
	cfg := &generators.Config{
		PartitionsRangeConfig: typedef.PartitionRangeConfig{
			MaxStringLength: 10,
			MinStringLength: 0,
			MaxBlobLength:   10,
			MinBlobLength:   0,
		},
		PkUsedBufferSize: 10000,
		PartitionsCount:  1000,
		PartitionsDistributionFunc: func() generators.TokenIndex {
			return generators.TokenIndex(atomic.LoadUint64(&current))
		},
	}
	logger, _ := zap.NewDevelopment()
	generator := generators.NewGenerator(table, cfg, logger)
	generator.Start(stop.NewFlag("main_test"))
	for i := uint64(0); i < cfg.PartitionsCount; i++ {
		atomic.StoreUint64(&current, i)
		v := generator.Get()
		n := generator.Get()
		if v.Token%generator.PartitionCount() != n.Token%generator.PartitionCount() {
			t.Errorf("expected %v, got %v", v, n)
		}
	}
}
