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

package generators

import (
	"context"
	"math"
	"sync"

	"github.com/scylladb/gemini/pkg/distributions"

	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/typedef"
)

type Generators struct {
	cancel context.CancelFunc
	Gens   []Generator
	wg     sync.WaitGroup
}

func New(
	ctx context.Context,
	schema *typedef.Schema,
	distFunc distributions.DistributionFunc,
	seed, distributionSize, pkBufferReuseSize uint64,
	logger *zap.Logger,
) *Generators {
	partitionRangeConfig := schema.Config.GetPartitionRangeConfig()

	cfg := Config{
		PartitionsRangeConfig:      partitionRangeConfig,
		PartitionsCount:            distributionSize,
		PartitionsDistributionFunc: distFunc,
		Seed:                       seed,
		PkUsedBufferSize:           pkBufferReuseSize,
	}

	ctx, cancel := context.WithCancel(ctx)

	gens := &Generators{
		Gens:   make([]Generator, 0, len(schema.Tables)),
		cancel: cancel,
	}

	gens.wg.Add(len(schema.Tables))

	for _, table := range schema.Tables {
		g := NewGenerator(table, cfg, logger.Named("generators-"+table.Name))
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			g.Start(ctx)

			if table.PartitionKeys.ValueVariationsNumber(&partitionRangeConfig) < math.MaxUint32 {
				// Low partition key variation can lead to having staled partitions
				// Let's detect and mark them before running test
				g.FindAndMarkStalePartitions()
			}
		}(&gens.wg)

		gens.Gens = append(gens.Gens, g)
	}

	return gens
}

func (g *Generators) Close() error {
	g.cancel()
	g.wg.Wait()

	return nil
}
