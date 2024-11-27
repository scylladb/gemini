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

	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/typedef"
)

type Generators struct {
	cancel context.CancelFunc
	wg     *sync.WaitGroup
	Gens   []Generator
}

func New(
	ctx context.Context,
	schema *typedef.Schema,
	distFunc DistributionFunc,
	seed, distributionSize, pkBufferReuseSize uint64,
	logger *zap.Logger,
) *Generators {
	gs := make([]Generator, 0, len(schema.Tables))

	cfg := Config{
		PartitionsRangeConfig:      schema.Config.GetPartitionRangeConfig(),
		PartitionsCount:            distributionSize,
		PartitionsDistributionFunc: distFunc,
		Seed:                       seed,
		PkUsedBufferSize:           pkBufferReuseSize,
	}

	wg := new(sync.WaitGroup)
	wg.Add(len(schema.Tables))

	ctx, cancel := context.WithCancel(ctx)

	partitionRangeConfig := schema.Config.GetPartitionRangeConfig()
	for _, table := range schema.Tables {
		g := NewGenerator(table, cfg, logger.Named("generators-"+table.Name))
		go func() {
			defer wg.Done()
			g.Start(ctx)
		}()

		if table.PartitionKeys.ValueVariationsNumber(&partitionRangeConfig) < math.MaxUint32 {
			// Low partition key variation can lead to having staled partitions
			// Let's detect and mark them before running test
			g.FindAndMarkStalePartitions()
		}

		gs = append(gs, g)
	}

	return &Generators{
		Gens:   gs,
		wg:     wg,
		cancel: cancel,
	}
}

func (g *Generators) Close() error {
	g.cancel()
	g.wg.Wait()

	return nil
}
