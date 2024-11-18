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
	"sync"

	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/typedef"
)

type Generators struct {
	wg         sync.WaitGroup
	generators []Generator
	cancel     context.CancelFunc
	idx        int
}

func (g *Generators) Get() *Generator {
	gen := &g.generators[g.idx%len(g.generators)]
	g.idx++
	return gen
}

func (g *Generators) Close() error {
	g.cancel()
	g.wg.Wait()

	return nil
}

func New(
	ctx context.Context,
	schema *typedef.Schema,
	schemaConfig typedef.SchemaConfig,
	seed, partitionsCount uint64,
	logger *zap.Logger,
	distFunc DistributionFunc,
	pkBufferReuseSize uint64,
) (*Generators, error) {
	partitionRangeConfig := schemaConfig.GetPartitionRangeConfig()
	ctx, cancel := context.WithCancel(ctx)

	gens := &Generators{
		generators: make([]Generator, 0, len(schema.Tables)),
		cancel:     cancel,
	}

	gens.wg.Add(len(schema.Tables))
	for _, table := range schema.Tables {
		pkVariations := table.PartitionKeys.ValueVariationsNumber(&partitionRangeConfig)

		tablePartConfig := Config{
			PartitionsRangeConfig:      partitionRangeConfig,
			PartitionsCount:            partitionsCount,
			PartitionsDistributionFunc: distFunc,
			Seed:                       seed,
			PkUsedBufferSize:           pkBufferReuseSize,
		}
		g := NewGenerator(table, tablePartConfig, logger.Named("generators"))
		if pkVariations < 2^32 {
			// Low partition key variation can lead to having staled partitions
			// Let's detect and mark them before running test
			g.FindAndMarkStalePartitions()
		}

		gens.generators = append(gens.generators, g)

		go func(g *Generator) {
			defer gens.wg.Done()

			g.Start(ctx)
		}(&g)
	}

	return gens, nil
}
