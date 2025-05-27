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
	"math/rand/v2"

	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/typedef"
)

type Generators struct {
	cancel context.CancelFunc
	Gens   map[string]*Generator
}

func New(
	ctx context.Context,
	schema *typedef.Schema,
	distFunc distributions.DistributionFunc,
	seed, distributionSize, pkBufferReuseSize uint64,
	logger *zap.Logger,
	source rand.Source,
) *Generators {
	base, cancel := context.WithCancel(ctx)

	cfg := Config{
		PartitionsRangeConfig:      schema.Config.GetPartitionRangeConfig(),
		PartitionsCount:            distributionSize,
		PartitionsDistributionFunc: distFunc,
		Seed:                       seed,
		PkUsedBufferSize:           pkBufferReuseSize,
	}

	gens := &Generators{
		Gens:   make(map[string]*Generator, len(schema.Tables)),
		cancel: cancel,
	}

	partitionRangeConfig := schema.Config.GetPartitionRangeConfig()
	for _, table := range schema.Tables {
		g := NewGenerator(base, table, cfg, logger.Named("generators-"+table.Name), source)

		if table.PartitionKeys.ValueVariationsNumber(&partitionRangeConfig) < math.MaxUint32 {
			// Low partition key variation can lead to having staled partitions
			// Let's detect and mark them before running test
			g.FindAndMarkStalePartitions()
		}

		gens.Gens[table.Name] = g
	}

	return gens
}

func (g *Generators) Get(table *typedef.Table) *Generator {
	gen, exists := g.Gens[table.Name]

	if !exists {
		panic("generator does not exist for table " + table.Name)
	}

	return gen
}

func (g *Generators) Close() error {
	g.cancel()

	for _, gen := range g.Gens {
		_ = gen.Close()
	}

	return nil
}
