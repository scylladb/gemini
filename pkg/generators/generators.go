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
	"math"
	"math/rand/v2"

	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/typedef"
)

type Generators struct {
	Gens map[string]*Generator
}

func New(
	schema *typedef.Schema,
	distFunc distributions.DistributionFunc,
	distributionSize, pkBufferReuseSize int,
	logger *zap.Logger,
	source *rand.ChaCha8,
) (*Generators, error) {
	cfg := Config{
		PartitionsRangeConfig:      schema.Config.GetPartitionRangeConfig(),
		PartitionsCount:            distributionSize,
		PartitionsDistributionFunc: distFunc,
		PkUsedBufferSize:           pkBufferReuseSize,
	}

	gens := &Generators{
		Gens: make(map[string]*Generator, len(schema.Tables)),
	}

	partitionRangeConfig := schema.Config.GetPartitionRangeConfig()
	for _, table := range schema.Tables {
		g := NewGenerator(table, cfg, logger.Named("generators-"+table.Name), source)

		if table.PartitionKeys.ValueVariationsNumber(&partitionRangeConfig) < math.MaxUint32 {
			// Low partition key variation can lead to having staled partitions
			// Let's detect and mark them before running test
			if err := g.FindAndMarkStalePartitions(); err != nil {
				return nil, err
			}
		}

		gens.Gens[table.Name] = g
	}

	return gens, nil
}

func (g *Generators) Get(table *typedef.Table) *Generator {
	gen, exists := g.Gens[table.Name]

	if !exists {
		panic("generator does not exist for table " + table.Name)
	}

	return gen
}

func (g *Generators) Close() error {
	for _, gen := range g.Gens {
		_ = gen.Close()
	}

	return nil
}
