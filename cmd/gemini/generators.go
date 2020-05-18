// Copyright 2019 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"

	"github.com/scylladb/gemini"
	"go.uber.org/zap"
)

func createGenerators(ctx context.Context, schema *gemini.Schema, schemaConfig gemini.SchemaConfig, distributionFunc gemini.DistributionFunc, actors, distributionSize uint64, logger *zap.Logger) []*gemini.Generator {
	partitionRangeConfig := gemini.PartitionRangeConfig{
		MaxBlobLength:   schemaConfig.MaxBlobLength,
		MinBlobLength:   schemaConfig.MinBlobLength,
		MaxStringLength: schemaConfig.MaxStringLength,
		MinStringLength: schemaConfig.MinStringLength,
	}

	var gs []*gemini.Generator
	for _, table := range schema.Tables {
		gCfg := &gemini.GeneratorConfig{
			PartitionsRangeConfig:      partitionRangeConfig,
			PartitionsCount:            distributionSize,
			PartitionsDistributionFunc: distributionFunc,
			Seed:                       seed,
			PkUsedBufferSize:           pkBufferReuseSize,
		}
		g := gemini.NewGenerator(ctx, table, gCfg, logger.Named("generator"))
		gs = append(gs, g)
	}
	return gs
}
