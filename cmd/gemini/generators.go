package main

import (
	"github.com/scylladb/gemini"
	"go.uber.org/zap"
)

func createGenerators(schema *gemini.Schema, schemaConfig gemini.SchemaConfig, distributionFunc gemini.DistributionFunc, actors, distributionSize uint64, logger *zap.Logger) []*gemini.Generator {
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
		g := gemini.NewGenerator(table, gCfg, logger.Named("generator"))
		gs = append(gs, g)
	}
	return gs
}
