package main

import (
	"github.com/scylladb/gemini"
	"go.uber.org/zap"
)

func createGenerators(schema *gemini.Schema, schemaConfig gemini.SchemaConfig, distributionFunc gemini.DistributionFunc, actors, distributionSize uint64, logger *zap.Logger) []*gemini.Generators {
	partitionRangeConfig := gemini.PartitionRangeConfig{
		MaxBlobLength:   schemaConfig.MaxBlobLength,
		MinBlobLength:   schemaConfig.MinBlobLength,
		MaxStringLength: schemaConfig.MaxStringLength,
		MinStringLength: schemaConfig.MinStringLength,
	}

	var gs []*gemini.Generators
	for _, table := range schema.Tables {
		gCfg := &gemini.GeneratorsConfig{
			Partitions:       partitionRangeConfig,
			Size:             actors,
			DistributionSize: distributionSize,
			DistributionFunc: distributionFunc,
			Seed:             seed,
			PkUsedBufferSize: pkBufferReuseSize,
		}
		g := gemini.NewGenerator(table, gCfg, logger.Named("generator"))
		gs = append(gs, g)
	}
	return gs
}
