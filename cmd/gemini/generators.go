package main

import "github.com/scylladb/gemini"

func createGenerators(schema *gemini.Schema, schemaConfig gemini.SchemaConfig, actors uint64) []*gemini.Generators {
	partitionRangeConfig := gemini.PartitionRangeConfig{
		MaxBlobLength:   schemaConfig.MaxBlobLength,
		MinBlobLength:   schemaConfig.MinBlobLength,
		MaxStringLength: schemaConfig.MaxStringLength,
		MinStringLength: schemaConfig.MinStringLength,
	}

	var gs []*gemini.Generators
	for _, table := range schema.Tables {
		gCfg := &gemini.GeneratorsConfig{
			Table:            table,
			Partitions:       partitionRangeConfig,
			Size:             actors,
			Seed:             seed,
			PkBufferSize:     pkBufferSize,
			PkUsedBufferSize: pkBufferReuseSize,
		}
		g := gemini.NewGenerator(gCfg)
		gs = append(gs, g)
	}
	return gs
}
