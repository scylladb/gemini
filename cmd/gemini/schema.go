package main

import (
	"strings"

	"github.com/scylladb/gemini"
	"go.uber.org/zap"
)

func createSchemaConfig(logger *zap.Logger) gemini.SchemaConfig {
	defaultConfig := createDefaultSchemaConfig(logger)
	switch strings.ToLower(datasetSize) {
	case "small":
		return gemini.SchemaConfig{
			CompactionStrategy: defaultConfig.CompactionStrategy,
			MaxPartitionKeys:   defaultConfig.MaxPartitionKeys,
			MaxClusteringKeys:  defaultConfig.MaxClusteringKeys,
			MaxColumns:         defaultConfig.MaxColumns,
			MaxUDTParts:        2,
			MaxTupleParts:      2,
			MaxBlobLength:      20,
			MaxStringLength:    20,
			CQLFeature:         defaultConfig.CQLFeature,
		}
	default:
		return defaultConfig
	}
}

func createDefaultSchemaConfig(logger *zap.Logger) gemini.SchemaConfig {
	const (
		MaxBlobLength   = 1e4
		MinBlobLength   = 0
		MaxStringLength = 1000
		MinStringLength = 0
		MaxTupleParts   = 20
		MaxUDTParts     = 20
	)
	return gemini.SchemaConfig{
		CompactionStrategy: getCompactionStrategy(compactionStrategy, logger),
		MaxPartitionKeys:   3,
		MaxClusteringKeys:  maxClusteringKeys,
		MaxColumns:         maxColumns,
		MaxUDTParts:        MaxUDTParts,
		MaxTupleParts:      MaxTupleParts,
		MaxBlobLength:      MaxBlobLength,
		MinBlobLength:      MinBlobLength,
		MaxStringLength:    MaxStringLength,
		MinStringLength:    MinStringLength,
		CQLFeature:         getCQLFeature(cqlFeatures),
	}
}
