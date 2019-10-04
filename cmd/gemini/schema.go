package main

import (
	"strings"

	"github.com/scylladb/gemini"
	"github.com/scylladb/gemini/replication"
	"go.uber.org/zap"
)

func createSchemaConfig(logger *zap.Logger) gemini.SchemaConfig {
	defaultConfig := createDefaultSchemaConfig(logger)
	switch strings.ToLower(datasetSize) {
	case "small":
		return gemini.SchemaConfig{
			CompactionStrategy:  defaultConfig.CompactionStrategy,
			ReplicationStrategy: defaultConfig.ReplicationStrategy,
			MaxTables:           defaultConfig.MaxTables,
			MaxPartitionKeys:    defaultConfig.MaxPartitionKeys,
			MinPartitionKeys:    defaultConfig.MinPartitionKeys,
			MaxClusteringKeys:   defaultConfig.MaxClusteringKeys,
			MinClusteringKeys:   defaultConfig.MinClusteringKeys,
			MaxColumns:          defaultConfig.MaxColumns,
			MinColumns:          defaultConfig.MinColumns,
			MaxUDTParts:         2,
			MaxTupleParts:       2,
			MaxBlobLength:       20,
			MaxStringLength:     20,
			UseCounters:         defaultConfig.UseCounters,
			CQLFeature:          defaultConfig.CQLFeature,
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
		CompactionStrategy:  getCompactionStrategy(compactionStrategy, logger),
		ReplicationStrategy: getReplicationStrategy(replicationStrategy, replication.NewSimpleStrategy(), logger),
		MaxTables:           maxTables,
		MaxPartitionKeys:    maxPartitionKeys,
		MinPartitionKeys:    minPartitionKeys,
		MaxClusteringKeys:   maxClusteringKeys,
		MinClusteringKeys:   minClusteringKeys,
		MaxColumns:          maxColumns,
		MinColumns:          minColumns,
		MaxUDTParts:         MaxUDTParts,
		MaxTupleParts:       MaxTupleParts,
		MaxBlobLength:       MaxBlobLength,
		MinBlobLength:       MinBlobLength,
		MaxStringLength:     MaxStringLength,
		MinStringLength:     MinStringLength,
		UseCounters:         useCounters,
		CQLFeature:          getCQLFeature(cqlFeatures),
	}
}
