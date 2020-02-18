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
			CompactionStrategy:               defaultConfig.CompactionStrategy,
			ReplicationStrategy:              defaultConfig.ReplicationStrategy,
			OracleReplicationStrategy:        defaultConfig.OracleReplicationStrategy,
			MaxTables:                        defaultConfig.MaxTables,
			MaxPartitionKeys:                 defaultConfig.MaxPartitionKeys,
			MinPartitionKeys:                 defaultConfig.MinPartitionKeys,
			MaxClusteringKeys:                defaultConfig.MaxClusteringKeys,
			MinClusteringKeys:                defaultConfig.MinClusteringKeys,
			MaxColumns:                       defaultConfig.MaxColumns,
			MinColumns:                       defaultConfig.MinColumns,
			MaxUDTParts:                      2,
			MaxTupleParts:                    2,
			MaxBlobLength:                    20,
			MaxStringLength:                  20,
			UseCounters:                      defaultConfig.UseCounters,
			CQLFeature:                       defaultConfig.CQLFeature,
			AsyncObjectStabilizationAttempts: defaultConfig.AsyncObjectStabilizationAttempts,
			AsyncObjectStabilizationDelay:    defaultConfig.AsyncObjectStabilizationDelay,
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
	rs := getReplicationStrategy(replicationStrategy, replication.NewSimpleStrategy(), logger)
	ors := getReplicationStrategy(oracleReplicationStrategy, rs, logger)
	return gemini.SchemaConfig{
		CompactionStrategy:               getCompactionStrategy(compactionStrategy, logger),
		ReplicationStrategy:              rs,
		OracleReplicationStrategy:        ors,
		MaxTables:                        maxTables,
		MaxPartitionKeys:                 maxPartitionKeys,
		MinPartitionKeys:                 minPartitionKeys,
		MaxClusteringKeys:                maxClusteringKeys,
		MinClusteringKeys:                minClusteringKeys,
		MaxColumns:                       maxColumns,
		MinColumns:                       minColumns,
		MaxUDTParts:                      MaxUDTParts,
		MaxTupleParts:                    MaxTupleParts,
		MaxBlobLength:                    MaxBlobLength,
		MinBlobLength:                    MinBlobLength,
		MaxStringLength:                  MaxStringLength,
		MinStringLength:                  MinStringLength,
		UseCounters:                      useCounters,
		CQLFeature:                       getCQLFeature(cqlFeatures),
		AsyncObjectStabilizationAttempts: asyncObjectStabilizationAttempts,
		AsyncObjectStabilizationDelay:    asyncObjectStabilizationDelay,
	}
}
