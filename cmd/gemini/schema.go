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
	"encoding/json"
	"os"
	"strings"

	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/builders"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/tableopts"
	"github.com/scylladb/gemini/pkg/typedef"
)

const (
	MaxBlobLength   = 1e4
	MinBlobLength   = 0
	MaxStringLength = 1000
	MinStringLength = 0
	MaxTupleParts   = 20
	MaxUDTParts     = 20
)

func createSchemaConfig(logger *zap.Logger) typedef.SchemaConfig {
	defaultConfig := createDefaultSchemaConfig(logger)
	switch strings.ToLower(datasetSize) {
	case "small":
		return typedef.SchemaConfig{
			ReplicationStrategy:              defaultConfig.ReplicationStrategy,
			OracleReplicationStrategy:        defaultConfig.OracleReplicationStrategy,
			TableOptions:                     defaultConfig.TableOptions,
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
			MinBlobLength:                    0,
			MinStringLength:                  0,
			UseCounters:                      defaultConfig.UseCounters,
			UseLWT:                           defaultConfig.UseLWT,
			UseMaterializedViews:             defaultConfig.UseMaterializedViews,
			CQLFeature:                       defaultConfig.CQLFeature,
			AsyncObjectStabilizationAttempts: defaultConfig.AsyncObjectStabilizationAttempts,
			AsyncObjectStabilizationDelay:    defaultConfig.AsyncObjectStabilizationDelay,
		}
	default:
		return defaultConfig
	}
}

func createDefaultSchemaConfig(logger *zap.Logger) typedef.SchemaConfig {
	return typedef.SchemaConfig{
		ReplicationStrategy:              replication.MustParseReplication(replicationStrategy),
		OracleReplicationStrategy:        replication.MustParseReplication(oracleReplicationStrategy),
		TableOptions:                     tableopts.CreateTableOptions(tableOptions, logger),
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
		UseLWT:                           useLWT,
		CQLFeature:                       typedef.ParseCQLFeature(cqlFeatures),
		UseMaterializedViews:             useMaterializedViews,
		AsyncObjectStabilizationAttempts: asyncObjectStabilizationAttempts,
		AsyncObjectStabilizationDelay:    asyncObjectStabilizationDelay,
	}
}

func readSchema(confFile string, schemaConfig typedef.SchemaConfig) (*typedef.Schema, error) {
	byteValue, err := os.ReadFile(confFile)
	if err != nil {
		return nil, err
	}

	var shm typedef.Schema

	err = json.Unmarshal(byteValue, &shm)
	if err != nil {
		return nil, err
	}

	schemaBuilder := builders.NewSchemaBuilder()
	schemaBuilder.Keyspace(shm.Keyspace).Config(schemaConfig)
	for t, tbl := range shm.Tables {
		shm.Tables[t].LinkIndexAndColumns()
		schemaBuilder.Table(tbl)
	}
	return schemaBuilder.Build(), nil
}
