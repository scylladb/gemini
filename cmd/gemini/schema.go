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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/builders"
	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/tableopts"
	"github.com/scylladb/gemini/pkg/typedef"
)

const (
	MaxBlobLength   = 256
	MinBlobLength   = 0
	MaxStringLength = 128
	MinStringLength = 0
	MaxTupleParts   = 5
	MaxUDTParts     = 5
)

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

	schemaBuilder := builders.SchemaBuilder{}
	schemaBuilder.Keyspace(shm.Keyspace).Config(schemaConfig)
	for t, tbl := range shm.Tables {
		shm.Tables[t].LinkIndexAndColumns()
		schemaBuilder.Table(tbl)
	}
	return schemaBuilder.Build(), nil
}

func getSchema(seed uint64, logger *zap.Logger) (*typedef.Schema, typedef.SchemaConfig, error) {
	var (
		err    error
		schema *typedef.Schema
	)

	schemaConfig := createSchemaConfig(logger)
	if err = schemaConfig.Valid(); err != nil {
		return nil, typedef.SchemaConfig{}, errors.Wrap(err, "invalid schema configuration")
	}

	if len(schemaFile) > 0 {
		schema, err = readSchema(schemaFile, schemaConfig)
		if err != nil {
			return nil, typedef.SchemaConfig{}, errors.Wrap(err, "cannot create schema")
		}
	} else {
		src := rand.NewChaCha8(sha256.Sum256([]byte(schemaSeed)))
		schema = generators.GenSchema(schemaConfig, src)
	}

	jsonSchema, _ := json.MarshalIndent(schema, "", "    ")

	printSetup(seed, seedFromString(schemaSeed))
	fmt.Printf("Schema: %v\n", string(jsonSchema)) //nolint:forbidigo

	return schema, schemaConfig, nil
}

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
			UseCounters:                      defaultConfig.UseCounters,
			UseLWT:                           defaultConfig.UseLWT,
			CQLFeature:                       defaultConfig.CQLFeature,
			AsyncObjectStabilizationAttempts: defaultConfig.AsyncObjectStabilizationAttempts,
			UseMaterializedViews:             defaultConfig.UseMaterializedViews,
			AsyncObjectStabilizationDelay:    defaultConfig.AsyncObjectStabilizationDelay,
		}
	default:
		return defaultConfig
	}
}

func getReplicationStrategy(
	rs string,
	fallback replication.Replication,
	logger *zap.Logger,
) replication.Replication {
	switch rs {
	case "network":
		return replication.NewNetworkTopologyStrategy()
	case "simple":
		return replication.NewSimpleStrategy()
	default:
		strategy := replication.Replication{}
		if err := json.Unmarshal([]byte(strings.ReplaceAll(rs, "'", "\"")), &strategy); err != nil {
			logger.Error(
				"unable to parse replication strategy",
				zap.String("strategy", rs),
				zap.Error(err),
			)
			return fallback
		}
		return strategy
	}
}

func createDefaultSchemaConfig(logger *zap.Logger) typedef.SchemaConfig {
	rs := getReplicationStrategy(replicationStrategy, replication.NewNetworkTopologyStrategy(), logger)
	ors := getReplicationStrategy(oracleReplicationStrategy, rs, logger)
	return typedef.SchemaConfig{
		ReplicationStrategy:              rs,
		OracleReplicationStrategy:        ors,
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
		CQLFeature:                       getCQLFeature(cqlFeatures),
		UseMaterializedViews:             useMaterializedViews,
		AsyncObjectStabilizationAttempts: asyncObjectStabilizationAttempts,
		AsyncObjectStabilizationDelay:    asyncObjectStabilizationDelay,
	}
}
