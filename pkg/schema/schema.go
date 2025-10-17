// Copyright 2025 ScyllaDB
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

package schema

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"os"

	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/builders"
	"github.com/scylladb/gemini/pkg/generators/statements"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

const (
	MaxBlobLength   = 256
	MinBlobLength   = 0
	MaxStringLength = 128
	MinStringLength = 0
	MaxTupleParts   = 5
	MaxUDTParts     = 5
)

func Read(confFile string, schemaConfig typedef.SchemaConfig) (*typedef.Schema, error) {
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

func Get(schemaConfig typedef.SchemaConfig, schemaSeed, schemaFile string) (*typedef.Schema, error) {
	var (
		err    error
		schema *typedef.Schema
	)

	if err = schemaConfig.Valid(); err != nil {
		return nil, errors.Join(err, errors.New("invalid schema configuration"))
	}

	if len(schemaFile) > 0 {
		schema, err = Read(schemaFile, schemaConfig)
		if err != nil {
			return nil, errors.Join(err, errors.New("cannot create schema"))
		}
	} else {
		src := rand.NewChaCha8(sha256.Sum256([]byte(schemaSeed)))
		schema = statements.GenSchema(schemaConfig, src)
	}

	return schema, nil
}

func GetReplicationStrategy(
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
		data := utils.UnsafeBytes(utils.SingleToDoubleQuoteReplacer.Replace(rs))
		if err := json.Unmarshal(data, &strategy); err != nil {
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
