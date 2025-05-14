// Copyright 2019 ScyllaDB
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

package typedef

import (
	"encoding/json"
	"strconv"

	"github.com/pkg/errors"

	"github.com/scylladb/gemini/pkg/murmur"
)

type MaterializedView struct {
	NonPrimaryKey          *ColumnDef
	Name                   string  `json:"name"`
	PartitionKeys          Columns `json:"partition_keys"`
	ClusteringKeys         Columns `json:"clustering_keys"`
	partitionKeysLenValues int
}

type Schema struct {
	Keyspace Keyspace     `json:"keyspace"`
	Tables   []*Table     `json:"tables"`
	Config   SchemaConfig `json:"-"`
}

func (s *Schema) GetHash() string {
	out, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return strconv.FormatUint(uint64(murmur.Murmur3H1(out)), 16)
}

func (s *Schema) Validate(distributionSize uint64) error {
	prConfig := s.Config.GetPartitionRangeConfig()
	for _, table := range s.Tables {
		pkVariations := table.PartitionKeys.ValueVariationsNumber(&prConfig)
		if pkVariations < 2^24 {
			// On small pk size gemini stuck due to the multiple reasons, all the values could stack in inflights
			// or partitions can become stale due to the murmur hash not hitting them, since pk low resolution
			return errors.Errorf("pk size %d is less than gemini can handle", uint64(pkVariations))
		}
		if float64(distributionSize*100) > pkVariations {
			// With low partition variations there is a chance that partition can become stale due to the
			// murmur hash not hitting it
			// To avoid this scenario we need to make sure that every given partition could hold at least 100 values
			return errors.Errorf(
				"pk size %d is less than --token-range-slices multiplied by 100",
				uint64(pkVariations),
			)
		}
	}
	return nil
}

func (m *MaterializedView) HaveNonPrimaryKey() bool {
	return m.NonPrimaryKey != nil
}

func (m *MaterializedView) PartitionKeysLenValues() int {
	if m.partitionKeysLenValues == 0 && m.PartitionKeys != nil {
		m.partitionKeysLenValues = m.PartitionKeys.LenValues()
	}
	return m.partitionKeysLenValues
}
