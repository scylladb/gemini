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

type MaterializedView struct {
	NonPrimaryKey          *ColumnDef
	Name                   string  `json:"name"`
	PartitionKeys          Columns `json:"partition_keys"`
	ClusteringKeys         Columns `json:"clustering_keys"`
	partitionKeysLenValues int
}

type Schema struct {
	Keyspace Keyspace `json:"keyspace"`
	Tables   []*Table `json:"tables"`
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
