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

	"github.com/samber/mo"

	"github.com/scylladb/gemini/pkg/murmur"
)

type (
	Schema struct {
		Keyspace Keyspace     `json:"keyspace"`
		Tables   []*Table     `json:"tables"`
		Config   SchemaConfig `json:"-"`
	}

	IndexDef struct {
		Column     ColumnDef
		IndexName  string `json:"index_name"`
		ColumnName string `json:"column_name"`
	}

	MaterializedView struct {
		NonPrimaryKey          mo.Option[ColumnDef]
		Name                   string  `json:"name"`
		PartitionKeys          Columns `json:"partition_keys"`
		ClusteringKeys         Columns `json:"clustering_keys"`
		partitionKeysLenValues int
	}
)

func (m *IndexDef) MarshalJSON() ([]byte, error) {
	var col *ColumnDef
	if m.Column != (ColumnDef{}) {
		col = &m.Column
	}

	data := struct {
		Column     *ColumnDef
		IndexName  string `json:"index_name"`
		ColumnName string `json:"column_name"`
	}{
		Column:     col,
		IndexName:  m.IndexName,
		ColumnName: m.ColumnName,
	}

	return json.Marshal(data)
}

func (m *IndexDef) UnmarshalJSON(data []byte) error {
	item := struct {
		Column     *ColumnDef
		IndexName  string `json:"index_name"`
		ColumnName string `json:"column_name"`
	}{}

	if err := json.Unmarshal(data, &item); err != nil {
		return err
	}

	if item.Column != nil {
		m.Column = *item.Column
	}

	m.ColumnName = item.ColumnName
	m.IndexName = item.IndexName
	return nil
}

func (m *MaterializedView) UnmarshalJSON(data []byte) error {
	d := struct {
		NonPrimaryKey  *ColumnDef `json:"NonPrimaryKey"`
		Name           string     `json:"name"`
		PartitionKeys  Columns    `json:"partition_keys"`
		ClusteringKeys Columns    `json:"clustering_keys"`
	}{}

	if err := json.Unmarshal(data, &d); err != nil {
		return err
	}

	m.Name = d.Name
	m.PartitionKeys = d.PartitionKeys
	m.ClusteringKeys = d.ClusteringKeys
	if d.NonPrimaryKey != nil {
		m.NonPrimaryKey = mo.Some(*d.NonPrimaryKey)
	} else {
		m.NonPrimaryKey = mo.None[ColumnDef]()
	}
	m.partitionKeysLenValues = m.PartitionKeys.LenValues()

	return nil
}

func (m *MaterializedView) MarshalJSON() ([]byte, error) {
	var nonPrimaryKey *ColumnDef
	if m.NonPrimaryKey.IsPresent() {
		val := m.NonPrimaryKey.MustGet()
		nonPrimaryKey = &val
	} else {
		nonPrimaryKey = nil
	}

	return json.Marshal(struct {
		NonPrimaryKey  *ColumnDef `json:"NonPrimaryKey"`
		Name           string     `json:"name"`
		PartitionKeys  Columns    `json:"partition_keys"`
		ClusteringKeys Columns    `json:"clustering_keys"`
	}{
		NonPrimaryKey:  nonPrimaryKey,
		Name:           m.Name,
		PartitionKeys:  m.PartitionKeys,
		ClusteringKeys: m.ClusteringKeys,
	})
}

func (s *Schema) GetHash() string {
	out, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return strconv.FormatUint(uint64(murmur.Murmur3H1(out)), 16)
}

func (m *MaterializedView) HaveNonPrimaryKey() bool {
	return m.NonPrimaryKey.IsPresent()
}

func (m *MaterializedView) PartitionKeysLenValues() int {
	if m.partitionKeysLenValues == 0 && m.PartitionKeys != nil {
		m.partitionKeysLenValues = m.PartitionKeys.LenValues()
	}
	return m.partitionKeysLenValues
}
