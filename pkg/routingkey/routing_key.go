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

package routingkey

import (
	"bytes"
	"encoding/binary"

	"github.com/gocql/gocql"
	"github.com/twmb/murmur3"

	"github.com/scylladb/gemini/pkg/typedef"
)

type Creator struct {
	table *typedef.Table
}

func New(table *typedef.Table) *Creator {
	return &Creator{
		table: table,
	}
}

func (rc *Creator) CreateRoutingKey(values map[string][]any) ([]byte, error) {
	if len(rc.table.PartitionKeys) == 1 && len(values[rc.table.PartitionKeys[0].Name]) == 1 {
		return gocql.Marshal(
			rc.table.PartitionKeys[0].Type.CQLType(),
			values[rc.table.PartitionKeys[0].Name][0],
		)
	}

	// composite routing key
	buf := bytes.NewBuffer(make([]byte, 0, 256))
	for _, pk := range rc.table.PartitionKeys {
		ty := pk.Type.CQLType()
		for _, v := range values[pk.Name] {
			encoded, err := gocql.Marshal(ty, v)
			if err != nil {
				return nil, err
			}
			var lenBuf [2]byte
			binary.BigEndian.PutUint16(lenBuf[:], uint16(len(encoded)))
			buf.Write(lenBuf[:])
			buf.Write(encoded)
			buf.WriteByte(0x00)
		}
	}

	return buf.Bytes(), nil
}

func (rc *Creator) GetHash(values map[string][]any) (uint32, error) {
	b, err := rc.CreateRoutingKey(values)
	if err != nil {
		return 0, err
	}

	hasher := murmur3.New32()

	if _, err = hasher.Write(b); err != nil {
		return 0, err
	}

	result := hasher.Sum32()

	return result, nil
}
