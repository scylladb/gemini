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

	"github.com/scylladb/gemini/pkg/murmur"
	"github.com/scylladb/gemini/pkg/typedef"
)

type Creator struct {
	table            *typedef.Table
	routingKeyBuffer []byte
}

func New(table *typedef.Table) *Creator {
	return &Creator{
		routingKeyBuffer: make([]byte, 0, 256),
		table:            table,
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
	buf := bytes.NewBuffer(rc.routingKeyBuffer)
	for _, pk := range rc.table.PartitionKeys {
		ty := pk.Type.CQLType()
		for _, v := range values[pk.Name] {
			encoded, err := gocql.Marshal(ty, v)
			if err != nil {
				return nil, err
			}
			lenBuf := []byte{0x00, 0x00}
			binary.BigEndian.PutUint16(lenBuf, uint16(len(encoded)))
			buf.Write(lenBuf)
			buf.Write(encoded)
			buf.WriteByte(0x00)
		}
	}

	routingKey := buf.Bytes()
	return routingKey, nil
}

func (rc *Creator) GetHash(values map[string][]any) (uint64, error) {
	b, err := rc.CreateRoutingKey(values)
	if err != nil {
		return 0, err
	}

	return uint64(murmur.Murmur3H1(b)), nil
}
