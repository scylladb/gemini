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

	"github.com/scylladb/gemini/pkg/murmur"
	"github.com/scylladb/gemini/pkg/typedef"

	"github.com/scylladb/gemini/pkg/testschema"

	"github.com/gocql/gocql"
)

type RoutingKeyCreator struct {
	routingKeyBuffer []byte
}

func (rc *RoutingKeyCreator) CreateRoutingKey(table *testschema.Table, values []interface{}) ([]byte, error) {
	partitionKeys := table.PartitionKeys
	if len(partitionKeys) == 1 {
		// single column routing key
		routingKey, err := gocql.Marshal(
			partitionKeys[0].Type.CQLType(),
			values[0],
		)
		if err != nil {
			return nil, err
		}
		return routingKey, nil
	}

	if rc.routingKeyBuffer == nil {
		rc.routingKeyBuffer = make([]byte, 0, 256)
	}

	// composite routing key
	buf := bytes.NewBuffer(rc.routingKeyBuffer)
	for i := range partitionKeys {
		encoded, err := gocql.Marshal(
			partitionKeys[i].Type.CQLType(),
			values[i],
		)
		if err != nil {
			return nil, err
		}
		lenBuf := []byte{0x00, 0x00}
		binary.BigEndian.PutUint16(lenBuf, uint16(len(encoded)))
		buf.Write(lenBuf)
		buf.Write(encoded)
		buf.WriteByte(0x00)
	}
	routingKey := buf.Bytes()
	return routingKey, nil
}

func (rc *RoutingKeyCreator) GetHash(t *testschema.Table, values typedef.Values) (uint64, error) {
	b, err := rc.CreateRoutingKey(t, values)
	if err != nil {
		return 0, err
	}
	return uint64(murmur.Murmur3H1(b)), nil
}
