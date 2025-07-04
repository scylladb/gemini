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
	"hash"

	"github.com/gocql/gocql"
	"github.com/twmb/murmur3"

	"github.com/scylladb/gemini/pkg/typedef"
)

type Creator struct {
	table  *typedef.Table
	hasher hash.Hash32
}

func New(table *typedef.Table) *Creator {
	return &Creator{
		table:  table,
		hasher: murmur3.New32(),
	}
}

//goroutine 1 [running]:
//github.com/twmb/murmur3.(*digest).Write(0xc00039c320, {0xc000818e00, 0xf, 0x100})
//github.com/twmb/murmur3@v1.1.8/murmur.go:36 +0xfe
//github.com/scylladb/gemini/pkg/routingkey.(*Creator).GetHash(0xc00032f038, 0xc000812840?)
//github.com/scylladb/gemini/pkg/routingkey/routing_key.go:75 +0x6c
//github.com/scylladb/gemini/pkg/generators.(*Generator).createPartitionKeyValues(0xc0000d22c0)
//github.com/scylladb/gemini/pkg/generators/generator.go:292 +0x11b
//github.com/scylladb/gemini/pkg/generators.(*Generator).FindAndMarkStalePartitions(0xc0000d22c0)
//github.com/scylladb/gemini/pkg/generators/generator.go:186 +0x98
//github.com/scylladb/gemini/pkg/generators.New(0xc0001800f0, 0xc000272840?, 0x174a7e0?, 0x0?, 0x0?, 0xc000194080, 0xc0003023c0)
//github.com/scylladb/gemini/pkg/generators/generators.go:57 +0x308
//main.run(0x16c63c0, {0xf6034a?, 0x4?, 0xf6030e?})
//github.com/scylladb/gemini/pkg/cmd/root.go:181 +0x9c8
//github.com/spf13/cobra.(*Command).execute(0x16c63c0, {0xc000036190, 0x16, 0x17})
//github.com/spf13/cobra@v1.9.1/command.go:1015 +0xaaa
//github.com/spf13/cobra.(*Command).ExecuteC(0x16c63c0)
//github.com/spf13/cobra@v1.9.1/command.go:1148 +0x46f
//github.com/spf13/cobra.(*Command).Execute(...)
//github.com/spf13/cobra@v1.9.1/command.go:1071
//main.main()
//github.com/scylladb/gemini/pkg/cmd/main.go:31 +0x34
//make: *** [Makefile:200: integration-test] Error 2

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

	if _, err = rc.hasher.Write(b); err != nil {
		return 0, err
	}

	result := rc.hasher.Sum32()
	rc.hasher.Reset()

	return result, nil
}
