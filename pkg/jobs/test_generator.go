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

package jobs

import (
	"fmt"

	"golang.org/x/exp/rand"

	"github.com/scylladb/gemini/pkg/routingkey"
	"github.com/scylladb/gemini/pkg/testschema"
	"github.com/scylladb/gemini/pkg/typedef"
)

type MockGenerator struct {
	table             *testschema.Table
	rand              *rand.Rand
	partitionsConfig  *typedef.PartitionRangeConfig
	routingKeyCreator *routingkey.Creator
}

func NewTestGenerator(
	table *testschema.Table,
	rnd *rand.Rand,
	partitionsConfig *typedef.PartitionRangeConfig,
	routingKeyCreator *routingkey.Creator,
) *MockGenerator {
	return &MockGenerator{table: table, rand: rnd, partitionsConfig: partitionsConfig, routingKeyCreator: routingKeyCreator}
}

func (g *MockGenerator) Get() *typedef.ValueWithToken {
	values := g.createPartitionKeyValues(g.rand)
	token, err := g.routingKeyCreator.GetHash(g.table, values)
	if err != nil {
		fmt.Printf("Error on get hash for table:%s, values:%v\nPartitionColumns:%v\nError is: %s\n", g.table.Name, g.table.PartitionKeys, values, err)
	}
	return &typedef.ValueWithToken{Token: token, Value: values}
}

func (g *MockGenerator) GetOld() *typedef.ValueWithToken {
	values := g.createPartitionKeyValues(g.rand)
	token, err := g.routingKeyCreator.GetHash(g.table, values)
	if err != nil {
		fmt.Printf("Error on get hash for table:%s, values:%v\nPartitionColumns:%v\nError is: %s\n", g.table.Name, g.table.PartitionKeys, values, err)
	}
	return &typedef.ValueWithToken{Token: token, Value: values}
}

func (g *MockGenerator) GiveOld(_ *typedef.ValueWithToken) {
}

func (g *MockGenerator) ReleaseToken(_ uint64) {
}

func (g *MockGenerator) createPartitionKeyValues(r *rand.Rand) []interface{} {
	var values []interface{}
	for _, pk := range g.table.PartitionKeys {
		values = append(values, pk.Type.GenValue(r, g.partitionsConfig)...)
	}
	return values
}
