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

package testutils

import (
	"context"
	"log"
	"math/rand/v2"

	"github.com/scylladb/gemini/pkg/routingkey"
	"github.com/scylladb/gemini/pkg/typedef"
)

type MockGenerator struct {
	table             *typedef.Table
	rand              *rand.Rand
	partitionsConfig  *typedef.PartitionRangeConfig
	routingKeyCreator *routingkey.Creator
}

func NewTestGenerator(
	table *typedef.Table,
	rnd *rand.Rand,
	partitionsConfig *typedef.PartitionRangeConfig,
	routingKeyCreator *routingkey.Creator,
) *MockGenerator {
	return &MockGenerator{
		table:             table,
		rand:              rnd,
		partitionsConfig:  partitionsConfig,
		routingKeyCreator: routingKeyCreator,
	}
}

func (g *MockGenerator) Get(_ context.Context) typedef.PartitionKeys {
	values := g.createPartitionKeyValues(g.rand)
	token, err := g.routingKeyCreator.GetHash(values)
	if err != nil {
		log.Printf(
			"Error on get hash for table:%s, values:%v\nPartitionColumns:%v\nError is: %s\n",
			g.table.Name,
			g.table.PartitionKeys,
			values,
			err,
		)
	}
	return typedef.PartitionKeys{Token: token, Values: values}
}

func (g *MockGenerator) GetOld(_ context.Context) typedef.PartitionKeys {
	values := g.createPartitionKeyValues(g.rand)
	token, err := g.routingKeyCreator.GetHash(values)
	if err != nil {
		log.Printf(
			"Error on get hash for table:%s, values:%v\nPartitionColumns:%v\nError is: %s\n",
			g.table.Name,
			g.table.PartitionKeys,
			values,
			err,
		)
	}
	return typedef.PartitionKeys{Token: token, Values: values}
}

func (g *MockGenerator) GiveOlds(_ context.Context, _ ...typedef.PartitionKeys) {}

func (g *MockGenerator) ReleaseToken(_ uint64) {
}

func (g *MockGenerator) createPartitionKeyValues(r *rand.Rand) typedef.Values {
	values := make(typedef.Values, len(g.table.PartitionKeys))

	for _, pk := range g.table.PartitionKeys {
		values[pk.Name] = pk.Type.GenValue(r, g.partitionsConfig)
	}

	return values
}
