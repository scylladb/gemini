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

package generators

import (
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/exp/rand"

	"github.com/scylladb/gemini/pkg/routingkey"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/typedef"
)

// TokenIndex represents the position of a token in the token ring.
// A token index is translated to a token by a generators. If the generators
// preserves the exact position, then the token index becomes the token;
// otherwise token index represents an approximation of the token.
//
// We use a token index approach, because our generators actually generate
// partition keys, and map them to tokens. The generators, therefore, do
// not populate the full token ring space. With token index, we can
// approximate different token distributions from a sparse set of tokens.
type TokenIndex uint64

type DistributionFunc func() TokenIndex

type GeneratorInterface interface {
	Get() *typedef.ValueWithToken
	GetOld() *typedef.ValueWithToken
	GiveOld(_ *typedef.ValueWithToken)
	ReleaseToken(_ uint64)
}

type Generator struct {
	logger            *zap.Logger
	table             *typedef.Table
	routingKeyCreator *routingkey.Creator
	r                 *rand.Rand
	wakeUpSignal      <-chan struct{}
	idxFunc           DistributionFunc
	partitions        Partitions
	partitionsConfig  typedef.PartitionRangeConfig
	partitionCount    uint64
	seed              uint64

	cntCreated uint64
	cntEmitted uint64
}

func (g *Generator) PartitionCount() uint64 {
	return g.partitionCount
}

type Generators []*Generator

func (g Generators) StartAll(stopFlag *stop.Flag) {
	for _, gen := range g {
		gen.Start(stopFlag)
	}
}

type Config struct {
	PartitionsDistributionFunc DistributionFunc
	PartitionsRangeConfig      typedef.PartitionRangeConfig
	PartitionsCount            uint64
	Seed                       uint64
	PkUsedBufferSize           uint64
}

func NewGenerator(table *typedef.Table, config *Config, logger *zap.Logger) *Generator {
	wakeUpSignal := make(chan struct{})
	return &Generator{
		partitions:       NewPartitions(int(config.PartitionsCount), int(config.PkUsedBufferSize), wakeUpSignal),
		partitionCount:   config.PartitionsCount,
		table:            table,
		partitionsConfig: config.PartitionsRangeConfig,
		seed:             config.Seed,
		idxFunc:          config.PartitionsDistributionFunc,
		logger:           logger,
		wakeUpSignal:     wakeUpSignal,
	}
}

func (g *Generator) Get() *typedef.ValueWithToken {
	return g.partitions.GetPartitionForToken(g.idxFunc()).get()
}

// GetOld returns a previously used value and token or a new if
// the old queue is empty.
func (g *Generator) GetOld() *typedef.ValueWithToken {
	return g.partitions.GetPartitionForToken(g.idxFunc()).getOld()
}

// GiveOld returns the supplied value for later reuse unless
func (g *Generator) GiveOld(v *typedef.ValueWithToken) {
	g.partitions.GetPartitionForToken(TokenIndex(v.Token)).giveOld(v)
}

// ReleaseToken removes the corresponding token from the in-flight tracking.
func (g *Generator) ReleaseToken(token uint64) {
	g.partitions.GetPartitionForToken(TokenIndex(token)).releaseToken(token)
}

func (g *Generator) Start(stopFlag *stop.Flag) {
	go func() {
		g.logger.Info("starting partition key generation loop")
		g.routingKeyCreator = &routingkey.Creator{}
		g.r = rand.New(rand.NewSource(g.seed))
		defer g.partitions.CloseAll()
		for {
			g.fillAllPartitions()
			select {
			case <-stopFlag.SignalChannel():
				g.logger.Debug("stopping partition key generation loop",
					zap.Uint64("keys_created", g.cntCreated),
					zap.Uint64("keys_emitted", g.cntEmitted))
				return
			case <-g.wakeUpSignal:
			}
		}
	}()
}

// fillAllPartitions guarantees that each partition was tested to be full
// at least once since the function started and before it ended.
// In other words no partition will be starved.
func (g *Generator) fillAllPartitions() {
	pFilled := make([]bool, len(g.partitions))
	allFilled := func() bool {
		for _, filled := range pFilled {
			if !filled {
				return false
			}
		}
		return true
	}
	for {
		values := g.createPartitionKeyValues()
		token, err := g.routingKeyCreator.GetHash(g.table, values)
		if err != nil {
			g.logger.Panic(errors.Wrap(err, "failed to get primary key hash").Error())
		}
		g.cntCreated++
		idx := token % g.partitionCount
		partition := g.partitions[idx]
		select {
		case partition.values <- &typedef.ValueWithToken{Token: token, Value: values}:
			g.cntEmitted++
		default:
			if !pFilled[idx] {
				pFilled[idx] = true
				if allFilled() {
					return
				}
			}
		}
	}
}

func (g *Generator) createPartitionKeyValues() []interface{} {
	values := make([]interface{}, 0, g.table.PartitionKeysLenValues())
	for _, pk := range g.table.PartitionKeys {
		values = append(values, pk.Type.GenValue(g.r, &g.partitionsConfig)...)
	}
	return values
}

func CreatePkColumns(cnt int, prefix string) typedef.Columns {
	var cols typedef.Columns
	for i := 0; i < cnt; i++ {
		cols = append(cols, &typedef.ColumnDef{
			Name: GenColumnName(prefix, i),
			Type: typedef.TYPE_INT,
		})
	}
	return cols
}
