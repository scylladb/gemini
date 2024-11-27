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
	"context"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/exp/rand"

	"github.com/scylladb/gemini/pkg/routingkey"
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

type Interface interface {
	Get() *typedef.ValueWithToken
	GetOld() *typedef.ValueWithToken
	GiveOld(_ *typedef.ValueWithToken)
	GiveOlds(_ []*typedef.ValueWithToken)
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

	cntCreated uint64
	cntEmitted uint64
}

func (g *Generator) PartitionCount() uint64 {
	return g.partitionCount
}

type Config struct {
	PartitionsDistributionFunc DistributionFunc
	PartitionsRangeConfig      typedef.PartitionRangeConfig
	PartitionsCount            uint64
	Seed                       uint64
	PkUsedBufferSize           uint64
}

func NewGenerator(table *typedef.Table, config Config, logger *zap.Logger) Generator {
	wakeUpSignal := make(chan struct{})
	return Generator{
		partitions:        NewPartitions(int(config.PartitionsCount), int(config.PkUsedBufferSize), wakeUpSignal),
		partitionCount:    config.PartitionsCount,
		table:             table,
		partitionsConfig:  config.PartitionsRangeConfig,
		idxFunc:           config.PartitionsDistributionFunc,
		logger:            logger,
		wakeUpSignal:      wakeUpSignal,
		routingKeyCreator: &routingkey.Creator{},
		r:                 rand.New(rand.NewSource(config.Seed)),
	}
}

func (g *Generator) Get() *typedef.ValueWithToken {
	targetPart := g.GetPartitionForToken(g.idxFunc())
	for targetPart.Stale() {
		targetPart = g.GetPartitionForToken(g.idxFunc())
	}
	out := targetPart.get()
	return out
}

func (g *Generator) GetPartitionForToken(token TokenIndex) *Partition {
	return g.partitions[g.shardOf(uint64(token))]
}

// GetOld returns a previously used value and token or a new if
// the old queue is empty.
func (g *Generator) GetOld() *typedef.ValueWithToken {
	targetPart := g.GetPartitionForToken(g.idxFunc())
	for targetPart.Stale() {
		targetPart = g.GetPartitionForToken(g.idxFunc())
	}
	return targetPart.getOld()
}

// GiveOld returns the supplied value for later reuse unless
func (g *Generator) GiveOld(v *typedef.ValueWithToken) {
	g.GetPartitionForToken(TokenIndex(v.Token)).giveOld(v)
}

// GiveOlds returns the supplied values for later reuse unless
func (g *Generator) GiveOlds(tokens []*typedef.ValueWithToken) {
	for _, token := range tokens {
		g.GiveOld(token)
	}
}

// ReleaseToken removes the corresponding token from the in-flight tracking.
func (g *Generator) ReleaseToken(token uint64) {
	g.GetPartitionForToken(TokenIndex(token)).releaseToken(token)
}

func (g *Generator) Start(ctx context.Context) {
	defer g.partitions.Close()
	g.logger.Info("starting partition key generation loop")
	for {
		g.fillAllPartitions(ctx)
		select {
		case <-ctx.Done():
			g.logger.Debug("stopping partition key generation loop",
				zap.Uint64("keys_created", g.cntCreated),
				zap.Uint64("keys_emitted", g.cntEmitted))
			return
		case <-g.wakeUpSignal:
		}
	}
}

func (g *Generator) FindAndMarkStalePartitions() {
	r := rand.New(rand.NewSource(10))

	for range g.partitionCount * 100 {
		token, _, err := g.createPartitionKeyValues(r)
		if err != nil {
			g.logger.Panic("failed to get primary key hash", zap.Error(err))
		}

		if err = g.partition(token).MarkStale(); err != nil {
			g.logger.Panic("failed to mark partition as stale", zap.Error(err))
		}
	}
}

// fillAllPartitions guarantees that each partition was tested to be full
// at least once since the function started and before it ended.
// In other words no partition will be starved.
func (g *Generator) fillAllPartitions(ctx context.Context) {
	pFilled := make([]bool, len(g.partitions))
	allFilled := func() bool {
		for idx, filled := range pFilled {
			if !filled {
				if g.partitions[idx].Stale() {
					continue
				}
				return false
			}
		}
		return true
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		token, values, err := g.createPartitionKeyValues()
		if err != nil {
			g.logger.Panic("failed to get primary key hash", zap.Error(err))
		}
		g.cntCreated++

		partition := g.partition(token)
		if partition.Stale() || partition.inFlight.Has(token) {
			continue
		}

		select {
		case partition.values <- &typedef.ValueWithToken{Token: token, Value: values}:
			g.cntEmitted++
		default:
			idx := g.shardOf(token)
			if !pFilled[idx] {
				pFilled[idx] = true
				if allFilled() {
					return
				}
			}
		}
	}
}

func (g *Generator) shardOf(token uint64) int {
	return int(token % g.partitionCount)
}

func (g *Generator) partition(token uint64) *Partition {
	return g.partitions[g.shardOf(token)]
}

func (g *Generator) createPartitionKeyValues(r ...*rand.Rand) (uint64, []any, error) {
	rnd := g.r

	if len(r) > 0 && r[0] != nil {
		rnd = r[0]
	}

	values := make([]any, 0, g.table.PartitionKeysLenValues())

	for _, pk := range g.table.PartitionKeys {
		values = append(values, pk.Type.GenValue(rnd, &g.partitionsConfig)...)
	}

	token, err := g.routingKeyCreator.GetHash(g.table, values)
	if err != nil {
		return 0, nil, errors.Wrap(err, "failed to get primary key hash")
	}

	return token, values, nil
}
