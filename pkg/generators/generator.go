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
	"math/rand/v2"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/metrics"
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

type Interface interface {
	Get() typedef.ValueWithToken
	GetOld() typedef.ValueWithToken
	GiveOlds(...typedef.ValueWithToken)
	ReleaseToken(uint64)
}

type Generator struct {
	logger            *zap.Logger
	table             *typedef.Table
	routingKeyCreator *routingkey.Creator
	r                 *rand.Rand
	idxFunc           distributions.DistributionFunc
	cancel            context.CancelFunc
	oldValuesMetrics  metrics.ChannelMetrics
	valuesMetrics     metrics.ChannelMetrics
	partitions        Partitions
	partitionsConfig  typedef.PartitionRangeConfig
	wg                sync.WaitGroup
	partitionCount    uint64
}

func (g *Generator) PartitionCount() uint64 {
	return g.partitionCount
}

type Config struct {
	PartitionsDistributionFunc distributions.DistributionFunc
	PartitionsRangeConfig      typedef.PartitionRangeConfig
	PartitionsCount            uint64
	Seed                       uint64
	PkUsedBufferSize           uint64
}

func NewGenerator(
	base context.Context,
	table *typedef.Table,
	config Config,
	logger *zap.Logger,
	source rand.Source,
) *Generator {
	ctx, cancel := context.WithCancel(base)

	metrics.GeneratorPartitionSize.WithLabelValues(table.Name).Set(float64(config.PartitionsCount))
	metrics.GeneratorBufferSize.WithLabelValues(table.Name).Set(float64(config.PkUsedBufferSize))

	g := &Generator{
		cancel:            cancel,
		logger:            logger,
		table:             table,
		routingKeyCreator: &routingkey.Creator{},
		r:                 rand.New(source),
		idxFunc:           config.PartitionsDistributionFunc,
		partitions:        NewPartitions(config.PartitionsCount, config.PkUsedBufferSize),
		partitionsConfig:  config.PartitionsRangeConfig,
		partitionCount:    config.PartitionsCount,
		oldValuesMetrics:  metrics.NewChannelMetrics[typedef.ValueWithToken]("generator", table.Name+"_old_values", config.PkUsedBufferSize),
		valuesMetrics:     metrics.NewChannelMetrics[typedef.ValueWithToken]("generator", table.Name+"_values", config.PkUsedBufferSize),
	}

	go g.start(ctx)

	return g
}

func (g *Generator) Get() typedef.ValueWithToken {
	targetPart := g.GetPartitionForToken(uint64(g.idxFunc()))
	for targetPart.Stale() {
		targetPart = g.GetPartitionForToken(uint64(g.idxFunc()))
	}

	v := targetPart.get()
	g.valuesMetrics.Dec(v)
	return v
}

func (g *Generator) GetPartitionForToken(token uint64) *Partition {
	return &g.partitions[g.shardOf(token)]
}

// GetOld returns a previously used value and token or a new if
// the old queue is empty.
func (g *Generator) GetOld() typedef.ValueWithToken {
	targetPart := g.GetPartitionForToken(uint64(g.idxFunc()))
	for targetPart.Stale() {
		targetPart = g.GetPartitionForToken(uint64(g.idxFunc()))
	}
	v, old := targetPart.getOld()
	if old {
		g.oldValuesMetrics.Dec(v)
	} else {
		g.valuesMetrics.Dec(v)
	}
	return v
}

// GiveOlds returns the supplied values for later reuse unless
func (g *Generator) GiveOlds(tokens ...typedef.ValueWithToken) {
	for _, token := range tokens {
		if g.GetPartitionForToken(token.Token).giveOld(token) {
			g.oldValuesMetrics.Inc(token)
		} else {
			metrics.GeneratorDroppedValues.WithLabelValues(g.table.Name, "old").Inc()
		}
	}
}

// ReleaseToken removes the corresponding token from the in-flight tracking.
func (g *Generator) ReleaseToken(token uint64) {
	g.GetPartitionForToken(token).releaseToken(token)
}

func (g *Generator) start(ctx context.Context) {
	g.wg.Add(1)
	defer g.wg.Done()

	g.logger.Info("starting partition key generation loop")
	g.fillAllPartitions(ctx)
}

func (g *Generator) FindAndMarkStalePartitions() {
	stalePartitions := 0
	nonStale := make([]bool, g.partitionCount)
	for range g.partitionCount * 100 {
		token, _, err := g.createPartitionKeyValues()
		if err != nil {
			g.logger.Panic("failed to get primary key hash", zap.Error(err))
		}

		nonStale[g.shardOf(token)] = true
	}

	for idx, v := range nonStale {
		if !v {
			stalePartitions++
			if err := g.partitions[idx].MarkStale(); err != nil {
				g.logger.Panic("failed to mark partition as stale", zap.Error(err))
			}
		}
	}

	g.logger.Info("marked stale partitions",
		zap.Int("stale_partitions", stalePartitions),
		zap.Int("total_partitions", len(g.partitions)),
	)

	metrics.StalePartitions.WithLabelValues(g.table.Name).Set(float64(stalePartitions))
}

const sleepTime = 5 * time.Millisecond

// fillAllPartitions guarantees that each partition was tested to be full
// at least once since the function started and before it ended.
// In other words no partition will be starved.
func (g *Generator) fillAllPartitions(ctx context.Context) {
	var dropped uint64

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		running := metrics.ExecutionTimeStart("value_generation")

		token, values, err := g.createPartitionKeyValues()
		if err != nil {
			g.logger.Panic("failed to get primary key hash", zap.Error(err))
		}

		partition := &g.partitions[g.shardOf(token)]
		if partition.Stale() || partition.inFlight.Has(token) {
			return
		}

		v := typedef.ValueWithToken{Token: token, Value: values}
		pushed := !partition.push(v).Full
		running.Record()

		if pushed {
			g.valuesMetrics.Inc(v)
			continue
		}

		if dropped%g.partitionCount == 0 {
			fullPartitions := g.partitions.FullValues()
			if fullPartitions-g.partitionCount <= 0 {
				metrics.GeneratorFilledPartitions.WithLabelValues(g.table.Name).Set(float64(fullPartitions))
				time.Sleep(sleepTime)
			}
		}
	}
}

func (g *Generator) shardOf(token uint64) int {
	return int(token % g.partitionCount)
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

func (g *Generator) Close() error {
	g.cancel()
	g.wg.Wait()

	_ = g.partitions.Close()
	return nil
}
