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
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
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
	Get(context.Context) typedef.PartitionKeys
	GetOld(context.Context) typedef.PartitionKeys
	GiveOlds(context.Context, ...typedef.PartitionKeys)
	ReleaseToken(uint64)
}

type Generator struct {
	oldValuesMetrics  metrics.ChannelMetrics
	oldDroppedValues  prometheus.Counter
	valuesMetrics     metrics.ChannelMetrics
	r                 *rand.Rand
	idxFunc           distributions.DistributionFunc
	cancel            context.CancelFunc
	logger            *zap.Logger
	routingKeyCreator *routingkey.Creator
	table             *typedef.Table
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
	table *typedef.Table,
	config Config,
	logger *zap.Logger,
	src *rand.ChaCha8,
) *Generator {
	ctx, cancel := context.WithCancel(context.Background())

	newSrc := [32]byte{}
	_, _ = src.Read(newSrc[:])
	rnd := rand.New(rand.NewChaCha8(newSrc))

	metrics.GeneratorPartitionSize.WithLabelValues(table.Name).Set(float64(config.PartitionsCount))
	metrics.GeneratorBufferSize.WithLabelValues(table.Name).Set(float64(config.PkUsedBufferSize))

	g := &Generator{
		cancel:            cancel,
		logger:            logger,
		table:             table,
		routingKeyCreator: routingkey.New(table),
		r:                 rnd,
		idxFunc:           config.PartitionsDistributionFunc,
		partitions:        NewPartitions(config.PartitionsCount, config.PkUsedBufferSize),
		partitionsConfig:  config.PartitionsRangeConfig,
		partitionCount:    config.PartitionsCount,
		oldValuesMetrics:  metrics.NewChannelMetrics("generator", table.Name+"_old_values"),
		valuesMetrics:     metrics.NewChannelMetrics("generator", table.Name+"_values"),
		oldDroppedValues:  metrics.GeneratorDroppedValues.WithLabelValues(table.Name, "old"),
	}

	go g.start(ctx)

	return g
}

func (g *Generator) Get(ctx context.Context) typedef.PartitionKeys {
	targetPart := g.GetPartitionForToken(uint64(g.idxFunc()))
	for targetPart.Stale() {
		targetPart = g.GetPartitionForToken(uint64(g.idxFunc()))
	}

	v := targetPart.get(ctx)
	g.valuesMetrics.Dec()
	return v
}

func (g *Generator) GetPartitionForToken(token uint64) *Partition {
	return &g.partitions[g.shardOf(token)]
}

// GetOld returns a previously used value and token or a new if
// the old queue is empty.
func (g *Generator) GetOld(ctx context.Context) typedef.PartitionKeys {
	targetPart := g.GetPartitionForToken(uint64(g.idxFunc()))
	for targetPart.Stale() {
		targetPart = g.GetPartitionForToken(uint64(g.idxFunc()))
	}
	v, exists := targetPart.getOld(ctx)
	if exists {
		g.oldValuesMetrics.Dec()
	}

	return v
}

// GiveOlds returns the supplied values for later reuse unless
func (g *Generator) GiveOlds(ctx context.Context, tokens ...typedef.PartitionKeys) {
	for _, token := range tokens {
		if g.GetPartitionForToken(token.Token).giveOld(ctx, token) {
			g.oldValuesMetrics.Inc()
		} else {
			g.oldDroppedValues.Inc()
		}
	}
}

// ReleaseToken removes the corresponding token from the in-flight tracking.
func (g *Generator) ReleaseToken(token uint64) {
	g.GetPartitionForToken(token).releaseToken(token)
}

func (g *Generator) start(ctx context.Context) {
	stopped := &atomic.Bool{}
	g.wg.Add(1)
	defer g.wg.Done()

	go func() {
		<-ctx.Done()
		stopped.Store(true)
		g.logger.Info("stopping partition key generation loop")
	}()

	g.logger.Info("starting partition key generation loop")
	g.fillAllPartitions(stopped)
}

func (g *Generator) FindAndMarkStalePartitions() {
	nonStale := make([][]typedef.PartitionKeys, g.partitionCount)
	for range g.partitionCount * 100 {
		token, values, err := g.createPartitionKeyValues()
		if err != nil {
			g.logger.Panic("failed to get primary key hash", zap.Error(err))
		}

		nonStale[g.shardOf(token)] = append(nonStale[g.shardOf(token)], typedef.PartitionKeys{
			Values: values,
			Token:  token,
		})
	}

	stalePartitions := 0
	for idx, v := range nonStale {
		if len(v) == 0 {
			stalePartitions++
			if err := g.partitions[idx].MarkStale(); err != nil {
				g.logger.Panic("failed to mark partition as stale", zap.Error(err))
			}
		} else {
			for _, item := range v {
				if g.partitions[idx].push(item) {
					g.valuesMetrics.Inc()
				}
			}
		}
	}

	g.logger.Info("marked stale partitions",
		zap.Int("stale_partitions", stalePartitions),
		zap.Int("total_partitions", len(g.partitions)),
	)

	metrics.StalePartitions.WithLabelValues(g.table.Name).Set(float64(stalePartitions))
}

// fillAllPartitions guarantees that each partition was tested to be full
// at least once since the function started and before it ended.
// In other words, no partition will be starved.
func (g *Generator) fillAllPartitions(stopped *atomic.Bool) {
	var dropped uint64

	running := metrics.ExecutionTimeStart("value_generation_" + g.table.Name)

	for !stopped.Load() {
		token, values, err := g.createPartitionKeyValues()
		if err != nil {
			g.logger.Error("failed to get primary key hash", zap.Error(err))
			running.Record()
			continue
		}

		partition := &g.partitions[g.shardOf(token)]
		if partition.Stale() || partition.inFlight.Has(token) {
			running.Record()
			continue
		}

		v := typedef.PartitionKeys{Token: token, Values: values}
		pushed := partition.push(v)
		running.Record()

		if pushed {
			g.valuesMetrics.Inc()
			continue
		}

		dropped++
	}
}

func (g *Generator) shardOf(token uint64) int {
	return int(token % g.partitionCount)
}

func (g *Generator) createPartitionKeyValues(r ...*rand.Rand) (uint64, map[string][]any, error) {
	rnd := g.r

	if len(r) > 0 && r[0] != nil {
		rnd = r[0]
	}

	values := make(map[string][]any, g.table.PartitionKeys.Len())

	for _, pk := range g.table.PartitionKeys {
		values[pk.Name] = append(values[pk.Name], pk.Type.GenValue(rnd, &g.partitionsConfig)...)
	}

	token, err := g.routingKeyCreator.GetHash(values)
	if err != nil {
		return 0, nil, err
	}

	return token, values, nil
}

func (g *Generator) Close() error {
	g.cancel()
	g.wg.Wait()

	return g.partitions.Close()
}
