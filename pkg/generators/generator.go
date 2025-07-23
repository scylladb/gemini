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
	"errors"
	"math/rand/v2"
	"sync"

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
	Get(context.Context) (typedef.PartitionKeys, error)
	GetOld(context.Context) (typedef.PartitionKeys, error)
	GiveOlds(context.Context, ...typedef.PartitionKeys)
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
	partitions        *Partitions
	partitionsConfig  typedef.PartitionRangeConfig
	wg                sync.WaitGroup
	partitionCount    int
}

func (g *Generator) PartitionCount() int {
	return g.partitionCount
}

type Config struct {
	PartitionsDistributionFunc distributions.DistributionFunc
	PartitionsRangeConfig      typedef.PartitionRangeConfig
	PartitionsCount            int
	PkUsedBufferSize           int
	Seed                       uint64
}

func NewGenerator(
	table *typedef.Table,
	config Config,
	logger *zap.Logger,
	src *rand.ChaCha8,
) *Generator {
	ctx, cancel := context.WithCancel(context.Background())
	rnd := rand.New(src)

	metrics.GeminiInformation.WithLabelValues("partition_count_" + table.Name).Set(float64(config.PartitionsCount))
	metrics.GeminiInformation.WithLabelValues("partition_buffer_size_" + table.Name).Set(float64(config.PkUsedBufferSize))

	wakeup := make(chan struct{}, 1)
	g := &Generator{
		cancel:            cancel,
		logger:            logger,
		table:             table,
		routingKeyCreator: routingkey.New(table),
		r:                 rnd,
		idxFunc:           config.PartitionsDistributionFunc,
		partitions:        NewPartitions(config.PartitionsCount, config.PkUsedBufferSize, wakeup),
		partitionsConfig:  config.PartitionsRangeConfig,
		partitionCount:    config.PartitionsCount,
		oldValuesMetrics:  metrics.NewChannelMetrics("generator", table.Name+"_old_values"),
		valuesMetrics:     metrics.NewChannelMetrics("generator", table.Name+"_values"),
		oldDroppedValues:  metrics.GeneratorDroppedValues.WithLabelValues(table.Name, "old"),
	}

	g.start(ctx, wakeup)

	return g
}

func (g *Generator) Get(ctx context.Context) (typedef.PartitionKeys, error) {
	targetPart := g.GetPartitionForToken(g.idxFunc())
	for targetPart.Stale() {
		targetPart = g.GetPartitionForToken(g.idxFunc())
	}

	v, err := targetPart.get(ctx)
	if err != nil {
		g.valuesMetrics.Dec()
	}
	return v, err
}

func (g *Generator) GetPartitionForToken(token uint32) *Partition {
	return g.partitions.Get(g.shardOf(token))
}

// GetOld returns a previously used value and token or a new if
// the old queue is empty.
func (g *Generator) GetOld(ctx context.Context) (typedef.PartitionKeys, error) {
	targetPart := g.GetPartitionForToken(g.idxFunc())
	for targetPart.Stale() {
		targetPart = g.GetPartitionForToken(g.idxFunc())
	}

	v, err := targetPart.getOld(ctx)
	if err == nil {
		g.oldValuesMetrics.Dec()
	}

	return v, err
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

func (g *Generator) start(ctx context.Context, wakeup chan struct{}) {
	g.wg.Add(1)
	defer g.wg.Done()

	go func() {
		<-ctx.Done()
		g.logger.Info("stopping partition key generation loop")
	}()

	g.logger.Info("starting partition key generation loop")

	wakeup <- struct{}{}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-wakeup:
				g.fillAllPartitions()
			}
		}
	}()
}

func (g *Generator) FindAndMarkStalePartitions() {
	nonStale := make([][]typedef.PartitionKeys, g.partitionCount)
	for range g.partitionCount * 100 {
		token, values, err := g.createPartitionKeyValues()
		if err != nil {
			g.logger.Panic("failed to get primary key hash", zap.Error(err))
		}

		nonStale[g.shardOf(token)] = append(nonStale[g.shardOf(token)], typedef.PartitionKeys{
			Values: typedef.NewValuesFromMap(values),
			Token:  token,
		})
	}

	stalePartitions := 0
	for idx, v := range nonStale {
		if len(v) == 0 {
			stalePartitions++
			if err := g.partitions.Get(idx).MarkStale(); err != nil {
				g.logger.Panic("failed to mark partition as stale", zap.Error(err))
			}
		} else {
			for _, item := range v {
				if g.partitions.Get(idx).push(item) {
					g.valuesMetrics.Inc()
				}
			}
		}
	}

	g.logger.Info("marked stale partitions",
		zap.Int("stale_partitions", stalePartitions),
		zap.Int("total_partitions", g.partitions.Len()),
	)

	metrics.GeminiInformation.WithLabelValues("stale_partition_" + g.table.Name).Set(float64(stalePartitions))
}

var errFullPartitions = errors.New("all partitions are full, cannot fill more")

// fillAllPartitions guarantees that each partition was tested to be full
// at least once since the function started and before it ended.
// In other words, no partition will be starved.
func (g *Generator) fillAllPartitions() {
	dropped := metrics.GeneratorDroppedValues.WithLabelValues("generator_"+g.table.Name, "new")
	executionDuration := metrics.ExecutionTimeStart("generator_" + g.table.Name + "_new")
	pFilled := make([]bool, g.partitions.Len())
	allFilled := func() bool {
		for _, filled := range pFilled {
			if !filled {
				return false
			}
		}
		return true
	}

	for {
		err := executionDuration.RunFuncE(func() error {
			token, values, err := g.createPartitionKeyValues()
			if err != nil {
				g.logger.Error("failed to get primary key hash", zap.Error(err))
				return nil
			}

			idx := g.shardOf(token)
			partition := g.partitions.Get(idx)
			if partition == nil { // stale partition
				return nil
			}

			v := typedef.PartitionKeys{Token: token, Values: typedef.NewValuesFromMap(values)}
			pushed := partition.push(v)

			if pushed {
				g.valuesMetrics.Inc()
				return nil
			}

			dropped.Inc()

			if !pFilled[idx] {
				pFilled[idx] = true
				if allFilled() {
					executionDuration.Record()

					return errFullPartitions
				}
			}

			return nil
		})

		if errors.Is(err, errFullPartitions) {
			return
		}
	}
}

func (g *Generator) shardOf(token uint32) int {
	return int(token % uint32(g.partitionCount))
}

func (g *Generator) createPartitionKeyValues() (uint32, map[string][]any, error) {
	values := make(map[string][]any, g.table.PartitionKeys.Len())

	for _, pk := range g.table.PartitionKeys {
		values[pk.Name] = pk.Type.GenValue(g.r, &g.partitionsConfig)
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
