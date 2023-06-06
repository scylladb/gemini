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

	"github.com/scylladb/gemini/pkg/coltypes"
	"github.com/scylladb/gemini/pkg/inflight"
	"github.com/scylladb/gemini/pkg/routingkey"
	"github.com/scylladb/gemini/pkg/testschema"
	"github.com/scylladb/gemini/pkg/typedef"

	"go.uber.org/zap"
	"golang.org/x/exp/rand"
	"golang.org/x/sync/errgroup"
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
	wakeUpSignal     chan struct{}
	ctx              context.Context
	logger           *zap.Logger
	table            *testschema.Table
	idxFunc          DistributionFunc
	partitions       Partitions
	partitionsConfig typedef.PartitionRangeConfig
	partitionCount   uint64
	seed             uint64
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

func NewGenerator(ctx context.Context, table *testschema.Table, config *Config, logger *zap.Logger) *Generator {
	partitions := make([]*Partition, config.PartitionsCount)
	for i := 0; i < len(partitions); i++ {
		partitions[i] = &Partition{
			ctx:       ctx,
			values:    make(chan *typedef.ValueWithToken, config.PkUsedBufferSize),
			oldValues: make(chan *typedef.ValueWithToken, config.PkUsedBufferSize),
			inFlight:  inflight.New(),
		}
	}
	gs := &Generator{
		ctx:              ctx,
		partitions:       partitions,
		partitionCount:   config.PartitionsCount,
		table:            table,
		partitionsConfig: config.PartitionsRangeConfig,
		seed:             config.Seed,
		idxFunc:          config.PartitionsDistributionFunc,
		logger:           logger,
		wakeUpSignal:     make(chan struct{}),
	}
	gs.start()
	return gs
}

func (g *Generator) isContextCanceled() bool {
	select {
	case <-g.ctx.Done():
		return true
	default:
		return false
	}
}

func (g *Generator) Get() *typedef.ValueWithToken {
	if g.isContextCanceled() {
		return nil
	}
	partition := g.partitions[uint64(g.idxFunc())%g.partitionCount]
	if partition.NeedMoreValues() {
		g.wakeupGenerator()
	}
	return partition.get()
}

func (g *Generator) wakeupGenerator() {
	select {
	case g.wakeUpSignal <- struct{}{}:
	default:
	}
}

func (g *Generator) waitForMoreValuesNeeded() {
	<-g.wakeUpSignal
}

// GetOld returns a previously used value and token or a new if
// the old queue is empty.
func (g *Generator) GetOld() *typedef.ValueWithToken {
	if g.isContextCanceled() {
		return nil
	}
	return g.partitions[uint64(g.idxFunc())%g.partitionCount].getOld()
}

// GiveOld returns the supplied value for later reuse unless
func (g *Generator) GiveOld(v *typedef.ValueWithToken) {
	if g.isContextCanceled() {
		return
	}
	g.partitions[v.Token%g.partitionCount].giveOld(v)
}

// ReleaseToken removes the corresponding token from the in-flight tracking.
func (g *Generator) ReleaseToken(token uint64) {
	if g.isContextCanceled() {
		return
	}
	g.partitions[token%g.partitionCount].releaseToken(token)
}

func (g *Generator) start() {
	grp, gCtx := errgroup.WithContext(g.ctx)
	g.ctx = gCtx
	for _, partition := range g.partitions {
		partition.ctx = gCtx
	}
	grp.Go(func() error {
		g.logger.Info("starting partition key generation loop")
		routingKeyCreator := &routingkey.Creator{}
		r := rand.New(rand.NewSource(g.seed))
		var (
			cntCreated uint64
			cntEmitted uint64
		)
		for {
			values := g.createPartitionKeyValues(r)
			token, err := routingKeyCreator.GetHash(g.table, values)
			if err != nil {
				g.logger.Panic(errors.Wrap(err, "failed to get primary key hash").Error())
			}
			idx := token % g.partitionCount
			partition := g.partitions[idx]
			cntCreated++
			select {
			case partition.values <- &typedef.ValueWithToken{Token: token, Value: values}:
				cntEmitted++
			case <-gCtx.Done():
				g.logger.Debug("stopping partition key generation loop",
					zap.Uint64("keys_created", cntCreated),
					zap.Uint64("keys_emitted", cntEmitted))
				return gCtx.Err()
			default:
				// This part is only get triggered when partition is full of values
				// Which is signal to stop generating
				// But if partitions values are not balanced, you can have case when one partition is full
				// While other partitions are low on values
				// To address this case before pausing generation we need to make sure that all partition are above the limit
				if g.partitions.NeedMoreValues() {
					g.waitForMoreValuesNeeded()
				}
			}
		}
	})
}

func (g *Generator) createPartitionKeyValues(r *rand.Rand) []interface{} {
	values := make([]interface{}, 0, g.table.PartitionKeysLenValues())
	for _, pk := range g.table.PartitionKeys {
		values = append(values, pk.Type.GenValue(r, &g.partitionsConfig)...)
	}
	return values
}

func CreatePkColumns(cnt int, prefix string) testschema.Columns {
	var cols testschema.Columns
	for i := 0; i < cnt; i++ {
		cols = append(cols, &testschema.ColumnDef{
			Name: GenColumnName(prefix, i),
			Type: coltypes.TYPE_INT,
		})
	}
	return cols
}
