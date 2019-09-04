package gemini

import (
	"sync"

	"github.com/scylladb/gemini/inflight"

	"github.com/scylladb/gemini/murmur"
	"go.uber.org/zap"
	"golang.org/x/exp/rand"
)

type DistributionFunc func() uint64

type Generator struct {
	sources          []*Source
	inFlight         inflight.InFlight
	size             uint64
	distributionSize uint64
	table            *Table
	partitionsConfig PartitionRangeConfig
	seed             uint64
	idxFunc          func() uint64
	doneCh           chan struct{}
	logger           *zap.Logger
}

type GeneratorConfig struct {
	Partitions       PartitionRangeConfig
	DistributionSize uint64
	DistributionFunc DistributionFunc
	Size             uint64
	Seed             uint64
	PkUsedBufferSize uint64
}

func NewGenerator(table *Table, config *GeneratorConfig, logger *zap.Logger) *Generator {
	sources := make([]*Source, config.Size)
	for i := uint64(0); i < config.Size; i++ {
		done := &sync.WaitGroup{}
		done.Add(1)
		sources[i] = &Source{
			values:    make(chan ValueWithToken, config.PkUsedBufferSize),
			oldValues: make(chan ValueWithToken, config.PkUsedBufferSize),
		}
	}
	gs := &Generator{
		sources:          sources,
		inFlight:         inflight.NewConcurrent(),
		size:             config.Size,
		distributionSize: config.DistributionSize,
		table:            table,
		partitionsConfig: config.Partitions,
		seed:             config.Seed,
		idxFunc:          config.DistributionFunc,
		doneCh:           make(chan struct{}, 1),
		logger:           logger,
	}
	gs.start()
	return gs
}

func (g Generator) Get() (ValueWithToken, bool) {
	source := g.sources[g.idxFunc()%g.size]
	for {
		v := source.pick()
		if g.inFlight.AddIfNotPresent(v.Token) {
			return v, true
		}
	}
}

// GetOld returns a previously used value and token or a new if
// the old queue is empty.
func (g Generator) GetOld() (ValueWithToken, bool) {
	return g.sources[g.idxFunc()%g.size].getOld()
}

type ValueWithToken struct {
	Token uint64
	Value Value
}

// GiveOld returns the supplied value for later reuse unless the value
// is empty in which case it removes the corresponding token from the
// in-flight tracking.
func (g *Generator) GiveOld(v ValueWithToken) {
	source := g.sources[v.Token%g.size]
	if len(v.Value) == 0 {
		g.inFlight.Delete(v.Token)
		return
	}
	source.giveOld(v)
}

func (g *Generator) Stop() {
	g.doneCh <- struct{}{}
	for _, s := range g.sources {
		close(s.oldValues)
	}
}

func (g *Generator) start() {
	go func() {
		g.logger.Info("starting partition key generation loop")
		routingKeyCreator := &RoutingKeyCreator{}
		r := rand.New(rand.NewSource(g.seed))
		for {
			values := g.createPartitionKeyValues(r)
			hash := hash(routingKeyCreator, g.table, values)
			idx := hash % g.size
			source := g.sources[idx]
			select {
			case source.values <- ValueWithToken{Token: hash, Value: values}:
			case <-g.doneCh:
				g.logger.Info("stopping partition key generation loop")
				return
			default:
			}
		}
	}()
}

func (g *Generator) createPartitionKeyValues(r *rand.Rand) []interface{} {
	var values []interface{}
	for _, pk := range g.table.PartitionKeys {
		values = append(values, pk.Type.GenValue(r, g.partitionsConfig)...)
	}
	return values
}

func hash(rkc *RoutingKeyCreator, t *Table, values []interface{}) uint64 {
	b, _ := rkc.CreateRoutingKey(t, values)
	return uint64(murmur.Murmur3H1(b))
}
