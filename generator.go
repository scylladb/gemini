package gemini

import (
	"time"

	"go.uber.org/zap"

	"github.com/scylladb/gemini/murmur"
	"github.com/scylladb/go-set/u64set"
	"golang.org/x/exp/rand"
)

type DistributionFunc func() uint64

type Source struct {
	values    []Value
	idxFunc   func() uint64
	oldValues chan Value
}

func (s *Source) Get() (Value, bool) {
	v := s.pick()
	values := make([]interface{}, len(v))
	// Make a copy to allow callers to work with the slice directly
	copy(values, v)
	select {
	case s.oldValues <- v:
	default:
		// Old source is full, just drop the value
	}
	return values, true
}

func (s *Source) GetOld() (Value, bool) {
	select {
	case v, ok := <-s.oldValues:
		return v, ok
	default:
		// There are no old values so we generate a new
		return s.pick(), true
	}
}

func (s *Source) pick() Value {
	return s.values[s.idxFunc()]
}

type Generators struct {
	generators       []*Source
	size             uint64
	distributionSize uint64
	table            *Table
	partitionsConfig PartitionRangeConfig
	seed             uint64
	logger           *zap.Logger
}

type GeneratorsConfig struct {
	Partitions       PartitionRangeConfig
	DistributionSize uint64
	DistributionFunc DistributionFunc
	Size             uint64
	Seed             uint64
	PkUsedBufferSize uint64
}

func NewGenerator(table *Table, config *GeneratorsConfig, logger *zap.Logger) *Generators {
	generators := make([]*Source, config.Size)
	for i := uint64(0); i < config.Size; i++ {
		generators[i] = &Source{
			values:    make([]Value, 0, config.DistributionSize),
			idxFunc:   config.DistributionFunc,
			oldValues: make(chan Value, config.PkUsedBufferSize),
		}
	}
	gs := &Generators{
		generators:       generators,
		size:             config.Size,
		distributionSize: config.DistributionSize,
		table:            table,
		partitionsConfig: config.Partitions,
		seed:             config.Seed,
		logger:           logger,
	}
	gs.create()
	return gs
}

func (gs Generators) Get(idx int) *Source {
	return gs.generators[idx]
}

func (gs *Generators) create() {
	gs.logger.Info("generating partition keys, this can take a while", zap.Uint64("distribution_size", gs.distributionSize))
	start := time.Now()
	routingKeyCreator := &RoutingKeyCreator{}
	r := rand.New(rand.NewSource(gs.seed))
	fullSources := u64set.New()
	for {
		values := gs.createPartitionKeyValues(r)
		hash := hash(routingKeyCreator, gs.table, values)
		idx := hash % gs.size
		source := gs.generators[idx]
		if fullSources.Has(idx) {
			continue
		}
		if uint64(len(source.values)) < gs.distributionSize {
			source.values = append(source.values, Value(values))
		}
		if uint64(len(source.values)) == gs.distributionSize {
			gs.logger.Debug("partial generation", zap.Uint64("source", idx), zap.Int("size", len(source.values)))
			fullSources.Add(idx)
		}
		if fullSources.Size() == len(gs.generators) {
			gs.logger.Info("finished generating partition ids", zap.Duration("duration", time.Since(start)))
			break
		}
	}
}

func (gs *Generators) createPartitionKeyValues(r *rand.Rand) []interface{} {
	var values []interface{}
	for _, pk := range gs.table.PartitionKeys {
		values = append(values, pk.Type.GenValue(r, gs.partitionsConfig)...)
	}
	return values
}

func hash(rkc *RoutingKeyCreator, t *Table, values []interface{}) uint64 {
	b, _ := rkc.CreateRoutingKey(t, values)
	return uint64(murmur.Murmur3H1(b))
}
