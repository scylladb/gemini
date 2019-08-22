package gemini

import (
	"sync"
	"time"

	"github.com/scylladb/gemini/murmur"
	"github.com/scylladb/go-set/u64set"
	"go.uber.org/zap"
	"golang.org/x/exp/rand"
)

type DistributionFunc func() uint64

type Source struct {
	values    []ValueWithToken
	idxFunc   func() uint64
	oldValues chan ValueWithToken
	inFlight  syncU64set
}

//Get returns a new value and ensures that it's corresponding token
//is not already in-flight.
func (s *Source) Get() (ValueWithToken, bool) {
	for {
		v := s.pick()
		if s.inFlight.addIfNotPresent(v.Token) {
			return v, true
		}
	}
}

//GetOld returns a previously used value and token or a new if
//the old queue is empty.
func (s *Source) GetOld() (ValueWithToken, bool) {
	select {
	case v, ok := <-s.oldValues:
		return v, ok
	default:
		// There are no old values so we generate a new
		return s.Get()
	}
}

// GiveOld returns the supplied value for later reuse unless the value
//is empty in which case it removes the corresponding token from the
// in-flight tracking.
func (s *Source) GiveOld(v ValueWithToken) {
	if len(v.Value) == 0 {
		s.inFlight.delete(v.Token)
		return
	}
	select {
	case s.oldValues <- v:
	default:
		// Old source is full, just drop the value
	}
}

func (s *Source) pick() ValueWithToken {
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
			values:    make([]ValueWithToken, 0, config.DistributionSize),
			idxFunc:   config.DistributionFunc,
			oldValues: make(chan ValueWithToken, config.PkUsedBufferSize),
			inFlight:  syncU64set{pks: u64set.New()},
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

type ValueWithToken struct {
	Token uint64
	Value Value
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
			source.values = append(source.values, ValueWithToken{Token: hash, Value: values})
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

type syncU64set struct {
	pks *u64set.Set
	mu  sync.Mutex
}

func (s *syncU64set) delete(v uint64) bool {
	s.mu.Lock()
	_, found := s.pks.Pop2()
	s.mu.Unlock()
	return found
}

func (s *syncU64set) addIfNotPresent(v uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pks.Has(v) {
		return false
	}
	s.pks.Add(v)
	return true
}
