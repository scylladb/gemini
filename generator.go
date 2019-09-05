package gemini

import (
	"github.com/scylladb/gemini/inflight"
	"github.com/scylladb/gemini/murmur"
	"go.uber.org/zap"
	"golang.org/x/exp/rand"
	"gopkg.in/tomb.v2"
)

// TokenIndex represents the position of a token in the token ring.
// A token index is translated to a token by a generator. If the generator
// preserves the exact position, then the token index becomes the token;
// otherwise token index represents an approximation of the token.
//
// We use a token index approach, because our generators actually generate
// partition keys, and map them to tokens. The generators, therefore, do
// not populate the full token ring space. With token index, we can
// approximate different token distributions from a sparse set of tokens.
type TokenIndex uint64

type DistributionFunc func() TokenIndex

type Generator struct {
	partitions       []*Partition
	inFlight         inflight.InFlight
	partitionCount   uint64
	table            *Table
	partitionsConfig PartitionRangeConfig
	seed             uint64
	idxFunc          DistributionFunc
	t                *tomb.Tomb
	logger           *zap.Logger
}

type GeneratorConfig struct {
	PartitionsRangeConfig      PartitionRangeConfig
	PartitionsCount            uint64
	PartitionsDistributionFunc DistributionFunc
	Seed                       uint64
	PkUsedBufferSize           uint64
}

func NewGenerator(table *Table, config *GeneratorConfig, logger *zap.Logger) *Generator {
	t := &tomb.Tomb{}
	partitions := make([]*Partition, config.PartitionsCount)
	for i := 0; i < len(partitions); i++ {
		partitions[i] = &Partition{
			values:    make(chan ValueWithToken, config.PkUsedBufferSize),
			oldValues: make(chan ValueWithToken, config.PkUsedBufferSize),
			t:         t,
		}
	}
	gs := &Generator{
		partitions:       partitions,
		inFlight:         inflight.New(),
		partitionCount:   config.PartitionsCount,
		table:            table,
		partitionsConfig: config.PartitionsRangeConfig,
		seed:             config.Seed,
		idxFunc:          config.PartitionsDistributionFunc,
		t:                t,
		logger:           logger,
	}
	gs.start()
	return gs
}

func (g Generator) Get() (ValueWithToken, bool) {
	select {
	case <-g.t.Dying():
		return emptyValueWithToken, false
	default:
	}
	partition := g.partitions[uint64(g.idxFunc())%g.partitionCount]
	for {
		v := partition.pick()
		if g.inFlight.AddIfNotPresent(v.Token) {
			return v, true
		}
	}
}

// GetOld returns a previously used value and token or a new if
// the old queue is empty.
func (g Generator) GetOld() (ValueWithToken, bool) {
	select {
	case <-g.t.Dying():
		return emptyValueWithToken, false
	default:
	}
	return g.partitions[uint64(g.idxFunc())%g.partitionCount].getOld()
}

type ValueWithToken struct {
	Token uint64
	Value Value
}

// GiveOld returns the supplied value for later reuse unless the value
// is empty in which case it removes the corresponding token from the
// in-flight tracking.
func (g *Generator) GiveOld(v ValueWithToken) {
	select {
	case <-g.t.Dying():
		return
	default:
	}
	partition := g.partitions[v.Token%g.partitionCount]
	if len(v.Value) == 0 {
		g.inFlight.Delete(v.Token)
		return
	}
	partition.giveOld(v)
}

func (g *Generator) Stop() {
	g.t.Kill(nil)
	_ = g.t.Wait()
}

func (g *Generator) start() {
	g.t.Go(func() error {
		g.logger.Info("starting partition key generation loop")
		routingKeyCreator := &RoutingKeyCreator{}
		r := rand.New(rand.NewSource(g.seed))
		var (
			cntCreated uint64
			cntEmitted uint64
		)
		for {
			values := g.createPartitionKeyValues(r)
			hash := hash(routingKeyCreator, g.table, values)
			idx := hash % g.partitionCount
			partition := g.partitions[idx]
			cntCreated++
			select {
			case partition.values <- ValueWithToken{Token: hash, Value: values}:
				cntEmitted++
			case <-g.t.Dying():
				g.logger.Info("stopping partition key generation loop",
					zap.Uint64("keys_created", cntCreated),
					zap.Uint64("keys_emitted", cntEmitted))
				return nil
			default:
			}
		}
	})
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
