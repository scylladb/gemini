package gemini

import (
	"github.com/scylladb/gemini/murmur"
	"golang.org/x/exp/rand"
)

type Value []interface{}

type source struct {
	newValues chan Value
	oldValues chan Value
}

type Generators struct {
	generators       []*source
	size             uint64
	table            *Table
	partitionsConfig PartitionRangeConfig
	seed             uint64
	done             chan struct{}
}

type GeneratorsConfig struct {
	Table      *Table
	Partitions *PartitionRangeConfig
	Size       uint64
	Seed       uint64
}

func NewGenerator(config *GeneratorsConfig) *Generators {
	generators := make([]*source, config.Size)
	for i := uint64(0); i < config.Size; i++ {
		generators[i] = &source{
			newValues: make(chan Value, 10000),
			oldValues: make(chan Value, 20000),
		}
	}
	gs := &Generators{
		generators: generators,
		size:       config.Size,
		table:      config.Table,
		seed:       config.Seed,
		done:       make(chan struct{}, 1),
	}
	gs.start()
	return gs
}

func (gs Generators) Stop() {
	gs.done <- struct{}{}
}

func (gs Generators) GetNew(idx int) <-chan Value {
	return gs.generators[idx].newValues
}

func (gs Generators) GetOld(idx int) chan Value {
	return gs.generators[idx].oldValues
}

func (gs *Generators) start() {
	go func() {
		routingKeyCreator := &RoutingKeyCreator{}
		r := rand.New(rand.NewSource(gs.seed))
		for {
			select {
			case <-gs.done:
				return
			default:
				var values []interface{}
				for _, pk := range gs.table.PartitionKeys {
					values = append(values, pk.Type.GenValue(r, gs.partitionsConfig)...)
				}
				b, _ := routingKeyCreator.CreateRoutingKey(gs.table, values)
				hash := uint64(murmur.Murmur3H1(b))
				g := gs.generators[hash%gs.size]
				g.newValues <- Value(values)
			}
		}
	}()
}
