package gemini

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/scylladb/gemini/murmur"
	"golang.org/x/exp/rand"
)

type Source struct {
	newValues    chan Value
	oldValues    chan Value
	valueCounter *prometheus.CounterVec
	bucket       string
}

func (s *Source) Get() (Value, bool) {
	var (
		v  Value
		ok bool
	)
	select {
	case v, ok = <-s.newValues:
		if !ok {
			return nil, false
		}
	}

	// Make a copy to allow callers to work with the slice directly
	// Argument could be made that this is callers responsibility
	// but we are also sending the value on down to another user of
	// the "old" values.
	values := make([]interface{}, len(v))
	copy(values, v)
	select {
	case s.oldValues <- v:
	default:
		// If the channel is full i.e.
		// the validators are slower or not started yet
		// then we just drop the value.
	}

	s.valueCounter.WithLabelValues("new", s.bucket).Inc()

	return values, true
}

func (s *Source) GetOld() (Value, bool) {
	v, ok := <-s.oldValues
	s.valueCounter.WithLabelValues("old", s.bucket).Inc()
	return v, ok
}

func (s *Source) stop() {
	fmt.Println("Closing source")
	close(s.newValues)
}

type Generators struct {
	generators       []*Source
	size             uint64
	table            *Table
	partitionsConfig PartitionRangeConfig
	seed             uint64
	done             chan struct{}
	counter          prometheus.Counter
}

type GeneratorsConfig struct {
	Table            *Table
	Partitions       PartitionRangeConfig
	Size             uint64
	Seed             uint64
	PkBufferSize     uint64
	PkUsedBufferSize uint64
}

func NewGenerator(config *GeneratorsConfig) *Generators {
	valueGenerationCounter := promauto.NewCounter(prometheus.CounterOpts{
		Name: "gemini_partition_key_value_creation",
		Help: "How many partition keys are created",
	})
	valueCounter := promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gemini_partition_key_value_consumption",
		Help: "How many partition keys are consumed, both new ald reused 'old'",
	}, []string{"generation", "bucket"},
	)
	generators := make([]*Source, config.Size)
	for i := uint64(0); i < config.Size; i++ {
		generators[i] = &Source{
			newValues:    make(chan Value, config.PkBufferSize),
			oldValues:    make(chan Value, config.PkUsedBufferSize),
			bucket:       fmt.Sprintf("bucket_%d", i),
			valueCounter: valueCounter,
		}
	}
	gs := &Generators{
		generators:       generators,
		size:             config.Size,
		table:            config.Table,
		partitionsConfig: config.Partitions,
		seed:             config.Seed,
		done:             make(chan struct{}, 1),
		counter:          valueGenerationCounter,
	}
	gs.start()
	return gs
}

func (gs Generators) Stop() {
	gs.done <- struct{}{}
}

func (gs Generators) Get(idx int) *Source {
	return gs.generators[idx]
}

func (gs *Generators) start() {
	go func() {
		routingKeyCreator := &RoutingKeyCreator{}
		r := rand.New(rand.NewSource(gs.seed))
		for {
			select {
			case <-gs.done:
				for _, s := range gs.generators {
					s.stop()
				}
				return
			default:
				var values []interface{}
				for _, pk := range gs.table.PartitionKeys {
					values = append(values, pk.Type.GenValue(r, gs.partitionsConfig)...)
				}
				b, _ := routingKeyCreator.CreateRoutingKey(gs.table, values)
				hash := uint64(murmur.Murmur3H1(b))
				source := gs.generators[hash%gs.size]
				source.newValues <- Value(values)
				gs.counter.Inc()
			}
		}
	}()
}
