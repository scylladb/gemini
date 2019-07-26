package gemini

import (
	"reflect"
	"sync/atomic"
	"testing"

	"go.uber.org/zap"
)

func TestGenerator(t *testing.T) {
	table := &Table{
		Name:          "tbl",
		PartitionKeys: createPkColumns(1, "pk"),
	}
	var current uint64
	cfg := &GeneratorsConfig{
		Partitions: PartitionRangeConfig{
			MaxStringLength: 10,
			MinStringLength: 0,
			MaxBlobLength:   10,
			MinBlobLength:   0,
		},
		Size:             1,
		PkUsedBufferSize: 10000,
		DistributionSize: 1000,
		DistributionFunc: func() uint64 {
			return atomic.LoadUint64(&current)
		},
	}
	logger, _ := zap.NewDevelopment()
	generators := NewGenerator(table, cfg, logger)
	source := generators.Get(0)
	for i := uint64(0); i < cfg.DistributionSize; i++ {
		atomic.StoreUint64(&current, i)
		v, _ := source.Get()
		n, _ := source.Get()
		if !reflect.DeepEqual(v, n) {
			t.Errorf("expected %v, got %v", v, n)
		}
	}
}
