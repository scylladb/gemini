package gemini

import (
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
	cfg := &GeneratorConfig{
		Partitions: PartitionRangeConfig{
			MaxStringLength: 10,
			MinStringLength: 0,
			MaxBlobLength:   10,
			MinBlobLength:   0,
		},
		Size:             10000,
		PkUsedBufferSize: 10000,
		DistributionSize: 1000,
		DistributionFunc: func() uint64 {
			return atomic.LoadUint64(&current)
		},
	}
	logger, _ := zap.NewDevelopment()
	generators := NewGenerator(table, cfg, logger)
	for i := uint64(0); i < cfg.DistributionSize; i++ {
		atomic.StoreUint64(&current, i)
		v, _ := generators.Get()
		n, _ := generators.Get()
		if v.Token%generators.size != n.Token%generators.size {
			t.Errorf("expected %v, got %v", v, n)
		}
	}
}
