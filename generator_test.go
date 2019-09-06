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
		PartitionsRangeConfig: PartitionRangeConfig{
			MaxStringLength: 10,
			MinStringLength: 0,
			MaxBlobLength:   10,
			MinBlobLength:   0,
		},
		Size:             10000,
		PkUsedBufferSize: 10000,
		PartitionsCount:  1000,
		PartitionsDistributionFunc: func() TokenIndex {
			return TokenIndex(atomic.LoadUint64(&current))
		},
	}
	logger, _ := zap.NewDevelopment()
	generators := NewGenerator(table, cfg, logger)
	for i := uint64(0); i < cfg.PartitionsCount; i++ {
		atomic.StoreUint64(&current, i)
		v, _ := generators.Get()
		n, _ := generators.Get()
		if v.Token%generators.partitionCount != n.Token%generators.partitionCount {
			t.Errorf("expected %v, got %v", v, n)
		}
	}
}
