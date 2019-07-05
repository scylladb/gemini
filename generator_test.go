package gemini

import (
	"testing"
	"time"
)

func TestGenerator(t *testing.T) {
	cfg := &GeneratorsConfig{
		Partitions: PartitionRangeConfig{
			MaxStringLength: 10,
			MinStringLength: 0,
			MaxBlobLength:   10,
			MinBlobLength:   0,
		},
		Size:             1,
		PkBufferSize:     10000,
		PkUsedBufferSize: 10000,
		Table: &Table{
			Name:          "tbl",
			PartitionKeys: createPkColumns(1, "pk"),
		},
	}
	generators := NewGenerator(cfg)
	source := generators.Get(0)

	time.Sleep(time.Second)

	if size := len(source.newValues); size != 10000 {
		t.Errorf("expected %d pks got %d", 10000, size)
	}
	if size := len(source.oldValues); size != 0 {
		t.Errorf("expected %d spent pks got %d", 0, size)
	}

	generators.Stop()
	cnt := 0
	for range source.newValues {
		cnt++
	}
	if cnt < 10000 {
		t.Errorf("expected at least %d pks after stop got %d", 10000, cnt)
	}
}
