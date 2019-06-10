package gemini

import (
	"encoding/json"
	"strings"
)

type CompactionStrategy struct {
	Class                       string  `json:"class"`
	Enabled                     bool    `json:"enabled,omitempty"`
	TombstoneThreshold          float32 `json:"tombstone_threshold,omitempty"`
	TombstoneCompactionInterval int     `json:"tombstone_compaction_interval,omitempty"`
	BucketHigh                  float32 `json:"bucket_high,omitempty"`
	BucketLow                   float32 `json:"bucket_low,omitempty"`
	MinSSTableSize              int     `json:"min_sstable_size,omitempty"`
	MinThreshold                int     `json:"min_threshold,omitempty"`
	MaxThreshold                int     `json:"max_threshold,omitempty"`
	SSTableSizeInMB             int     `json:"sstable_size_in_mb,omitempty"`
	CompactionWindowUnit        string  `json:"compaction_window_unit,omitempty"`
	CompactionWindowSize        int     `json:"compaction_window_size,omitempty"`
	SplitDuringFlush            bool    `json:"split_during_flush,omitempty"`
}

func (cs *CompactionStrategy) ToCQL() string {
	b, _ := json.Marshal(cs)
	return strings.ReplaceAll(string(b), "\"", "'")
}

func NewSizeTieredCompactionStrategy() *CompactionStrategy {
	return &CompactionStrategy{
		Class:                       "SizeTieredCompactionStrategy",
		Enabled:                     true,
		TombstoneThreshold:          0.2,
		TombstoneCompactionInterval: 86400,
		BucketHigh:                  1.5,
		BucketLow:                   0.5,
		MinSSTableSize:              50,
		MinThreshold:                4,
		MaxThreshold:                32,
	}
}

func NewLeveledCompactionStrategy() *CompactionStrategy {
	return &CompactionStrategy{
		Class:                       "LeveledCompactionStrategy",
		Enabled:                     true,
		TombstoneThreshold:          0.2,
		TombstoneCompactionInterval: 86400,
		SSTableSizeInMB:             160,
	}
}

func NewTimeWindowCompationStrategy() *CompactionStrategy {
	return &CompactionStrategy{
		Class:                       "TimeWindowCompationStrategy",
		Enabled:                     true,
		TombstoneThreshold:          0.2,
		TombstoneCompactionInterval: 86400,
		CompactionWindowUnit:        "DAYS",
		CompactionWindowSize:        1,
		MinThreshold:                4,
		MaxThreshold:                32,
	}
}
