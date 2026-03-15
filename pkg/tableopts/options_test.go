// Copyright 2019 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tableopts_test

import (
	"testing"

	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/tableopts"
)

func TestToCQL(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		rs   string
		want string
	}{
		"map with only strings": {
			rs:   "compression = {'sstable_compression':'LZ4Compressor'}",
			want: "compression = {'sstable_compression':'LZ4Compressor'}",
		},
		"simple numeric type": {
			rs:   "read_repair_chance = 1.0",
			want: "read_repair_chance = 1.0",
		},
		"simple string type": {
			rs:   "comment = 'Important biological records'",
			want: "comment = 'Important biological records'",
		},
		"cdc": {
			rs:   "cdc = {'enabled':'true','preimage':'true'}",
			want: "cdc = {'enabled':'true','preimage':'true'}",
		},
		"size tiered compaction strategy": {
			rs: "compaction = {'bucket_high':1.5,'bucket_low':0.5,'class':'SizeTieredCompactionStrategy','enabled':true," +
				"'max_threshold':32,'min_sstable_size':50,'min_threshold':4,'tombstone_compaction_interval':86400,'tombstone_threshold':0.2}",
			want: "compaction = {'bucket_high':1.5,'bucket_low':0.5,'class':'SizeTieredCompactionStrategy','enabled':true," +
				"'max_threshold':32,'min_sstable_size':50,'min_threshold':4,'tombstone_compaction_interval':86400,'tombstone_threshold':0.2}",
		},
		"size leveled compaction strategy": {
			rs:   "compaction = {'class':'LeveledCompactionStrategy','enabled':true,'sstable_size_in_mb':160,'tombstone_compaction_interval':86400,'tombstone_threshold':0.2}",
			want: "compaction = {'class':'LeveledCompactionStrategy','enabled':true,'sstable_size_in_mb':160,'tombstone_compaction_interval':86400,'tombstone_threshold':0.2}",
		},
		"size time window compaction strategy": {
			rs: "compaction = {'class':'TimeWindowCompactionStrategy','compaction_window_size':1,'compaction_window_unit':'DAYS'," +
				"'enabled':true,'max_threshold':32,'min_threshold':4,'tombstone_compaction_interval':86400,'tombstone_threshold':0.2}",
			want: "compaction = {'class':'TimeWindowCompactionStrategy','compaction_window_size':1,'compaction_window_unit':'DAYS'," +
				"'enabled':true,'max_threshold':32,'min_threshold':4,'tombstone_compaction_interval':86400,'tombstone_threshold':0.2}",
		},
	}
	for name := range tests {
		test := tests[name]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if o, err := tableopts.FromCQL(test.rs); err != nil {
				t.Error(err)
			} else {
				got := o.ToCQL()
				if got != test.want {
					t.Fatalf("expected\t'%s', \ngot\t'%s'", test.want, got)
				}
			}
		})
	}
}

// TestCreateTableOptions verifies that valid options are parsed and invalid ones are skipped.
func TestCreateTableOptions(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()

	tests := map[string]struct {
		inputs  []string
		wantLen int
	}{
		"all valid": {
			inputs:  []string{"read_repair_chance = 0.1", "comment = 'test'"},
			wantLen: 2,
		},
		"one invalid one valid": {
			inputs:  []string{"no_equals_sign_here", "read_repair_chance = 0.5"},
			wantLen: 1,
		},
		"all invalid": {
			inputs:  []string{"nope", "alsonope"},
			wantLen: 0,
		},
		"empty slice": {
			inputs:  []string{},
			wantLen: 0,
		},
		"nil slice": {
			inputs:  nil,
			wantLen: 0,
		},
		"map option": {
			inputs:  []string{"compression = {'sstable_compression':'LZ4Compressor'}"},
			wantLen: 1,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			opts := tableopts.CreateTableOptions(tc.inputs, logger)
			if len(opts) != tc.wantLen {
				t.Errorf("want %d options, got %d", tc.wantLen, len(opts))
			}
		})
	}
}
