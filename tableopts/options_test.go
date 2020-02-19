// Copyright 2019 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tableopts

import (
	"testing"
)

func TestToCQL(t *testing.T) {
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
			rs:   "compaction = {'bucket_high':1.5,'bucket_low':0.5,'class':'SizeTieredCompactionStrategy','enabled':true,'max_threshold':32,'min_sstable_size':50,'min_threshold':4,'tombstone_compaction_interval':86400,'tombstone_threshold':0.2}",
			want: "compaction = {'bucket_high':1.5,'bucket_low':0.5,'class':'SizeTieredCompactionStrategy','enabled':true,'max_threshold':32,'min_sstable_size':50,'min_threshold':4,'tombstone_compaction_interval':86400,'tombstone_threshold':0.2}",
		},
		"size leveled compaction strategy": {
			rs:   "compaction = {'class':'LeveledCompactionStrategy','enabled':true,'sstable_size_in_mb':160,'tombstone_compaction_interval':86400,'tombstone_threshold':0.2}",
			want: "compaction = {'class':'LeveledCompactionStrategy','enabled':true,'sstable_size_in_mb':160,'tombstone_compaction_interval':86400,'tombstone_threshold':0.2}",
		},
		"size time window compaction strategy": {
			rs:   "compaction = {'class':'TimeWindowCompactionStrategy','compaction_window_size':1,'compaction_window_unit':'DAYS','enabled':true,'max_threshold':32,'min_threshold':4,'tombstone_compaction_interval':86400,'tombstone_threshold':0.2}",
			want: "compaction = {'class':'TimeWindowCompactionStrategy','compaction_window_size':1,'compaction_window_unit':'DAYS','enabled':true,'max_threshold':32,'min_threshold':4,'tombstone_compaction_interval':86400,'tombstone_threshold':0.2}",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if o, err := FromCQL(test.rs); err != nil {
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
