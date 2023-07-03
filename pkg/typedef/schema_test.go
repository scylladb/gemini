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

//nolint:lll
package typedef

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSchemaConfigValidate(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		config *SchemaConfig
		want   error
	}{
		"empty": {
			config: &SchemaConfig{},
			want:   ErrSchemaConfigInvalidRangePK,
		},
		"valid": {
			config: &SchemaConfig{
				MaxPartitionKeys:  3,
				MinPartitionKeys:  2,
				MaxClusteringKeys: 3,
				MinClusteringKeys: 2,
				MaxColumns:        3,
				MinColumns:        2,
			},
			want: nil,
		},
		"min_pk_gt_than_max_pk": {
			config: &SchemaConfig{
				MaxPartitionKeys: 2,
				MinPartitionKeys: 3,
			},
			want: ErrSchemaConfigInvalidRangePK,
		},
		"ck_missing": {
			config: &SchemaConfig{
				MaxPartitionKeys: 3,
				MinPartitionKeys: 2,
			},
			want: ErrSchemaConfigInvalidRangeCK,
		},
		"min_ck_gt_than_max_ck": {
			config: &SchemaConfig{
				MaxPartitionKeys:  3,
				MinPartitionKeys:  2,
				MaxClusteringKeys: 2,
				MinClusteringKeys: 3,
			},
			want: ErrSchemaConfigInvalidRangeCK,
		},
		"columns_missing": {
			config: &SchemaConfig{
				MaxPartitionKeys:  3,
				MinPartitionKeys:  2,
				MaxClusteringKeys: 3,
				MinClusteringKeys: 2,
			},
			want: ErrSchemaConfigInvalidRangeCols,
		},
		"min_cols_gt_than_max_cols": {
			config: &SchemaConfig{
				MaxPartitionKeys:  3,
				MinPartitionKeys:  2,
				MaxClusteringKeys: 3,
				MinClusteringKeys: 2,
				MaxColumns:        2,
				MinColumns:        3,
			},
			want: ErrSchemaConfigInvalidRangeCols,
		},
	}
	cmp.AllowUnexported()
	for name := range tests {
		test := tests[name]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := test.config.Valid()
			//nolint:errorlint
			if got != test.want {
				t.Fatalf("expected '%s', got '%s'", test.want, got)
			}
		})
	}
}
