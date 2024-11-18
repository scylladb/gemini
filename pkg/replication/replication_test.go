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

package replication_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/scylladb/gemini/pkg/replication"
)

func TestToCQL(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		rs   *replication.Replication
		want string
	}{
		"simple": {
			rs:   replication.NewSimpleStrategy(),
			want: "{'class':'SimpleStrategy','replication_factor':1}",
		},
		"network": {
			rs:   replication.NewNetworkTopologyStrategy(),
			want: "{'class':'NetworkTopologyStrategy','datacenter1':1}",
		},
	}
	for name := range tests {
		test := tests[name]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := test.rs.ToCQL()
			if got != test.want {
				t.Fatalf("expected '%s', got '%s'", test.want, got)
			}
		})
	}
}

func TestGetReplicationStrategy(t *testing.T) {
	tests := map[string]struct {
		strategy string
		expected string
	}{
		"simple strategy": {
			strategy: "{\"class\": \"SimpleStrategy\", \"replication_factor\": \"1\"}",
			expected: "{'class':'SimpleStrategy','replication_factor':'1'}",
		},
		"simple strategy single quotes": {
			strategy: "{'class': 'SimpleStrategy', 'replication_factor': '1'}",
			expected: "{'class':'SimpleStrategy','replication_factor':'1'}",
		},
		"network topology strategy": {
			strategy: "{\"class\": \"NetworkTopologyStrategy\", \"dc1\": 3, \"dc2\": 3}",
			expected: "{'class':'NetworkTopologyStrategy','dc1':3,'dc2':3}",
		},
		"network topology strategy single quotes": {
			strategy: "{'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}",
			expected: "{'class':'NetworkTopologyStrategy','dc1':3,'dc2':3}",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := replication.MustParseReplication(tc.strategy)
			if diff := cmp.Diff(got.ToCQL(), tc.expected); diff != "" {
				t.Errorf("expected=%s, got=%s,diff=%s", tc.strategy, got.ToCQL(), diff)
			}
		})
	}
}
