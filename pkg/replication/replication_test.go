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
	"encoding/json"
	"testing"

	"github.com/scylladb/gemini/pkg/replication"
)

func TestMarshalJSON(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		r    replication.Replication
		want map[string]any
	}{
		"simple_strategy": {
			r: replication.NewSimpleStrategy(),
			want: map[string]any{
				"class":              "SimpleStrategy",
				"replication_factor": float64(1),
			},
		},
		"network_topology_strategy": {
			r: replication.NewNetworkTopologyStrategy(),
			want: map[string]any{
				"class":              "NetworkTopologyStrategy",
				"datacenter1":        float64(1),
				"replication_factor": float64(1),
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			data, err := tc.r.MarshalJSON()
			if err != nil {
				t.Fatalf("MarshalJSON() error = %v", err)
			}
			var got map[string]any
			if unmarshalErr := json.Unmarshal(data, &got); unmarshalErr != nil {
				t.Fatalf("json.Unmarshal() error = %v", unmarshalErr)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("key %q: want %v, got %v", k, v, got[k])
				}
			}
		})
	}
}

func TestUnmarshalJSON(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		check   func(tb testing.TB, r replication.Replication)
		input   string
		wantErr bool
	}{
		"simple_strategy": {
			input: `{"class":"SimpleStrategy","replication_factor":1}`,
			check: func(tb testing.TB, r replication.Replication) {
				tb.Helper()
				if r["class"] != "SimpleStrategy" {
					tb.Errorf("class: want SimpleStrategy, got %v", r["class"])
				}
				if r["replication_factor"] != 1 {
					tb.Errorf("replication_factor: want 1, got %v (%T)", r["replication_factor"], r["replication_factor"])
				}
			},
		},
		"float_values_converted_to_int": {
			input: `{"class":"NetworkTopologyStrategy","datacenter1":3,"replication_factor":2}`,
			check: func(tb testing.TB, r replication.Replication) {
				tb.Helper()
				if r["datacenter1"] != 3 {
					tb.Errorf("datacenter1: want int(3), got %v (%T)", r["datacenter1"], r["datacenter1"])
				}
			},
		},
		"null_json": {
			// Unmarshaling null should leave the receiver unchanged (no error).
			input: `null`,
			check: func(tb testing.TB, _ replication.Replication) {
				tb.Helper()
				// After null unmarshal the pointer receiver stays nil — nothing to check.
			},
		},
		"invalid_json": {
			input:   `{not valid json`,
			wantErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var r replication.Replication
			err := json.Unmarshal([]byte(tc.input), &r)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("UnmarshalJSON() error = %v", err)
			}
			if tc.check != nil {
				tc.check(t, r)
			}
		})
	}
}

// TestJSONRoundtrip verifies that marshal → unmarshal preserves the replication map.
func TestJSONRoundtrip(t *testing.T) {
	t.Parallel()

	original := replication.NewNetworkTopologyStrategy()
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var decoded replication.Replication
	if err = json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if decoded["class"] != original["class"] {
		t.Errorf("class mismatch: want %v, got %v", original["class"], decoded["class"])
	}
	// replication_factor is int(1) in original but comes back as int after our UnmarshalJSON conversion
	if decoded["replication_factor"] != int(original["replication_factor"].(int)) {
		t.Errorf("replication_factor mismatch: want %v, got %v",
			original["replication_factor"], decoded["replication_factor"])
	}
}

func TestToCQL(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		rs   replication.Replication
		want string
	}{
		"simple": {
			rs:   replication.NewSimpleStrategy(),
			want: "{'class':'SimpleStrategy','replication_factor':1}",
		},
		"network": {
			rs:   replication.NewNetworkTopologyStrategy(),
			want: "{'class':'NetworkTopologyStrategy','datacenter1':1,'replication_factor':1}",
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
