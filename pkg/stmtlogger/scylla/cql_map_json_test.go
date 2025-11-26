// Copyright 2025 ScyllaDB
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

package scylla

import (
	"encoding/hex"
	"encoding/json"
	"testing"
)

// Ensure that cqlDataMap marshals with string keys (hex-encoded) and copies
// entries into a map[string]cqlData so it can be JSON-encoded easily.
func TestCQLDataMap_MarshalJSON_Keys(t *testing.T) {
	t.Parallel()

	var k1, k2 [32]byte
	k1[0] = 0x01
	k2[0] = 0xAB

	m := cqlDataMap{
		k1: {},
		k2: {},
	}

	bs, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var out map[string]json.RawMessage
	if err = json.Unmarshal(bs, &out); err != nil {
		t.Fatalf("Unmarshal failed: %v; data=%s", err, string(bs))
	}

	if len(out) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(out))
	}

	if _, ok := out[hex.EncodeToString(k1[:])]; !ok {
		t.Fatalf("missing key for k1")
	}
	if _, ok := out[hex.EncodeToString(k2[:])]; !ok {
		t.Fatalf("missing key for k2")
	}
}
