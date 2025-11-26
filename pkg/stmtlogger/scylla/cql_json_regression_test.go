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
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"
)

// TestEncodeRowToJSONShape ensures that our manual JSON encoding of fragments
// keeps critical types in a string form compatible with Scylla's SELECT JSON
// expectations (e.g., timestamps and blobs are stringified).
func TestEncodeRowToJSONShape(t *testing.T) {
	t.Parallel()

	when := time.Unix(1732550400, 123456789) // 2024-11-25T00:00:00.123456789Z
	blob := []byte{0x01, 0x02, 0xFF, 0x00}

	row := map[string]any{
		"text":  "hello",
		"num":   int32(42),
		"ts":    when,
		"bytes": blob,
	}

	raw, err := encodeRowToJSON(row)
	if err != nil {
		t.Fatalf("encodeRowToJSON failed: %v", err)
	}

	// Unmarshal to a generic map to assert JSON types
	var out map[string]any
	if err = json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unexpected JSON output: %v; data=%s", err, string(raw))
	}

	// text should be a JSON string
	if _, ok := out["text"].(string); !ok {
		t.Fatalf("text not a string: %#v", out["text"])
	}

	// num should be a JSON number (decoded by Go as float64)
	if _, ok := out["num"].(float64); !ok {
		t.Fatalf("num not a number: %#v", out["num"])
	}

	// ts should be a JSON string in RFC3339 format
	tsStr, ok := out["ts"].(string)
	if !ok {
		t.Fatalf("ts not a string: %#v", out["ts"])
	}
	if _, err = time.Parse(time.RFC3339Nano, tsStr); err != nil {
		t.Fatalf("ts not RFC3339Nano parseable: %q err=%v", tsStr, err)
	}

	// bytes should be a base64 JSON string
	b64, ok := out["bytes"].(string)
	if !ok {
		t.Fatalf("bytes not a string: %#v", out["bytes"])
	}
	dec, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		t.Fatalf("bytes not base64 string: %q err=%v", b64, err)
	}
	if string(dec) != string(blob) {
		t.Fatalf("bytes mismatch after base64 round-trip: got=%v want=%v", dec, blob)
	}
}
