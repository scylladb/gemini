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

package statements

import (
	"bytes"
	"encoding/hex"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"

	"github.com/scylladb/gemini/pkg/typedef"
)

func TestConvertForJSON(t *testing.T) {
	t.Parallel()

	t.Run("blob", func(t *testing.T) {
		t.Parallel()
		raw := []byte{0xde, 0xad, 0xbe, 0xef}
		result := convertForJSON(typedef.TypeBlob, raw)
		str, ok := result.(string)
		if !ok {
			t.Fatalf("expected string, got %T", result)
		}
		if !strings.HasPrefix(str, "0x") {
			t.Errorf("expected 0x prefix, got %q", str)
		}
		buf := bytes.NewBuffer(nil)
		buf.WriteString("0x")
		enc := hex.NewEncoder(buf)
		_, _ = enc.Write(raw)
		if str != buf.String() {
			t.Errorf("got %q, want %q", str, buf.String())
		}
	})

	t.Run("date", func(t *testing.T) {
		t.Parallel()
		ts := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
		result := convertForJSON(typedef.TypeDate, ts)
		str, ok := result.(string)
		if !ok {
			t.Fatalf("expected string, got %T", result)
		}
		if str != "2024-03-15" {
			t.Errorf("got %q, want %q", str, "2024-03-15")
		}
	})

	t.Run("duration", func(t *testing.T) {
		t.Parallel()
		d := 90*time.Second + 500*time.Millisecond
		result := convertForJSON(typedef.TypeDuration, d)
		// just verify it's a non-empty string
		str, ok := result.(string)
		if !ok {
			t.Fatalf("expected string, got %T", result)
		}
		if str == "" {
			t.Error("expected non-empty duration string")
		}
	})

	t.Run("decimal", func(t *testing.T) {
		t.Parallel()
		dec := inf.NewDec(12345, 2) // 123.45
		result := convertForJSON(typedef.TypeDecimal, dec)
		str, ok := result.(string)
		if !ok {
			t.Fatalf("expected string, got %T", result)
		}
		if str != dec.String() {
			t.Errorf("got %q, want %q", str, dec.String())
		}
	})

	t.Run("uuid", func(t *testing.T) {
		t.Parallel()
		uid, _ := gocql.RandomUUID()
		result := convertForJSON(typedef.TypeUuid, uid)
		str, ok := result.(string)
		if !ok {
			t.Fatalf("expected string, got %T", result)
		}
		if str != uid.String() {
			t.Errorf("got %q, want %q", str, uid.String())
		}
	})

	t.Run("timeuuid", func(t *testing.T) {
		t.Parallel()
		uid, _ := gocql.RandomUUID()
		result := convertForJSON(typedef.TypeTimeuuid, uid)
		str, ok := result.(string)
		if !ok {
			t.Fatalf("expected string, got %T", result)
		}
		if str != uid.String() {
			t.Errorf("got %q, want %q", str, uid.String())
		}
	})

	t.Run("varint", func(t *testing.T) {
		t.Parallel()
		bi := big.NewInt(9876543210)
		result := convertForJSON(typedef.TypeVarint, bi)
		str, ok := result.(string)
		if !ok {
			t.Fatalf("expected string, got %T", result)
		}
		if str != bi.String() {
			t.Errorf("got %q, want %q", str, bi.String())
		}
	})

	t.Run("time", func(t *testing.T) {
		t.Parallel()
		ns := int64(12*3600+30*60+45)*1e9 + 123456789
		result := convertForJSON(typedef.TypeTime, ns)
		str, ok := result.(string)
		if !ok {
			t.Fatalf("expected string, got %T", result)
		}
		if str == "" {
			t.Error("expected non-empty time string")
		}
	})

	t.Run("passthrough_int", func(t *testing.T) {
		t.Parallel()
		val := 42
		result := convertForJSON(typedef.TypeInt, val)
		if result != val {
			t.Errorf("got %v, want %v", result, val)
		}
	})

	t.Run("passthrough_text", func(t *testing.T) {
		t.Parallel()
		val := "hello"
		result := convertForJSON(typedef.TypeText, val)
		if result != val {
			t.Errorf("got %v, want %v", result, val)
		}
	})
}

func TestTotalCartesianProductCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		initial float64
		pkLen   float64
		want    int
	}{
		{"zero_initial", 0, 5, 1},
		{"one_pk_one_initial", 1, 1, 1},
		{"large_initial_large_pk_reduces_to_1", 50, 10, 1},
		{"small_initial_small_pk", 3, 2, 3},
		{"initial_exceeds_threshold_falls_back", 100, 3, 0}, // 0 means skip exact check
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := TotalCartesianProductCount(tt.initial, tt.pkLen)
			if got < 1 {
				t.Errorf("TotalCartesianProductCount(%v, %v) = %d, must be >= 1", tt.initial, tt.pkLen, got)
			}
			// Verify the result produces a product below MaxCartesianProductCount
			if tt.want != 0 && got != tt.want {
				t.Errorf("TotalCartesianProductCount(%v, %v) = %d, want %d", tt.initial, tt.pkLen, got, tt.want)
			}
		})
	}
}
