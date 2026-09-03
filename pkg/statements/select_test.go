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
	"encoding/json"
	"math/big"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gopkg.in/inf.v0"

	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

func TestConvertForJSON_EveryPartitionKeyTypeKeepsItsValue(t *testing.T) {
	t.Parallel()

	utils.PreallocateRandomString(rand.New(rand.NewChaCha8([32]byte{})), 1<<20)

	valueRange := &typedef.ValueRangeConfig{
		MaxBlobLength:   32,
		MinBlobLength:   1,
		MaxStringLength: 32,
		MinStringLength: 1,
	}

	for _, pkType := range typedef.PartitionKeyTypes {
		t.Run(pkType.Name(), func(t *testing.T) {
			t.Parallel()

			rng := rand.New(rand.NewChaCha8([32]byte{}))
			encodings := make(map[string]struct{}, 32)

			for range 32 {
				generated := pkType.GenValue(rng, valueRange)
				require.Len(t, generated, 1)

				encoded, err := json.Marshal(convertForJSON(pkType, generated[0]))
				require.NoError(t, err)
				require.NotEqual(t, `"`+string(pkType)+`"`, string(encoded),
					"conversion returned the CQL type name instead of the value")

				encodings[string(encoded)] = struct{}{}
			}

			require.Greater(t, len(encodings), 1,
				"every generated value encoded to the same JSON, so the conversion discards the value")
		})
	}
}

func TestInsertJSON_TuplePartitionKeyCarriesItsValues(t *testing.T) {
	t.Parallel()

	tupleType := &typedef.TupleType{
		ComplexType: typedef.TypeTuple,
		ValueTypes:  []typedef.SimpleType{typedef.TypeInt, typedef.TypeText, typedef.TypeUUID},
	}

	table := &typedef.Table{
		Name:          "tuple_pk_table",
		PartitionKeys: typedef.Columns{{Name: "pk1", Type: tupleType}},
	}

	wantUUID, err := gocql.RandomUUID()
	require.NoError(t, err)

	mp := newMockPartitions(1)
	mp.nextKeys.Store(&typedef.PartitionKeys{
		ID:     uuid.New(),
		Values: typedef.NewValuesFromMap(map[string][]any{"pk1": {int32(7), "seven", wantUUID}}),
	})

	rng := rand.New(rand.NewChaCha8([32]byte{}))
	rc, err := NewRatioController(DefaultStatementRatios(), rng)
	require.NoError(t, err)

	valueRange := &typedef.ValueRangeConfig{
		MaxBlobLength:   32,
		MinBlobLength:   1,
		MaxStringLength: 32,
		MinStringLength: 1,
	}

	gen := New("ks", mp, table, rng, valueRange, rc, false)

	stmt, err := gen.InsertJSON(t.Context())
	require.NoError(t, err)
	require.Len(t, stmt.Values, 1)

	payload, ok := stmt.Values[0].(string)
	require.True(t, ok)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal([]byte(payload), &decoded))

	require.Equal(t, []any{float64(7), "seven", wantUUID.String()}, decoded["pk1"])
}

func TestConvertForJSON_GeneratedTimeKeepsItsValue(t *testing.T) {
	t.Parallel()

	generated := typedef.TypeTime.GenValue(rand.New(rand.NewPCG(1, 2)), typedef.ValueRangeConfig{})
	for _, value := range generated {
		result := convertForJSON(typedef.TypeTime, value)

		str, ok := result.(string)
		if !ok {
			t.Fatalf("expected string, got %T", result)
		}

		if str == "00:00:00.000000000" {
			t.Errorf("generated time collapsed to zero: %v (%T)", value, value)
		}
	}
}

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
		result := convertForJSON(typedef.TypeUUID, uid)
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
		sinceMidnight := time.Duration(int64(12*3600+30*60+45)*1e9 + 123456789)
		result := convertForJSON(typedef.TypeTime, sinceMidnight)
		str, ok := result.(string)
		if !ok {
			t.Fatalf("expected string, got %T", result)
		}
		if want := "12:30:45.123456789"; str != want {
			t.Errorf("got %q, want %q", str, want)
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
