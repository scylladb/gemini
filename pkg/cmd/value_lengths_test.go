// Copyright 2026 ScyllaDB
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

package main

import (
	"strconv"
	"testing"

	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/schema"
)

// setValueLengths sets the four length flags for one test and restores them
// after it. Tests that call it change package state, so they do not run in
// parallel.
func setValueLengths(t *testing.T, minString, maxString, minBlob, maxBlob int) {
	t.Helper()

	saved := [4]int{minStringLength, maxStringLength, minBlobLength, maxBlobLength}
	t.Cleanup(func() {
		minStringLength, maxStringLength, minBlobLength, maxBlobLength = saved[0], saved[1], saved[2], saved[3]
	})

	minStringLength, maxStringLength, minBlobLength, maxBlobLength = minString, maxString, minBlob, maxBlob
}

func TestValueLengthFlagsReachSchemaConfig(t *testing.T) {
	setValueLengths(t, 3, 40, 5, 70)

	cfg := createSchemaConfig("", zap.NewNop())
	vrc := cfg.GetValueRangeConfig()

	got := [4]int{vrc.MinStringLength, vrc.MaxStringLength, vrc.MinBlobLength, vrc.MaxBlobLength}
	if want := [4]int{3, 40, 5, 70}; got != want {
		t.Fatalf("value range (min string, max string, min blob, max blob) = %v, want %v", got, want)
	}
}

func TestValueLengthFlagDefaults(t *testing.T) {
	t.Parallel()

	want := map[string]int{
		"min-string-length": schema.MinStringLength,
		"max-string-length": schema.MaxStringLength,
		"min-blob-length":   schema.MinBlobLength,
		"max-blob-length":   schema.MaxBlobLength,
	}
	for name, value := range want {
		flag := rootCmd.Flags().Lookup(name)
		if flag == nil {
			t.Fatalf("--%s is not defined", name)
		}
		if flag.DefValue != strconv.Itoa(value) {
			t.Errorf("--%s default = %s, want %d", name, flag.DefValue, value)
		}
	}
}

func TestValidateValueLengths(t *testing.T) {
	cases := []struct {
		name    string
		lengths [4]int
		wantErr bool
	}{
		{name: "defaults", lengths: [4]int{0, 128, 0, 256}},
		{name: "equal bounds", lengths: [4]int{8, 8, 9, 9}},
		{name: "string min above max", lengths: [4]int{9, 8, 0, 1}, wantErr: true},
		{name: "blob min above max", lengths: [4]int{0, 1, 9, 8}, wantErr: true},
		{name: "negative string min", lengths: [4]int{-1, 8, 0, 1}, wantErr: true},
		{name: "negative blob max", lengths: [4]int{0, 8, 0, -1}, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setValueLengths(t, tc.lengths[0], tc.lengths[1], tc.lengths[2], tc.lengths[3])

			err := validateValueLengths()
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateValueLengths() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
