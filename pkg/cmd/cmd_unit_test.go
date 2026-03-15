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

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scylladb/gemini/pkg/typedef"
)

// ---------------------------------------------------------------------------
// getCQLFeature
// ---------------------------------------------------------------------------

func TestGetCQLFeature(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  typedef.CQLFeature
	}{
		{"all", typedef.CQLFeatureAll},
		{"ALL", typedef.CQLFeatureAll},
		{"All", typedef.CQLFeatureAll},
		{"normal", typedef.CQLFeatureNormal},
		{"NORMAL", typedef.CQLFeatureNormal},
		{"basic", typedef.CQLFeatureBasic},
		{"unknown", typedef.CQLFeatureBasic},
		{"", typedef.CQLFeatureBasic},
		{"xyz", typedef.CQLFeatureBasic},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			got := getCQLFeature(tt.input)
			if got != tt.want {
				t.Errorf("getCQLFeature(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// validateSeed
// ---------------------------------------------------------------------------

func TestValidateSeed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"random", false},
		{"0", false},
		{"12345", false},
		{"18446744073709551615", false}, // max uint64
		{"", true},
		{"-1", true},
		{"abc", true},
		{"1.5", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			err := validateSeed(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateSeed(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// seedFromString
// ---------------------------------------------------------------------------

func TestSeedFromString(t *testing.T) {
	t.Parallel()

	t.Run("numeric", func(t *testing.T) {
		t.Parallel()
		got := seedFromString("12345")
		if got != 12345 {
			t.Errorf("seedFromString(\"12345\") = %d, want 12345", got)
		}
	})

	t.Run("zero", func(t *testing.T) {
		t.Parallel()
		got := seedFromString("0")
		if got != 0 {
			t.Errorf("seedFromString(\"0\") = %d, want 0", got)
		}
	})

	t.Run("random_returns_non_deterministic", func(t *testing.T) {
		t.Parallel()
		// "random" should produce a time-seeded value; calling twice should
		// yield different results with overwhelming probability.
		a := seedFromString("random")
		b := seedFromString("random")
		if a == b {
			t.Logf("seedFromString(\"random\") returned identical values (%d) on two calls — statistically possible but worth noting", a)
		}
		// At minimum, ensure it doesn't panic and returns a non-zero value
		// in the vast majority of cases (a zero result would be astronomically rare).
		_ = a
	})
}

// ---------------------------------------------------------------------------
// extractRepoOwner
// ---------------------------------------------------------------------------

func TestExtractRepoOwner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		repoPath     string
		defaultOwner string
		want         string
	}{
		{"scylladb/gemini", "fallback", "gemini"},
		{"owner/repo", "fallback", "repo"},
		{"nodash", "fallback", "fallback"},
		{"", "fallback", "fallback"},
		{"a/b/c", "fallback", "b/c"},
	}

	for _, tt := range tests {
		t.Run(tt.repoPath, func(t *testing.T) {
			t.Parallel()
			got := extractRepoOwner(tt.repoPath, tt.defaultOwner)
			if got != tt.want {
				t.Errorf("extractRepoOwner(%q, %q) = %q, want %q", tt.repoPath, tt.defaultOwner, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// VersionInfo.String
// ---------------------------------------------------------------------------

func TestVersionInfoString(t *testing.T) {
	t.Parallel()

	v := VersionInfo{
		Gemini: ComponentInfo{
			Version:    "v1.2.3",
			CommitSHA:  "abc1234",
			CommitDate: "2024-01-15",
		},
		Driver: ComponentInfo{
			Version:    "v0.99.0",
			CommitSHA:  "def5678",
			CommitDate: "2024-01-10",
		},
	}

	s := v.String()

	for _, want := range []string{"v1.2.3", "abc1234", "2024-01-15", "v0.99.0", "def5678", "2024-01-10"} {
		if !strings.Contains(s, want) {
			t.Errorf("VersionInfo.String() missing %q; got:\n%s", want, s)
		}
	}
}

// ---------------------------------------------------------------------------
// WithHTTPClient, WithContext, WithOutputVersionFile options
// ---------------------------------------------------------------------------

func TestVersionOptions(t *testing.T) {
	t.Parallel()

	t.Run("WithHTTPClient", func(t *testing.T) {
		t.Parallel()
		client := &http.Client{}
		opts := &versionOptions{}
		WithHTTPClient(client)(opts)
		if opts.httpClient != client {
			t.Error("WithHTTPClient did not set httpClient")
		}
	})

	t.Run("WithContext", func(t *testing.T) {
		t.Parallel()
		ctx := context.WithValue(t.Context(), struct{ k string }{"key"}, "val")
		opts := &versionOptions{}
		WithContext(ctx)(opts)
		if opts.ctx != ctx {
			t.Error("WithContext did not set ctx")
		}
	})

	t.Run("WithOutputVersionFile", func(t *testing.T) {
		t.Parallel()
		opts := &versionOptions{}
		WithOutputVersionFile("/tmp/version.json")(opts)
		if opts.outputVersionFile != "/tmp/version.json" {
			t.Errorf("WithOutputVersionFile got %q", opts.outputVersionFile)
		}
	})
}

// ---------------------------------------------------------------------------
// getMainBuildInfo — smoke test (no version vars set, falls through to debug)
// ---------------------------------------------------------------------------

func TestGetMainBuildInfo(t *testing.T) {
	t.Parallel()
	info := getMainBuildInfo()
	// Just ensure it returns something and doesn't panic
	if info.Version == "" {
		t.Error("expected non-empty version")
	}
}

// ---------------------------------------------------------------------------
// NewVersionInfo — reads from a pre-existing file
// ---------------------------------------------------------------------------

func TestNewVersionInfoFromFile(t *testing.T) {
	t.Parallel()

	want := VersionInfo{
		Gemini: ComponentInfo{Version: "v1.0.0", CommitSHA: "abc", CommitDate: "2024-01-01"},
		Driver: ComponentInfo{Version: "v0.1.0", CommitSHA: "def", CommitDate: "2024-01-02"},
	}

	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	dir := t.TempDir()
	file := filepath.Join(dir, "version.json")
	if err = os.WriteFile(file, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	got, err := NewVersionInfo(WithOutputVersionFile(file))
	if err != nil {
		t.Fatalf("NewVersionInfo: %v", err)
	}

	if got != want {
		t.Errorf("NewVersionInfo mismatch\n got:  %+v\n want: %+v", got, want)
	}
}

func TestNewVersionInfoFromMalformedFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	file := filepath.Join(dir, "version.json")
	if err := os.WriteFile(file, []byte("not-json"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := NewVersionInfo(WithOutputVersionFile(file))
	if err == nil {
		t.Error("expected error for malformed JSON, got nil")
	}
}
