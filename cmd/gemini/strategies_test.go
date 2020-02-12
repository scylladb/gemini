package main

import (
	"testing"

	"github.com/scylladb/gemini/replication"

	"github.com/google/go-cmp/cmp"

	"go.uber.org/zap"
)

func TestGetReplicationStrategy(t *testing.T) {
	var tests = map[string]struct {
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
	logger := zap.NewNop()
	fallback := replication.NewSimpleStrategy()
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := getReplicationStrategy(tc.strategy, fallback, logger)
			if diff := cmp.Diff(got.ToCQL(), tc.expected); diff != "" {
				t.Errorf("expected=%s, got=%s,diff=%s", tc.strategy, got.ToCQL(), diff)
			}
		})
	}
}
