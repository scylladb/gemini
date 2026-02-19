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

package store

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestCreateCluster_AppliesPortToHostsWithoutPort(t *testing.T) {
	cfg := ScyllaClusterConfig{
		Name:        "test",
		Hosts:       []string{"127.0.0.1", "localhost"},
		Port:        9043,
		Consistency: gocql.Quorum.String(),
	}

	cluster, err := CreateCluster(cfg, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, cluster)

	require.Equal(t, 9043, cluster.Port)
	require.ElementsMatch(t, []string{"127.0.0.1", "localhost"}, cluster.Hosts)
}

func TestCreateCluster_DoesNotOverrideExplicitPorts(t *testing.T) {
	cfg := ScyllaClusterConfig{
		Name:        "test",
		Hosts:       []string{"127.0.0.1:9999", "localhost"},
		Port:        9043,
		Consistency: gocql.Quorum.String(),
	}

	cluster, err := CreateCluster(cfg, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, cluster)

	require.Equal(t, 9043, cluster.Port)
	require.ElementsMatch(t, []string{"127.0.0.1:9999", "localhost"}, cluster.Hosts)
}
