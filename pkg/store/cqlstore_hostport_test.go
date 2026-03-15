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
	"net"
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

func TestCreateCluster_DockerMode_SetsAddressTranslator(t *testing.T) {
	cfg := ScyllaClusterConfig{
		Name:        "test",
		Hosts:       []string{"127.0.0.1"},
		Port:        9042,
		Consistency: gocql.Quorum.String(),
		DockerMode:  true,
	}

	cluster, err := CreateCluster(cfg, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, cluster)

	// DockerMode must install an AddressTranslator.
	require.NotNil(t, cluster.AddressTranslator, "expected AddressTranslator to be set when DockerMode=true")
}

func TestCreateCluster_NoDockerMode_NoAddressTranslator(t *testing.T) {
	cfg := ScyllaClusterConfig{
		Name:        "test",
		Hosts:       []string{"192.168.1.10"},
		Port:        9042,
		Consistency: gocql.Quorum.String(),
		DockerMode:  false,
	}

	cluster, err := CreateCluster(cfg, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, cluster)

	// Without DockerMode no translator should be installed.
	require.Nil(t, cluster.AddressTranslator, "expected no AddressTranslator when DockerMode=false")
}

func TestCreateCluster_DockerMode_TranslatorRewritesIP(t *testing.T) {
	cfg := ScyllaClusterConfig{
		Name:        "test",
		Hosts:       []string{"127.0.0.1"},
		Port:        9042,
		Consistency: gocql.Quorum.String(),
		DockerMode:  true,
	}

	cluster, err := CreateCluster(cfg, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, cluster.AddressTranslator)

	// The translator must rewrite any peer IP to 127.0.0.1 and preserve the port.
	translatedIP, translatedPort := cluster.AddressTranslator.Translate(
		net.ParseIP("10.0.0.5"), 9042,
	)
	require.Equal(t, "127.0.0.1", translatedIP.String())
	require.Equal(t, 9042, translatedPort)
}
