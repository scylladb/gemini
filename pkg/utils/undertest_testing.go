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

//go:build testing

package utils

import (
	"os"
	"testing"
	"time"

	"github.com/gocql/gocql"

	dockernetwork "github.com/docker/docker/api/types/network"
	"github.com/testcontainers/testcontainers-go/modules/scylladb"
	"github.com/testcontainers/testcontainers-go/network"
)

const ContainerNetworkName = "scylla-gemini"

func IsUnderTest() bool {
	return true
}

func TestContainers(tb testing.TB) (*gocql.Session, *gocql.Session) {
	tb.Helper()

	oracleVersion, exists := os.LookupEnv("GEMINI_SCYLLA_ORACLE")
	if !exists || oracleVersion == "" {
		oracleVersion = "6.2"
	}

	testVersion, exists := os.LookupEnv("GEMINI_SCYLLA_TEST")
	if !exists || testVersion == "" {
		testVersion = "2025.1"
	}

	sharedNetwork, err := network.New(
		tb.Context(),
		network.WithDriver("bridge"),
		network.WithAttachable(),
		network.WithIPAM(&dockernetwork.IPAM{
			Driver: "default",
			Config: []dockernetwork.IPAMConfig{
				{
					Subnet:  "192.168.105.0/24",
					Gateway: "192.168.105.1",
				},
			},
		}),
	)
	if err != nil {
		tb.Fatalf("failed to create shared network: %v", err)
	}

	tb.Cleanup(func() {
		_ = sharedNetwork.Remove(tb.Context())
	})

	oracle, err := scylladb.Run(tb.Context(),
		"scylladb/scylla:"+oracleVersion,
		scylladb.WithCustomCommands("--memory=512M", "--smp=1", "--developer-mode=1", "--overprovisioned=1"),
		scylladb.WithShardAwareness(),
		network.WithNetwork([]string{ContainerNetworkName}, sharedNetwork),
	)

	if err != nil {
		tb.Fatalf("failed to start oracle ScyllaDB container: %v", err)
	}

	tb.Cleanup(func() {
		_ = oracle.Terminate(tb.Context())
	})

	test, err := scylladb.Run(tb.Context(),
		"scylladb/scylla:"+testVersion,
		scylladb.WithCustomCommands("--memory=512M", "--smp=1", "--developer-mode=1", "--overprovisioned=1"),
		scylladb.WithShardAwareness(),
		network.WithNetwork([]string{ContainerNetworkName}, sharedNetwork),
	)

	if err != nil {
		tb.Fatalf("failed to start oracle ScyllaDB container: %v", err)
	}

	tb.Cleanup(func() {
		_ = test.Terminate(tb.Context())
	})

	oracleCluster := gocql.NewCluster(Must(oracle.ContainerIP(tb.Context())))
	oracleCluster.Timeout = 10 * time.Second
	oracleCluster.ConnectTimeout = 10 * time.Second
	oracleCluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 0}
	oracleCluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	oracleCluster.Consistency = gocql.Quorum
	oracleCluster.DefaultTimestamp = false
	if err = oracleCluster.Validate(); err != nil {
		tb.Fatalf("failed to validate oracle ScyllaDB cluster: %v", err)
	}

	testCluster := gocql.NewCluster(Must(test.ContainerIP(tb.Context())))
	testCluster.Timeout = 10 * time.Second
	testCluster.ConnectTimeout = 10 * time.Second
	testCluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 0}
	testCluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	testCluster.Consistency = gocql.Quorum
	testCluster.DefaultTimestamp = false
	if err = testCluster.Validate(); err != nil {
		tb.Fatalf("failed to validate test ScyllaDB cluster: %v", err)
	}

	oracleSession, err := oracleCluster.CreateSession()
	if err != nil {
		tb.Fatalf("failed to create oracle ScyllaDB session: %v", err)
	}

	tb.Cleanup(func() {
		oracleSession.Close()
	})

	testSession, err := testCluster.CreateSession()
	if err != nil {
		tb.Fatalf("failed to create test ScyllaDB session: %v", err)
	}

	tb.Cleanup(func() {
		testSession.Close()
	})

	return testSession, oracleSession
}
