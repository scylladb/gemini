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

package testutils

import (
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"time"

	dockernetwork "github.com/docker/docker/api/types/network"
	"github.com/gocql/gocql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/scylladb"
	"github.com/testcontainers/testcontainers-go/network"
)

const ContainerNetworkName = "scylla-gemini"

type (
	ScyllaContainer struct {
		Oracle *gocql.Session
		Test   *gocql.Session

		OracleHosts []string
		TestHosts   []string
	}

	ipUsed struct {
		data map[byte]bool
		sync.Mutex
	}
)

var (
	randomMutex         = &sync.Mutex{}
	random              = rand.New(rand.NewPCG(uint64(time.Now().Second()), uint64(time.Now().Nanosecond())))
	spawningScyllaMutex = &sync.Mutex{}
	usedIPs             = &ipUsed{data: make(map[byte]bool, 256)}
)

func createScyllaNetwork(tb testing.TB) *testcontainers.DockerNetwork {
	tb.Helper()

	var ipPart byte
	usedIPs.Lock()
	defer usedIPs.Unlock()

	for {
		randomMutex.Lock()
		ipPart = byte(random.IntN(255))
		randomMutex.Unlock()

		if _, ok := usedIPs.data[ipPart]; !ok {
			usedIPs.data[ipPart] = true
			break
		}
	}

	sharedNetwork, err := network.New(
		tb.Context(),
		network.WithDriver("bridge"),
		network.WithAttachable(),
		network.WithIPAM(&dockernetwork.IPAM{
			Driver: "default",
			Config: []dockernetwork.IPAMConfig{
				{
					Subnet:  fmt.Sprintf("172.31.%d.0/24", ipPart),
					Gateway: fmt.Sprintf("172.31.%d.1", ipPart),
				},
			},
		}),
	)
	if err != nil {
		tb.Fatalf("failed to create shared network: %v", err)
	}

	tb.Cleanup(func() {
		usedIPs.Lock()
		defer usedIPs.Unlock()
		_ = sharedNetwork.Remove(tb.Context())
		delete(usedIPs.data, ipPart)
	})

	return sharedNetwork
}

func spawnScylla(tb testing.TB, version string, sharedNetwork *testcontainers.DockerNetwork, timeouts ...time.Duration) *ScyllaContainer {
	tb.Helper()
	spawningScyllaMutex.Lock()
	scyllaContainer, err := scylladb.Run(tb.Context(),
		"scylladb/scylla:"+version,
		scylladb.WithCustomCommands("--memory=512M", "--smp=1", "--developer-mode=1", "--overprovisioned=1"),
		scylladb.WithShardAwareness(),
		network.WithNetwork([]string{ContainerNetworkName}, sharedNetwork),
	)
	spawningScyllaMutex.Unlock()
	if err != nil {
		tb.Fatalf("failed to start oracle ScyllaDB container: %v", err)
	}

	tb.Cleanup(func() {
		spawningScyllaMutex.Lock()
		_ = scyllaContainer.Terminate(tb.Context())
		spawningScyllaMutex.Unlock()
	})

	testCluster := gocql.NewCluster(Must(scyllaContainer.ContainerIP(tb.Context())))
	if len(timeouts) > 0 {
		testCluster.Timeout = timeouts[0]
	} else {
		testCluster.Timeout = 10 * time.Second
	}
	testCluster.ConnectTimeout = 10 * time.Second
	testCluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		Min:        20 * time.Microsecond,
		Max:        50 * time.Millisecond,
		NumRetries: 10,
	}
	testCluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	testCluster.Consistency = gocql.Quorum
	testCluster.DefaultTimestamp = false
	if err = testCluster.Validate(); err != nil {
		tb.Fatalf("failed to validate test ScyllaDB cluster: %v", err)
	}

	session, err := testCluster.CreateSession()
	if err != nil {
		tb.Fatalf("failed to create test ScyllaDB session: %v", err)
	}

	tb.Cleanup(func() {
		session.Close()
	})

	return &ScyllaContainer{
		Test:      session,
		TestHosts: []string{Must(scyllaContainer.ContainerIP(tb.Context()))},
	}
}

func SingleScylla(tb testing.TB, timeouts ...time.Duration) *ScyllaContainer {
	tb.Helper()

	testVersion, exists := os.LookupEnv("GEMINI_SCYLLA_TEST")
	if !exists || testVersion == "" {
		testVersion = "2025.1"
	}

	sharedNetwork := createScyllaNetwork(tb)

	return spawnScylla(tb, testVersion, sharedNetwork, timeouts...)
}

func TestContainers(tb testing.TB, timeouts ...time.Duration) *ScyllaContainer {
	tb.Helper()

	oracleVersion, exists := os.LookupEnv("GEMINI_SCYLLA_ORACLE")
	if !exists || oracleVersion == "" {
		oracleVersion = "6.2"
	}

	testVersion, exists := os.LookupEnv("GEMINI_SCYLLA_TEST")
	if !exists || testVersion == "" {
		testVersion = "2025.1"
	}

	sharedNetwork := createScyllaNetwork(tb)

	oracleScylla := spawnScylla(tb, oracleVersion, sharedNetwork, timeouts...)
	testScylla := spawnScylla(tb, testVersion, sharedNetwork, timeouts...)

	return &ScyllaContainer{
		Oracle:      oracleScylla.Test,
		OracleHosts: oracleScylla.TestHosts,
		Test:        testScylla.Test,
		TestHosts:   testScylla.TestHosts,
	}
}
