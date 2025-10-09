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
	"strconv"
	"strings"
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

func createScyllaSession(tb testing.TB, hosts ...string) *ScyllaContainer {
	tb.Helper()

	testCluster := gocql.NewCluster(hosts...)
	testCluster.Timeout = 10 * time.Second
	testCluster.ConnectTimeout = 10 * time.Second
	testCluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		Min:        20 * time.Microsecond,
		Max:        50 * time.Millisecond,
		NumRetries: 10,
	}
	testCluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	testCluster.Consistency = gocql.Quorum
	testCluster.DefaultTimestamp = false
	if err := testCluster.Validate(); err != nil {
		tb.Fatalf("failed to validate test ScyllaDB cluster: %v", err)
	}

	session, err := testCluster.CreateSession()
	if err != nil {
		tb.Fatalf("failed to create test ScyllaDB session: %v", err)
	}

	tb.Cleanup(func() {
		keyspace := GenerateUniqueKeyspaceName(tb)
		_ = session.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s;", keyspace)).Exec()

		session.Close()
	})

	return &ScyllaContainer{
		Test:      session,
		TestHosts: hosts,
	}
}

func spawnScylla(tb testing.TB, version string, sharedNetwork *testcontainers.DockerNetwork) *ScyllaContainer {
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
		_ = scyllaContainer.Terminate(tb.Context())
	})

	return createScyllaSession(tb, Must(scyllaContainer.ContainerIP(tb.Context())))
}

func SingleScylla(tb testing.TB) *ScyllaContainer {
	tb.Helper()

	val, existsDockerScylla := os.LookupEnv("GEMINI_USE_DOCKER_SCYLLA")
	if !existsDockerScylla {
		testVersion, exists := os.LookupEnv("GEMINI_SCYLLA_TEST")
		if !exists || testVersion == "" {
			testVersion = "2025.1"
		}

		sharedNetwork := createScyllaNetwork(tb)

		return spawnScylla(tb, testVersion, sharedNetwork)
	}

	valBool, err := strconv.ParseBool(val)
	if err != nil {
		tb.Fatalf("failed to parse GEMINI_USE_DOCKER_SCYLLA: %v", err)
		return nil
	}

	if valBool {
		hosts, existsTestClusterIP := os.LookupEnv("GEMINI_TEST_CLUSTER_IP")
		if !existsTestClusterIP || hosts == "" {
			tb.Fatal("GEMINI_TEST_CLUSTER_IP is not set")
		}

		return createScyllaSession(tb, strings.Split(hosts, ",")...)
	}

	tb.Fatal("GEMINI_USE_DOCKER_SCYLLA is set to false, but using in-memory ScyllaDB is not supported in this build")
	return nil
}

func TestContainers(tb testing.TB) *ScyllaContainer {
	tb.Helper()
	val, exists := os.LookupEnv("GEMINI_USE_DOCKER_SCYLLA")

	if !exists {
		oracleVersion, existsScyllaOracle := os.LookupEnv("GEMINI_SCYLLA_ORACLE")
		if !existsScyllaOracle || oracleVersion == "" {
			oracleVersion = "6.2"
		}

		testVersion, existsScyllaTest := os.LookupEnv("GEMINI_SCYLLA_TEST")
		if !existsScyllaTest || testVersion == "" {
			testVersion = "2025.1"
		}

		sharedNetwork := createScyllaNetwork(tb)

		oracleScylla := spawnScylla(tb, oracleVersion, sharedNetwork)
		testScylla := spawnScylla(tb, testVersion, sharedNetwork)

		return &ScyllaContainer{
			Oracle:      oracleScylla.Test,
			OracleHosts: oracleScylla.TestHosts,
			Test:        testScylla.Test,
			TestHosts:   testScylla.TestHosts,
		}
	}

	valBool, err := strconv.ParseBool(val)
	if err != nil {
		tb.Fatalf("failed to parse GEMINI_USE_DOCKER_SCYLLA: %v", err)
		return nil
	}

	if valBool {
		oracleHosts, existsOracleClusterIP := os.LookupEnv("GEMINI_ORACLE_CLUSTER_IP")
		if !existsOracleClusterIP || oracleHosts == "" {
			tb.Fatal("GEMINI_ORACLE_CLUSTER_IP is not set")
			return nil
		}

		testHosts, existsTestClusterIP := os.LookupEnv("GEMINI_TEST_CLUSTER_IP")
		if !existsTestClusterIP || testHosts == "" {
			tb.Fatal("GEMINI_TEST_CLUSTER_IP is not set")
			return nil
		}

		oracleSession := createScyllaSession(tb, strings.Split(oracleHosts, ",")...)
		testSession := createScyllaSession(tb, strings.Split(testHosts, ",")...)

		return &ScyllaContainer{
			Oracle:      oracleSession.Test,
			OracleHosts: oracleSession.TestHosts,
			Test:        testSession.Test,
			TestHosts:   testSession.TestHosts,
		}
	}

	tb.Fatal("GEMINI_USE_DOCKER_SCYLLA is set to false, but using in-memory ScyllaDB is not supported in this build")
	return nil
}

func GenerateUniqueKeyspaceName(tb testing.TB) string {
	tb.Helper()

	return "ks" + strings.ReplaceAll(tb.Name(), "/", "_")
}
