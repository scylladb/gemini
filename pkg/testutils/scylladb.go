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
	"net"
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
		OracleContainer testcontainers.Container
		TestContainer   testcontainers.Container
		OracleCluster   *gocql.ClusterConfig
		TestCluster     *gocql.ClusterConfig
		Oracle          *gocql.Session
		Test            *gocql.Session
		OracleHosts     []string
		TestHosts       []string
		// DockerMode is true when the clusters were started via testcontainers
		// and require address translation (all peer IPs rewritten to 127.0.0.1).
		DockerMode bool
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

func createClusterConfig(port int, dockerMode bool, hosts ...string) *gocql.ClusterConfig {
	cluster := gocql.NewCluster(hosts...)
	if port > 0 {
		cluster.Port = port
	}
	cluster.Timeout = 10 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		Min:        20 * time.Microsecond,
		Max:        50 * time.Millisecond,
		NumRetries: 10,
	}
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	cluster.Consistency = gocql.Quorum
	cluster.DefaultTimestamp = false

	// When dockerMode is true, tests connect via a host-mapped port (e.g.
	// localhost:19042) and the node advertises its internal Docker IP as
	// rpc_address.  Gocql would then try to reconnect to that internal IP which
	// is unreachable from the host.  Translate every discovered address back to
	// 127.0.0.1 so gocql always uses the mapped port.
	if dockerMode {
		localhostIP := net.ParseIP("127.0.0.1")
		cluster.AddressTranslator = gocql.AddressTranslatorFunc(func(_ net.IP, p int) (net.IP, int) {
			return localhostIP, p
		})
	}

	return cluster
}

func createScyllaSession(tb testing.TB, port int, dockerMode bool, hosts ...string) *ScyllaContainer {
	tb.Helper()

	testCluster := createClusterConfig(port, dockerMode, hosts...)
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
		Test:        session,
		TestCluster: testCluster,
		TestHosts:   hosts,
		DockerMode:  dockerMode,
	}
}

func spawnScylla(tb testing.TB, name, version string, sharedNetwork *testcontainers.DockerNetwork) *ScyllaContainer {
	tb.Helper()
	spawningScyllaMutex.Lock()
	defer spawningScyllaMutex.Unlock()

	var opts []testcontainers.ContainerCustomizer
	opts = append(opts,
		scylladb.WithCustomCommands("--memory=512M", "--smp=1", "--developer-mode=1", "--overprovisioned=1"),
		scylladb.WithShardAwareness(),
		network.WithNetwork([]string{ContainerNetworkName}, sharedNetwork),
	)

	switch name {
	case "oracle":
		if p := os.Getenv("GEMINI_ORACLE_PORT"); p != "" {
			opts = append(opts, testcontainers.WithExposedPorts(p+":9042/tcp"))
		}
	case "test":
		if p := os.Getenv("GEMINI_TEST_PORT"); p != "" {
			opts = append(opts, testcontainers.WithExposedPorts(p+":9042/tcp"))
		}
	}

	scyllaContainer, err := scylladb.Run(tb.Context(),
		"scylladb/scylla:"+version,
		opts...,
	)
	if err != nil {
		tb.Fatalf("failed to start %s ScyllaDB container: %v", name, err)
	}

	tb.Cleanup(func() {
		_ = scyllaContainer.Terminate(tb.Context())
	})

	host := Must(scyllaContainer.Host(tb.Context()))
	port := Must(scyllaContainer.MappedPort(tb.Context(), "9042/tcp"))

	container := createScyllaSession(tb, int(Must(strconv.ParseInt(port.Port(), 10, 32))), true, host)
	switch name {
	case "oracle":
		container.OracleContainer = scyllaContainer
	case "test":
		container.TestContainer = scyllaContainer
	default:
		tb.Fatalf("unknown ScyllaDB: %s", name)
	}
	return container
}

func SingleScylla(tb testing.TB, forceSpawn ...bool) *ScyllaContainer {
	tb.Helper()

	val, existsDockerScylla := os.LookupEnv("GEMINI_USE_DOCKER_SCYLLA")
	if (len(forceSpawn) > 0 && forceSpawn[0]) || !existsDockerScylla {
		testVersion, exists := os.LookupEnv("GEMINI_SCYLLA_TEST")
		if !exists || testVersion == "" {
			testVersion = "2025.1"
		}

		sharedNetwork := createScyllaNetwork(tb)

		return spawnScylla(tb, "test", testVersion, sharedNetwork)
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

		portStr, ok := os.LookupEnv("GEMINI_TEST_PORT")
		if !ok || portStr == "" {
			portStr = "9042"
		}

		port := int(Must(strconv.ParseInt(portStr, 10, 32)))

		return createScyllaSession(tb, port, false, strings.Split(hosts, ",")...)
	}

	tb.Fatal("GEMINI_USE_DOCKER_SCYLLA is set to false, but using in-memory ScyllaDB is not supported in this build")
	return nil
}

//nolint:gocyclo
func TestContainers(tb testing.TB, forceSpawn ...bool) *ScyllaContainer {
	tb.Helper()
	val, exists := os.LookupEnv("GEMINI_USE_DOCKER_SCYLLA")

	if (len(forceSpawn) > 0 && forceSpawn[0]) || !exists {
		oracleVersion, existsScyllaOracle := os.LookupEnv("GEMINI_SCYLLA_ORACLE")
		if !existsScyllaOracle || oracleVersion == "" {
			oracleVersion = "6.2"
		}

		testVersion, existsScyllaTest := os.LookupEnv("GEMINI_SCYLLA_TEST")
		if !existsScyllaTest || testVersion == "" {
			testVersion = "2025.1"
		}

		sharedNetwork := createScyllaNetwork(tb)

		oracleScylla := spawnScylla(tb, "oracle", oracleVersion, sharedNetwork)
		testScylla := spawnScylla(tb, "test", testVersion, sharedNetwork)

		oraclePortStr, ok := os.LookupEnv("GEMINI_ORACLE_PORT")
		if !ok || oraclePortStr != "" {
			oraclePortStr = "9042"
		}

		oraclePort := int(Must(strconv.ParseInt(oraclePortStr, 10, 32)))

		testPortStr, ok := os.LookupEnv("GEMINI_TEST_PORT")
		if !ok || testPortStr == "" {
			testPortStr = "9042"
		}

		testPort := int(Must(strconv.ParseInt(testPortStr, 10, 32)))

		return &ScyllaContainer{
			Oracle:          oracleScylla.Test,
			OracleHosts:     oracleScylla.TestHosts,
			Test:            testScylla.Test,
			TestHosts:       testScylla.TestHosts,
			TestContainer:   testScylla.TestContainer,
			OracleContainer: oracleScylla.OracleContainer,
			// Provide fresh ClusterConfig instances for use by stores to avoid
			// reusing a ClusterConfig that already created a session (which would panic
			// due to sharing host selection policy between sessions).
			TestCluster:   createClusterConfig(testPort, true, testScylla.TestHosts...),
			OracleCluster: createClusterConfig(oraclePort, true, oracleScylla.TestHosts...),
			DockerMode:    true,
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

		oraclePortStr, ok := os.LookupEnv("GEMINI_ORACLE_PORT")
		if !ok || oraclePortStr == "" {
			oraclePortStr = "9042"
		}

		oraclePort := int(Must(strconv.ParseInt(oraclePortStr, 10, 32)))

		testPortStr, ok := os.LookupEnv("GEMINI_TEST_PORT")
		if !ok || testPortStr == "" {
			testPortStr = "9042"
		}

		testPort := int(Must(strconv.ParseInt(testPortStr, 10, 32)))

		oracleSession := createScyllaSession(tb, oraclePort, false, strings.Split(oracleHosts, ",")...)
		testSession := createScyllaSession(tb, testPort, false, strings.Split(testHosts, ",")...)

		return &ScyllaContainer{
			Oracle:        oracleSession.Test,
			OracleHosts:   oracleSession.TestHosts,
			OracleCluster: createClusterConfig(oraclePort, false, strings.Split(oracleHosts, ",")...),
			Test:          testSession.Test,
			TestHosts:     testSession.TestHosts,
			TestCluster:   createClusterConfig(testPort, false, strings.Split(testHosts, ",")...),
		}
	}

	tb.Fatal("GEMINI_USE_DOCKER_SCYLLA is set to false, but using in-memory ScyllaDB is not supported in this build")
	return nil
}

func GenerateUniqueKeyspaceName(tb testing.TB) string {
	tb.Helper()

	return "ks" + strings.ReplaceAll(tb.Name(), "/", "_")
}
