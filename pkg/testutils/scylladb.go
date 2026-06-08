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
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/scylladb"
	"github.com/testcontainers/testcontainers-go/wait"
)

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
)

// TestPort returns the port the test cluster is reachable on — the random
// host-mapped port under testcontainers, or the fixed port (9042 / GEMINI_TEST_PORT)
// for an external cluster. Returns 0 if no test cluster is configured.
func (c *ScyllaContainer) TestPort() int {
	if c.TestCluster != nil {
		return c.TestCluster.Port
	}
	return 0
}

// OraclePort returns the port the oracle cluster is reachable on, or 0 when
// there is no oracle cluster (single-node setups).
func (c *ScyllaContainer) OraclePort() int {
	if c.OracleCluster != nil {
		return c.OracleCluster.Port
	}
	return 0
}

var (
	aioSetupOnce sync.Once

	reachabilityMu    sync.Mutex
	reachabilityCache = map[string]bool{}
)

// ensureHostAIOCapacity raises the Docker host/VM's fs.aio-max-nr so that
// several ScyllaDB (seastar) nodes can each acquire their AIO contexts.
//
// seastar needs ~66k AIO contexts per node; fs.aio-max-nr is a SYSTEM-WIDE
// (non-namespaced) kernel limit; and those contexts are not reclaimed promptly
// when a container exits. So the common default of 65536 is exhausted after the
// very first node, and every subsequent ScyllaDB container then fails to start
// with "Your system does not satisfy minimum AIO requirements". Because the
// limit is non-namespaced it cannot be set per-container via --sysctl (Docker
// only permits namespaced sysctls), so we raise it VM-wide once, via a
// short-lived privileged container.
//
// Best-effort and idempotent (runs at most once per test binary): if it fails,
// the subsequent ScyllaDB start will surface the underlying AIO error itself.
func ensureHostAIOCapacity(tb testing.TB) {
	tb.Helper()
	aioSetupOnce.Do(func() {
		const want = "1048576"
		out, err := exec.Command(
			"docker", "run", "--rm", "--privileged",
			"alpine", "sysctl", "-w", "fs.aio-max-nr="+want,
		).CombinedOutput()
		if err != nil {
			tb.Logf("warning: could not raise fs.aio-max-nr to %s (ScyllaDB nodes may fail to start): %v: %s",
				want, err, strings.TrimSpace(string(out)))
		}
	})
}

// hostReachable reports whether a TCP connection to addr can be established
// within a short timeout. Results are cached per address so the (possibly slow)
// probe is paid at most once for a given host:port across the whole test run.
func hostReachable(addr string) bool {
	reachabilityMu.Lock()
	defer reachabilityMu.Unlock()

	if reachable, ok := reachabilityCache[addr]; ok {
		return reachable
	}

	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	reachable := err == nil
	if conn != nil {
		_ = conn.Close()
	}
	reachabilityCache[addr] = reachable

	return reachable
}

// skipIfUnreachable skips the test when none of the configured external ScyllaDB
// hosts can be reached on the given port. This keeps the suite green on
// platforms where the Docker bridge IPs are not routable from the host — most
// notably macOS, where Docker Desktop cannot route to container bridge networks
// (the 192.168.100.x / 172.30.x subnets used by docker-compose). On Linux CI,
// where the IPs are reachable, the probe succeeds and the test runs as normal.
//
// It only guards the external-IP path (GEMINI_USE_DOCKER_SCYLLA=true); the
// testcontainers path uses host-mapped localhost ports that work everywhere.
func skipIfUnreachable(tb testing.TB, port int, hosts ...string) {
	tb.Helper()

	for _, h := range hosts {
		if hostReachable(net.JoinHostPort(strings.TrimSpace(h), strconv.Itoa(port))) {
			return // at least one host is reachable — run the test
		}
	}

	tb.Skipf(
		"skipping ScyllaDB integration test: none of the hosts %v are reachable on port %d; "+
			"Docker bridge IPs are not routable from the host on this platform (e.g. macOS Docker Desktop)",
		hosts, port,
	)
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
	// localhost:<random>) and the node advertises its internal Docker IP as
	// rpc_address.  Gocql would then try to reconnect to that internal IP which
	// is unreachable from the host.  Translate every discovered address back to
	// 127.0.0.1 so gocql always uses the mapped port. Also disable the
	// shard-aware port: it lives on a different container port (19042) that is
	// published on a *different* random host port, so the shard-aware address
	// (127.0.0.1:19042) is not reachable — sticking to the single mapped CQL port
	// avoids a stream of failed shard-aware connection attempts.
	if dockerMode {
		localhostIP := net.ParseIP("127.0.0.1")
		cluster.AddressTranslator = gocql.AddressTranslatorFunc(func(_ net.IP, p int) (net.IP, int) {
			return localhostIP, p
		})
		cluster.DisableShardAwarePort = true
	}

	return cluster
}

func createScyllaSession(tb testing.TB, port int, dockerMode bool, hosts ...string) *ScyllaContainer {
	tb.Helper()

	// Detect readiness by connecting to the (host-mapped) CQL port directly and
	// retrying until ScyllaDB is actually serving CQL. This deliberately does NOT
	// go through the Docker API (exec/log readiness probes), which becomes
	// unreliable under the heavy container churn these tests produce on Docker
	// Desktop — host port-forwarding keeps working even when the API is busy.
	const connectDeadline = 3 * time.Minute

	var (
		testCluster *gocql.ClusterConfig
		session     *gocql.Session
		err         error
	)
	deadline := time.Now().Add(connectDeadline)
	for {
		// Build a fresh ClusterConfig each attempt: a config that has already
		// produced a session must not be reused for another.
		testCluster = createClusterConfig(port, dockerMode, hosts...)
		if err = testCluster.Validate(); err != nil {
			tb.Fatalf("failed to validate ScyllaDB cluster config: %v", err)
		}

		session, err = testCluster.CreateSession()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			tb.Fatalf("ScyllaDB at %v:%d did not become reachable within %s: %v", hosts, port, connectDeadline, err)
		}
		time.Sleep(time.Second)
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

// scyllaNode is a single reusable ScyllaDB container plus a long-lived session
// to it. Nodes are pooled and handed out to tests one at a time, then reset and
// returned for the next test (see scyllaPool). They are reaped by the
// testcontainers ryuk reaper when the test process exits.
type scyllaNode struct {
	container *scylladb.Container
	session   *gocql.Session
	host      string
	version   string
	token     string // machine-wide registry slot held by this node
	port      int
}

// reset drops every user keyspace so the node is clean for the next borrower.
// Best-effort: errors are ignored (the next test uses a uniquely-named keyspace
// anyway). Called while the node is owned exclusively, so no concurrent access.
func (n *scyllaNode) reset() {
	iter := n.session.Query(
		"SELECT keyspace_name FROM system_schema.keyspaces",
	).Iter()

	var ks string
	var drop []string
	for iter.Scan(&ks) {
		if !strings.HasPrefix(ks, "system") {
			drop = append(drop, ks)
		}
	}
	_ = iter.Close()

	for _, ks := range drop {
		_ = n.session.Query("DROP KEYSPACE IF EXISTS " + ks).Exec()
	}
}

// scyllaPool is a process-local pool of reusable ScyllaDB nodes. Reuse keeps the
// container count down within a process; the machine-wide cap across all test
// processes is enforced separately, by the file-locked registry (see
// scylla_registry.go) which every node creation must reserve a slot from. Nodes
// are keyed by image version because the oracle and test clusters may run
// different ScyllaDB versions.
type scyllaPool struct {
	idle map[string][]*scyllaNode
	all  []*scyllaNode // every node this process created, for teardown
	cap  int
	mu   sync.Mutex
}

var (
	scyllaPoolOnce sync.Once
	scyllaPoolInst *scyllaPool
)

// pool returns the process-wide ScyllaDB node pool. Its cap — the maximum number
// of live ScyllaDB instances allowed MACHINE-WIDE across all parallel test
// processes — defaults to the number of executing threads (GOMAXPROCS) with a
// floor of 2 (a single oracle+test pair), and can be overridden with
// GEMINI_MAX_SCYLLA_NODES.
func pool() *scyllaPool {
	scyllaPoolOnce.Do(func() {
		maxNodes := runtime.GOMAXPROCS(0)
		if v := os.Getenv("GEMINI_MAX_SCYLLA_NODES"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				maxNodes = n
			}
		}
		if maxNodes < 2 {
			maxNodes = 2
		}
		scyllaPoolInst = &scyllaPool{cap: maxNodes, idle: make(map[string][]*scyllaNode)}
		installSignalCleanup()
	})
	return scyllaPoolInst
}

// installSignalCleanup makes a catchable termination signal (Ctrl-C / SIGINT,
// SIGTERM) tear the pool down before exiting, so an interrupted `go test` run
// does not leave ScyllaDB containers behind. It covers the gap between the two
// other mechanisms:
//   - graceful exit is handled by CloseScyllaPool in TestMain;
//   - uncatchable kills (SIGKILL, and the SIGQUIT `go test -timeout` raises) are
//     handled by testcontainers' ryuk reaper, which removes a process's
//     containers shortly after the process dies, plus the registry's reaping of
//     dead-owner orphans on the next run.
//
// SIGQUIT is intentionally NOT trapped so `go test`'s timeout stack dumps are
// preserved. After cleanup the signal is restored and re-raised so the process
// still exits with the normal signal semantics.
func installSignalCleanup() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-ch
		CloseScyllaPool()
		signal.Stop(ch)
		if s, ok := sig.(syscall.Signal); ok {
			signal.Reset(s)
			_ = syscall.Kill(os.Getpid(), s)
		}
		os.Exit(1)
	}()
}

// acquire borrows one node per requested version. It first satisfies what it can
// from idle (reused) nodes, then for the remainder reserves machine-wide slots
// from the file-locked registry and creates fresh nodes. To stay deadlock-free
// it never holds nodes while waiting: if it cannot reserve every slot it needs
// right now, it returns the idle nodes it took and retries, so other tests (and
// other processes) can make progress.
func (p *scyllaPool) acquire(tb testing.TB, versions []string) []*scyllaNode {
	tb.Helper()
	if len(versions) > p.cap {
		tb.Fatalf("requested %d ScyllaDB nodes but the machine-wide cap is %d (raise GEMINI_MAX_SCYLLA_NODES)", len(versions), p.cap)
	}

	for {
		// Take whatever we can from idle, recording the versions still missing.
		result := make([]*scyllaNode, len(versions))
		var taken []*scyllaNode
		var shortfall []int

		p.mu.Lock()
		for i, v := range versions {
			if idle := p.idle[v]; len(idle) > 0 {
				n := idle[len(idle)-1]
				p.idle[v] = idle[:len(idle)-1]
				result[i] = n
				taken = append(taken, n)
			} else {
				shortfall = append(shortfall, i)
			}
		}
		p.mu.Unlock()

		if len(shortfall) == 0 {
			return result
		}

		// Reserve a machine-wide slot per node we still need. Atomic (all or
		// nothing) so we never hold a partial reservation across processes.
		tokens := registryTryReserve(len(shortfall), p.cap)
		if tokens == nil {
			// Machine is at capacity. Return the idle nodes we grabbed so others
			// can use them, back off, and retry the whole request.
			p.release(taken)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		created, err := p.createNodes(tb, versions, shortfall, tokens)
		if err != nil {
			p.release(append(taken, created...))
			registryRelease(tokens[len(created):]...) // unused reservations
			tb.Fatalf("failed to create ScyllaDB node: %v", err)
			return nil
		}
		for j, i := range shortfall {
			result[i] = created[j]
		}
		return result
	}
}

// createNodes spins up a fresh node for each shortfall version, binding each to
// a pre-reserved registry token. On error it returns the nodes created so far so
// the caller can release them; the leftover tokens belong to the caller.
func (p *scyllaPool) createNodes(tb testing.TB, versions []string, shortfall []int, tokens []string) ([]*scyllaNode, error) {
	tb.Helper()
	created := make([]*scyllaNode, 0, len(shortfall))
	for j, i := range shortfall {
		n, err := newScyllaNode(tb, versions[i], tokens[j])
		if err != nil {
			return created, err
		}

		p.mu.Lock()
		p.all = append(p.all, n)
		p.mu.Unlock()

		created = append(created, n)
	}
	return created, nil
}

// release resets each node and returns it to the idle pool for reuse. The
// machine-wide registry slot is intentionally NOT freed here — the container
// stays alive for reuse and is released only at CloseScyllaPool.
func (p *scyllaPool) release(nodes []*scyllaNode) {
	for _, n := range nodes {
		n.reset()
	}
	p.mu.Lock()
	for _, n := range nodes {
		p.idle[n.version] = append(p.idle[n.version], n)
	}
	p.mu.Unlock()
}

// CloseScyllaPool closes the sessions and terminates the containers of every
// pooled ScyllaDB node. It MUST be called once, after all tests in a package
// have finished (i.e. from TestMain, after m.Run()), because pooled nodes are
// shared across tests and intentionally outlive any single one — without this
// the containers would keep running after the test binary exits. Safe to call
// when no pool was ever created (it is then a no-op).
func CloseScyllaPool() {
	if scyllaPoolInst == nil {
		return
	}

	scyllaPoolInst.mu.Lock()
	nodes := scyllaPoolInst.all
	scyllaPoolInst.all = nil
	scyllaPoolInst.idle = make(map[string][]*scyllaNode)
	scyllaPoolInst.mu.Unlock()

	ctx := context.Background()
	for _, n := range nodes {
		if n.session != nil {
			n.session.Close()
		}
		if n.container != nil {
			_ = n.container.Terminate(ctx)
		}
	}
	// Drop ALL of this process's registry slots and force-remove any container
	// still recorded for them. Using PID rather than the tracked node list also
	// cleans up reservations/containers from a creation that was interrupted
	// (e.g. by a signal) before the node was tracked.
	registryReleaseMine()
}

// acquireNodes borrows nodes for the lifetime of the test, returning them to
// the pool (reset and reusable) when the test finishes.
func acquireNodes(tb testing.TB, versions ...string) []*scyllaNode {
	tb.Helper()
	p := pool()
	nodes := p.acquire(tb, versions)
	tb.Cleanup(func() { p.release(nodes) })
	return nodes
}

// newScyllaNode starts a single ScyllaDB container of the given image version
// and connects a session to it.
//
// It MUST use context.Background(), NOT tb.Context(): a pooled node outlives the
// test that first created it and is reused by others. testcontainers binds the
// ryuk reaper connection to the context of the first container it creates — if
// that were the creating test's context, the reaper connection would drop when
// that test ends, ryuk would reap the still-in-use pooled containers, and other
// tests would see "connection reset by peer". Teardown is explicit, via
// CloseScyllaPool from TestMain.
func newScyllaNode(tb testing.TB, version, token string) (*scyllaNode, error) {
	tb.Helper()

	// ScyllaDB (seastar) needs enough system-wide AIO contexts; raise the limit
	// once before spawning any node.
	ensureHostAIOCapacity(tb)

	// NOTE: deliberately context.Background(), not tb.Context() (see the doc
	// comment above) — a pooled node outlives its creating test, and tying the
	// container/ryuk lifecycle to that test's context tears it down mid-reuse.
	//nolint:usetesting // pooled container must not be bound to a single test's context
	ctx := context.Background()

	opts := []testcontainers.ContainerCustomizer{
		// - 1 GiB (vs the module default 512 MiB) so ScyllaDB bootstraps reliably
		//   on resource-constrained Docker hosts.
		// - reactor-backend=epoll avoids the linux-aio reactor, whose per-node
		//   demand on the (system-wide, non-namespaced) fs.aio-max-nr budget is
		//   ~66k contexts. With many ScyllaDB nodes running in parallel that
		//   budget is exhausted and seastar refuses to start ("does not satisfy
		//   minimum AIO requirements"); the epoll backend needs only a tiny
		//   fraction, so nodes no longer contend for it.
		scylladb.WithCustomCommands(
			"--memory=1G", "--smp=1", "--developer-mode=1", "--overprovisioned=1",
			"--reactor-backend=epoll",
		),
		// Only the regular CQL port (9042) is published, on a random free host
		// port (the scylladb module exposes it by default). Shard-awareness is
		// intentionally NOT enabled: it would expose a second port (19042) on yet
		// another random host port the client can't address through localhost.
		// The client disables the shard-aware port to match. No custom network
		// either: single nodes reached via the host-mapped port never need to
		// talk to each other, and the default bridge avoids network churn.
		//
		// Wait only for the port *mapping* to be published (a quick inspect, no
		// docker exec / log streaming, both of which get wedged under heavy
		// container churn on Docker Desktop); real CQL readiness is polled
		// directly against the mapped port below.
		testcontainers.WithWaitStrategyAndDeadline(
			time.Minute,
			wait.ForListeningPort("9042/tcp").SkipInternalCheck().SkipExternalCheck(),
		),
	}

	// ScyllaDB occasionally fails to come up on the first try under resource
	// contention; retry a couple of times, terminating the failed container.
	const maxStartAttempts = 3
	var (
		ctr *scylladb.Container
		err error
	)
	for attempt := 1; attempt <= maxStartAttempts; attempt++ {
		ctr, err = scylladb.Run(ctx, "scylladb/scylla:"+version, opts...)
		if err == nil {
			break
		}
		if ctr != nil {
			_ = ctr.Terminate(ctx)
			ctr = nil
		}
		tb.Logf("ScyllaDB %q container failed to start (attempt %d/%d): %v", version, attempt, maxStartAttempts, err)
	}
	if err != nil {
		return nil, fmt.Errorf("start ScyllaDB %q container: %w", version, err)
	}

	// Record the container ID against our reserved registry slot immediately,
	// before the (slow) readiness poll — so if this process is interrupted while
	// waiting for ScyllaDB to come up, teardown (and dead-owner reaping) can still
	// find and remove the container.
	registryBindContainer(token, ctr.GetContainerID())

	host := Must(ctr.Host(ctx))
	mapped := Must(ctr.MappedPort(ctx, "9042/tcp"))
	port := int(Must(strconv.ParseInt(mapped.Port(), 10, 32)))

	// Detect readiness by connecting to the mapped CQL port directly, retrying
	// until ScyllaDB is serving. This avoids the Docker API entirely (host
	// port-forwarding keeps working even when the API is busy).
	deadline := time.Now().Add(3 * time.Minute)
	var session *gocql.Session
	for {
		cluster := createClusterConfig(port, true, host)
		if err = cluster.Validate(); err != nil {
			_ = ctr.Terminate(ctx)
			return nil, fmt.Errorf("validate cluster config: %w", err)
		}
		if session, err = cluster.CreateSession(); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = ctr.Terminate(ctx)
			return nil, fmt.Errorf("ScyllaDB %s:%d not reachable: %w", host, port, err)
		}
		time.Sleep(time.Second)
	}

	return &scyllaNode{container: ctr, session: session, host: host, port: port, version: version, token: token}, nil
}

func SingleScylla(tb testing.TB, forceSpawn ...bool) *ScyllaContainer {
	tb.Helper()

	val, existsDockerScylla := os.LookupEnv("GEMINI_USE_DOCKER_SCYLLA")
	if (len(forceSpawn) > 0 && forceSpawn[0]) || !existsDockerScylla {
		testVersion, exists := os.LookupEnv("GEMINI_SCYLLA_TEST")
		if !exists || testVersion == "" {
			testVersion = "2025.1"
		}

		test := acquireNodes(tb, testVersion)[0]
		return &ScyllaContainer{
			Test:          test.session,
			TestHosts:     []string{test.host},
			TestContainer: test.container,
			// Fresh ClusterConfig carrying the real host-mapped port so stores
			// built by tests dial the right place.
			TestCluster: createClusterConfig(test.port, true, test.host),
			DockerMode:  true,
		}
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

		hostList := strings.Split(hosts, ",")
		skipIfUnreachable(tb, port, hostList...)

		return createScyllaSession(tb, port, false, hostList...)
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

		// Borrow an oracle node and a test node together (atomically) from the
		// pool so concurrent oracle+test requests can't deadlock each other.
		nodes := acquireNodes(tb, oracleVersion, testVersion)
		oracle, test := nodes[0], nodes[1]

		return &ScyllaContainer{
			Oracle:          oracle.session,
			OracleHosts:     []string{oracle.host},
			OracleContainer: oracle.container,
			Test:            test.session,
			TestHosts:       []string{test.host},
			TestContainer:   test.container,
			// Fresh ClusterConfig instances carrying the real host-mapped ports
			// (NOT a fixed 9042 — testcontainers publishes each 9042 on a random
			// host port). Fresh configs avoid reusing one that already created a
			// session (which would panic on the shared host-selection policy).
			TestCluster:   createClusterConfig(test.port, true, test.host),
			OracleCluster: createClusterConfig(oracle.port, true, oracle.host),
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

		oracleHostList := strings.Split(oracleHosts, ",")
		testHostList := strings.Split(testHosts, ",")
		skipIfUnreachable(tb, oraclePort, oracleHostList...)
		skipIfUnreachable(tb, testPort, testHostList...)

		oracleSession := createScyllaSession(tb, oraclePort, false, oracleHostList...)
		testSession := createScyllaSession(tb, testPort, false, testHostList...)

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
