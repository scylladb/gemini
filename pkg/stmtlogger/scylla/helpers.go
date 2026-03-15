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

package scylla

import (
	"net"
	"slices"
	"time"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
)

func GetScyllaStatementLogsKeyspace(originalKeyspace string) string {
	return originalKeyspace + "_logs"
}

func GetScyllaStatementLogsTable(originalTable string) string {
	return originalTable + "_statements"
}

// applyLocalhostTranslator installs an AddressTranslator on cluster that
// rewrites every peer IP to 127.0.0.1, preserving the port.  This is the
// Docker/testcontainer workaround: when a node advertises its internal
// container IP as rpc_address, gocql would otherwise try to reconnect to an
// IP that is unreachable from the host.  A warning is logged because silently
// rerouting all traffic to a single address causes confusing behaviour in
// production.
func applyLocalhostTranslator(cluster *gocql.ClusterConfig, logger *zap.Logger) {
	logger.Warn("DockerMode is enabled: all discovered peer addresses will be translated to 127.0.0.1; do NOT enable this in production")
	localhostIP := net.ParseIP("127.0.0.1")
	cluster.AddressTranslator = gocql.AddressTranslatorFunc(func(_ net.IP, p int) (net.IP, int) {
		return localhostIP, p
	})
}

func newSession(hosts []string, port int, dockerMode bool, username, password string, logger *zap.Logger) (*gocql.Session, error) {
	cluster := gocql.NewCluster(slices.Clone(hosts)...)
	cluster.Consistency = gocql.Quorum
	cluster.DefaultTimestamp = false
	cluster.Logger = zap.NewStdLog(logger.Named("statements-scylla"))
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	cluster.Timeout = 10 * time.Second
	cluster.ConnectTimeout = 60 * time.Second
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		Min:        time.Second,
		Max:        10 * time.Second,
		NumRetries: 5,
	}
	cluster.Compressor = &gocql.SnappyCompressor{}

	if port > 0 {
		cluster.Port = port
	}

	// DockerMode: translate every discovered peer address back to 127.0.0.1.
	// Must only be enabled explicitly — never inferred from port/host values.
	if dockerMode {
		applyLocalhostTranslator(cluster, logger)
	}

	if username != "" && password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}

	return cluster.CreateSession()
}
