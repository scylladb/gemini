// Copyright (C) 2018 ScyllaDB

package cmd

import (
	"fmt"
	"os"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/spf13/cobra"
)

var (
	testClusterHost   string
	oracleClusterHost string
	maxTests          int
	verbose           bool
)

type checkSession struct {
	testSession   *gocql.Session
	oracleSession *gocql.Session
}

func start(testClusterHost string, oracleClusterHost string) *checkSession {
	testCluster := gocql.NewCluster(testClusterHost)
	testSession, err := testCluster.CreateSession()
	if err != nil {
		panic(err)
	}

	oracleCluster := gocql.NewCluster(oracleClusterHost)
	oracleSession, err := oracleCluster.CreateSession()
	if err != nil {
		panic(err)
	}

	return &checkSession{
		testSession:   testSession,
		oracleSession: oracleSession,
	}
}

func (s *checkSession) close() {
	s.testSession.Close()
	s.oracleSession.Close()
}

func (s *checkSession) mutate(query string) error {
	if err := s.testSession.Query(query).Exec(); err != nil {
		return fmt.Errorf("%v [cluster = test, query = '%s']", err, query)
	}
	if err := s.oracleSession.Query(query).Exec(); err != nil {
		return fmt.Errorf("%v [cluster = oracle, query = '%s']", err, query)
	}
	return nil
}

func (s *checkSession) check(query string) string {
	testIter := s.testSession.Query(query).Iter()
	oracleIter := s.oracleSession.Query(query).Iter()
	for {
		testRow := make(map[string]interface{})
		if !testIter.MapScan(testRow) {
			break
		}
		oracleRow := make(map[string]interface{})
		if !oracleIter.MapScan(oracleRow) {
			break
		}
		if diff := cmp.Diff(oracleRow, testRow); diff != "" {
			return diff
		}
	}
	return ""
}

func run(cmd *cobra.Command, args []string) {
	nrPassedTests := 0
	createKeyspace := "CREATE KEYSPACE IF NOT EXISTS check WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
	createTable := "CREATE TABLE IF NOT EXISTS check.data (id int PRIMARY KEY)"

	fmt.Printf("Test cluster: %s\n", testClusterHost)
	fmt.Printf("Oracle cluster: %s\n", oracleClusterHost)

	session := start(testClusterHost, oracleClusterHost)
	defer session.close()

	if verbose {
		fmt.Printf("%s\n", createKeyspace)
	}
	if err := session.mutate(createKeyspace); err != nil {
		fmt.Printf("%v", err)
		return
	}
	if verbose {
		fmt.Printf("%s\n", createTable)
	}
	if err := session.mutate(createTable); err != nil {
		fmt.Printf("%v", err)
		return
	}

	for i := 0; i < maxTests; i++ {
		mutate := "INSERT INTO check.data (id) VALUES (1)"
		if verbose {
			fmt.Printf("%s\n", mutate)
		}
		if err := session.mutate(mutate); err != nil {
			fmt.Printf("Failed! Mutation '%s' caused an error: '%v'\n", mutate, err)
			return
		}

		check := "SELECT * FROM check.data"
		if verbose {
			fmt.Printf("%s\n", check)
		}
		if diff := session.check(check); diff != "" {
			fmt.Printf("Failed! Check '%s' rows differ (-oracle +test)\n%s", check, diff)
			return
		}
		nrPassedTests++
	}
	fmt.Printf("OK, passed %d tests.\n", nrPassedTests)
}

var rootCmd = &cobra.Command{
	Use:   "gemini",
	Short: "Gemini is an automatic random testing tool for Scylla.",
	Run:   run,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().StringVarP(&testClusterHost, "test-cluster", "t", "", "Host name of the test cluster that is system under test")
	rootCmd.MarkFlagRequired("test-cluster")
	rootCmd.Flags().StringVarP(&oracleClusterHost, "oracle-cluster", "o", "", "Host name of the oracle cluster that provides correct answers")
	rootCmd.MarkFlagRequired("oracle-cluster")
	rootCmd.Flags().IntVarP(&maxTests, "max-tests", "m", 100, "Maximum number of test iterations to run")
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose output during test run")
}
