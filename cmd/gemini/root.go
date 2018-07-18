// Copyright (C) 2018 ScyllaDB

package main

import (
	"fmt"

	"github.com/scylladb/gemini"
	"github.com/spf13/cobra"
)

var (
	testClusterHost   string
	oracleClusterHost string
	maxTests          int
	verbose           bool
)

func run(cmd *cobra.Command, args []string) {
	nrPassedTests := 0
	createKeyspace := "CREATE KEYSPACE IF NOT EXISTS check WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
	createTable := "CREATE TABLE IF NOT EXISTS check.data (id int PRIMARY KEY)"

	fmt.Printf("Test cluster: %s\n", testClusterHost)
	fmt.Printf("Oracle cluster: %s\n", oracleClusterHost)

	session := gemini.NewSession(testClusterHost, oracleClusterHost)
	defer session.Close()

	if verbose {
		fmt.Printf("%s\n", createKeyspace)
	}
	if err := session.Mutate(createKeyspace); err != nil {
		fmt.Printf("%v", err)
		return
	}
	if verbose {
		fmt.Printf("%s\n", createTable)
	}
	if err := session.Mutate(createTable); err != nil {
		fmt.Printf("%v", err)
		return
	}

	for i := 0; i < maxTests; i++ {
		mutate := "INSERT INTO check.data (id) VALUES (1)"
		if verbose {
			fmt.Printf("%s\n", mutate)
		}
		if err := session.Mutate(mutate); err != nil {
			fmt.Printf("Failed! Mutation '%s' caused an error: '%v'\n", mutate, err)
			return
		}

		check := "SELECT * FROM check.data"
		if verbose {
			fmt.Printf("%s\n", check)
		}
		if diff := session.Check(check); diff != "" {
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
}

func init() {
	rootCmd.Flags().StringVarP(&testClusterHost, "test-cluster", "t", "", "Host name of the test cluster that is system under test")
	rootCmd.MarkFlagRequired("test-cluster")
	rootCmd.Flags().StringVarP(&oracleClusterHost, "oracle-cluster", "o", "", "Host name of the oracle cluster that provides correct answers")
	rootCmd.MarkFlagRequired("oracle-cluster")
	rootCmd.Flags().IntVarP(&maxTests, "max-tests", "m", 100, "Maximum number of test iterations to run")
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose output during test run")
}
