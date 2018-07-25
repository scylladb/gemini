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
	dropSchema        bool
	verbose           bool
)

func run(cmd *cobra.Command, args []string) {
	fmt.Printf("Test cluster: %s\n", testClusterHost)
	fmt.Printf("Oracle cluster: %s\n", oracleClusterHost)

	session := gemini.NewSession(testClusterHost, oracleClusterHost)
	defer session.Close()

	schemaBuilder := gemini.NewSchemaBuilder()
	schemaBuilder.Keyspace(gemini.Keyspace{
		Name: "gemini",
	})
	schemaBuilder.Table(gemini.Table{
		Name: "data",
		PartitionKeys: []gemini.ColumnDef{
			gemini.ColumnDef{
				Name: "pk",
				Type: "int",
			},
		},
		ClusteringKeys: []gemini.ColumnDef{
			gemini.ColumnDef{
				Name: "ck",
				Type: "int",
			},
		},
		Columns: []gemini.ColumnDef{
			gemini.ColumnDef{
				Name: "n",
				Type: "int",
			},
		},
	})
	schema := schemaBuilder.Build()
	if dropSchema {
		for _, stmt := range schema.GetDropSchema() {
			if verbose {
				fmt.Println(stmt)
			}
			if err := session.Mutate(stmt); err != nil {
				fmt.Printf("%v", err)
				return
			}
		}
	}
	for _, stmt := range schema.GetCreateSchema() {
		if verbose {
			fmt.Println(stmt)
		}
		if err := session.Mutate(stmt); err != nil {
			fmt.Printf("%v", err)
			return
		}
	}

	nrPassedTests := 0

	for i := 0; i < maxTests; i++ {
		mutateStmt := schema.GenMutateStmt()
		mutateQuery := mutateStmt.Query
		mutateValues := mutateStmt.Values()
		if verbose {
			fmt.Printf("%s (values=%v)\n", mutateQuery, mutateValues)
		}
		if err := session.Mutate(mutateQuery, mutateValues...); err != nil {
			fmt.Printf("Failed! Mutation '%s' (values=%v) caused an error: '%v'\n", mutateQuery, mutateValues, err)
			return
		}

		checkStmt := schema.GenCheckStmt()
		checkQuery := checkStmt.Query
		checkValues := checkStmt.Values()
		if verbose {
			fmt.Printf("%s (values=%v)\n", checkQuery, checkValues)
		}
		if diff := session.Check(checkQuery, checkValues...); diff != "" {
			fmt.Printf("Failed! Check '%s' (values=%v) rows differ (-oracle +test)\n%s", checkQuery, checkValues, diff)
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
	rootCmd.Flags().BoolVarP(&dropSchema, "drop-schema", "d", false, "Drop schema before starting tests run")
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose output during test run")
}
