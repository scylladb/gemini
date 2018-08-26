// Copyright (C) 2018 ScyllaDB

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"

	"github.com/scylladb/gemini"
	"github.com/spf13/cobra"
)

var (
	testClusterHost   string
	oracleClusterHost string
	maxTests          int
	threads           int
	pkNumberPerThread int
	seed              int
	dropSchema        bool
	verbose           bool
	mode              string
)

const confFile = "schema.json"

type Status struct {
	WriteOps    int
	WriteErrors int
	ReadOps     int
	ReadErrors  int
}

type Results interface {
	Merge(*Status) Status
	Print()
}

type jsonSchema struct {
	Keyspace gemini.Keyspace `json:"keyspace"`
	Tables   []gemini.Table  `json:"tables"`
}

type testJob func(gemini.Schema, gemini.Table, *gemini.Session, gemini.PartitionRange, chan Status, string)

func (r *Status) Merge(sum *Status) Status {
	sum.WriteOps += r.WriteOps
	sum.WriteErrors += r.WriteErrors
	sum.ReadOps += r.ReadOps
	sum.ReadErrors += r.ReadErrors
	return *sum
}

func (r *Status) Print() {
	fmt.Println("Results:")
	fmt.Printf("\twrite ops: %v\n", r.WriteOps)
	fmt.Printf("\twrite errors: %v\n", r.WriteErrors)
	fmt.Printf("\tread ops: %v\n", r.ReadOps)
	fmt.Printf("\tread errors: %v\n", r.ReadErrors)
}

func createSchema() (gemini.Schema, error) {
	conf, err := os.Open(confFile)
	if err != nil {
		return nil, err
	}
	defer conf.Close()

	byteValue, err := ioutil.ReadAll(conf)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Schema: %v", string(byteValue))

	var shm jsonSchema

	err = json.Unmarshal(byteValue, &shm)
	if err != nil {
		return nil, err
	}

	schemaBuilder := gemini.NewSchemaBuilder()
	schemaBuilder.Keyspace(shm.Keyspace)
	for _, tbl := range shm.Tables {
		schemaBuilder.Table(tbl)
	}
	return schemaBuilder.Build(), nil
}

func run(cmd *cobra.Command, args []string) {
	rand.Seed(int64(seed))
	fmt.Printf("Seed: %d\n", seed)
	fmt.Printf("Test cluster: %s\n", testClusterHost)
	fmt.Printf("Oracle cluster: %s\n", oracleClusterHost)

	schema, err := createSchema()
	if err != nil {
		fmt.Printf("cannot create schema: %v", err)
		return
	}

	session := gemini.NewSession(testClusterHost, oracleClusterHost)
	defer session.Close()

	if dropSchema && mode != "read" {
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

	runJob(Job, schema, session, mode)
}

func runJob(f testJob, schema gemini.Schema, s *gemini.Session, mode string) {
	c := make(chan Status)
	minRange := 0
	maxRange := pkNumberPerThread

	for _, table := range schema.Tables() {
		for i := 0; i < threads; i++ {
			p := gemini.PartitionRange{Min: minRange + i*maxRange, Max: maxRange + i*maxRange}
			go f(schema, table, s, p, c, mode)
		}
	}

	var testRes Status
	for i := 0; i < threads*len(schema.Tables()); i++ {
		res := <-c
		testRes = res.Merge(&testRes)
	}

	testRes.Print()
}

func Job(schema gemini.Schema, table gemini.Table, s *gemini.Session, p gemini.PartitionRange, c chan Status, mode string) {
	testStatus := Status{}

	for i := 0; i < maxTests; i++ {
		if mode == "write" || mode == "mixed" {
			mutateStmt := schema.GenMutateStmt(table, &p)
			mutateQuery := mutateStmt.Query
			mutateValues := mutateStmt.Values()
			if verbose {
				fmt.Printf("%s (values=%v)\n", mutateQuery, mutateValues)
			}
			testStatus.WriteOps++
			if err := s.Mutate(mutateQuery, mutateValues...); err != nil {
				fmt.Printf("Failed! Mutation '%s' (values=%v) caused an error: '%v'\n", mutateQuery, mutateValues, err)
				testStatus.WriteErrors++
			}
		}
		if mode == "read" || mode == "mixed" {
			checkStmt := schema.GenCheckStmt(table, &p)
			checkQuery := checkStmt.Query
			checkValues := checkStmt.Values()
			if verbose {
				fmt.Printf("%s (values=%v)\n", checkQuery, checkValues)
			}
			err := s.Check(checkQuery, checkValues...)
			if err == nil {
				testStatus.ReadOps++
			} else {
				if err != gemini.ErrReadNoDataReturned {
					fmt.Printf("Failed! Check '%s' (values=%v)\n%s\n", checkQuery, checkValues, err)
					testStatus.ReadErrors++
				}
			}
		}
	}

	c <- testStatus
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
	rootCmd.Flags().StringVarP(&mode, "mode", "m", "mixed", "Mode options: write, read, mixed(default)")
	rootCmd.Flags().IntVarP(&maxTests, "max-tests", "n", 100, "Maximum number of test iterations to run")
	rootCmd.Flags().IntVarP(&threads, "threads", "c", 10, "Number of threads to run concurrently")
	rootCmd.Flags().IntVarP(&pkNumberPerThread, "max-pk-per-thread", "p", 50, "Maximum number of partition keys per thread")
	rootCmd.Flags().IntVarP(&seed, "seed", "s", 1, "PRNG seed value")
	rootCmd.Flags().BoolVarP(&dropSchema, "drop-schema", "d", false, "Drop schema before starting tests run")
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose output during test run")
}
