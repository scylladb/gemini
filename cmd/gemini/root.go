// Copyright (C) 2018 ScyllaDB

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/briandowns/spinner"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/scylladb/gemini"
	"github.com/scylladb/gemini/store"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

var (
	testClusterHost    []string
	oracleClusterHost  []string
	schemaFile         string
	outFileArg         string
	concurrency        int
	pkNumberPerThread  int
	seed               int
	dropSchema         bool
	verbose            bool
	mode               string
	failFast           bool
	nonInteractive     bool
	duration           time.Duration
	bind               string
	warmup             time.Duration
	compactionStrategy string
)

const (
	writeMode = "write"
	readMode  = "read"
	mixedMode = "mixed"
)

type Status struct {
	WriteOps    int        `json:"write_ops"`
	WriteErrors int        `json:"write_errors"`
	ReadOps     int        `json:"read_ops"`
	ReadErrors  int        `json:"read_errors"`
	Errors      []JobError `json:"errors,omitempty"`
}

type JobError struct {
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
	Query     string    `json:"query"`
}

type Results interface {
	Merge(*Status) Status
	Print()
}

func interactive() bool {
	return !nonInteractive
}

type testJob func(context.Context, *sync.WaitGroup, *gemini.Schema, *gemini.Table, store.Store, gemini.PartitionRange, chan Status, string, *os.File, time.Duration)

func (r *Status) Merge(sum *Status) Status {
	sum.WriteOps += r.WriteOps
	sum.WriteErrors += r.WriteErrors
	sum.ReadOps += r.ReadOps
	sum.ReadErrors += r.ReadErrors
	sum.Errors = append(sum.Errors, r.Errors...)
	return *sum
}

func (r *Status) PrintResult(w io.Writer) {
	if err := r.PrintResultAsJSON(w); err != nil {
		// In case there has been it has been a long run we want to display it anyway...
		fmt.Printf("Unable to print result as json, using plain text to stdout, error=%s\n", err)
		fmt.Printf("Gemini version: %s\n", version)
		fmt.Printf("Results:\n")
		fmt.Printf("\twrite ops:    %v\n", r.WriteOps)
		fmt.Printf("\tread ops:     %v\n", r.ReadOps)
		fmt.Printf("\twrite errors: %v\n", r.WriteErrors)
		fmt.Printf("\tread errors:  %v\n", r.ReadErrors)
		for i, err := range r.Errors {
			fmt.Printf("Error %d: %s\n", i, err)
		}
	}
}

func (r *Status) PrintResultAsJSON(w io.Writer) error {
	result := map[string]interface{}{
		"result":         r,
		"gemini_version": version,
	}
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent(" ", " ")
	if err := encoder.Encode(result); err != nil {
		return errors.Wrap(err, "unable to create json from result")
	}
	return nil
}

func (r Status) String() string {
	return fmt.Sprintf("write ops: %v | read ops: %v | write errors: %v | read errors: %v", r.WriteOps, r.ReadOps, r.WriteErrors, r.ReadErrors)
}

func readSchema(confFile string) (*gemini.Schema, error) {
	byteValue, err := ioutil.ReadFile(confFile)
	if err != nil {
		return nil, err
	}

	var shm gemini.Schema

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

type createBuilder struct {
	stmt string
}

func (cb createBuilder) ToCql() (stmt string, names []string) {
	return cb.stmt, nil
}

func run(cmd *cobra.Command, args []string) {

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		_ = http.ListenAndServe(bind, nil)
	}()

	if pkNumberPerThread <= 0 || pkNumberPerThread > (math.MaxInt32/concurrency) {
		pkNumberPerThread = math.MaxInt32 / concurrency
	}
	if err := printSetup(); err != nil {
		fmt.Println(err)
		return
	}

	outFile := os.Stdout
	if outFileArg != "" {
		of, err := os.Create(outFileArg)
		if err != nil {
			fmt.Printf("Unable to open output file %s, error=%s\n", outFileArg, err)
			return
		}
		outFile = of
	}
	defer outFile.Sync()

	var schema *gemini.Schema
	if len(schemaFile) > 0 {
		var err error
		schema, err = readSchema(schemaFile)
		if err != nil {
			fmt.Printf("cannot create schema: %v", err)
			return
		}
	} else {
		schema = gemini.GenSchema(getCompactionStrategy(compactionStrategy))
	}

	jsonSchema, _ := json.MarshalIndent(schema, "", "    ")
	fmt.Printf("Schema: %v\n", string(jsonSchema))

	store := store.New(schema, testClusterHost, oracleClusterHost)
	defer store.Close()

	if dropSchema && mode != readMode {
		for _, stmt := range schema.GetDropSchema() {
			if verbose {
				fmt.Println(stmt)
			}
			if err := store.Mutate(context.Background(), createBuilder{stmt: stmt}); err != nil {
				fmt.Printf("%v", err)
				return
			}
		}
	}
	for _, stmt := range schema.GetCreateSchema() {
		if verbose {
			fmt.Println(stmt)
		}
		if err := store.Mutate(context.Background(), createBuilder{stmt: stmt}); err != nil {
			fmt.Printf("%v", err)
			return
		}
	}

	runJob(Job, schema, store, mode, outFile)
}

func getCompactionStrategy(cs string) *gemini.CompactionStrategy {
	switch cs {
	case "stcs":
		return gemini.NewSizeTieredCompactionStrategy()
	case "twcs":
		return gemini.NewTimeWindowCompactionStrategy()
	case "lcs":
		return gemini.NewLeveledCompactionStrategy()
	case "":
		return nil
	default:
		compactionStrategy := &gemini.CompactionStrategy{}
		if err := json.Unmarshal([]byte(strings.ReplaceAll(cs, "'", "\"")), compactionStrategy); err != nil {
			fmt.Printf("unable to parse compaction strategy '%s', err=%s\n", cs, err)
			return nil
		}
		return compactionStrategy
	}
}

func runJob(f testJob, schema *gemini.Schema, s store.Store, mode string, out *os.File) {
	defer out.Sync()
	c := make(chan Status)
	minRange := 0
	maxRange := pkNumberPerThread

	// Wait group for the worker goroutines.
	var workers sync.WaitGroup
	workerCtx, cancelWorkers := context.WithCancel(context.Background())
	workers.Add(len(schema.Tables) * concurrency)

	for _, table := range schema.Tables {
		for i := 0; i < concurrency; i++ {
			p := gemini.PartitionRange{
				Min:  minRange + i*maxRange,
				Max:  maxRange + i*maxRange,
				Rand: rand.New(rand.NewSource(int64(seed))),
			}
			go f(workerCtx, &workers, schema, table, s, p, c, mode, out, warmup)
		}
	}

	// Gracefully terminate
	var gracefulStop = make(chan os.Signal)
	signal.Notify(gracefulStop, syscall.SIGTERM, syscall.SIGINT)

	// Wait group for the reporter goroutine.
	var reporter sync.WaitGroup
	reporter.Add(1)
	reporterCtx, cancelReporter := context.WithCancel(context.Background())
	go func(d time.Duration) {
		defer reporter.Done()
		var testRes Status
		timer := time.NewTimer(d)
		var sp *spinner.Spinner = nil
		if interactive() {
			spinnerCharSet := []string{"|", "/", "-", "\\"}
			sp = spinner.New(spinnerCharSet, 1*time.Second)
			sp.Color("black")
			sp.Start()
			defer sp.Stop()
		}
		for {
			select {
			case <-gracefulStop:
				stop(cancelWorkers, c, out, testRes)
				fmt.Println("Test run aborted. Exiting.")
			case <-timer.C:
				stop(cancelWorkers, c, out, testRes)
				fmt.Println("Test run completed. Exiting.")
				return
			case <-reporterCtx.Done():
				return
			case res := <-c:
				testRes = res.Merge(&testRes)
				if sp != nil {
					sp.Suffix = fmt.Sprintf(" Running Gemini... %v", testRes)
				}
				if testRes.ReadErrors > 0 {
					if failFast {
						fmt.Println("Error in data validation. Exiting.")
						stop(cancelWorkers, c, out, testRes)
						return
					}
					testRes.PrintResult(out)
				}
			}
		}
	}(duration + warmup)

	workers.Wait()
	close(c)
	cancelReporter()
	reporter.Wait()
}

func stop(cancel context.CancelFunc, c chan Status, out io.Writer, res Status) {
	cancel()
	res = drain(c, res)
	res.PrintResult(out)
}

func mutationJob(ctx context.Context, schema *gemini.Schema, table *gemini.Table, s store.Store, p gemini.PartitionRange, testStatus *Status, out *os.File, deletes bool) {
	mutateStmt, err := schema.GenMutateStmt(table, &p, deletes)
	if err != nil {
		fmt.Printf("Failed! Mutation statement generation failed: '%v'\n", err)
		testStatus.WriteErrors++
		return
	}
	mutateQuery := mutateStmt.Query
	mutateValues := mutateStmt.Values()
	if verbose {
		fmt.Println(mutateStmt.PrettyCQL())
	}
	if err := s.Mutate(ctx, mutateQuery, mutateValues...); err != nil {
		e := JobError{
			Timestamp: time.Now(),
			Message:   "Mutation failed: " + err.Error(),
			Query:     mutateStmt.PrettyCQL(),
		}
		testStatus.Errors = append(testStatus.Errors, e)
		testStatus.WriteErrors++
	} else {
		testStatus.WriteOps++
	}
}

func validationJob(ctx context.Context, schema *gemini.Schema, table *gemini.Table, s store.Store, p gemini.PartitionRange, testStatus *Status, out *os.File) {
	checkStmt := schema.GenCheckStmt(table, &p)
	checkQuery := checkStmt.Query
	checkValues := checkStmt.Values()
	if verbose {
		fmt.Println(checkStmt.PrettyCQL())
	}
	if err := s.Check(ctx, table, checkQuery, checkValues...); err != nil {
		// De-duplication needed?
		e := JobError{
			Timestamp: time.Now(),
			Message:   "Validation failed: " + err.Error(),
			Query:     checkStmt.PrettyCQL(),
		}
		testStatus.Errors = append(testStatus.Errors, e)
		testStatus.ReadErrors++
	} else {
		testStatus.ReadOps++
	}
}

func Job(ctx context.Context, wg *sync.WaitGroup, schema *gemini.Schema, table *gemini.Table, s store.Store, p gemini.PartitionRange, c chan Status, mode string, out *os.File, warmup time.Duration) {
	defer wg.Done()
	testStatus := Status{}
	var i int
	warmupTimer := time.NewTimer(warmup)
warmup:
	for {
		select {
		case <-ctx.Done():
			return
		case <-warmupTimer.C:
			break warmup
		default:
			mutationJob(ctx, schema, table, s, p, &testStatus, out, false)
			if i%1000 == 0 {
				c <- testStatus
				testStatus = Status{}
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		switch mode {
		case writeMode:
			mutationJob(ctx, schema, table, s, p, &testStatus, out, true)
		case readMode:
			validationJob(ctx, schema, table, s, p, &testStatus, out)
		default:
			ind := p.Rand.Intn(100000) % 2
			if ind == 0 {
				mutationJob(ctx, schema, table, s, p, &testStatus, out, true)
			} else {
				validationJob(ctx, schema, table, s, p, &testStatus, out)
			}
		}

		if i%1000 == 0 {
			c <- testStatus
			testStatus = Status{}
		}
		if failFast && (testStatus.ReadErrors > 0 || testStatus.WriteErrors > 0) {
			break
		}
		i++
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

	rootCmd.Version = version + ", commit " + commit + ", date " + date
	rootCmd.Flags().StringSliceVarP(&testClusterHost, "test-cluster", "t", []string{}, "Host names or IPs of the test cluster that is system under test")
	rootCmd.MarkFlagRequired("test-cluster")
	rootCmd.Flags().StringSliceVarP(&oracleClusterHost, "oracle-cluster", "o", []string{}, "Host names or IPs of the oracle cluster that provides correct answers")
	rootCmd.MarkFlagRequired("oracle-cluster")
	rootCmd.Flags().StringVarP(&schemaFile, "schema", "", "", "Schema JSON config file")
	rootCmd.Flags().StringVarP(&mode, "mode", "m", mixedMode, "Query operation mode. Mode options: write, read, mixed (default)")
	rootCmd.Flags().IntVarP(&concurrency, "concurrency", "c", 10, "Number of threads per table to run concurrently")
	rootCmd.Flags().IntVarP(&pkNumberPerThread, "max-pk-per-thread", "p", 0, "Maximum number of partition keys per thread")
	rootCmd.Flags().IntVarP(&seed, "seed", "s", 1, "PRNG seed value")
	rootCmd.Flags().BoolVarP(&dropSchema, "drop-schema", "d", false, "Drop schema before starting tests run")
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose output during test run")
	rootCmd.Flags().BoolVarP(&failFast, "fail-fast", "f", false, "Stop on the first failure")
	rootCmd.Flags().BoolVarP(&nonInteractive, "non-interactive", "", false, "Run in non-interactive mode (disable progress indicator)")
	rootCmd.Flags().DurationVarP(&duration, "duration", "", 30*time.Second, "")
	rootCmd.Flags().StringVarP(&outFileArg, "outfile", "", "", "Specify the name of the file where the results should go")
	rootCmd.Flags().StringVarP(&bind, "bind", "b", ":2112", "Specify the interface and port which to bind prometheus metrics on. Default is ':2112'")
	rootCmd.Flags().DurationVarP(&warmup, "warmup", "", 30*time.Second, "Specify the warmup perid as a duration for example 30s or 10h")
	rootCmd.Flags().StringVarP(&compactionStrategy, "compaction-strategy", "", "", "Specify the desired CS as either the coded short hand stcs|twcs|lcs to get the default for each type or provide the entire specification in the form {'class':'....'}")
}

func printSetup() error {
	tw := new(tabwriter.Writer)
	tw.Init(os.Stdout, 0, 8, 2, '\t', tabwriter.AlignRight)
	rand.Seed(int64(seed))
	fmt.Fprintf(tw, "Seed:\t%d\n", seed)
	fmt.Fprintf(tw, "Maximum duration:\t%s\n", duration)
	fmt.Fprintf(tw, "Warmup duration:\t%s\n", warmup)
	fmt.Fprintf(tw, "Concurrency:\t%d\n", concurrency)
	fmt.Fprintf(tw, "Number of partitions per thread:\t%d\n", pkNumberPerThread)
	fmt.Fprintf(tw, "Test cluster:\t%s\n", testClusterHost)
	fmt.Fprintf(tw, "Oracle cluster:\t%s\n", oracleClusterHost)
	if outFileArg == "" {
		fmt.Fprintf(tw, "Output file:\t%s\n", "<stdout>")
	} else {
		fmt.Fprintf(tw, "Output file:\t%s\n", outFileArg)
	}
	tw.Flush()
	return nil
}

func drain(ch chan Status, testRes Status) Status {
	for res := range ch {
		testRes = res.Merge(&testRes)
	}
	return testRes
}
