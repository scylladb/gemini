```bash
Gemini is an automatic random testing tool for Scylla.

Usage:
  gemini [flags]

Flags:
      --async-objects-stabilization-attempts int       Maximum number of attempts to validate result sets from MV and SI (default 10)
      --async-objects-stabilization-backoff duration   Duration between attempts to validate result sets from MV and SI for example 10ms or 1s (default 10ms)
  -b, --bind string                                    Specify the interface and port which to bind prometheus metrics on. Default is ':2112' (default ":2112")
  -c, --concurrency uint                               Number of threads per table to run concurrently (default 10)
      --consistency string                             Specify the desired consistency as ANY|ONE|TWO|THREE|QUORUM|LOCAL_QUORUM|EACH_QUORUM|LOCAL_ONE (default "QUORUM")
      --cql-features string                            Specify the type of cql features to use, basic|normal|all (default "basic")
      --dataset-size string                            Specify the type of dataset size to use, small|large (default "large")
  -d, --drop-schema                                    Drop schema before starting tests run
      --duration duration                               (default 30s)
  -f, --fail-fast                                      Stop on the first failure
  -h, --help                                           help for gemini
      --level string                                   Specify the logging level, debug|info|warn|error|dpanic|panic|fatal (default "info")
      --max-clustering-keys int                        Maximum number of generated clustering keys (default 4)
      --max-columns int                                Maximum number of generated columns (default 16)
      --max-mutation-retries int                       Maximum number of attempts to apply a mutation (default 2)
      --max-mutation-retries-backoff duration          Duration between attempts to apply a mutation for example 10ms or 1s (default 10ms)
      --max-partition-keys int                         Maximum number of generated partition keys (default 6)
      --max-tables int                                 Maximum number of generated tables (default 1)
      --min-clustering-keys int                        Minimum number of generated clustering keys (default 2)
      --min-columns int                                Minimum number of generated columns (default 8)
      --min-partition-keys int                         Minimum number of generated partition keys (default 2)
  -m, --mode string                                    Query operation mode. Mode options: write, read, mixed (default) (default "mixed")
      --non-interactive                                Run in non-interactive mode (disable progress indicator)
      --normal-dist-mean float                         Mean of the normal distribution (default 9.223372036854776e+18)
      --normal-dist-sigma float                        Sigma of the normal distribution, defaults to one standard deviation ~0.341 (default 6.290339729134958e+18)
  -o, --oracle-cluster strings                         Host names or IPs of the oracle cluster that provides correct answers. If omitted no oracle will be used
      --oracle-replication-strategy string             Specify the desired replication strategy of the oracle cluster as either the coded short hand simple|network to get the default for each type or provide the entire specification in the form {'class':'....'} (default "simple")
      --outfile string                                 Specify the name of the file where the results should go
      --partition-key-buffer-reuse-size uint           Number of reused buffered partition keys (default 100)
      --partition-key-distribution string              Specify the distribution from which to draw partition keys, supported values are currently uniform|normal|zipf (default "uniform")
      --replication-strategy string                    Specify the desired replication strategy as either the coded short hand simple|network to get the default for each type or provide the entire specification in the form {'class':'....'} (default "simple")
      --schema string                                  Schema JSON config file
  -s, --seed uint                                      PRNG seed value (default 1)
      --table-options stringArray                      Repeatable argument to set table options to be added to the created tables
  -t, --test-cluster strings                           Host names or IPs of the test cluster that is system under test
      --token-range-slices uint                        Number of slices to divide the token space into (default 10000)
      --tracing-outfile string                         Specify the file to which tracing information gets written. Two magic names are available, 'stdout' and 'stderr'. By default tracing is disabled.
      --use-counters                                   Ensure that at least one table is a counter table
  -v, --verbose                                        Verbose output during test run
      --version                                        version for gemini
      --warmup duration                                Specify the warmup perid as a duration for example 30s or 10h (default 30s)
```