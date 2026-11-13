```bash
Gemini is an automatic random testing tool for Scylla.

Usage:
  gemini [flags]
  gemini [command]

Available Commands:
  benchmark   Run benchmarks and track results over time
  completion  Generate the autocompletion script for the specified shell
  help        Help about any command

Flags:
      --async-objects-stabilization-attempts int       Maximum number of attempts to validate result sets from MV and SI (default 10)
      --async-objects-stabilization-backoff duration   Maximum backoff delay between validation retry attempts (exponential backoff starting at 100ms) (default 2s)
  -b, --bind string                                    Specify the interface and port which to bind prometheus metrics on. Default is ':2112' (default "0.0.0.0:2112")
  -c, --concurrency int                                Maximum number of workers of each kind per table. Gemini runs fewer while latency shows the clusters queue requests. 0 sizes it from the clusters: 16 per shard of the cluster with fewer shards, or 4 per CPU when the clusters report no shards
      --connect-timeout duration                       Duration of waiting connection established (default 30s)
      --consistency string                             Specify the desired consistency as ANY|ONE|TWO|THREE|QUORUM|LOCAL_QUORUM|EACH_QUORUM|LOCAL_ONE (default "QUORUM")
      --cql-features string                            Specify the type of cql features to use, basic|normal|all (default "normal")
      --dataset-size string                            Specify the type of dataset size to use, small|large (default "large")
      --deleted-partitions-time-bucket durationSlice   Comma-separated delays after which a deleted partition is checked for data resurrection, for example 30s,3m,30h (default [1m0s,10m0s])
  -d, --drop-schema                                    Drop schema before starting tests run (default true)
      --duration duration                               (default 30s)
  -f, --fail-fast                                      Stop on the first failure
      --flight-recorder string                         If set, keeps a recent execution trace in memory and writes it to this path when gemini exits
  -h, --help                                           help for gemini
      --level string                                   Specify the logging level, debug|info|warn|error|dpanic|panic|fatal (default "info")
      --materialized-views                             Run gemini with materialized views support
      --max-blob-length int                            Maximum blob length (default 10000)
      --max-clustering-keys int                        Maximum number of generated clustering keys (default 5)
      --max-columns int                                Maximum number of generated columns (default 12)
      --max-deleted-heap-size int                      Maximum number of entries in the deleted-partitions heap. Gemini lowers a value above 3000000 to 3000000 (default 3000000)
      --max-errors-to-store int                        Maximum number of errors to store and show at the end. Gemini stops when read and write errors reach this number (default 10)
      --max-mutation-retries int                       Maximum number of attempts to apply a mutation (default 10)
      --max-mutation-retries-backoff duration          Duration between attempts to apply a mutation for example 10ms or 1s (default 10s)
      --max-partition-keys int                         Maximum number of generated partition keys (default 8)
      --max-pk-blob-length int                         Maximum blob length (default 128)
      --max-pk-string-length int                       Maximum string length (default 64)
      --max-string-length int                          Maximum string length (default 1024)
      --max-tables int                                 Maximum number of generated tables (default 1)
      --min-blob-length int                            Minimum blob length (default 1)
      --min-clustering-keys int                        Minimum number of generated clustering keys
      --min-columns int                                Minimum number of generated columns (default 5)
      --min-partition-keys int                         Minimum number of generated partition keys (default 1)
      --min-pk-blob-length int                         Minimum blob length (default 32)
      --min-pk-string-length int                       Minimum string length (default 16)
      --min-string-length int                          Minimum string length (default 1)
      --minimum-delay duration                         Minimum delay between mutations (default 25µs)
  -m, --mode string                                    Query operation mode. Mode options: write, read, mixed (default) (default "mixed")
      --mutation-concurrency int                       Maximum number of mutation workers per table. 0 uses --concurrency
      --normal-dist-mean float                         Mean of the normal distribution (default 9.223372036854776e+18)
      --normal-dist-sigma float                        Sigma of the normal distribution, defaults to one standard deviation ~0.341 (default 6.290339729134958e+18)
  -o, --oracle-cluster strings                         Host names or IPs of the oracle cluster that provides correct answers. If omitted no oracle will be used
      --oracle-host-selection-policy string            Host selection policy used by the driver for the oracle cluster: round-robin|host-pool|token-aware (default "token-aware")
      --oracle-password string                         Password for the oracle cluster
      --oracle-replication-strategy string             Specify the desired replication strategy of the oracle cluster as either the coded short hand simple|network to get the default for each type or provide the entire specification in the form {'class':'....'} (default "network")
      --oracle-statement-log-file string               File to write statements flow to
      --oracle-username string                         Username for the oracle cluster
      --outfile string                                 Specify the name of the file where the results should go
      --partition-count uint                           Number of Scylla Partitions (default 2000000)
      --partition-key-distribution string              Specify the distribution from which to draw partition keys, supported values are currently uniform|normal|zipf (default "uniform")
      --profiling-port int                             If non-zero starts the pprof server on the given port at 'http://0.0.0.0:<port>/debug/pprof/' and records mutex and block profiles
      --random-string-buffer-size int                  Size of the buffer used for random strings (default 104857600)
      --read-concurrency int                           Maximum number of validation workers per table. 0 uses --concurrency
      --replication-strategy string                    Specify the desired replication strategy as either the coded short hand simple|network to get the default for each type or provide the entire specification in the form {'class':'....'} (default "network")
      --request-timeout duration                       Duration of waiting request execution (default 30s)
      --row-tracker-capacity int                       Maximum capacity for the row tracker used by targeted delete operations. N > 0 (default 100000): auto-sized from deletion ratios, clamped to N. -1: fully auto-sized (no cap). 0: disable row tracking. (default 100000)
      --schema string                                  Schema JSON config file
      --schema-seed string                             Schema seed value (default "random")
  -s, --seed string                                    Statement seed value (default "random")
      --statement-log-file-compression string          Compression algorithm to use for statement log files (default "none")
      --statement-ratios string                        Statement ratios configuration in JSON format (e.g., '{"mutation_ratios":{"insert_ratio":0.7,"update_ratio":0.2,"delete_ratio":0.1}}')
      --summary-file string                            Path to the file where the human-readable corruption summary is written (default "./gemini-summary.txt")
      --table-options stringArray                      Repeatable argument to set table options to be added to the created tables
  -t, --test-cluster strings                           Host names or IPs of the test cluster that is system under test
      --test-host-selection-policy string              Host selection policy used by the driver for the test cluster: round-robin|host-pool|token-aware (default "token-aware")
      --test-password string                           Password for the test cluster
      --test-statement-log-file string                 File to write statements flow to
      --test-username string                           Username for the test cluster
      --tracing-outdir string                          Specify the file to which tracing information gets written. Two magic names are available, 'stdout' and 'stderr'. By default tracing is disabled.
      --use-counters                                   Ensure that at least one table is a counter table
      --use-lwt                                        Emit LWT based updates
      --use-server-timestamps                          Use server-side generated timestamps for writes
  -v, --verbose                                        Verbose output during test run
      --version                                        Print version information
      --version-json                                   Print version information in JSON format
      --warmup duration                                Specify the warmup period as a duration for example 30s or 10h

Use "gemini [command] --help" for more information about a command.
```
