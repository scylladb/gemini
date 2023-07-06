# Changelog
## [1.8.6] - 2023-07-06
- Test stops after warmup ([9aa8f65](https://github.com/scylladb/gemini/commit/9aa8f65209d00b3a9cbb4ed40ca975dcf2236653))
- Make goreleaser work after deprication of replacements ([99ddf27](https://github.com/scylladb/gemini/commit/99ddf274a81088a573bece7c11b358e405f588bb))

## [1.8.5] - 2023-07-05

- Set oracle timeout properly ([3e8f096](https://github.com/scylladb/gemini/commit/3e8f096a9c7b9bf170761e417188c312e002b35b))
- Update gocql to 1.8.0 ([6bbb4fb](https://github.com/scylladb/gemini/commit/6bbb4fbc6e68762271a233780592e8a26dc60c5b))
- Gemini can get stuck at the end of test ([#372](https://github.com/scylladb/gemini/pull/372))

## [1.8.4] - 2023-06-20

- Populate version info ([#361](https://github.com/scylladb/gemini/pull/361))
- Fix false-positives when test cycle is getting terminated ([#363](https://github.com/scylladb/gemini/pull/363))
- Fix schema loading to support complex types ([#360](https://github.com/scylladb/gemini/pull/360))

## [1.8.3] - 2023-06-15

- Pause generator when partitions are full ([#332](https://github.com/scylladb/gemini/pull/332))
- Fix TYPE_TIME value generations ([#337](https://github.com/scylladb/gemini/pull/337))
- Make validation retry on SelectFromMaterializedViewStatementType ([#347](https://github.com/scylladb/gemini/pull/347))
- Make gemini avoid running DROP COLUMN when it is not suppported ([#350](https://github.com/scylladb/gemini/pull/350))
- Fix UDT value generations ([357](https://github.com/scylladb/gemini/pull/357))

## [1.8.2] - 2023-06-01

- Redo jobs execution: warm up and work cycles execution should be clear and predictable ([#310](https://github.com/scylladb/gemini/pull/310))
- Use request-timeout parameter for Oracle ([#323](https://github.com/scylladb/gemini/pull/323))
- Speedup random string generation ([#325](https://github.com/scylladb/gemini/pull/325))
- Implement statement cache ([#328](https://github.com/scylladb/gemini/pull/328))

## [1.8.1] - 2023-05-15

- Fix value corruption ([#318](https://github.com/scylladb/gemini/pull/318))
- Make gemini work properly when min=max ([#317](https://github.com/scylladb/gemini/pull/317))
- Fix pk values spoiling ([#314](https://github.com/scylladb/gemini/pull/314))
- Fix lt function have wrong compare for big.Int ([#311](https://github.com/scylladb/gemini/pull/311))
- Reduce memory allocations ([#308](https://github.com/scylladb/gemini/pull/308))
- Reduce inflight shrinking limit ([#306](https://github.com/scylladb/gemini/pull/306))
- Fix gemini table data corruption on DDL statement execution error ([#305](https://github.com/scylladb/gemini/pull/305))
- Package reorganization ([#303](https://github.com/scylladb/gemini/pull/303))
- Use atomic global status instead of channel pipeline ([#301](https://github.com/scylladb/gemini/pull/301))
- Delivery values and partition range config as pointers ([#299](https://github.com/scylladb/gemini/pull/299))
- Added soft stop ([#297](https://github.com/scylladb/gemini/pull/297))

## [1.8.0] - 2023-05-03

- Update packages and golang ([#296](https://github.com/scylladb/gemini/pull/296))
- Fix(inflight): reduce memory footprint by using struct{} instead of bool ([#291](https://github.com/scylladb/gemini/pull/291))
- Fix warmup hunging ([#292](https://github.com/scylladb/gemini/pull/292))

## [1.7.9] - 2023-04-25

- Fix endless warmup ([#278](https://github.com/scylladb/gemini/issues/278))
- Fix EOM due to the errors ([#282](https://github.com/scylladb/gemini/issues/282))
- Fix test jobs scheduling ([#284](https://github.com/scylladb/gemini/pull/284))
- Fix per table job scheduling ([0140acb1241e7347be7a23215c9a869dfa7dec35]https://github.com/scylladb/gemini/commit/0140acb1241e7347be7a23215c9a869dfa7dec35)

## [1.7.8] - 2023-02-02

- Fix issue: memory leak due golang maps are not shrinking when you do delete on them ([#267](https://github.com/scylladb/gemini/issues/267))

## [1.7.7] - 2022-08-24

- Fix issue: max-partition-keys should align with Scylla's --max-partition-key-restrictions-per-query and --max-clustering-key-restrictions-per-query configuration options([#271](https://github.com/scylladb/gemini/issues/271))
- Fix issue: wrong number of values passed to prepared select query ([272](https://github.com/scylladb/gemini/issues/272))
- Fix issue: Gemini issues a query involved with data filtering without using ALLOW FILTERING and thus may have unpredictable performance ([273](https://github.com/scylladb/gemini/issues/273))
- Support request and connection timeout. Could be set via cli parameters

## [1.7.6] - 2022-08-03

### Fixed

- fixed error handling in load function


## [1.7.6] - 2022-08-03

### Fixed

- fixed error handling in load function


## [1.7.5] - 2021-04-28

### Fixed

- Fix materialized view creation if the view already exists.

## [1.7.4] - 2021-01-04

### Added

- Print errors to standard out to debug cases where Gemini detects an error but
  no error log is generated.

## [1.7.3] - 2020-09-21

### Added

- Add options to choose host selection policy.
- Add option to use server-side timestamps for queries.
- Add support for password authentication.

### Fixed

- Don't enable tracing if it's not requested.

## [1.7.2]

- schema: add "IF NOT EXISTS" check when creating a new type

## [1.6.9]

- Avoid DDL operations for a table with MV ([#198](https://github.com/scylladb/gemini/issues/198))

## [1.6.1]

- Add support for generating multiple tables in schema with the `--max-tables`
  command line option.

## [1.6.0] - 2019-09-06

- Bumped driver version to v1.3.0-rc.1
- Lazy partition key generation reintroduced to avoid out of memory issues. 
  This brings in a new CLI arg `--token-range-slices` that defines how many slices the
  partition keyspace should be divided into when applying the different distribution functions.
  The default value of this is set to an ad-hoc value of 10000 which should supply ample possibilities
  for varying selection of values according to the chosen probability distribution.

## [1.5.0] - 2019-08-26

- Fix overlapping operations on the same partition key ([#198](https://github.com/scylladb/gemini/issues/198)).
- Partition keys can now be drawn from various distributions such as ___"zipf"___,
  ___"uniform"___ and ___"normal"___. The CLI argument `--partition-key-distribution` is used
  to select which distribution to use. The default is `normal`.
- The CLI argument `partition-key-buffer-size` is removed since it carries no meaning any more.
- Added the possibility to run without any validations against the Oracle. Simply do not
  supply a host for the Oracle and Gemini will assume you want to only run against Test.
- Replication strategy is now configurable via the CLI argument `--replication-strategy`.

## 1.4.4

- Mutations on Test are only applied if they first succeeded on the Oracle.

## 1.4.3

- Bugfix that makes sure that when a job terminates early, the result status is
  properly sent to the collector.
- Gemini ensures that material views can be created in the default case by simply
  creating enough keys and columns.

## 1.4.2

- Reused primary keys does no longer block the caller if none are available.
- Primary key generation no longer blocks if the targeted source is full.
- Upgraded driver to 1.2.0

## v1.4.1

- Bug in shutdown handling that caused deadlock is fixed.
- Index queries reapplied with low frequency for certain types.
- Fix for invalid materialized view ddl statement.

## 1.4.0

- A `source` concept is used to coordinate the creation, consumption and reuse of
  partition keys.
- Two new CLI args are introduced to control the buffer sizes of the new and reusable
  partition keys `partition-key-buffer-size` and `partition-key-buffer-reuse-size`.
- The CLI arg `concurrency` now means the total number of actors per job type. 
  You may need to scale down your settings for this argument since for example a
  mixed mode execution will run with twice as many goroutines. Experimentation is
  encouraged since a high number will also yield much greater throughput.

## 1.3.4

- Shutdown is no longer waiting for the warmup phase to complete.
- Partition keys can now be any supported type.
- The size of the partition key buffers can be configured on the commandline through
  `partition-key-buffer-size` and `partition-key-buffer-reuse-size`.

## 1.3.0

- Partitioning bewteen workers are now handled by a `generator` that generates 
  random partition keys and dispatches them to the relevant worker. This construct
  also provides a channel for `"old"` values to be used downstream by validators.
  The reason for this is that if the validators read newly created partition keys
  then the chances of finding data in the database is near to zero unless we load
  it up with a truly huge data set.
- Log levels introduced and configured via CLI arg `level`. Common values such as 
  `info`, `debug` and `error` are supported.
- Mutations are now retried at a considerably greater number of times than reads.
  The number of retries and the time between them are configure via `--max-mutation-retries`
  and `--max-mutation-retries-backoff`.

## [1.2.0] - 2019-06-20

- DDL statements are now emitted with low frequency if the `--cql-features` is set to at
  least `"all"` level.
- Data sizes are configurable though a CLI argument `--dataset-size` and the currently
  supported values are "small" and "large".
- CLI toggle `--cql-features` added to let the user select which type of CQL features
  to use. The current levels are `basic`, `normal` and `all`. The `basic` level have only
  regular columns and no indexes nor materialized views. `normal` adds these two constructs
  and `all` currently the same as `normal` but will be used to differentiate more advanced
  features in the future.

## [1.1.0] - 2019-06-11

- Exponential backoff retry policy added with 5 retries between 1 and 10 seconds.
- Support for changing consistency level via a CLI argument `consistency`.
- Support for compaction strategies added via a CLI argument `compaction-strategy`
  as a set of string values "stcs", "twcs" or "lcs" which will make Gemini choose
  the default values for the properties of the respective compaction strategies.
  Alternatively the JSON-like definition of the compaction-strategy can be supplied
  in a form like: `{"class"="SizeTieredCompactionStrategy", "enabled"=true, ....}`.
  Note that the form needs to be given as actual valid JSON.
- Prometheus metrics added that exposes internal runtime properties
  as well as counts fo CQL operations 'batch', 'delete', 'insert', 'select'
  and 'update'.
- Warmup duration added during which only inserts are performed.
- Generating valid single index queries when a complex primary key is used.
- Gracefully stopping on sigint and sigterm.
- JSON marshalling of Schema fixed. The schema input file has changed to
  ensure marshalling of lists and sets. These types now have a _kind_
  property with possible values (_list_,_set_).
- Correctly pretty printing map and UDT types.
- Skipping HTML escaping of resulting JSON.
- Ensure proper termination when errors happen.
- Fix mutation timestamps to match on system under test and test oracle.
- Gemini now tries to perform mutation on both systems regardless of
  if one of them fail.
- Gemini now timestamps errors for easier correlation.
- A new Store abstraction is introduced in preparation to enable
  implementations such as an in-memory store.
- Gemini now uses github.com/scylladb/gocqlx/qb builder.
- Gemini ensures that primary key buckets do not overflow int32.
- Gemini now accepts a list of node host names or IPs for the test
  and Oracle clusters.
- Default maximum primary keys increased to MAX_INT32/concurrency.
- Range tombstones are being generated. 

## [1.0.0] - 2019-05-06

- Gemini version is now available in the resulting output.
- Materialized Views support.
- Improved error handling in validation code.
- Avoiding small double booking of write ops in case of mutation errors.
- Printing executable CQL statements when logging errors or in verbose mode.
- JSON schema definition file has simpler index definition.

## [0.9.2] - 2019-04-18

- Support for User Defined Types (UDT) added for simple columns.
- Support for collections such as sets, lists and maps added for simple columns.
- Support for writing the result to a file in JSON format.

## [0.9.1] - 2019-04-11

### Added

- Tuple support added for simple columns.
- Added version info printing using '--version' program argument.
- CQL `INSERT JSON` statement support.

### Fixed

- Panic when `--non-interactive` command line option is passed ([#69](https://github.com/scylladb/gemini/issues/69))

## [0.9.0] - 2019-04-03

### Added

- Support for queries using secondary indexes added.
- Switched to using the upstream Go driver https://github.com/scylladb/gocql instead
  of the regular driver. The goal is performance gains by using that shard awareness
  feature as well as providing proper more real testing of the driver.
- Support for additional types for the clustering keys. TimeUUID is used runtime
  to accomplish sorted UUIDs. Blob, text and varchar types are based on
  [ksuid](https://github.com/segmentio/ksuid) to be sortable.
- Launcher script (`scripts/gemini-launcher`) that starts an Apache Cassandra
  node as the test oracle and a Scylla node as the system under test using
  Docker.
- Schema generation support. Gemini generates a random schema unless user
  specifies one with the `--schema` command line option.

### Changed

- Improve progress indicator ([#14](https://github.com/scylladb/gemini/issues/14)).

[1.7.3]: https://github.com/scylladb/gemini/compare/v1.7.2...v1.7.3
[1.7.2]: https://github.com/scylladb/gemini/compare/v1.7.1...v1.7.2
[1.7.1]: https://github.com/scylladb/gemini/compare/v1.7.0...v1.7.1
[1.6.9]: https://github.com/scylladb/gemini/compare/v1.6.8...v1.6.9
[1.6.1]: https://github.com/scylladb/gemini/compare/v1.6.0...v1.6.1
[1.6.0]: https://github.com/scylladb/gemini/compare/v1.5.0...v1.6.0
[1.5.0]: https://github.com/scylladb/gemini/compare/v1.4.4...v1.5.0
[1.2.0]: https://github.com/scylladb/gemini/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/scylladb/gemini/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/scylladb/gemini/compare/v0.9.2...v1.0.0
[0.9.2]: https://github.com/scylladb/gemini/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/scylladb/gemini/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/scylladb/gemini/releases/tag/v0.9.0
