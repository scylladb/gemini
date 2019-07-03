# Changelog

## Unreleased

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

[1.1.0]: https://github.com/scylladb/gemini/compare/v1.1.0...v1.0.0
[1.0.0]: https://github.com/scylladb/gemini/compare/v0.9.2...v1.0.0
[0.9.2]: https://github.com/scylladb/gemini/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/scylladb/gemini/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/scylladb/gemini/releases/tag/v0.9.0
