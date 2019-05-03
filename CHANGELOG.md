# Changelog

## Unreleased

- Gemini version is now available in the resulting output.
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

[0.9.2]: https://github.com/scylladb/gemini/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/scylladb/gemini/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/scylladb/gemini/releases/tag/v0.9.0
