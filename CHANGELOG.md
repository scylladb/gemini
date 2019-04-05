# Changelog

## Unreleased

### Added

- Added version info printing using '--version' program argument.
- CQL `INSERT JSON` statement support.

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

[0.9.0]: https://github.com/scylladb/gemini/releases/tag/v0.9.0
