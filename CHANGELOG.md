# Changelog

## Unreleased

### Added

- Support for additional types for the clustering keys. TimeUUID is used runtime
  to accomplish sorted UUIDs. Blob, text and varchar types are based on
  [ksuid](https://github.com/segmentio/ksuid) to be sortable.
- Launcher script (`scripts/gemini-launcher`) that starts an Apache Cassandra
  node as the test oracle and a Scylla node as the system under test using
  Docker.
- Schema generation support. Gemini generates a random schema unless user
  specifies one with the `--schema` command line option.
