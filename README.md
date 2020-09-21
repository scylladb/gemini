![Gemini](docs/images/gemini.png)

<p align="center"><i>An automatic randomized testing suite for Scylla and Apache Cassandra.</i></p>

## How does Gemini work?

Gemini operates on two clusters, a system under test (SUT) and a test oracle, by generating random mutations (`INSERT`, `UPDATE`) and verifying them (`SELECT`) using the CQL protocol and query language. As the mutations are performed on both systems, their client-visible state is assumed to be the same, unless either of the systems (usually system under test) have bugs. If a verification step fails, Gemini reports the CQL query and its results for further analysis.

## Getting started with Gemini

### TLDR: Running with default arguments

1. Download a release from http://downloads.scylladb.com/gemini/

2. Make sure you have two scylla clusters setup from here on referred to as ORACLE_CLUSTER and TEST_CLUSTER

3. Unzip the tarball and run `./gemini --oracle-cluster=<ORACLE_CLUSTER> --test-cluster=<TEST_CLUSTER>`

Enjoy!

### Further CLI arguments

Execute `./gemini --help` to see the entire list of arguments available.
Their explanation in some detail is available the [CLI arguments](cli-arguments.md).

## Features

* Random schema generation
* Data generation using uniform, normal, and zipf distributions
* Materialized views
* Secondary indexes
* Counters

## Contributing

Gemini is already being used for testing Scylla releases, but there's still plenty to do.
To contribute to Gemini, please fork the repository and send a pull request.

## Documentation

* [Gemini Architecture](docs/architecture.md)
* [Gemini Command Line Arguments](docs/cli-arguments.md)
* [Gemini Developers Guide](docs/development.md)

## License

Gemini is distributed under the Apache 2.0 license. See [LICENSE](LICENSE) for more information.
