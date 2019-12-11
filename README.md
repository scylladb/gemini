# Gemini

Gemini is an is an automatic random testing suite for Scylla and Apache Cassandra.

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

## Development

### Running Gemini

The easiest way to run Gemini is to run:


```sh
./scripts/gemini-launcher
```

The script starts a Scylla cluster as the system under test and an Apache Cassandra clusters a test oracle using [docker-compose](https://docs.docker.com/compose/) and starts a Gemini run using the two clusters.

You can also launch the clusters yourself with:

```sh
docker-compose -f scripts/docker-compose.yml up -d
```

And run Gemini against the test oracle and system under test clusters as follows:

```sh
gemini \\
--test-cluster=$(docker inspect --format='{{ .NetworkSettings.Networks.gemini.IPAddress }}' gemini-test) \\
--oracle-cluster=$(docker inspect --format='{{ .NetworkSettings.Networks.gemini.IPAddress }}' gemini-oracle)
```

### Running unit tests

Geminis own test suite so far only consists of unit tests. Run these in the standard Go way: 

```
go test -v -race -cover
```

The suite has one build tag that controls what is being run: `slow`. This tag should be used for standard unit tests that
for some reason take a long time to run perhaps because they do fuzzing or quick checks. 

Run these tests like this:

```
go test -v -race -tags slow
```

## Contributing

Gemini is already being used for testing Scylla releases, but there's still plenty to do.
To contribute to Gemini, please fork the repository and send a pull request.

## Documentation

* [Gemini command line arguments](https://github.com/scylladb/gemini/blob/readme/docs/cli-arguments.md)
* [Gemini architecture](https://github.com/scylladb/gemini/blob/readme/docs/architecture.md)

## License

Gemini is distributed under the Apache 2.0 license. See [LICENSE](LICENSE) for more information.
