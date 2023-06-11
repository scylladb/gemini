# Gemini Developers Guide

## Running Gemini

The easiest way to run Gemini for development is to use:


```sh
./scripts/test.sh <test parameters>
```

The script starts two single node scylla clusters as the system under test and as an oracle using [docker-compose](https://docs.docker.com/compose/) and starts a Gemini run against these clusters

You can make it to run Cassandra as an oracle:
```sh
./scripts/test.sh cassandra <test parameters>
```

You can also launch the clusters yourself with:

```sh
./scripts/prepare-environment.sh
```

And run Gemini against the test oracle and system under test clusters as follows:

```sh
./scripts/run-gemini-test.sh <test parameters>
```

### Running unit tests

Geminis own test suite so far only consists of unit tests. Run these in the standard Go way: 

```
go test -v -race -cover
```
