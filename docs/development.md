# Gemini Developers Guide

## Running Gemini

The easiest way to run Gemini for development is to use:


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

