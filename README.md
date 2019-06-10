# Gemini

Gemini is an is an automatic random testing tool for Scylla.

## Getting Started

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

To run Gemini, you need two clusters: an oracle cluster and a test cluster. The tool assumes that the oracle cluster is behaving correctly and compares the behavior of the test cluster to it.

For development, install [docker-compose](https://docs.docker.com/compose/) for your platform.

To start both clusters, type:

```sh
docker-compose -f scripts/docker-compose.yml up -d
```

You can now run Gemini against the oracle and test clusters as follows:

```sh
gemini \\
--test-cluster=$(docker inspect --format='{{ .NetworkSettings.Networks.gemini.IPAddress }}' gemini-test) \\
--oracle-cluster=$(docker inspect --format='{{ .NetworkSettings.Networks.gemini.IPAddress }}' gemini-oracle)
```

Alternatively, to start the clusters and run gemini in one go, type:

```sh
./scripts/gemini-launcher
```

## Important CLI arguments

### ___--concurrency___

The _concurrency_ argument controls how many goroutines that are concurrently
executing operations against each of the tables. For example, if we have a _concurrency_ of
10 and 4 tables there will be 40 goroutines each operating independently.
When calculating the concurrency to use it is wise to remember the above. A good target concurrency
could for example be the total shard count of the target cluster but to set this parameter correctly
it needs to be divided by number of tables.

### ___--max-pk-per-thread___

This is the range of partitioned keys that each goroutine handles. If an invalid number is given
the value will set to `MAX(int32)/concurrency`. Invalid values are 0 or negative or greater than `MAX(int32)/concurrency`.
If you are unsure of what to set for this value you should just not use it, the default is `MAX(int32)/concurrency`.
The effect of this parameter is that the space of possible keys increase which will allow for a much larger working set.

### ___--seed___

This parameter controls the initialization of the _Random Number Generators (RNG)_ that gemini uses for various things.
This includes generating random data as well as randomly selecting which types of queries should be executed.
Being able to set it allows for a near replay of the queries and the data being generated.