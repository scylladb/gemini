# Gemini

Gemini is an is an automatic random testing tool for Scylla.

## Getting Started

To run Gemini, you need two clusters: an oracle cluster and a test cluster. The tool assumes that the oracle cluster is behaving correctly and compares the behavior of the test cluster to it.

To start Cassandra as the oracle cluster, type:

```sh
docker run --name gemini-oracle -d cassandra
```

Alternatively, to start Scylla as the oracle cluster, type:

```sh
docker run --name gemini-oracle -d scylladb/scylla --smp 1 --memory 1G
```

For test cluster, you can, for example, run upstream Scylla:

```sh
docker run --name gemini-test -d scylladb/scylla --smp 1 --memory 1G
```

You can now run Gemini against the oracle and test clusters as follows:

```sh
gemini --test-cluster=$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' gemini-test) --oracle-cluster=$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' gemini-oracle)
```
