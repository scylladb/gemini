# Gemini

Gemini is an is an automatic random testing tool for Scylla.

## Getting Started

Start two Scylla clusters, one as the system under test and another as the test oracle:

```
$ docker run --name scylla-test -d scylladb/scylla --smp 1 --memory 1G
$ docker run --name scylla-oracle -d scylladb/scylla --smp 1 --memory 1G
```

Run Gemini against the clusters:

```
$ gemini --test-cluster=$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' scylla-test) --oracle-cluster=$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' scylla-oracle)
```
