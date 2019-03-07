# Gemini

Gemini is an is an automatic random testing tool for Scylla.

## Getting Started

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
