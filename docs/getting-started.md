# Getting started with Gemini

## TLDR: Running with default arguments

1. Download a release from http://downloads.scylladb.com/gemini/

2. Make sure you have two scylla clusters setup from here on referred to as ORACLE_CLUSTER and TEST_CLUSTER

3. Unzip the tarball and run `./gemini --oracle-cluster=<ORACLE_CLUSTER> --test-cluster=<TEST_CLUSTER>`

Enjoy!

## Further CLI arguments

Execute `./gemini --help` to see the entire list of arguments available.
Their explanation in some detail is available the [CLI arguments](cli-arguments.md).

## Further reading

If you want to add functionality to gemini reading the [architecture](architecture.md)
document is a good introduction into the Gemini design.