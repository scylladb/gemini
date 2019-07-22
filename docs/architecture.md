# Gemini architecture

## Introduction

Gemini is designed around the concept of a `job`. There are a number of defined jobs and each
of them perform a limited function for example `MutationJob` applies mutations to the
database clusters.

## The different Jobs

1. ___MutationJob___: This job applies mutations to the clusters. The mutations can be of several types.
   The basic _INSERT_ and _DELETE_ with various conditions or ___DDL___ type statements such as _ALTER_ the 
   structure of the table. These type of mutations happen with different frequency with normal _INSERT_
   being the most common and _ALTER_ the most infrequent.

2. ___ValidationJob___: This job simply reads one or rows from both clusters and compares them.
   In case they differ, an error is raised and the program can either terminate or continue based
   on the users preference.

3. ___WarmupJob___: This job is much like a regular _MutationJob_ but it never issues any _DELETE_
   or _ALTER_ operations. The purpose of this job is to allow for a proper buildup of data in
   the clusters. It has a separate timeout to allow for the user to determine how long it
   should run before the ordinary jobs run.

## Modes of operation

Gemini has three modes of operation to allow for various types of workloads.

1. ___READ___: This mode is intended to be used with a known schema on an existing set of clusters.
   In this mode only validation jobs are used.

2. ___WRITE___: This mode just applies mutations for the entire program execution.

3. ___MIXED___: This is the most common mode and it applies both mutations and validations.

## Concurrency

The application allows the user to decide the level of concurrency that Gemini operates at.
The toggle `--concurrency` currently means that the application will create that number of
___READ___ and ___WRITE___ jobs when running _mixed_ mode. If running in ___WRITE___ or ___READ___
mode it will correspond to the exact number of job executing goroutines. Each goroutine is only
working on a subset of the data (from now known as bucket) when mutating and validating to avoid 
concurrent modification races when validating the system under test.
This can still happen when executing read queries that performs an index scan.

## The Pump

The pump has an almost trivial purpose. It's job is to generate signals for the goroutines that
are executing jobs. When the pump channel is closed the goroutines know that it is time to stop.
Each heartbeat that the pum emits also carries a `time.Duration` indicating that the goroutine that
receives this heartbeat should wait a little while before executing. This feature is not currently
in use but the idea is to introduce some jitter into the execution flow.

## Partition Keys

The application generates partition ids through a `Generator` that creates a steady flow of partition
key components for the desired [concurrency](architecture.md#Concurrency).
Each goroutine is connected to a `source` that continuously emits new partition ids in the form of
a `[]interface{}`. These keys are created in the same way as the the driver does to ensure that each
goroutine only processes partition keys from it's designated bucket.
These partition keys These values are copied into another list that keeps the old partition ids for
later reuse. The idea of reusing the partition keys is that probability of hitting the same partition
key kan be so small that we never actually read any data at all in the validation jobs if just generate
a new random key whenever we attempt a validation. Instead we just reuse previously known inserted
partition keys so we can be sure that at one point we operated on this partition key. We may have
deleted the key but at least the resulting "empty set" makes sense in then.

___NB___:There are probably issues with this approach and we may want to refine this further.

