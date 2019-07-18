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

