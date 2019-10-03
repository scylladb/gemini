# Gemini CLI arguments

Gemini has a large number of CLI arguments and this document is an attempt to document them.

The complete [help output](cmdhelp.md) from `gemini -h` is a good reference.

## Mandatory arguments

1. ___--oracle-cluster___, ___-o___: This parameter takes a comma separated list of hosts that are
part of the so called `Oracle` which is to say the super stable bug free cluster that Gemini uses as
the blueprint for it's validations.

## Optional arguments

1. ___--test-cluster___, ___-t___: This parameter takes a comma separated list of hosts that are 
part of the ___SUT___ or commonly, ___system under test___. If omitted then Gemini will not use the Oracle
at all and simply execute a lot of queries against the ___SUT___.

2. ___---mode___, ___-m___: This is a string parameter with the acceptable values "mixed","read" and "write".

3. ___--concurrency___, ___-c___: An int describing the number of concurrent jobs in the case of "read"
or "write" mode. In the case of "mixed" mode it represents half of the number of jobs that will be executed
concurrently.

4. ___--schema___: The path to a file containing a JSON representation of the schema to be
used during a run.

5. ___--seed___, ___-s___: The seed parameter denotes the seed from where to start the random number
generators that Gemini is using.

6. ___--drop-schema___, ___-d___: Boolean value that instructs Gemini to issue a __DROP SCHEMA__ 
statement before starting to run. Make sure you use it with care.

7. ___--fail-fast___, ___-f___: Boolean value that instructs Gemini to stop running as soon as it
encounters a validation error. If set to false, then Gemini will collect the errors and report them 
once normal program end is reached.

8. ___--duration___: The duration of a run. Defaults to 30 seconds.

9. ___--warmup___: The duration of the warmup phase during which only additive mutations will be
performed. Default is 30 seconds.

10. ___--outfile___: Path to a file where Gemini should store it's result. If not provided then
standard out is used.