# Statement Ration CLI Flag

This new flag controls the ration of statements that Gemini will execute during a run.
This is percentage based and need to add up to 1. All values must be between 0 and 1, and when adding up,
tolerance is 0.001 (due to how floating point numbers work).
This CLI flag takes shape of a JSON object with the following structure:

```json
{
  "mutations": {
    "insert": 0.5,
    "update": 0.3,
    "delete": 0.1,
    
    "insert_subtypes": {
      "regular_insert": 0.9,
      "json_insert": 0.1
    },
    
    "delete_subtypes": {
      "whole_partition": 0.5,
      "single_row": 0.3,
      "single_column": 0.1,
      "multiple_partitions": 0.1
    }
  },
  "validation": {
    "select_subtypes": {
      "single_partition": 0.6,
      "multiple_partition": 0.1,
      "clustering_range": 0.1,
      "multiple_partition_clustering_range": 0.1,
      "single_index": 0.1
    }
  }
}
```
This values in the above example are not the default values, but rather an example of how the JSON object should look like.
Let's break down the structure:
(The subtypes allow for more granular control over the types of mutations that Gemini will perform)
- `mutations`: This object contains the ratios for different mutation types.
  - `insert`, `update`, `delete`: These are the main mutation types and their ratios. They describe what percentage of the total mutations will be of each type.
  - `insert_subtypes`: This object contains subtypes of insert mutations and their ratios. 
  - `delete_subtypes`: This object contains subtypes of delete mutations and their ratios.

Since the validation only have SELECT statement, there is nothing beside "select_subtypes" in the validation object.
  - `single_partition`, 
  - `multiple_partition`, 
  - `clustering_range`, 
  - `multiple_partition_clustering_range`, 
  - `single_index` (if the table contains any indexes, if not, falls back to `single_partition`): These are the main validation types and their ratios. They describe what percentage of the total validations will be of each type.

Some parts can be left out, and Gemini will use the default values for those parts, but **be aware that the sum of all values must be 1**.
If the sum of all values is not 1, Gemini will throw an error and exit. It will be a best practice to just provide all parts, even if you want to use the default values, to avoid confusion.
This flag allows for really granular control over the types of statements that Gemini will execute, allowing for more tailored testing scenarios and performance tuning, JSON object can be really large, so in the future
we will add a way to load it from a file, but for now, you can just pass it as a string in the CLI.

## Missing features

- Update subtypes: Currently, Gemini does not support update subtypes, but it is planned for the future. The only thing we have is REGULAR UPDATE statement, there is no update for multiple rows or even multiple partitions.

## Performance impact

There is no performance impact of using this flag, it is just a way to control the types of statements that Gemini will execute during a run, most costly part of this is generating random numbers which exists even today in gemini to select which statement will be executed.

