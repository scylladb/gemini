# Statement Ratio CLI Flag

The statement ratio feature provides fine-grained control over the distribution of different statement types that Gemini executes during a test run. This allows for precise workload modeling and targeted performance testing scenarios.

## Overview

This feature controls the ratio of statements that Gemini will execute during a run using percentage-based configuration. All values must be between 0 and 1, and when adding up within each category, they must sum to 1.0 with a tolerance of 0.001 (due to floating-point precision).

## How It Works

The statement ratio system uses cumulative distribution functions (CDFs) to efficiently select statement types based on the configured ratios:

1. **Configuration**: Ratios are specified via JSON (inline or file)
2. **Validation**: Gemini validates that all ratio groups sum to 1.0
3. **CDF Building**: Internal CDFs are built for efficient random selection
4. **Runtime Selection**: During execution, random numbers are used to select statement types according to the configured distribution

## CLI Flag

### `--statement-ratios`

This flag accepts statement ratio configuration in JSON format. You can provide the JSON in two ways:

1. **Inline JSON**: Provide the JSON configuration directly as a command line argument
2. **File Path**: Provide a path to a JSON file containing the configuration

**Examples:**

```bash
# Inline JSON (use single quotes to avoid shell interpretation)
gemini --statement-ratios='{"mutation":{"insert":0.7,"update":0.2,"delete":0.1}}' \
       --oracle-cluster=192.168.1.10

# JSON file path
gemini --statement-ratios=/path/to/ratios.json \
       --oracle-cluster=192.168.1.10

# Complex inline configuration (escaped for shell)
gemini --statement-ratios='{
  "mutation": {
    "insert": 0.5,
    "update": 0.3,
    "delete": 0.2,
    "insert_subtypes": {
      "regular_insert": 0.9,
      "json_insert": 0.1
    }
  }
}' --oracle-cluster=192.168.1.10
```

## JSON Configuration Structure

The configuration uses a hierarchical JSON structure with two main categories:

```json
{
  "mutation": {
    "insert": 0.5,
    "update": 0.3,
    "delete": 0.2,

    "insert_subtypes": {
      "regular_insert": 0.9,
      "json_insert": 0.1
    },

    "delete_subtypes": {
      "whole_partition": 0.4,
      "single_row": 0.3,
      "single_column": 0.2,
      "multiple_partitions": 0.1
    }
  },
  "validation": {
    "select_subtypes": {
      "single_partition": 0.6,
      "multiple_partition": 0.3,
      "clustering_range": 0.05,
      "multiple_partition_clustering_range": 0.04,
      "single_index": 0.01
    }
  }
}
```

**Note**: The values in the above example are not the default values, but demonstrate the JSON structure.

### Configuration Breakdown

#### Mutation Ratios (`mutation`)

Controls the distribution of data modification operations:

- **`insert`**: Ratio of INSERT statements (0.0-1.0)
- **`update`**: Ratio of UPDATE statements (0.0-1.0)
- **`delete`**: Ratio of DELETE statements (0.0-1.0)

**Note**: These three values must sum to 1.0

#### Insert Subtypes (`insert_subtypes`)

Fine-grained control over INSERT statement types:

- **`regular_insert`**: Standard INSERT statements
- **`json_insert`**: INSERT statements with JSON data

**Note**: These values must sum to 1.0

#### Delete Subtypes (`delete_subtypes`)

Fine-grained control over DELETE statement types:

- **`whole_partition`**: DELETE entire partitions
- **`single_row`**: DELETE single rows
- **`single_column`**: DELETE specific columns
- **`multiple_partitions`**: DELETE across multiple partitions

**Note**: These values must sum to 1.0

#### Validation Ratios (`validation`)

Controls the distribution of data validation operations:

Since validation currently only supports SELECT statements, only `select_subtypes` is available:

- **`single_partition`**: SELECT from a single partition
- **`multiple_partition`**: SELECT from multiple partitions
- **`clustering_range`**: SELECT with clustering range conditions
- **`multiple_partition_clustering_range`**: SELECT with clustering ranges across partitions
- **`single_index`**: SELECT using indexes (falls back to `single_partition` if no indexes exist)

**Note**: These values must sum to 1.0

## Default Configuration

If no ratios are specified, Gemini uses this balanced default configuration:

```json
{
  "mutation": {
    "insert": 0.75,
    "update": 0.20,
    "delete": 0.05,
    "insert_subtypes": {
      "regular_insert": 0.9,
      "json_insert": 0.1
    },
    "delete_subtypes": {
      "whole_partition": 0.4,
      "single_row": 0.3,
      "single_column": 0.2,
      "multiple_partitions": 0.1
    }
  },
  "validation": {
    "select_subtypes": {
      "single_partition": 0.6,
      "multiple_partition": 0.3,
      "clustering_range": 0.05,
      "multiple_partition_clustering_range": 0.04,
      "single_index": 0.01
    }
  }
}
```

## Partial Configuration

Gemini supports partial configurations with intelligent defaults. When you provide incomplete subtype configurations, Gemini will automatically calculate the missing values to ensure all ratios sum to 1.0.

### Automatic Subtype Completion

When specifying subtypes partially, Gemini uses these rules:

1. **Insert Subtypes**: If only one subtype is specified, the other is automatically calculated as `1.0 - specified_value`
2. **Delete Subtypes**: If some subtypes are specified, the remaining values are distributed equally among unspecified subtypes
3. **Select Subtypes**: If some subtypes are specified, the remaining values are distributed equally among unspecified subtypes

### Examples of Partial Configuration

**Only main mutation ratios:**

```json
{
  "mutation": {
    "insert": 0.8,
    "update": 0.15,
    "delete": 0.05
  }
}
```

**Result:** All subtypes use default values

**Partial insert subtypes:**

```json
{
  "mutation": {
    "insert": 0.7,
    "update": 0.2,
    "delete": 0.1,
    "insert_subtypes": {
      "regular_insert": 0.8
    }
  }
}
```

**Result:** `json_insert` automatically set to 0.2 (1.0 - 0.8)

**Partial delete subtypes:**

```json
{
  "mutation": {
    "delete_subtypes": {
      "whole_partition": 0.5,
      "single_row": 0.3
    }
  }
}
```

**Result:** `single_column` and `multiple_partitions` each set to 0.1 ((1.0 - 0.8) / 2)

**Empty subtype objects:**

```json
{
  "mutation": {
    "insert_subtypes": {}
  }
}
```

**Result:** Default values are preserved

## File Path Support

The `--statement-ratios` flag supports JSON file paths for complex configurations:

```bash
# Create a ratios configuration file
cat > custom-ratios.json << EOF
{
  "mutation": {
    "insert": 0.6,
    "update": 0.3,
    "delete": 0.1,
    "insert_subtypes": {
      "regular_insert": 0.8,
      "json_insert": 0.2
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
      "single_partition": 0.7,
      "multiple_partition": 0.2,
      "clustering_range": 0.05,
      "multiple_partition_clustering_range": 0.04,
      "single_index": 0.01
    }
  }
}
EOF

# Use the file
gemini --statement-ratios=custom-ratios.json --oracle-cluster=192.168.1.10
```

## Validation and Error Handling

Gemini performs strict validation on ratio configurations:

1. **Range Check**: All values must be between 0.0 and 1.0
2. **Sum Validation**: Each ratio group must sum to 1.0 (±0.001 tolerance)
3. **Structure Validation**: JSON structure must match expected format
4. **File Validation**: If a file path is provided, the file must exist and be readable

If validation fails, Gemini will display a clear error message and exit.

## Use Cases

### High-Insert Workload Testing

```json
{
  "mutation": {
    "insert": 0.9,
    "update": 0.08,
    "delete": 0.02
  }
}
```

### Balanced CRUD Operations

```json
{
  "mutation": {
    "insert": 0.4,
    "update": 0.4,
    "delete": 0.2
  }
}
```

### JSON-Heavy Workload

```json
{
  "mutation": {
    "insert": 0.8,
    "update": 0.15,
    "delete": 0.05,
    "insert_subtypes": {
      "regular_insert": 0.3,
      "json_insert": 0.7
    }
  }
}
```

## Missing Features

- **Update Subtypes**: Currently, Gemini only supports regular UPDATE statements. Planned features include updates for multiple rows and multiple partitions.

## Performance Impact

There is no performance impact from using statement ratios. The feature only controls which types of statements are executed - the overhead of random number generation for statement selection already exists in Gemini's core operation.
