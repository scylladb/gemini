# Schema Configuration Guide

Gemini can either generate a random schema automatically or use a custom schema defined in a JSON file. This guide covers both approaches.

## Important Limitations

### UPDATE Statements Temporarily Disabled

UPDATE statements are currently disabled in Gemini. When the statement ratio includes updates, they are internally converted to INSERT statements. Full UPDATE support will return in v2.1.0 once Gemini v2 is fully stable.

To work around this, focus on INSERT and DELETE ratios:
```bash
--statement-ratios='{"mutation":{"insert":0.95,"update":0.0,"delete":0.05}}'
```

## Automatic Schema Generation

By default, Gemini generates a random schema based on CLI parameters:

```bash
./gemini \
  --max-tables=2 \
  --max-partition-keys=4 \
  --min-partition-keys=1 \
  --max-clustering-keys=3 \
  --min-clustering-keys=0 \
  --max-columns=10 \
  --min-columns=3 \
  --test-cluster=... \
  --oracle-cluster=...
```

### Schema Generation Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--max-tables` | 1 | Maximum number of tables to generate |
| `--max-partition-keys` | 8 | Maximum partition key columns |
| `--min-partition-keys` | 1 | Minimum partition key columns |
| `--max-clustering-keys` | 5 | Maximum clustering key columns |
| `--min-clustering-keys` | 0 | Minimum clustering key columns |
| `--max-columns` | 12 | Maximum regular columns |
| `--min-columns` | 5 | Minimum regular columns |
| `--dataset-size` | large | Size preset: `small` or `large` |
| `--cql-features` | normal | Feature set: `basic`, `normal`, or `all` |

### CQL Feature Levels

- **basic** - Simple types only (int, text, boolean, etc.)
- **normal** - Adds collections (list, set, map) and tuples
- **all** - Adds UDTs (User-Defined Types) and complex nested types

### Reproducible Schemas

Use `--schema-seed` to generate the same schema across runs:

```bash
# First run - note the schema seed from output
./gemini --test-cluster=... --oracle-cluster=...

# Later - reproduce exact schema
./gemini --schema-seed=12345 --test-cluster=... --oracle-cluster=...
```

## Custom Schema File

For precise control, provide a JSON schema file with `--schema`:

```bash
./gemini --schema=my_schema.json --test-cluster=... --oracle-cluster=...
```

### Schema JSON Structure

```json
{
  "keyspace": {
    "name": "my_keyspace",
    "replication": {
      "class": "SimpleStrategy",
      "replication_factor": 1
    },
    "oracle_replication": {
      "class": "SimpleStrategy",
      "replication_factor": 1
    }
  },
  "tables": [
    {
      "name": "users",
      "partition_keys": [
        {"name": "user_id", "type": "uuid"}
      ],
      "clustering_keys": [
        {"name": "created_at", "type": "timestamp"}
      ],
      "columns": [
        {"name": "name", "type": "text"},
        {"name": "age", "type": "int"}
      ]
    }
  ]
}
```

### Column Definition Examples

Simple types:
```json
{"name": "user_id", "type": "int"}
```

Set collection:
```json
{
  "name": "tags",
  "type": {
    "complex_type": "set",
    "value_type": "text",
    "frozen": false
  }
}
```

Map type:
```json
{
  "name": "metadata",
  "type": {
    "complex_type": "map",
    "key_type": "text",
    "value_type": "int",
    "frozen": false
  }
}
```

List type:
```json
{
  "name": "scores",
  "type": {
    "complex_type": "list",
    "value_type": "double",
    "frozen": true
  }
}
```

Tuple type:
```json
{
  "name": "coordinates",
  "type": {
    "complex_type": "tuple",
    "value_types": ["double", "double"],
    "frozen": false
  }
}
```

UDT (User-Defined Type):
```json
{
  "name": "address",
  "type": {
    "complex_type": "udt",
    "type_name": "address_type",
    "frozen": true,
    "value_types": {
      "street": "text",
      "city": "text",
      "zip": "int"
    }
  }
}
```

## Supported Data Types

Gemini supports CQL data types that are compatible with the Go driver (gocql). The following tables describe what's supported and what isn't.

### Simple Types (Fully Supported)

| Type | Description | Partition Key | Clustering Key | Map Key |
|------|-------------|:-------------:|:--------------:|:-------:|
| `ascii` | ASCII string | ✓ | ✓ | ✓ |
| `bigint` | 64-bit signed integer | ✓ | ✓ | ✓ |
| `boolean` | True/false | ✓ | ✓ | ✓ |
| `date` | Date without time | ✓ | ✓ | ✓ |
| `double` | 64-bit floating point | ✓ | ✓ | ✓ |
| `float` | 32-bit floating point | ✓ | ✓ | ✓ |
| `inet` | IP address | ✓ | ✓ | ✓ |
| `int` | 32-bit signed integer | ✓ | ✓ | ✓ |
| `smallint` | 16-bit signed integer | ✓ | ✓ | ✓ |
| `text` | UTF-8 string | ✓ | ✓ | ✓ |
| `time` | Time without date | ✓ | ✓ | ✓ |
| `timestamp` | Date and time | ✓ | ✓ | ✓ |
| `timeuuid` | Type 1 UUID (time-based) | ✓ | ✓ | ✓ |
| `tinyint` | 8-bit signed integer | ✓ | ✓ | ✓ |
| `uuid` | UUID | ✓ | ✓ | ✓ |
| `varchar` | UTF-8 string (alias for text) | ✓ | ✓ | ✓ |

### Types with Restrictions

| Type | Description | Partition Key | Clustering Key | Map Key | Reason |
|------|-------------|:-------------:|:--------------:|:-------:|--------|
| `blob` | Binary data | ✓ | ✓ | ✗ | Go maps cannot use byte slices as keys (slices are not comparable) |
| `decimal` | Variable-precision decimal | ✓ | ✓ | ✗ | Uses `*inf.Dec` pointer type which is not comparable in Go maps |
| `duration` | Time duration | ✗ | ✗ | ✗ | CQL restriction: duration cannot be used in primary keys or as map keys |
| `varint` | Arbitrary-precision integer | ✓ | ✓ | ✗ | Uses `*big.Int` pointer type which is not comparable in Go maps |

### Complex Types (Fully Supported)

| Type | Description | As Column | As Partition Key |
|------|-------------|:---------:|:----------------:|
| `list<T>` | Ordered collection | ✓ | Only if frozen |
| `set<T>` | Unique unordered collection | ✓ | Only if frozen |
| `map<K,V>` | Key-value pairs | ✓ | Only if frozen |
| `tuple<T1,T2,...>` | Fixed-length typed sequence | ✓ | Only if frozen |
| `udt` | User-defined type | ✓ | Only if frozen |
| `counter` | Distributed counter | ✓ | ✗ |

### Unsupported ScyllaDB Types

The following ScyllaDB 2025.1 types are NOT supported by Gemini:

| Type | Reason |
|------|--------|
| `vector<T, N>` | Vector type for ML/AI workloads - not implemented in gocql driver |
| `frozen<T>` (standalone) | Frozen is a modifier, not a standalone type |

## Why Certain Types Can't Be Map Keys

Gemini uses Go maps internally to track partition keys and compare results between oracle and test clusters. Go maps require keys to be "comparable" types. The following types cannot be used as map keys:

1. **`blob`** - Represented as `[]byte` (byte slice) in Go. Slices are not comparable because they are reference types with pointer semantics.

2. **`decimal`** - Represented as `*inf.Dec` (pointer to Decimal). Pointers compare by address, not value, making them unsuitable for map keys.

3. **`varint`** - Represented as `*big.Int` (pointer to arbitrary-precision integer). Same pointer comparison issue as decimal.

4. **`duration`** - CQL itself prohibits duration in primary keys and map keys due to its complex internal representation (months, days, nanoseconds).

## Primary Key Restrictions

### Partition Keys

Gemini can use these types for partition keys:
- All simple types except `duration`
- Frozen complex types (frozen list, frozen set, frozen map, frozen tuple, frozen UDT)

### Clustering Keys

Gemini can use these types for clustering keys:
- All simple types except `duration`
- `blob` is allowed (unlike partition keys in some configurations)

### Why These Restrictions Exist

1. **Go Language Limitations**: Go's map type requires comparable keys. Types backed by pointers (`*big.Int`, `*inf.Dec`) or slices (`[]byte`) cannot be directly compared.

2. **CQL Restrictions**: Some restrictions come from CQL itself - `duration` cannot be part of a primary key because it lacks a natural total ordering.

3. **Driver Limitations**: The gocql driver maps CQL types to Go types, inheriting Go's type system constraints.

## Replication Strategies

### SimpleStrategy

For single datacenter:
```json
{
  "class": "SimpleStrategy",
  "replication_factor": 3
}
```

### NetworkTopologyStrategy

For multiple datacenters:
```json
{
  "class": "NetworkTopologyStrategy",
  "datacenter1": 3,
  "datacenter2": 2
}
```

### CLI Shorthand

```bash
# SimpleStrategy with RF=1
--replication-strategy=simple

# NetworkTopologyStrategy
--replication-strategy=network

# Custom (JSON inline)
--replication-strategy="{'class':'NetworkTopologyStrategy','dc1':3}"
```

## Complete Example Schema

```json
{
  "keyspace": {
    "name": "ecommerce",
    "replication": {
      "class": "NetworkTopologyStrategy",
      "datacenter1": 3
    },
    "oracle_replication": {
      "class": "SimpleStrategy",
      "replication_factor": 1
    }
  },
  "tables": [
    {
      "name": "orders",
      "partition_keys": [
        {"name": "customer_id", "type": "uuid"}
      ],
      "clustering_keys": [
        {"name": "order_date", "type": "timestamp"},
        {"name": "order_id", "type": "timeuuid"}
      ],
      "columns": [
        {"name": "total", "type": "decimal"},
        {"name": "status", "type": "text"},
        {
          "name": "items",
          "type": {
            "complex_type": "list",
            "value_type": "text",
            "frozen": false
          }
        }
      ]
    }
  ]
}
```
