# datamorph

> Composable data transformation and validation pipeline for JSON, CSV, YAML, XML, and NDJSON.

[![GitHub](https://img.shields.io/badge/GitHub-bhaskar21--7%2Fdatamorph-blue?logo=github)](https://github.com/bhaskar21-7/datamorph)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python)](python/)
[![Node](https://img.shields.io/badge/Node.js-18%2B-green?logo=node.js)](js/)

Available as both a **Python library** and a **Node.js CLI + library**.
Available as both a **Python library** and a **Node.js CLI + library**.

---

## Features

- **Pipeline API** — chain transforms fluently, with per-step timing and error handling
- **Transforms** — flatten/unflatten, rename keys, filter fields, cast types, drop nulls, deduplicate, snake_case
- **Converters** — bidirectional JSON ↔ CSV ↔ YAML ↔ XML ↔ NDJSON
- **Validators** — schema validation with required fields, types, and range/choice constraints
- **CLI** — convert, transform, validate, and inspect data files from your terminal
- Zero required dependencies (YAML support is optional)

---

## Installation

### Python

```bash
pip install datamorph
# With YAML support:
pip install "datamorph[yaml]"
```

### Node.js (CLI)

```bash
npm install -g datamorph
# or run locally:
npx datamorph --help
```

---

## Python Usage

### Pipeline

```python
from datamorph import Pipeline, flatten, drop_nulls, cast_types, snake_case_keys

data = [
    {"firstName": "Alice", "age": "30", "address": {"city": "Berlin"}},
    {"firstName": "Bob",   "age": "25", "address": {"city": "Tokyo"}},
]

result = (
    Pipeline(data)
    .apply(snake_case_keys)
    .apply(flatten)
    .apply(cast_types, schema={"age": int})
    .apply(drop_nulls)
    .result()
)
# [{"first_name": "Alice", "age": 30, "address.city": "Berlin"}, ...]

# Inspect per-step timing
for step in Pipeline(data).apply(flatten).result() and p.stats():
    print(step)
```

### Transforms

```python
from datamorph import (
    flatten, unflatten,
    rename_keys, filter_keys,
    map_values, cast_types,
    drop_nulls, deduplicate,
    snake_case_keys, add_field, compute_field,
)

data = [{"firstName": "Alice", "score": "9.5", "age": "30"}]

# Rename keys
rename_keys(data, {"firstName": "first_name"})

# Cast types
cast_types(data, {"score": float, "age": int})

# Flatten nested
nested = [{"user": {"name": "Alice", "address": {"city": "Berlin"}}}]
flatten(nested)
# [{"user.name": "Alice", "user.address.city": "Berlin"}]

# Compute a derived field
compute_field(data, "is_adult", lambda r: int(r["age"]) >= 18)
```

### Converters

```python
from datamorph import json_to_csv, csv_to_json, json_to_yaml, json_to_xml

records = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]

# JSON → CSV
csv_str = json_to_csv(records)

# CSV → JSON
records = csv_to_json(csv_str)

# JSON → YAML (requires pyyaml)
yaml_str = json_to_yaml(records)

# JSON → XML
xml_str = json_to_xml(records, root="users", item_tag="user")
```

### Validators

```python
from datamorph import validate_schema

schema = {
    "id":     {"type": int, "required": True, "min": 1},
    "name":   {"type": str, "required": True, "min_len": 1},
    "status": {"type": str, "choices": ["active", "inactive"]},
    "score":  {"type": float, "min": 0.0, "max": 100.0},
}

result = validate_schema(data, schema)
if not result:
    print(result.summary())
```

---

## Node.js / JavaScript Usage

### Library

```js
const { Pipeline, flatten, castTypes, dropNulls, snakeCaseKeys } = require("datamorph");

const data = [
  { firstName: "Alice", age: "30", junk: null },
];

const result = new Pipeline(data)
  .apply(snakeCaseKeys)
  .apply(castTypes, { age: "int" })
  .apply(dropNulls)
  .result();
```

### CLI

```bash
# Convert CSV to JSON
datamorph convert --input data.csv --to json

# Convert JSON to XML, write to file
datamorph convert --input records.json --to xml --output out.xml

# Apply a transform
datamorph transform --input data.json --op snake-case
datamorph transform --input data.json --op flatten --output flat.json

# Validate against a schema
datamorph validate --input data.json --schema schema.json

# Show file info / stats
datamorph info --input data.csv
```

#### Available CLI transforms

| Operation     | Description                                |
|---------------|--------------------------------------------|
| `flatten`     | Flatten nested objects to dot-notation     |
| `unflatten`   | Reconstruct nested objects from dot keys   |
| `drop-nulls`  | Remove null/undefined fields               |
| `deduplicate` | Remove duplicate records                   |
| `snake-case`  | Convert all keys to snake_case             |

---

## Project Structure

```
datamorph/
├── python/
│   ├── datamorph/
│   │   ├── __init__.py
│   │   ├── pipeline.py     # Pipeline class
│   │   ├── transforms.py   # Transform functions
│   │   ├── converters.py   # Format converters
│   │   └── validators.py   # Schema validators
│   └── pyproject.toml
├── js/
│   ├── src/
│   │   ├── index.js
│   │   ├── pipeline.js
│   │   ├── transforms.js
│   │   ├── converters.js
│   │   └── validators.js
│   ├── cli.js
│   └── package.json
└── tests/
    ├── python/
    │   └── test_datamorph.py
    └── js/
        └── test.js
```

---

## Running Tests

### Python

```bash
cd python
pip install -e ".[dev]"
pytest ../tests/python/ -v
```

### Node.js

```bash
cd js
node --test ../tests/js/test.js
```

---

## Contributing

Contributions are welcome. Please:

1. Fork the repo and create a feature branch
2. Add tests for new functionality
3. Ensure all existing tests pass
4. Open a pull request with a clear description

---

## License

MIT
