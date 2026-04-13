"""
datamorph — composable data transformation & validation pipeline
Supports: JSON, CSV, YAML, XML, NDJSON
"""

from .pipeline import Pipeline
from .transforms import (
    flatten,
    unflatten,
    rename_keys,
    filter_keys,
    map_values,
    cast_types,
    drop_nulls,
    deduplicate,
)
from .converters import (
    json_to_csv,
    csv_to_json,
    json_to_yaml,
    yaml_to_json,
    json_to_xml,
    xml_to_json,
    csv_to_ndjson,
    ndjson_to_csv,
)
from .validators import (
    validate_schema,
    validate_types,
    validate_required,
    validate_range,
)

__version__ = "0.1.0"
__all__ = [
    "Pipeline",
    "flatten", "unflatten", "rename_keys", "filter_keys",
    "map_values", "cast_types", "drop_nulls", "deduplicate",
    "json_to_csv", "csv_to_json", "json_to_yaml", "yaml_to_json",
    "json_to_xml", "xml_to_json", "csv_to_ndjson", "ndjson_to_csv",
    "validate_schema", "validate_types", "validate_required", "validate_range",
]
