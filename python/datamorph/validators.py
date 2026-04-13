"""
validators.py — schema and data validation utilities.

All validators return a ValidationResult with errors list.
They do NOT mutate data.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Type, Union


@dataclass
class ValidationResult:
    valid: bool
    errors: List[str] = field(default_factory=list)

    def __bool__(self):
        return self.valid

    def __repr__(self):
        status = "✓ valid" if self.valid else f"✗ invalid ({len(self.errors)} error(s))"
        return f"ValidationResult({status})"

    def summary(self) -> str:
        if self.valid:
            return "All checks passed."
        return "\n".join(f"  - {e}" for e in self.errors)


def _records(data: Any) -> List[dict]:
    return data if isinstance(data, list) else [data]


def validate_required(data: Any, keys: List[str]) -> ValidationResult:
    """
    Check that all required keys are present and non-null in every record.

    Args:
        data: List of dicts or single dict.
        keys: Required key names.

    Returns:
        ValidationResult
    """
    errors = []
    for i, record in enumerate(_records(data)):
        for k in keys:
            if k not in record:
                errors.append(f"Record {i}: missing required key '{k}'")
            elif record[k] is None:
                errors.append(f"Record {i}: key '{k}' is null")
    return ValidationResult(valid=not errors, errors=errors)


def validate_types(
    data: Any,
    schema: Dict[str, Union[type, Tuple[type, ...]]],
    allow_none: bool = True,
) -> ValidationResult:
    """
    Validate that field values match expected types.

    Args:
        data: List of dicts or single dict.
        schema: Dict mapping key -> expected type or tuple of types.
        allow_none: If True, null values pass type checks.

    Returns:
        ValidationResult
    """
    errors = []
    for i, record in enumerate(_records(data)):
        for k, expected in schema.items():
            if k not in record:
                continue
            v = record[k]
            if v is None and allow_none:
                continue
            if not isinstance(v, expected):
                actual = type(v).__name__
                exp_name = (
                    " | ".join(t.__name__ for t in expected)
                    if isinstance(expected, tuple)
                    else expected.__name__
                )
                errors.append(
                    f"Record {i}: key '{k}' expected {exp_name}, got {actual} ({v!r})"
                )
    return ValidationResult(valid=not errors, errors=errors)


def validate_range(
    data: Any,
    rules: Dict[str, Dict[str, Any]],
) -> ValidationResult:
    """
    Validate numeric/string values are within allowed ranges.

    Args:
        data: List of dicts or single dict.
        rules: Dict mapping key -> constraint dict.
               Supported constraints:
                 min, max         — numeric bounds (inclusive)
                 min_len, max_len — string length bounds
                 choices          — allowed values list

    Example:
        validate_range(data, {
            "age":    {"min": 0, "max": 120},
            "name":   {"min_len": 1, "max_len": 100},
            "status": {"choices": ["active", "inactive"]},
        })
    """
    errors = []
    for i, record in enumerate(_records(data)):
        for k, constraints in rules.items():
            if k not in record or record[k] is None:
                continue
            v = record[k]

            if "min" in constraints and v < constraints["min"]:
                errors.append(f"Record {i}: '{k}' = {v} is below min {constraints['min']}")
            if "max" in constraints and v > constraints["max"]:
                errors.append(f"Record {i}: '{k}' = {v} exceeds max {constraints['max']}")
            if "min_len" in constraints and len(str(v)) < constraints["min_len"]:
                errors.append(f"Record {i}: '{k}' length {len(str(v))} below min_len {constraints['min_len']}")
            if "max_len" in constraints and len(str(v)) > constraints["max_len"]:
                errors.append(f"Record {i}: '{k}' length {len(str(v))} exceeds max_len {constraints['max_len']}")
            if "choices" in constraints and v not in constraints["choices"]:
                errors.append(f"Record {i}: '{k}' = {v!r} not in allowed choices {constraints['choices']}")

    return ValidationResult(valid=not errors, errors=errors)


def validate_schema(
    data: Any,
    schema: Dict[str, Any],
) -> ValidationResult:
    """
    Full schema validation combining required, type, and range checks.

    Schema format:
        {
            "field_name": {
                "type": str | int | float | bool | list | dict,
                "required": True | False,
                "min": ..., "max": ...,
                "min_len": ..., "max_len": ...,
                "choices": [...],
            }
        }

    Example:
        schema = {
            "id":     {"type": int, "required": True, "min": 1},
            "name":   {"type": str, "required": True, "min_len": 1},
            "status": {"type": str, "choices": ["active", "inactive"]},
        }
    """
    errors = []

    required_keys = [k for k, v in schema.items() if v.get("required")]
    type_schema = {k: v["type"] for k, v in schema.items() if "type" in v}
    range_rules = {
        k: {rk: rv for rk, rv in v.items() if rk in ("min", "max", "min_len", "max_len", "choices")}
        for k, v in schema.items()
    }

    if required_keys:
        r = validate_required(data, required_keys)
        errors.extend(r.errors)

    if type_schema:
        r = validate_types(data, type_schema)
        errors.extend(r.errors)

    if any(range_rules.values()):
        r = validate_range(data, range_rules)
        errors.extend(r.errors)

    return ValidationResult(valid=not errors, errors=errors)
