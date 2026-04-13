"""
transforms.py — pure, composable data transformation functions.

All transforms accept a list of dicts (records) or a single dict,
and return the same type they receive.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Union
import re


# ── Helpers ──────────────────────────────────────────────────────────────────

def _is_records(data: Any) -> bool:
    return isinstance(data, list)

def _wrap(data: Any) -> List[dict]:
    return data if _is_records(data) else [data]

def _unwrap(data: List[dict], was_list: bool) -> Any:
    return data if was_list else data[0]


# ── Transforms ───────────────────────────────────────────────────────────────

def flatten(data: Any, separator: str = ".") -> Any:
    """
    Flatten nested dicts into dot-separated keys.

    Example:
        {"a": {"b": {"c": 1}}} -> {"a.b.c": 1}
    """
    def _flatten(obj: dict, prefix: str = "") -> dict:
        result = {}
        for k, v in obj.items():
            key = f"{prefix}{separator}{k}" if prefix else k
            if isinstance(v, dict):
                result.update(_flatten(v, key))
            else:
                result[key] = v
        return result

    was_list = _is_records(data)
    records = _wrap(data)
    out = [_flatten(r) for r in records]
    return _unwrap(out, was_list)


def unflatten(data: Any, separator: str = ".") -> Any:
    """
    Reverse of flatten — reconstruct nested dicts from dot-separated keys.

    Example:
        {"a.b.c": 1} -> {"a": {"b": {"c": 1}}}
    """
    def _unflatten(obj: dict) -> dict:
        result = {}
        for key, value in obj.items():
            parts = key.split(separator)
            d = result
            for part in parts[:-1]:
                d = d.setdefault(part, {})
            d[parts[-1]] = value
        return result

    was_list = _is_records(data)
    records = _wrap(data)
    out = [_unflatten(r) for r in records]
    return _unwrap(out, was_list)


def rename_keys(data: Any, mapping: Dict[str, str]) -> Any:
    """
    Rename keys according to a mapping dict.

    Example:
        rename_keys(data, {"firstName": "first_name"})
    """
    was_list = _is_records(data)
    records = _wrap(data)
    out = [
        {mapping.get(k, k): v for k, v in r.items()}
        for r in records
    ]
    return _unwrap(out, was_list)


def filter_keys(data: Any, keys: List[str], exclude: bool = False) -> Any:
    """
    Keep or exclude specific keys.

    Args:
        keys: List of keys to include (or exclude if exclude=True).
        exclude: If True, remove the listed keys instead of keeping them.
    """
    was_list = _is_records(data)
    records = _wrap(data)
    if exclude:
        out = [{k: v for k, v in r.items() if k not in keys} for r in records]
    else:
        out = [{k: v for k, v in r.items() if k in keys} for r in records]
    return _unwrap(out, was_list)


def map_values(data: Any, mapping: Dict[str, Callable]) -> Any:
    """
    Apply a function to values of specific keys.

    Example:
        map_values(data, {"name": str.upper, "price": lambda x: round(x, 2)})
    """
    was_list = _is_records(data)
    records = _wrap(data)
    out = []
    for r in records:
        new = dict(r)
        for k, fn in mapping.items():
            if k in new:
                new[k] = fn(new[k])
        out.append(new)
    return _unwrap(out, was_list)


def cast_types(data: Any, schema: Dict[str, type]) -> Any:
    """
    Cast values to specified types. Silently skips keys not in schema.

    Example:
        cast_types(data, {"age": int, "score": float, "active": bool})
    """
    def _cast(value: Any, target: type) -> Any:
        if target is bool:
            if isinstance(value, str):
                return value.lower() in ("true", "1", "yes")
            return bool(value)
        return target(value)

    was_list = _is_records(data)
    records = _wrap(data)
    out = []
    for r in records:
        new = dict(r)
        for k, t in schema.items():
            if k in new and new[k] is not None:
                new[k] = _cast(new[k], t)
        out.append(new)
    return _unwrap(out, was_list)


def drop_nulls(data: Any, keys: Optional[List[str]] = None) -> Any:
    """
    Remove keys with None/null values.

    Args:
        keys: If provided, only drop nulls for these specific keys.
              Otherwise, drops all null-valued keys.
    """
    was_list = _is_records(data)
    records = _wrap(data)
    if keys:
        out = [
            {k: v for k, v in r.items() if not (k in keys and v is None)}
            for r in records
        ]
    else:
        out = [{k: v for k, v in r.items() if v is not None} for r in records]
    return _unwrap(out, was_list)


def deduplicate(data: Any, key: Optional[str] = None) -> Any:
    """
    Remove duplicate records.

    Args:
        key: If provided, deduplicate by this key's value.
             Otherwise, deduplicates by full record equality.
    """
    if not _is_records(data):
        return data  # single dict — nothing to deduplicate

    seen: Set = set()
    out = []
    for r in data:
        fingerprint = r.get(key) if key else repr(sorted(r.items()))
        if fingerprint not in seen:
            seen.add(fingerprint)
            out.append(r)
    return out


def snake_case_keys(data: Any) -> Any:
    """
    Convert all keys to snake_case.

    Example:
        {"firstName": "Jane", "LastName": "Doe"} -> {"first_name": "Jane", "last_name": "Doe"}
    """
    def _to_snake(s: str) -> str:
        s = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', s)
        s = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', s)
        return s.replace("-", "_").lower()

    was_list = _is_records(data)
    records = _wrap(data)
    out = [{_to_snake(k): v for k, v in r.items()} for r in records]
    return _unwrap(out, was_list)


def add_field(data: Any, key: str, value: Any) -> Any:
    """
    Add a static field to every record.

    Example:
        add_field(data, "source", "api_v2")
    """
    was_list = _is_records(data)
    records = _wrap(data)
    out = [{**r, key: value} for r in records]
    return _unwrap(out, was_list)


def compute_field(data: Any, key: str, fn: Callable[[dict], Any]) -> Any:
    """
    Add a computed field derived from each record.

    Example:
        compute_field(data, "full_name", lambda r: f"{r['first']} {r['last']}")
    """
    was_list = _is_records(data)
    records = _wrap(data)
    out = [{**r, key: fn(r)} for r in records]
    return _unwrap(out, was_list)
