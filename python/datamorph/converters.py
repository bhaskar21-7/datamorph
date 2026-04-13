"""
converters.py — bidirectional format conversion utilities.

Supported formats: JSON, CSV, YAML, XML, NDJSON
"""

import csv
import io
import json
from typing import Any, Dict, List, Optional, Union


# ── JSON ↔ CSV ────────────────────────────────────────────────────────────────

def json_to_csv(
    data: Union[List[dict], dict],
    fieldnames: Optional[List[str]] = None,
    delimiter: str = ",",
) -> str:
    """
    Convert a list of dicts (JSON records) to CSV string.

    Args:
        data: List of dicts or a single dict.
        fieldnames: Column order. If None, uses keys from first record.
        delimiter: Field separator (default: ',').

    Returns:
        CSV-formatted string.
    """
    if isinstance(data, dict):
        data = [data]
    if not data:
        return ""
    fieldnames = fieldnames or list(data[0].keys())
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=fieldnames,
        delimiter=delimiter,
        extrasaction="ignore",
        lineterminator="\n",
    )
    writer.writeheader()
    writer.writerows(data)
    return output.getvalue()


def csv_to_json(
    text: str,
    delimiter: str = ",",
    infer_types: bool = True,
) -> List[dict]:
    """
    Parse CSV string into a list of dicts.

    Args:
        text: Raw CSV string.
        delimiter: Field separator.
        infer_types: If True, auto-cast integers, floats, and booleans.

    Returns:
        List of dicts.
    """
    reader = csv.DictReader(io.StringIO(text.strip()), delimiter=delimiter)
    records = []
    for row in reader:
        if infer_types:
            row = {k: _infer(v) for k, v in row.items()}
        records.append(dict(row))
    return records


def _infer(value: str) -> Any:
    """Attempt to cast a string to int, float, bool, or None."""
    if value == "":
        return None
    if value.lower() in ("true", "false"):
        return value.lower() == "true"
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


# ── JSON ↔ YAML ───────────────────────────────────────────────────────────────

def json_to_yaml(data: Any, indent: int = 2) -> str:
    """
    Convert JSON-serializable data to YAML string.
    Requires PyYAML (pip install pyyaml).
    """
    try:
        import yaml
    except ImportError:
        raise ImportError("PyYAML is required: pip install pyyaml")
    return yaml.dump(data, default_flow_style=False, indent=indent, allow_unicode=True)


def yaml_to_json(text: str, pretty: bool = False) -> str:
    """
    Parse YAML string and return JSON string.
    Requires PyYAML (pip install pyyaml).
    """
    try:
        import yaml
    except ImportError:
        raise ImportError("PyYAML is required: pip install pyyaml")
    parsed = yaml.safe_load(text)
    return json.dumps(parsed, indent=2 if pretty else None, ensure_ascii=False)


# ── JSON ↔ XML ────────────────────────────────────────────────────────────────

def json_to_xml(
    data: Any,
    root: str = "root",
    item_tag: str = "item",
    indent: int = 2,
) -> str:
    """
    Convert a list of dicts or dict to XML string.

    Args:
        data: List of dicts or a single dict.
        root: Name of the root XML element.
        item_tag: Name for list item elements.
        indent: Indentation spaces.

    Returns:
        XML-formatted string.
    """
    pad = " " * indent

    def _to_xml(obj: Any, tag: str, level: int = 0) -> str:
        prefix = pad * level
        if isinstance(obj, dict):
            children = "\n".join(_to_xml(v, k, level + 1) for k, v in obj.items())
            return f"{prefix}<{tag}>\n{children}\n{prefix}</{tag}>"
        elif isinstance(obj, list):
            children = "\n".join(_to_xml(item, item_tag, level + 1) for item in obj)
            return f"{prefix}<{tag}>\n{children}\n{prefix}</{tag}>"
        else:
            return f"{prefix}<{tag}>{_escape(str(obj))}</{tag}>"

    def _escape(s: str) -> str:
        return (
            s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;")
        )

    return '<?xml version="1.0" encoding="UTF-8"?>\n' + _to_xml(data, root)


def xml_to_json(text: str) -> Any:
    """
    Parse XML string into a Python dict.
    Uses xml.etree.ElementTree (stdlib).
    """
    import xml.etree.ElementTree as ET

    def _parse(element: ET.Element) -> Any:
        children = list(element)
        if not children:
            return element.text.strip() if element.text and element.text.strip() else None
        result: Dict[str, Any] = {}
        for child in children:
            value = _parse(child)
            if child.tag in result:
                existing = result[child.tag]
                if not isinstance(existing, list):
                    result[child.tag] = [existing]
                result[child.tag].append(value)
            else:
                result[child.tag] = value
        return result

    root = ET.fromstring(text.strip())
    return {root.tag: _parse(root)}


# ── CSV ↔ NDJSON ──────────────────────────────────────────────────────────────

def csv_to_ndjson(text: str, delimiter: str = ",") -> str:
    """
    Convert CSV string to NDJSON (newline-delimited JSON) string.
    One JSON object per line.
    """
    records = csv_to_json(text, delimiter=delimiter)
    return "\n".join(json.dumps(r, ensure_ascii=False) for r in records)


def ndjson_to_csv(text: str, delimiter: str = ",") -> str:
    """
    Convert NDJSON (newline-delimited JSON) to CSV string.
    """
    records = [json.loads(line) for line in text.strip().splitlines() if line.strip()]
    return json_to_csv(records, delimiter=delimiter)
