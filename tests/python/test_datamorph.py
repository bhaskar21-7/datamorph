"""
Tests for datamorph Python library.
Run: pytest tests/python/
"""

import pytest
from datamorph.transforms import (
    flatten, unflatten, rename_keys, filter_keys,
    map_values, cast_types, drop_nulls, deduplicate,
    snake_case_keys, add_field, compute_field,
)
from datamorph.converters import (
    json_to_csv, csv_to_json, json_to_xml, xml_to_json,
    csv_to_ndjson, ndjson_to_csv,
)
from datamorph.validators import (
    validate_required, validate_types, validate_range, validate_schema,
)
from datamorph.pipeline import Pipeline, PipelineError


# ── Transforms ───────────────────────────────────────────────────────────────

class TestFlatten:
    def test_basic(self):
        data = {"a": {"b": 1, "c": {"d": 2}}}
        assert flatten(data) == {"a.b": 1, "a.c.d": 2}

    def test_list_of_records(self):
        data = [{"x": {"y": 1}}, {"x": {"y": 2}}]
        assert flatten(data) == [{"x.y": 1}, {"x.y": 2}]

    def test_custom_separator(self):
        data = {"a": {"b": 1}}
        assert flatten(data, separator="/") == {"a/b": 1}

    def test_already_flat(self):
        data = {"a": 1, "b": 2}
        assert flatten(data) == {"a": 1, "b": 2}


class TestUnflatten:
    def test_basic(self):
        data = {"a.b": 1, "a.c": 2}
        assert unflatten(data) == {"a": {"b": 1, "c": 2}}

    def test_roundtrip(self):
        original = {"a": {"b": {"c": 42}}}
        assert unflatten(flatten(original)) == original


class TestRenameKeys:
    def test_basic(self):
        data = {"firstName": "Jane", "lastName": "Doe"}
        result = rename_keys(data, {"firstName": "first_name", "lastName": "last_name"})
        assert result == {"first_name": "Jane", "last_name": "Doe"}

    def test_partial_rename(self):
        data = {"a": 1, "b": 2}
        assert rename_keys(data, {"a": "x"}) == {"x": 1, "b": 2}


class TestFilterKeys:
    def test_include(self):
        data = {"a": 1, "b": 2, "c": 3}
        assert filter_keys(data, ["a", "c"]) == {"a": 1, "c": 3}

    def test_exclude(self):
        data = {"a": 1, "b": 2, "c": 3}
        assert filter_keys(data, ["b"], exclude=True) == {"a": 1, "c": 3}


class TestCastTypes:
    def test_int_float_bool(self):
        data = {"age": "25", "score": "9.5", "active": "true"}
        result = cast_types(data, {"age": int, "score": float, "active": bool})
        assert result == {"age": 25, "score": 9.5, "active": True}

    def test_bool_variants(self):
        for val in ("true", "1", "yes"):
            assert cast_types({"v": val}, {"v": bool})["v"] is True
        for val in ("false", "0", "no"):
            assert cast_types({"v": val}, {"v": bool})["v"] is False


class TestDropNulls:
    def test_all_nulls(self):
        data = {"a": 1, "b": None, "c": None}
        assert drop_nulls(data) == {"a": 1}

    def test_specific_keys(self):
        data = {"a": None, "b": None}
        assert drop_nulls(data, keys=["a"]) == {"b": None}


class TestDeduplicate:
    def test_full_record(self):
        data = [{"a": 1}, {"a": 2}, {"a": 1}]
        assert deduplicate(data) == [{"a": 1}, {"a": 2}]

    def test_by_key(self):
        data = [{"id": 1, "v": "x"}, {"id": 1, "v": "y"}, {"id": 2, "v": "z"}]
        result = deduplicate(data, key="id")
        assert len(result) == 2


class TestSnakeCaseKeys:
    def test_camel(self):
        data = {"firstName": "Jane", "lastName": "Doe"}
        assert snake_case_keys(data) == {"first_name": "Jane", "last_name": "Doe"}

    def test_pascal(self):
        data = {"UserId": 1}
        assert snake_case_keys(data) == {"user_id": 1}


# ── Converters ───────────────────────────────────────────────────────────────

class TestJsonCsv:
    def test_roundtrip(self):
        data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        csv_str = json_to_csv(data)
        result = csv_to_json(csv_str)
        assert result[0]["name"] == "Alice"
        assert result[0]["age"] == 30

    def test_single_dict(self):
        data = {"name": "Alice", "age": 30}
        csv_str = json_to_csv(data)
        assert "Alice" in csv_str


class TestJsonXml:
    def test_basic(self):
        data = {"name": "Alice", "age": 30}
        xml_str = json_to_xml(data, root="person")
        assert "<name>Alice</name>" in xml_str
        assert "<person>" in xml_str

    def test_roundtrip(self):
        data = {"name": "Alice", "age": "30"}
        xml_str = json_to_xml(data)
        result = xml_to_json(xml_str)
        assert result["root"]["name"] == "Alice"


class TestNdjson:
    def test_csv_to_ndjson(self):
        csv_str = "name,age\nAlice,30\nBob,25"
        ndjson = csv_to_ndjson(csv_str)
        lines = ndjson.strip().split("\n")
        assert len(lines) == 2

    def test_roundtrip(self):
        csv_str = "name,age\nAlice,30\nBob,25"
        result = ndjson_to_csv(csv_to_ndjson(csv_str))
        assert "Alice" in result


# ── Validators ───────────────────────────────────────────────────────────────

class TestValidateRequired:
    def test_passes(self):
        data = [{"a": 1, "b": 2}]
        assert validate_required(data, ["a", "b"]).valid

    def test_fails_missing(self):
        data = [{"a": 1}]
        result = validate_required(data, ["a", "b"])
        assert not result.valid
        assert any("'b'" in e for e in result.errors)

    def test_fails_null(self):
        data = [{"a": None}]
        result = validate_required(data, ["a"])
        assert not result.valid


class TestValidateTypes:
    def test_passes(self):
        data = [{"age": 25, "name": "Alice"}]
        assert validate_types(data, {"age": int, "name": str}).valid

    def test_fails(self):
        data = [{"age": "25"}]
        result = validate_types(data, {"age": int})
        assert not result.valid


class TestValidateRange:
    def test_numeric_bounds(self):
        data = [{"score": 150}]
        result = validate_range(data, {"score": {"min": 0, "max": 100}})
        assert not result.valid

    def test_choices(self):
        data = [{"status": "pending"}]
        result = validate_range(data, {"status": {"choices": ["active", "inactive"]}})
        assert not result.valid

    def test_passes(self):
        data = [{"score": 85, "status": "active"}]
        result = validate_range(data, {
            "score": {"min": 0, "max": 100},
            "status": {"choices": ["active", "inactive"]},
        })
        assert result.valid


# ── Pipeline ─────────────────────────────────────────────────────────────────

class TestPipeline:
    def test_basic_chain(self):
        data = [{"firstName": "Jane", "age": "30", "junk": None}]
        result = (
            Pipeline(data)
            .apply(snake_case_keys)
            .apply(cast_types, schema={"age": int})
            .apply(drop_nulls)
            .result()
        )
        assert result[0]["first_name"] == "Jane"
        assert result[0]["age"] == 30
        assert "junk" not in result[0]

    def test_strict_raises(self):
        data = [{"a": 1}]
        def bad_transform(d):
            raise ValueError("intentional failure")
        with pytest.raises(PipelineError):
            Pipeline(data).apply(bad_transform).result()

    def test_non_strict_skips(self):
        data = [{"a": 1}]
        def bad_transform(d):
            raise ValueError("intentional failure")
        result = Pipeline(data, strict=False).apply(bad_transform).result()
        assert result == data

    def test_stats(self):
        data = [{"a": 1}, {"a": 2}]
        p = Pipeline(data)
        p.apply(drop_nulls).result()
        stats = p.stats()
        assert len(stats) == 1
        assert stats[0].name == "drop_nulls"
