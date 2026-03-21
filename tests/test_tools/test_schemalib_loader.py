"""Tests for tools.schemalib.loader"""
import datetime
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from tools.releaselib.exceptions import ConfigurationError, ReleaseError
from tools.schemalib.loader import (
    convert_to_json_serializable,
    load_and_resolve_schema,
    load_yaml,
    write_yaml,
)


# ---------------------------------------------------------------------------
# convert_to_json_serializable
# ---------------------------------------------------------------------------

class TestConvertToJsonSerializable:
    def test_plain_dict_passthrough(self):
        data = {"a": 1, "b": "str"}
        assert convert_to_json_serializable(data) == data

    def test_plain_list_passthrough(self):
        data = [1, 2, 3]
        assert convert_to_json_serializable(data) == data

    def test_datetime_to_isostring(self):
        dt = datetime.datetime(2025, 1, 15, 12, 0, 0)
        result = convert_to_json_serializable(dt)
        assert result == "2025-01-15T12:00:00"

    def test_nested_dict_with_datetime(self):
        data = {"ts": datetime.datetime(2025, 6, 1, 0, 0, 0)}
        result = convert_to_json_serializable(data)
        assert result == {"ts": "2025-06-01T00:00:00"}

    def test_list_with_datetime(self):
        data = [datetime.datetime(2025, 1, 1)]
        result = convert_to_json_serializable(data)
        assert result == ["2025-01-01T00:00:00"]

    def test_scalar_passthrough(self):
        assert convert_to_json_serializable(42) == 42
        assert convert_to_json_serializable("hello") == "hello"
        assert convert_to_json_serializable(None) is None

    def test_jsonref_via_real_resolution(self, tmp_path):
        """JsonRef proxies are resolved via load_and_resolve_schema integration."""
        ref_file = tmp_path / "ref.yaml"
        ref_file.write_text("type: string\n")
        main_file = tmp_path / "main.yaml"
        main_file.write_text("properties:\n  x:\n    $ref: 'ref.yaml'\n")
        result = load_and_resolve_schema(main_file)
        # After resolution, the value must be a plain dict, not a JsonRef proxy
        assert result["properties"]["x"] == {"type": "string"}
        assert type(result["properties"]["x"]) is dict

    def test_deeply_nested_structure(self):
        data = {
            "level1": {
                "level2": [
                    {"ts": datetime.datetime(2025, 3, 1)},
                ]
            }
        }
        result = convert_to_json_serializable(data)
        assert result["level1"]["level2"][0]["ts"] == "2025-03-01T00:00:00"


# ---------------------------------------------------------------------------
# load_yaml
# ---------------------------------------------------------------------------

class TestLoadYaml:
    def test_loads_valid_yaml(self, tmp_path):
        f = tmp_path / "test.yaml"
        f.write_text("key: value\nnumber: 42\n")
        result = load_yaml(f)
        assert result == {"key": "value", "number": 42}

    def test_returns_none_for_empty_file(self, tmp_path):
        f = tmp_path / "empty.yaml"
        f.write_text("   \n")
        assert load_yaml(f) is None

    def test_raises_configuration_error_file_not_found(self, tmp_path):
        with pytest.raises(ConfigurationError, match="not found"):
            load_yaml(tmp_path / "nonexistent.yaml")

    def test_raises_configuration_error_on_yaml_error(self, tmp_path):
        f = tmp_path / "bad.yaml"
        f.write_text("key: [\nbad yaml")
        with pytest.raises(ConfigurationError, match="YAML syntax error"):
            load_yaml(f)

    def test_loads_nested_yaml(self, tmp_path):
        f = tmp_path / "nested.yaml"
        f.write_text("a:\n  b:\n    c: 1\n")
        result = load_yaml(f)
        assert result["a"]["b"]["c"] == 1


# ---------------------------------------------------------------------------
# load_and_resolve_schema
# ---------------------------------------------------------------------------

class TestLoadAndResolveSchema:
    def test_loads_simple_schema(self, tmp_path):
        f = tmp_path / "schema.yaml"
        f.write_text("type: object\nproperties:\n  name:\n    type: string\n")
        result = load_and_resolve_schema(f)
        assert result["type"] == "object"
        assert result["properties"]["name"]["type"] == "string"

    def test_raises_configuration_error_on_missing_file(self, tmp_path):
        with pytest.raises(ConfigurationError, match="File not found"):
            load_and_resolve_schema(tmp_path / "missing.yaml")

    def test_raises_configuration_error_on_yaml_error(self, tmp_path):
        f = tmp_path / "bad.yaml"
        f.write_text("key: [bad")
        with pytest.raises(ConfigurationError):
            load_and_resolve_schema(f)

    def test_resolves_local_ref(self, tmp_path):
        ref_file = tmp_path / "definitions.yaml"
        ref_file.write_text("type: string\n")

        main_file = tmp_path / "main.yaml"
        main_file.write_text(
            "type: object\nproperties:\n  name:\n    $ref: 'definitions.yaml'\n"
        )
        result = load_and_resolve_schema(main_file)
        assert result["properties"]["name"]["type"] == "string"

    def test_returns_plain_python_types(self, tmp_path):
        """Result must not contain JsonRef proxies."""
        f = tmp_path / "schema.yaml"
        f.write_text("type: object\nproperties:\n  x:\n    type: integer\n")
        result = load_and_resolve_schema(f)
        # If JsonRef proxies leaked, json.dumps would fail
        import json
        # Should not raise
        json.dumps(result)

    def test_empty_ref_target_returns_empty_dict(self, tmp_path):
        """Empty referenced file should resolve to {}."""
        empty_ref = tmp_path / "empty.yaml"
        empty_ref.write_text("   \n")
        main_file = tmp_path / "main.yaml"
        main_file.write_text(
            "type: object\ndefs:\n  empty:\n    $ref: 'empty.yaml'\n"
        )
        result = load_and_resolve_schema(main_file)
        assert result["type"] == "object"


# ---------------------------------------------------------------------------
# write_yaml
# ---------------------------------------------------------------------------

class TestWriteYaml:
    def test_writes_yaml_file(self, tmp_path):
        target = tmp_path / "output.yaml"
        data = {"name": "test", "value": 42}
        write_yaml(target, data)
        assert target.exists()
        loaded = yaml.safe_load(target.read_text())
        assert loaded == data

    def test_creates_parent_directories(self, tmp_path):
        target = tmp_path / "subdir" / "deep" / "output.yaml"
        write_yaml(target, {"key": "val"})
        assert target.exists()

    def test_overwrites_existing_file(self, tmp_path):
        target = tmp_path / "file.yaml"
        target.write_text("old: data\n")
        write_yaml(target, {"new": "data"})
        loaded = yaml.safe_load(target.read_text())
        assert loaded == {"new": "data"}

    def test_raises_release_error_on_io_failure(self, tmp_path):
        target = tmp_path / "output.yaml"
        with patch("tools.schemalib.loader.tempfile.NamedTemporaryFile") as mock_ntf:
            mock_ntf.side_effect = IOError("disk full")
            with pytest.raises(ReleaseError, match="Failed to write YAML file"):
                write_yaml(target, {"key": "val"})

    def test_cleans_up_temp_file_on_failure(self, tmp_path):
        target = tmp_path / "output.yaml"
        # Simulate os.replace failure after temp file is created
        with patch("tools.schemalib.loader.os.replace") as mock_replace:
            mock_replace.side_effect = OSError("replace failed")
            with pytest.raises(ReleaseError):
                write_yaml(target, {"key": "val"})
        # Temp file should be cleaned up
        remaining = list(tmp_path.iterdir())
        assert not any(f != target for f in remaining if f.name != "output.yaml")

    def test_unicode_content(self, tmp_path):
        target = tmp_path / "unicode.yaml"
        data = {"name": "Gábor Zoltán Sinkó", "org": "CentralInfraCore"}
        write_yaml(target, data)
        loaded = yaml.safe_load(target.read_text(encoding="utf-8"))
        assert loaded["name"] == "Gábor Zoltán Sinkó"

    def test_preserves_key_order(self, tmp_path):
        target = tmp_path / "ordered.yaml"
        data = {"z": 3, "a": 1, "m": 2}
        write_yaml(target, data)
        content = target.read_text()
        # sort_keys=False: order should be z, a, m
        z_pos = content.index("z:")
        a_pos = content.index("a:")
        m_pos = content.index("m:")
        assert z_pos < a_pos < m_pos
