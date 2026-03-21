"""Tests for tools.schemalib.validator"""
import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from tools.releaselib.exceptions import ConfigurationError
from tools.schemalib.validator import (
    ValidationFailureError,
    get_validator_schema,
    run_validation,
    verify_validator_integrity,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_checksum(spec: dict) -> str:
    canonical = json.dumps(spec, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _make_validator_schema(name="my-validator", version="v1.0.0", spec=None):
    if spec is None:
        spec = {"type": "object"}
    return {
        "metadata": {
            "name": name,
            "version": version,
            "checksum": _make_checksum(spec),
        },
        "spec": spec,
    }


# ---------------------------------------------------------------------------
# verify_validator_integrity
# ---------------------------------------------------------------------------

class TestVerifyValidatorIntegrity:
    def test_passes_with_correct_checksum(self):
        spec = {"type": "object", "properties": {"x": {"type": "string"}}}
        schema = _make_validator_schema(spec=spec)
        # Should not raise
        verify_validator_integrity(schema)

    def test_raises_if_checksum_missing(self):
        schema = {
            "metadata": {"name": "test"},
            "spec": {"type": "object"},
        }
        with pytest.raises(ValidationFailureError, match="missing 'metadata.checksum'"):
            verify_validator_integrity(schema)

    def test_raises_if_checksum_empty(self):
        schema = {
            "metadata": {"name": "test", "checksum": ""},
            "spec": {"type": "object"},
        }
        with pytest.raises(ValidationFailureError, match="missing 'metadata.checksum'"):
            verify_validator_integrity(schema)

    def test_raises_if_checksum_wrong(self):
        schema = {
            "metadata": {"name": "tampered", "checksum": "deadbeef" * 8},
            "spec": {"type": "object"},
        }
        with pytest.raises(ValidationFailureError, match="integrity check FAILED"):
            verify_validator_integrity(schema)

    def test_raises_if_metadata_missing(self):
        schema = {"spec": {"type": "object"}}
        with pytest.raises(ValidationFailureError, match="missing 'metadata.checksum'"):
            verify_validator_integrity(schema)

    def test_name_in_error_message(self):
        schema = {
            "metadata": {"name": "bad-schema", "checksum": "wrongchecksum"},
            "spec": {"type": "object"},
        }
        with pytest.raises(ValidationFailureError, match="bad-schema"):
            verify_validator_integrity(schema)


# ---------------------------------------------------------------------------
# get_validator_schema
# ---------------------------------------------------------------------------

class TestGetValidatorSchema:
    def test_self_validation_returns_source(self):
        """If validator_name == source schema name, return source itself."""
        source = _make_validator_schema(name="template-schema", version="v1.0.0")
        result = get_validator_schema(
            validator_name="template-schema",
            validator_version="v1.0.0",
            source_schema=source,
            dependencies_dir=Path("/any"),
        )
        assert result is source

    def test_loads_external_validator(self, tmp_path):
        spec = {"type": "object"}
        validator = _make_validator_schema(name="ext-validator", version="v2.0.0", spec=spec)

        # Write validator file to tmp_path
        import yaml
        validator_file = tmp_path / "ext-validator-v2.0.0.yaml"
        validator_file.write_text(yaml.dump(validator))

        source = {"metadata": {"name": "my-schema"}}
        result = get_validator_schema(
            validator_name="ext-validator",
            validator_version="v2.0.0",
            source_schema=source,
            dependencies_dir=tmp_path,
        )
        assert result["metadata"]["name"] == "ext-validator"

    def test_raises_configuration_error_if_file_missing(self, tmp_path):
        source = {"metadata": {"name": "my-schema"}}
        with pytest.raises(ConfigurationError):
            get_validator_schema(
                validator_name="nonexistent",
                validator_version="v1.0.0",
                source_schema=source,
                dependencies_dir=tmp_path,
            )

    def test_raises_validation_failure_if_integrity_fails(self, tmp_path):
        """Validator file exists but has wrong checksum."""
        import yaml
        validator = {
            "metadata": {
                "name": "bad-validator",
                "version": "v1.0.0",
                "checksum": "wrongchecksum",
            },
            "spec": {"type": "object"},
        }
        validator_file = tmp_path / "bad-validator-v1.0.0.yaml"
        validator_file.write_text(yaml.dump(validator))

        source = {"metadata": {"name": "my-schema"}}
        with pytest.raises(ValidationFailureError, match="integrity check FAILED"):
            get_validator_schema(
                validator_name="bad-validator",
                validator_version="v1.0.0",
                source_schema=source,
                dependencies_dir=tmp_path,
            )


# ---------------------------------------------------------------------------
# run_validation
# ---------------------------------------------------------------------------

class TestRunValidation:
    def test_valid_instance_passes(self):
        validator_schema = {
            "metadata": {"name": "test", "version": "v1"},
            "spec": {
                "type": "object",
                "required": ["name"],
                "properties": {"name": {"type": "string"}},
            },
        }
        instance = {"name": "hello"}
        # Should not raise
        run_validation(instance, validator_schema)

    def test_invalid_instance_raises(self):
        validator_schema = {
            "metadata": {"name": "test", "version": "v1"},
            "spec": {
                "type": "object",
                "required": ["name"],
                "properties": {"name": {"type": "string"}},
            },
        }
        instance = {"name": 123}  # should be string
        with pytest.raises(ValidationFailureError, match="Schema validation FAILED"):
            run_validation(instance, validator_schema)

    def test_missing_required_field_raises(self):
        validator_schema = {
            "metadata": {"name": "test", "version": "v1"},
            "spec": {
                "type": "object",
                "required": ["name"],
                "properties": {"name": {"type": "string"}},
            },
        }
        instance = {}  # missing required 'name'
        with pytest.raises(ValidationFailureError, match="Schema validation FAILED"):
            run_validation(instance, validator_schema)

    def test_missing_spec_block_raises(self):
        validator_schema = {
            "metadata": {"name": "no-spec", "version": "v1"},
        }
        with pytest.raises(ValidationFailureError, match="missing the 'spec' block"):
            run_validation({}, validator_schema)

    def test_complex_schema_passes(self):
        validator_schema = {
            "metadata": {"name": "complex", "version": "v1"},
            "spec": {
                "type": "object",
                "properties": {
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "count": {"type": "integer", "minimum": 0},
                },
            },
        }
        instance = {"tags": ["a", "b"], "count": 5}
        run_validation(instance, validator_schema)

    def test_error_message_includes_validator_name(self):
        validator_schema = {
            "metadata": {"name": "my-special-validator", "version": "v3.1.4"},
            "spec": {"type": "object", "required": ["must_have"]},
        }
        with pytest.raises(ValidationFailureError, match="my-special-validator"):
            run_validation({}, validator_schema)

    def test_validation_failure_error_is_release_error_subclass(self):
        from tools.releaselib.exceptions import ReleaseError
        assert issubclass(ValidationFailureError, ReleaseError)