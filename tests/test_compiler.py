import argparse
import base64
import copy
import hashlib
import os
import sys
from datetime import datetime, timezone

import pytest
import requests
import yaml
from jsonschema import ValidationError
from OpenSSL import crypto

from tools import compiler

# Dummy schema data for testing
DUMMY_SCHEMA_DATA = {
    "metadata": {
        "name": "test-schema",
        "version": "v1.0.0.dev",
        "description": "A dummy schema for testing.",
        "owner": "Test Team",
        "validatedBy": {"name": "meta-schema", "version": "v1.0.0"},
    },
    "spec": {"type": "object", "properties": {"field1": {"type": "string"}}},
}

DUMMY_META_SCHEMA_DATA = {
    "metadata": {
        "name": "meta-schema",
        "version": "v1.0.0",
        "description": "A dummy meta-schema.",
        "owner": "Meta Team",
        "validatedBy": {"name": "template-schema", "version": "v0.1.dev"},
        "checksum": "d41d8cd98f00b204e9800998ecf8427e",  # Dummy checksum
    },
    "spec": {
        "type": "object",
        "properties": {"metadata": {"type": "object"}, "spec": {"type": "object"}},
    },
}


# Expected signature response from Vault
VAULT_SIGNATURE_RESPONSE = {
    "data": {
        "signature": "vault:v1:MEUCIQCbi5ghHvps5L8qTNtyTJtKghDApzgmjverpF7NqnK9lwIgSnVVEx5SZxNIL33CH0ErAGdmIrmLU4jMhLkM9mNxMLQ="
    }
}

# Mock certificate data from Vault
VAULT_CERT_RESPONSE = {
    "data": {
        "data": {
            "bar": "-----BEGIN CERTIFICATE-----\nMIIC... (dummy content) ...END CERTIFICATE-----\n"
        }
    }
}

# Mock Root CA data from Vault
VAULT_ROOT_CA_RESPONSE = {
    "data": {
        "data": {
            "bar": "-----BEGIN CERTIFICATE-----\nMIIC... (dummy root ca content) ...END CERTIFICATE-----\n"
        }
    }
}


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc if tz is None else tz)


def test_load_and_resolve_schema_valid(tmp_path):
    """Test that load_and_resolve_schema correctly loads a valid YAML file."""
    data = {"name": "test", "version": "1.0.0"}
    yaml_path = tmp_path / "schema.yaml"
    yaml_path.write_text(yaml.safe_dump(data))
    result = compiler.load_and_resolve_schema(str(yaml_path))
    assert result == data


def test_load_and_resolve_schema_file_not_found(tmp_path):
    """Test that load_and_resolve_schema raises SystemExit if file does not exist."""
    missing_file = tmp_path / "missing.yaml"
    with pytest.raises(SystemExit) as excinfo:
        compiler.load_and_resolve_schema(str(missing_file))
    assert excinfo.value.code == 1


def test_load_and_resolve_schema_invalid_yaml(tmp_path):
    """Test that load_and_resolve_schema raises SystemExit if YAML is invalid."""
    bad_yaml = "name: test: version: 1.0.0"
    yaml_path = tmp_path / "invalid.yaml"
    yaml_path.write_text(bad_yaml)
    with pytest.raises(SystemExit) as excinfo:
        compiler.load_and_resolve_schema(str(yaml_path))
    assert excinfo.value.code == 1


def test_run_validation_success(mocker):
    """Test that run_validation succeeds with valid data."""
    args = argparse.Namespace(file="dummy.yaml")
    mocker.patch(
        "tools.compiler.load_and_resolve_schema",
        side_effect=[
            copy.deepcopy(DUMMY_SCHEMA_DATA),
            copy.deepcopy(DUMMY_META_SCHEMA_DATA),
        ],
    )
    mocker.patch(
        "tools.compiler.get_sha256_hex", return_value="d41d8cd98f00b204e9800998ecf8427e"
    )
    mock_validate = mocker.patch("tools.compiler.validate")

    compiler.run_validation(args)
    mock_validate.assert_called_once()


def test_run_validation_schema_validation_failure(mocker):
    """Test that run_validation exits if a schema fails validation."""
    args = argparse.Namespace(file="dummy.yaml")
    mocker.patch(
        "tools.compiler.load_and_resolve_schema",
        side_effect=[
            copy.deepcopy(DUMMY_SCHEMA_DATA),
            copy.deepcopy(DUMMY_META_SCHEMA_DATA),
        ],
    )
    mocker.patch(
        "tools.compiler.get_sha256_hex", return_value="d41d8cd98f00b204e9800998ecf8427e"
    )
    mocker.patch(
        "tools.compiler.validate", side_effect=ValidationError("Schema invalid")
    )

    with pytest.raises(SystemExit) as excinfo:
        compiler.run_validation(args)
    assert excinfo.value.code == 1


def test_main_no_arguments(mocker):
    """Test that main exits with code 2 if no arguments are provided."""
    mocker.patch.object(sys, "argv", ["compiler.py"])
    with pytest.raises(SystemExit) as excinfo:
        compiler.main()
    assert excinfo.value.code == 2


def test_main_unknown_command(mocker):
    """Test that main exits with code 2 if an unknown command is provided."""
    mocker.patch.object(sys, "argv", ["compiler.py", "unknown_command"])
    with pytest.raises(SystemExit) as excinfo:
        compiler.main()
    assert excinfo.value.code == 2


def test_run_release_dependency_no_token_file(mocker):
    """Test that run_release_dependency exits if the token file is not found."""
    args = argparse.Namespace(source="dummy.yaml", version="v1.0.0")
    mocker.patch.object(os, "getenv", return_value="http://localhost:8200")
    mocker.patch("builtins.open", side_effect=FileNotFoundError)

    with pytest.raises(SystemExit) as excinfo:
        compiler.run_release_dependency(args)
    assert excinfo.value.code == 1


def test_run_release_dependency_vault_signing_failure(mocker):
    """Test that run_release_dependency exits if Vault signing fails."""
    args = argparse.Namespace(source="dummy.yaml", version="v1.0.0")
    mocker.patch.object(os, "getenv", return_value="http://localhost:8200")
    mocker.patch("builtins.open", mocker.mock_open(read_data="test_token"))
    mocker.patch(
        "tools.compiler.load_and_resolve_schema",
        return_value=copy.deepcopy(DUMMY_SCHEMA_DATA),
    )
    mocker.patch(
        "tools.compiler._get_validator_schema",
        return_value=copy.deepcopy(DUMMY_META_SCHEMA_DATA),
    )
    mocker.patch(
        "requests.post",
        side_effect=requests.exceptions.RequestException("Vault is down"),
    )

    with pytest.raises(SystemExit) as excinfo:
        compiler.run_release_dependency(args)
    assert excinfo.value.code == 1


def test_run_release_dependency_final_validation_failure(mocker):
    """Test that run_release exits with code 1 if final validation fails."""
    args = argparse.Namespace(source="dummy.yaml", version="v1.0.0")
    mocker.patch.object(os, "getenv", return_value="http://localhost:8200")
    mocker.patch("builtins.open", mocker.mock_open(read_data="test_token"))
    mocker.patch(
        "tools.compiler.load_and_resolve_schema",
        return_value=copy.deepcopy(DUMMY_SCHEMA_DATA),
    )
    mocker.patch(
        "tools.compiler._get_validator_schema",
        return_value=copy.deepcopy(DUMMY_META_SCHEMA_DATA),
    )

    mock_requests_post = mocker.patch("requests.post")
    mock_requests_post.return_value.raise_for_status.return_value = None
    mock_requests_post.return_value.json.return_value = VAULT_SIGNATURE_RESPONSE

    mock_requests_get = mocker.patch("requests.get")
    mock_cert_response = mocker.Mock()
    mock_cert_response.raise_for_status.return_value = None
    mock_cert_response.json.return_value = VAULT_CERT_RESPONSE
    mock_root_ca_response = mocker.Mock()
    mock_root_ca_response.raise_for_status.return_value = None
    mock_root_ca_response.json.return_value = VAULT_ROOT_CA_RESPONSE
    mock_requests_get.side_effect = [mock_cert_response, mock_root_ca_response]

    mocker.patch(
        "tools.compiler._parse_certificate_info",
        return_value=("Test User", "test@example.com"),
    )

    # Final validation fails
    mocker.patch(
        "tools.compiler.validate",
        side_effect=[None, ValidationError("Final schema invalid")],
    )

    with pytest.raises(SystemExit) as excinfo:
        compiler.run_release_dependency(args)
    assert excinfo.value.code == 1


def test_run_release_dependency_success(mocker):
    """Test that the run_release_dependency function executes successfully."""
    args = argparse.Namespace(source="dummy.yaml", version="v1.0.0")
    mocker.patch.object(os, "getenv", return_value="http://localhost:8200")
    mocker.patch("builtins.open", mocker.mock_open(read_data="test_token"))
    mock_load = mocker.patch(
        "tools.compiler.load_and_resolve_schema",
        return_value=copy.deepcopy(DUMMY_SCHEMA_DATA),
    )
    mocker.patch(
        "tools.compiler._get_validator_schema",
        return_value=copy.deepcopy(DUMMY_META_SCHEMA_DATA),
    )

    mock_requests_post = mocker.patch("requests.post")
    mock_requests_post.return_value.raise_for_status.return_value = None
    mock_requests_post.return_value.json.return_value = VAULT_SIGNATURE_RESPONSE

    mock_requests_get = mocker.patch("requests.get")
    mock_cert_response = mocker.Mock()
    mock_cert_response.raise_for_status.return_value = None
    mock_cert_response.json.return_value = VAULT_CERT_RESPONSE
    mock_root_ca_response = mocker.Mock()
    mock_root_ca_response.raise_for_status.return_value = None
    mock_root_ca_response.json.return_value = VAULT_ROOT_CA_RESPONSE
    mock_requests_get.side_effect = [mock_cert_response, mock_root_ca_response]

    mocker.patch(
        "tools.compiler._parse_certificate_info",
        return_value=("Test User", "test@example.com"),
    )
    mocker.patch("tools.compiler.validate", return_value=None)
    mock_write_yaml = mocker.patch("tools.compiler.write_yaml")
    mocker.patch("tools.compiler.datetime.datetime", FixedDateTime)

    print(f"Mocked load_and_resolve_schema will return: {mock_load.return_value}")
    compiler.run_release_dependency(args)

    # Assertions
    mock_write_yaml.assert_called_once()
    written_data = mock_write_yaml.call_args[0][1]
    assert written_data["metadata"]["version"] == "v1.0.0"
    assert "checksum" in written_data["metadata"]
    assert "sign" in written_data["metadata"]
    assert "createdBy" in written_data["metadata"]
    assert written_data["metadata"]["createdBy"]["name"] == "Test User"
    assert written_data["metadata"]["build_timestamp"] == "2024-01-01T12:00:00+00:00"


def test_write_yaml(tmp_path):
    """Test that write_yaml correctly writes data to a YAML file."""
    test_file = tmp_path / "test_output.yaml"
    test_data = {"key": "value"}
    compiler.write_yaml(str(test_file), test_data)
    assert test_file.exists()
    loaded_data = yaml.safe_load(test_file.read_text())
    assert loaded_data == test_data


def test_get_sha256_hex():
    """Test that get_sha256_hex correctly calculates the SHA256 hash."""
    data = b"test_string"
    expected_hash = hashlib.sha256(data).hexdigest()
    assert compiler.get_sha256_hex(data) == expected_hash


def test_get_sha256_b64():
    """Test that get_sha256_b64 correctly calculates the SHA256 hash and base64 encodes it."""
    data = b"test_string"
    expected_b64_hash = base64.b64encode(hashlib.sha256(data).digest()).decode("utf-8")
    assert compiler.get_sha256_b64(data) == expected_b64_hash


def test_parse_certificate_info_error(mocker):
    """Test that _parse_certificate_info handles OpenSSLError."""
    mocker.patch(
        "OpenSSL.crypto.load_certificate",
        side_effect=crypto.Error("Mocked OpenSSL error"),
    )
    name, email = compiler._parse_certificate_info("dummy_cert_data")
    assert name == "Unknown"
    assert email == "unknown@example.com"


def test_get_validator_schema_missing_validated_by():
    """Test _get_validator_schema with missing 'validatedBy' block."""
    with pytest.raises(
        ValueError, match="Source schema is missing the 'metadata.validatedBy' block."
    ):
        compiler._get_validator_schema({"metadata": {}})


def test_get_validator_schema_incomplete_validated_by():
    """Test _get_validator_schema with incomplete 'validatedBy' block."""
    with pytest.raises(
        ValueError, match="'validatedBy' block must contain 'name' and 'version'."
    ):
        compiler._get_validator_schema({"metadata": {"validatedBy": {"name": "test"}}})


def test_get_validator_schema_checksum_mismatch(mocker):
    """Test _get_validator_schema with a checksum mismatch."""
    mocker.patch(
        "tools.compiler.load_and_resolve_schema",
        return_value=copy.deepcopy(DUMMY_META_SCHEMA_DATA),
    )
    mocker.patch("tools.compiler.get_sha256_hex", return_value="incorrect_checksum")
    with pytest.raises(
        RuntimeError,
        match="FATAL: Validator schema .* is corrupt or has been tampered with!",
    ):
        compiler._get_validator_schema(copy.deepcopy(DUMMY_SCHEMA_DATA))


def test_run_release_schema_success(mocker):
    """Test the run_release_schema command for successful execution."""
    args = argparse.Namespace(source="dummy.yaml", version="v1.0.0")
    mocker.patch(
        "tools.compiler.load_and_resolve_schema",
        return_value=copy.deepcopy(DUMMY_SCHEMA_DATA),
    )
    mock_generate = mocker.patch(
        "tools.compiler._generate_signed_artifact", return_value={"metadata": {}}
    )
    mock_write = mocker.patch("tools.compiler.write_yaml")

    compiler.run_release_schema(args)

    mock_generate.assert_called_once()
    mock_write.assert_called_once()
    assert "release" in mock_write.call_args[0][0]


def test_run_release_schema_invalid_dev_version(mocker):
    """Test run_release_schema with an invalid .dev version in source."""
    args = argparse.Namespace(source="dummy.yaml", version="v1.0.0")
    schema_data = copy.deepcopy(DUMMY_SCHEMA_DATA)
    schema_data["metadata"]["version"] = "v1.0.0"  # Not a .dev version
    mocker.patch("tools.compiler.load_and_resolve_schema", return_value=schema_data)

    with pytest.raises(SystemExit) as excinfo:
        compiler.run_release_schema(args)
    assert excinfo.value.code == 1


def test_run_release_schema_target_dev_version(mocker):
    """Test run_release_schema with a .dev target version."""
    args = argparse.Namespace(source="dummy.yaml", version="v1.0.0.dev")
    mocker.patch(
        "tools.compiler.load_and_resolve_schema",
        return_value=copy.deepcopy(DUMMY_SCHEMA_DATA),
    )

    with pytest.raises(SystemExit) as excinfo:
        compiler.run_release_schema(args)
    assert excinfo.value.code == 1


def test_run_release_schema_missing_name(mocker):
    """Test run_release_schema with missing schema name."""
    args = argparse.Namespace(source="dummy.yaml", version="v1.0.0")
    schema_data = copy.deepcopy(DUMMY_SCHEMA_DATA)
    del schema_data["metadata"]["name"]
    mocker.patch("tools.compiler.load_and_resolve_schema", return_value=schema_data)

    with pytest.raises(SystemExit) as excinfo:
        compiler.run_release_schema(args)
    assert excinfo.value.code == 1


def test_run_get_name_success(mocker, capsys):
    """Test the get-name command for successful execution."""
    args = argparse.Namespace()
    mocker.patch(
        "tools.compiler.load_and_resolve_schema",
        return_value=copy.deepcopy(DUMMY_SCHEMA_DATA),
    )
    compiler.run_get_name(args)
    captured = capsys.readouterr()
    assert captured.out.strip() == "test-schema"


def test_run_get_name_failure(mocker):
    """Test the get-name command when the name is missing."""
    args = argparse.Namespace()
    schema_data = copy.deepcopy(DUMMY_SCHEMA_DATA)
    del schema_data["metadata"]["name"]
    mocker.patch("tools.compiler.load_and_resolve_schema", return_value=schema_data)
    with pytest.raises(SystemExit) as excinfo:
        compiler.run_get_name(args)
    assert excinfo.value.code == 1


def test_generate_signed_artifact_no_vault_ca(mocker):
    """Test _generate_signed_artifact without a Vault CA cert."""
    mocker.patch.object(os, "getenv", return_value="http://localhost:8200")
    mocker.patch("builtins.open", mocker.mock_open(read_data="test_token"))
    mocker.patch(
        "tools.compiler.load_and_resolve_schema",
        side_effect=[
            copy.deepcopy(DUMMY_META_SCHEMA_DATA),
            copy.deepcopy(DUMMY_META_SCHEMA_DATA),
        ],
    )
    mocker.patch(
        "tools.compiler.get_sha256_hex", return_value="d41d8cd98f00b204e9800998ecf8427e"
    )
    mocker.patch("tools.compiler.validate")
    mock_post = mocker.patch("requests.post")
    mock_post.return_value.raise_for_status.return_value = None
    mock_post.return_value.json.return_value = VAULT_SIGNATURE_RESPONSE

    mock_get = mocker.patch("requests.get")
    mock_cert_response = mocker.Mock()
    mock_cert_response.raise_for_status.return_value = None
    mock_cert_response.json.return_value = VAULT_CERT_RESPONSE
    mock_root_ca_response = mocker.Mock()
    mock_root_ca_response.raise_for_status.return_value = None
    mock_root_ca_response.json.return_value = VAULT_ROOT_CA_RESPONSE
    mock_get.side_effect = [mock_cert_response, mock_root_ca_response]

    # Mock the OpenSSL part to avoid real crypto operations
    mock_cert_obj = mocker.Mock()
    mock_subject = mocker.Mock()
    mock_subject.CN = "Test User"
    mock_subject.emailAddress = "test@example.com"
    mock_cert_obj.get_subject.return_value = mock_subject
    mock_cert_obj.get_extension_count.return_value = 0
    mocker.patch("OpenSSL.crypto.load_certificate", return_value=mock_cert_obj)

    mocker.patch.object(
        os.path, "exists", return_value=False
    )  # Simulate missing CA file

    compiler._generate_signed_artifact(
        copy.deepcopy(DUMMY_SCHEMA_DATA), "v1.0.0", "release"
    )


def test_generate_signed_artifact_missing_cert_data(mocker):
    """Test _generate_signed_artifact with missing certificate data in Vault response."""
    mocker.patch.object(os, "getenv", return_value="http://localhost:8200")
    mocker.patch("builtins.open", mocker.mock_open(read_data="test_token"))
    mocker.patch(
        "tools.compiler.load_and_resolve_schema",
        side_effect=[
            copy.deepcopy(DUMMY_META_SCHEMA_DATA),  # For _get_validator_schema
            copy.deepcopy(
                DUMMY_META_SCHEMA_DATA
            ),  # For final meta-meta-schema validation
        ],
    )
    mocker.patch(
        "tools.compiler.get_sha256_hex", return_value="d41d8cd98f00b204e9800998ecf8427e"
    )
    mocker.patch("tools.compiler.validate")

    mock_requests_post = mocker.patch("requests.post")
    mock_requests_post.return_value.raise_for_status.return_value = None
    mock_requests_post.return_value.json.return_value = VAULT_SIGNATURE_RESPONSE

    # Mock response with missing 'bar' key
    bad_cert_response = {"data": {"data": {"foo": "not-the-cert"}}}
    mock_requests_get = mocker.patch("requests.get")
    mock_cert_resp_obj = mocker.Mock()
    mock_cert_resp_obj.raise_for_status.return_value = None
    mock_cert_resp_obj.json.return_value = bad_cert_response
    mock_requests_get.return_value = mock_cert_resp_obj

    with pytest.raises(
        RuntimeError,
        match="Certificate PEM data not found in Vault response for 'crt'.",
    ):
        compiler._generate_signed_artifact(
            copy.deepcopy(DUMMY_SCHEMA_DATA), "v1.0.0", "release"
        )


def test_parse_certificate_info_with_san(mocker):
    """Test that _parse_certificate_info correctly extracts email from SubjectAltName."""
    mock_cert = mocker.Mock()
    mock_subject = mocker.Mock()
    mock_subject.CN = "Test User"
    mock_subject.emailAddress = "wrong@example.com"
    mock_cert.get_subject.return_value = mock_subject

    mock_ext = mocker.MagicMock()
    mock_ext.get_short_name.return_value = b"subjectAltName"
    mock_ext.__str__.return_value = "DNS:example.com, email:correct@example.com"
    mock_cert.get_extension_count.return_value = 1
    mock_cert.get_extension.return_value = mock_ext

    mocker.patch("OpenSSL.crypto.load_certificate", return_value=mock_cert)
    name, email = compiler._parse_certificate_info("dummy_cert_data")
    assert name == "Test User"
    assert email == "correct@example.com"


def test_get_validator_schema_bootstrap_case(mocker):
    """Test _get_validator_schema for the self-validation bootstrap case."""
    schema_data = copy.deepcopy(DUMMY_SCHEMA_DATA)
    schema_data["metadata"]["validatedBy"]["name"] = "template-schema"
    mock_load = mocker.patch(
        "tools.compiler.load_and_resolve_schema", return_value={"spec": {}}
    )

    compiler._get_validator_schema(schema_data)

    mock_load.assert_called_once_with(compiler.META_META_SCHEMA_FILE)


def test_run_get_name_exception(mocker):
    """Test that run_get_name exits on a generic exception."""
    mocker.patch(
        "tools.compiler.load_and_resolve_schema",
        side_effect=Exception("Unexpected error"),
    )
    with pytest.raises(SystemExit) as excinfo:
        compiler.run_get_name(argparse.Namespace())
    assert excinfo.value.code == 1


def test_main_release_schema_command(mocker):
    """Test that main correctly calls run_release_schema."""
    mocker.patch.object(
        sys,
        "argv",
        ["compiler.py", "release-schema", "--source", "s.yaml", "--version", "v1"],
    )
    mock_run = mocker.patch("tools.compiler.run_release_schema")
    compiler.main()
    mock_run.assert_called_once()
    # We can also check the arguments if needed
    call_args = mock_run.call_args[0][0]
    assert call_args.source == "s.yaml"
    assert call_args.version == "v1"


def test_main_get_name_command(mocker):
    """Test that main correctly calls run_get_name."""
    mocker.patch.object(sys, "argv", ["compiler.py", "get-name"])
    mock_run = mocker.patch("tools.compiler.run_get_name")
    compiler.main()
    mock_run.assert_called_once()
