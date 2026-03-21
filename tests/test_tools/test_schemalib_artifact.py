"""Tests for tools.schemalib.artifact"""
import base64
import hashlib
import json
from unittest.mock import MagicMock, patch

import pytest

from tools.schemalib.artifact import (
    build_signing_payload,
    compute_spec_checksum,
    generate_signed_artifact,
    get_sha256_b64,
    get_sha256_hex,
    parse_certificate_info,
    to_canonical_json,
)


# ---------------------------------------------------------------------------
# to_canonical_json
# ---------------------------------------------------------------------------

class TestToCanonicalJson:
    def test_sorted_keys(self):
        data = {"b": 2, "a": 1}
        result = to_canonical_json(data)
        assert result == b'{"a":1,"b":2}'

    def test_no_whitespace(self):
        data = {"key": "value"}
        result = to_canonical_json(data)
        assert b" " not in result

    def test_deterministic(self):
        data = {"z": 3, "a": 1, "m": 2}
        r1 = to_canonical_json(data)
        r2 = to_canonical_json(data)
        assert r1 == r2

    def test_returns_bytes(self):
        result = to_canonical_json({"x": 1})
        assert isinstance(result, bytes)


# ---------------------------------------------------------------------------
# compute_spec_checksum
# ---------------------------------------------------------------------------

class TestComputeSpecChecksum:
    def test_returns_hex_string(self):
        spec = {"type": "object"}
        result = compute_spec_checksum(spec)
        assert isinstance(result, str)
        assert len(result) == 64

    def test_deterministic(self):
        spec = {"type": "object", "properties": {"x": {"type": "string"}}}
        assert compute_spec_checksum(spec) == compute_spec_checksum(spec)

    def test_different_specs_different_checksums(self):
        spec1 = {"type": "object"}
        spec2 = {"type": "string"}
        assert compute_spec_checksum(spec1) != compute_spec_checksum(spec2)

    def test_known_value(self):
        spec = {"type": "object"}
        canonical = json.dumps(spec, sort_keys=True, separators=(",", ":")).encode("utf-8")
        expected = hashlib.sha256(canonical).hexdigest()
        assert compute_spec_checksum(spec) == expected

    def test_key_order_invariant(self):
        """Checksum must be the same regardless of key insertion order."""
        spec_a = {"b": 2, "a": 1}
        spec_b = {"a": 1, "b": 2}
        assert compute_spec_checksum(spec_a) == compute_spec_checksum(spec_b)


# ---------------------------------------------------------------------------
# get_sha256_hex / get_sha256_b64
# ---------------------------------------------------------------------------

class TestHashHelpers:
    def test_get_sha256_hex_known(self):
        data = b"hello"
        expected = hashlib.sha256(data).hexdigest()
        assert get_sha256_hex(data) == expected

    def test_get_sha256_b64_known(self):
        data = b"hello"
        expected = base64.b64encode(hashlib.sha256(data).digest()).decode("utf-8")
        assert get_sha256_b64(data) == expected

    def test_hex_is_string(self):
        assert isinstance(get_sha256_hex(b"data"), str)

    def test_b64_is_string(self):
        assert isinstance(get_sha256_b64(b"data"), str)


# ---------------------------------------------------------------------------
# parse_certificate_info
# ---------------------------------------------------------------------------

FAKE_PEM = "-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n"


class TestParseCertificateInfo:
    def test_returns_cn_and_email_from_san(self):
        mock_cert = MagicMock()
        mock_cert.get_subject.return_value.CN = "Gabor Sinko"
        mock_cert.get_subject.return_value.emailAddress = None

        mock_ext = MagicMock()
        mock_ext.get_short_name.return_value = b"subjectAltName"
        mock_ext.__str__ = MagicMock(return_value="email:sgz@example.com, DNS:example.com")
        mock_cert.get_extension_count.return_value = 1
        mock_cert.get_extension.return_value = mock_ext

        with patch("tools.schemalib.artifact.crypto.load_certificate", return_value=mock_cert):
            name, email = parse_certificate_info(FAKE_PEM)

        assert name == "Gabor Sinko"
        assert email == "sgz@example.com"

    def test_falls_back_to_subject_email(self):
        mock_cert = MagicMock()
        mock_cert.get_subject.return_value.CN = "Test User"
        mock_cert.get_subject.return_value.emailAddress = "fallback@example.com"
        mock_cert.get_extension_count.return_value = 0

        with patch("tools.schemalib.artifact.crypto.load_certificate", return_value=mock_cert):
            name, email = parse_certificate_info(FAKE_PEM)

        assert name == "Test User"
        assert email == "fallback@example.com"

    def test_returns_defaults_on_parse_error(self):
        with patch(
            "tools.schemalib.artifact.crypto.load_certificate",
            side_effect=Exception("bad cert"),
        ):
            name, email = parse_certificate_info(FAKE_PEM)

        assert name == "Unknown"
        assert email == "unknown@example.com"

    def test_no_san_no_email_returns_none_email(self):
        mock_cert = MagicMock()
        mock_cert.get_subject.return_value.CN = "No Email User"
        mock_cert.get_subject.return_value.emailAddress = None
        mock_ext = MagicMock()
        mock_ext.get_short_name.return_value = b"keyUsage"  # Not SAN
        mock_cert.get_extension_count.return_value = 1
        mock_cert.get_extension.return_value = mock_ext

        with patch("tools.schemalib.artifact.crypto.load_certificate", return_value=mock_cert):
            name, email = parse_certificate_info(FAKE_PEM)

        assert name == "No Email User"
        assert email is None


# ---------------------------------------------------------------------------
# build_signing_payload
# ---------------------------------------------------------------------------

class TestBuildSigningPayload:
    def test_returns_base64_string(self):
        result = build_signing_payload("my-schema", "v1.0.0", "abc123", "2025-01-01T00:00:00")
        assert isinstance(result, str)
        # Must be valid base64
        decoded = base64.b64decode(result)
        assert len(decoded) == 32  # SHA-256 = 32 bytes

    def test_deterministic(self):
        args = ("name", "v1.0.0", "checksum", "2025-01-01T00:00:00")
        assert build_signing_payload(*args) == build_signing_payload(*args)

    def test_different_inputs_different_payloads(self):
        p1 = build_signing_payload("a", "v1", "ck1", "ts1")
        p2 = build_signing_payload("b", "v1", "ck1", "ts1")
        assert p1 != p2

    def test_known_value(self):
        metadata = {
            "name": "test",
            "version": "v1.0.0",
            "checksum": "abc",
            "build_timestamp": "2025-01-01T00:00:00",
        }
        canonical = json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode("utf-8")
        expected = base64.b64encode(hashlib.sha256(canonical).digest()).decode("utf-8")
        result = build_signing_payload("test", "v1.0.0", "abc", "2025-01-01T00:00:00")
        assert result == expected


# ---------------------------------------------------------------------------
# generate_signed_artifact
# ---------------------------------------------------------------------------

class TestGenerateSignedArtifact:
    def _call(self, **kwargs):
        defaults = dict(
            spec={"type": "object"},
            name="my-schema",
            version="v1.0.0",
            checksum="abc123",
            build_timestamp="2025-01-01T00:00:00",
            developer_cert=FAKE_PEM,
            issuer_cert="-----BEGIN CERTIFICATE-----\nissuer\n-----END CERTIFICATE-----\n",
            signature="vault:v1:fakesig",
        )
        defaults.update(kwargs)

        mock_cert = MagicMock()
        mock_cert.get_subject.return_value.CN = "Test Author"
        mock_cert.get_subject.return_value.emailAddress = "author@example.com"
        mock_cert.get_extension_count.return_value = 0

        with patch("tools.schemalib.artifact.crypto.load_certificate", return_value=mock_cert):
            return generate_signed_artifact(**defaults)

    def test_result_has_metadata_and_spec(self):
        result = self._call()
        assert "metadata" in result
        assert "spec" in result

    def test_spec_is_preserved(self):
        spec = {"type": "object", "properties": {"x": {"type": "string"}}}
        result = self._call(spec=spec)
        assert result["spec"] == spec

    def test_metadata_contains_expected_fields(self):
        result = self._call()
        md = result["metadata"]
        assert md["name"] == "my-schema"
        assert md["version"] == "v1.0.0"
        assert md["checksum"] == "abc123"
        assert md["sign"] == "vault:v1:fakesig"
        assert md["build_timestamp"] == "2025-01-01T00:00:00"

    def test_created_by_has_cert_info(self):
        result = self._call()
        cb = result["metadata"]["createdBy"]
        assert cb["name"] == "Test Author"
        assert cb["email"] == "author@example.com"
        assert cb["certificate"] == FAKE_PEM

    def test_placeholder_fields_present(self):
        result = self._call()
        md = result["metadata"]
        assert md["buildHash"] == ""
        assert md["cicSign"] == ""
        assert md["cicSignedCA"] == {"certificate": ""}

    def test_no_validated_by_if_not_provided(self):
        result = self._call()
        assert "validatedBy" not in result["metadata"]

    def test_validated_by_included_when_provided(self):
        result = self._call(
            validator_name="template-schema",
            validator_version="v2.0.0",
            validator_checksum="deadbeef",
        )
        vb = result["metadata"]["validatedBy"]
        assert vb["name"] == "template-schema"
        assert vb["version"] == "v2.0.0"
        assert vb["checksum"] == "deadbeef"

    def test_validated_by_without_checksum(self):
        result = self._call(
            validator_name="template-schema",
            validator_version="v2.0.0",
        )
        vb = result["metadata"]["validatedBy"]
        assert "checksum" not in vb

    def test_issuer_cert_stored(self):
        issuer = "-----BEGIN CERTIFICATE-----\nISSUER\n-----END CERTIFICATE-----\n"
        result = self._call(issuer_cert=issuer)
        assert result["metadata"]["createdBy"]["issuer_certificate"] == issuer