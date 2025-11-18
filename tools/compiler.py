import argparse
import base64
import datetime
import hashlib
import json
import os
import sys

import requests
import yaml
from jsonref import JsonRef
from jsonschema import ValidationError, validate
from OpenSSL import crypto
from OpenSSL.SSL import Error as OpenSSLError

# --- Configuration ---
SCHEMAS_DIR = "schemas"
SOURCES_DIR = "sources"
DEPENDENCIES_DIR = "dependencies"
RELEASES_DIR = "release"
META_META_SCHEMA_FILE = os.path.join(SCHEMAS_DIR, "index.yaml")
CANONICAL_SOURCE_FILE = os.path.join(SCHEMAS_DIR, "index.yaml")
VAULT_KEY_NAME = "cic-my-sign-key"  # Default key name for signing
VAULT_TOKEN_FILE = "/var/run/secrets/vault-token"
VAULT_CA_CERT_FILE = "/var/run/secrets/vault-ca.crt"


# --- Helper Functions ---


def load_and_resolve_schema(path):
    """
    Loads a YAML file and resolves all $ref references.
    The base URI is the directory of the file, allowing for relative references.
    """
    try:
        # Custom loader to handle YAML files for $ref resolution.
        def yaml_loader(uri):
            from urllib.parse import urlparse
            from urllib.request import url2pathname
            local_path = url2pathname(urlparse(uri).path)

            # --- DEBUG ---
            print(f"--> [DEBUG] yaml_loader invoked for URI: {uri}")
            print(f"--> [DEBUG] Resolved file path: {local_path}")
            # --- END DEBUG ---

            with open(local_path, 'r') as f_loader:
                content = f_loader.read()
                if not content.strip():
                    # --- DEBUG ---
                    print(f"--> [DEBUG] WARNING: File {local_path} is empty or contains only whitespace.")
                    # --- END DEBUG ---
                    return {} # Return an empty dict for empty files

                return yaml.safe_load(content)

        with open(path, "r") as f:
            base_uri = f"file://{os.path.dirname(os.path.abspath(path))}/"
            unresolved_data = yaml.safe_load(f)

            # Directly pass the custom loader to replace_refs. This is the simplest approach.
            resolved_data = JsonRef.replace_refs(
                unresolved_data, base_uri=base_uri, loader=yaml_loader
            )
            return resolved_data

    except FileNotFoundError:
        print(f"[FATAL] File not found: {path}")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"[FATAL] YAML parsing error in {path}: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[FATAL] An unexpected error occurred during schema resolution: {e}")
        sys.exit(1)


def write_yaml(path, data):
    """Writes data to a YAML file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(data, f, sort_keys=False, indent=2)


def to_canonical_json(data):
    """Converts a Python object to a canonical (sorted, no whitespace)
    JSON string."""
    return json.dumps(data, sort_keys=True, separators=(",", ":")).encode("utf-8")


def get_sha256_hex(data_bytes):
    """Calculates the SHA256 hash and returns it as a hex digest."""
    return hashlib.sha256(data_bytes).hexdigest()


def get_sha256_b64(data_bytes):
    """Calculates the SHA256 hash and returns it as a base64 encoded string."""
    return base64.b64encode(hashlib.sha256(data_bytes).digest()).decode("utf-8")


def _parse_certificate_info(pem_cert_data):
    """
    Parses a PEM-encoded certificate to extract Common Name and Email.
    Returns (name, email).
    """
    try:
        cert = crypto.load_certificate(
            crypto.FILETYPE_PEM, pem_cert_data.encode("utf-8")
        )
        subject = cert.get_subject()
        name = subject.CN
        email = None

        # Try to get email from subjectAltName
        for i in range(cert.get_extension_count()):
            ext = cert.get_extension(i)
            if ext.get_short_name() == b"subjectAltName":
                alt_names = str(ext).split(", ")
                for alt_name in alt_names:
                    if alt_name.startswith("email:"):
                        email = alt_name[len("email:") :]
                        break

        if not email:
            email = subject.emailAddress

        return name, email
    except OpenSSLError as e:
        print(f"[WARNING] Could not parse certificate with pyOpenSSL: {e}")
        return "Unknown", "unknown@example.com"
    except Exception as e:
        print(f"[WARNING] An unexpected error occurred while parsing certificate: {e}")
        return "Unknown", "unknown@example.com"


# --- Core Logic ---


def _get_validator_schema(source_data):
    """
    Finds and loads the correct validator schema based on the source file's
    'validatedBy' block.
    """
    validated_by = source_data.get("metadata", {}).get("validatedBy")
    if not validated_by:
        raise ValueError("Source schema is missing the 'metadata.validatedBy' block.")

    validator_name = validated_by.get("name")
    validator_version = validated_by.get("version")

    if not validator_name or not validator_version:
        raise ValueError("'validatedBy' block must contain 'name' and 'version'.")

    # Bootstrap case: the meta-schema validates itself against the base rules.
    if validator_name == "template-schema":
        print("[INFO] Bootstrapping: using meta-meta-schema for validation.")
        return load_and_resolve_schema(META_META_SCHEMA_FILE)

    # Standard case: find the validator in the dependencies directory.
    validator_filename = f"{validator_name}-{validator_version}.yaml"
    validator_path = os.path.join(DEPENDENCIES_DIR, validator_filename)
    print(f"[INFO] Loading and resolving validator: {validator_path}")

    validator_schema = load_and_resolve_schema(validator_path)

    # --- Security Check: Verify the integrity of the validator itself ---
    print("[INFO] Verifying integrity of the validator schema...")
    # IMPORTANT: We calculate the checksum on the *resolved* spec block.
    spec_bytes = to_canonical_json(validator_schema["spec"])
    expected_checksum = validator_schema.get("metadata", {}).get("checksum")
    actual_checksum = get_sha256_hex(spec_bytes)

    if not expected_checksum or actual_checksum != expected_checksum:
        raise RuntimeError(
            f"FATAL: Validator schema {validator_path} is corrupt or has been"
            f" tampered with! Checksum mismatch."
        )
    print("  [92m✓ Validator integrity OK[0m")
    # --- End Security Check ---

    return validator_schema


def _generate_signed_artifact(source_data, target_version, output_dir):
    """
    Generates a signed schema artifact from source data.
    This includes validation, checksum calculation, Vault signing,
    and filling in all release-specific metadata.
    """
    vault_addr = os.getenv("VAULT_ADDR")

    try:
        with open(VAULT_TOKEN_FILE, "r") as f:
            vault_token = f.read().strip()
    except FileNotFoundError:
        raise RuntimeError(f"Vault token file not found at {VAULT_TOKEN_FILE}")

    if not vault_addr:
        raise RuntimeError("[FATAL] VAULT_ADDR must be set for release.")

    verify_param = VAULT_CA_CERT_FILE if os.path.exists(VAULT_CA_CERT_FILE) else False
    if not verify_param:
        print(
            "[93m[WARNING] Vault CA certificate not found. Proceeding without TLS verification.[0m"
        )

    print(
        f"--- Generating Signed Artifact for {source_data['metadata']['name']}@{target_version} ---"
    )

    # 1. Validate the source data against its declared validator
    print("[INFO] Validating source data...")
    try:
        validator_schema = _get_validator_schema(source_data)
        validate(instance=source_data, schema=validator_schema["spec"])
        print(
            f"  [92m✓ Source schema is valid against"
            f" {validator_schema['metadata']['name']}@"
            f"{validator_schema['metadata']['version']}[0m"
        )
    except (ValueError, RuntimeError, ValidationError) as e:
        print(f"\n  [91m✗ SOURCE VALIDATION FAILED: {e}[0m")
        sys.exit(1)

    # 2. Prepare the artifact data
    artifact_data = source_data.copy()
    artifact_data["metadata"]["version"] = target_version

    # Fill validatedBy.checksum
    validator_spec_bytes = to_canonical_json(validator_schema["spec"])
    artifact_data["metadata"]["validatedBy"]["checksum"] = get_sha256_hex(
        validator_spec_bytes
    )

    # 3. Calculate checksum of the 'spec' block
    spec_bytes = to_canonical_json(artifact_data["spec"])
    checksum = get_sha256_hex(spec_bytes)
    print(f"  - Calculated spec checksum: {checksum[:12]}...")

    # 4. Prepare metadata for signing
    metadata_for_signing = artifact_data["metadata"].copy()
    metadata_for_signing.pop(
        "checksum", None
    )  # Ensure these are not part of the signed payload
    metadata_for_signing.pop("sign", None)

    # Add build_timestamp
    metadata_for_signing["build_timestamp"] = datetime.datetime.now(
        datetime.timezone.utc
    ).isoformat()

    # Add checksum to the signed metadata
    metadata_for_signing["checksum"] = checksum

    # 5. Get signature from Vault
    digest_bytes = hashlib.sha256(to_canonical_json(metadata_for_signing)).digest()
    digest_to_sign_b64 = base64.b64encode(digest_bytes).decode("utf-8")

    print("  - Requesting signature from Vault...")
    try:
        response = requests.post(
            f"{vault_addr}/v1/transit/sign/{VAULT_KEY_NAME}",
            headers={"X-Vault-Token": vault_token},
            json={
                "input": digest_to_sign_b64,
                "prehashed": True,
                "hash_algorithm": "sha2-256",
            },
            verify=verify_param,
        )
        response.raise_for_status()
        signature = response.json()["data"]["signature"]
        print("  - Signature received successfully.")
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Vault signing failed: {e}")

    # 6. Get certificate info from Vault to populate 'createdBy'
    print("  - Fetching signing certificate from Vault...")
    try:
        # Fetch main certificate
        cert_response = requests.get(
            f"{vault_addr}/v1/{VAULT_KEY_NAME}/data/crt",  # Assuming KV v2 mount at VAULT_KEY_NAME, secret 'crt'
            headers={"X-Vault-Token": vault_token},
            verify=verify_param,
        )
        cert_response.raise_for_status()
        certificate_pem = cert_response.json()["data"]["data"].get(
            "bar"
        )  # Assuming PEM data is under 'bar' key

        if not certificate_pem:
            raise RuntimeError(
                "Certificate PEM data not found in Vault response for 'crt'."
            )

        # Fetch Root CA certificate
        root_ca_response = requests.get(
            f"{vault_addr}/v1/{VAULT_KEY_NAME}/data/CICRootCA",  # Assuming KV v2 mount at VAULT_KEY_NAME, secret 'CICRootCA'
            headers={"X-Vault-Token": vault_token},
            verify=verify_param,
        )
        root_ca_response.raise_for_status()
        root_ca_pem = root_ca_response.json()["data"]["data"].get(
            "bar"
        )  # Assuming PEM data is under 'bar' key

        if not root_ca_pem:
            raise RuntimeError(
                "Root CA PEM data not found in Vault response for 'CICRootCA'."
            )

        # Parse certificate to get name and email
        name, email = _parse_certificate_info(certificate_pem)

        created_by = {
            "name": name,
            "email": email,
            "certificate": certificate_pem,
            "issuer_certificate": root_ca_pem,
        }
        print("  - Certificate fetched and parsed successfully.")
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to fetch certificate from Vault: {e}")
    except (KeyError, TypeError) as e:
        raise RuntimeError(
            f"Could not parse certificate data from Vault KV response: {e}. Check Vault path and key names."
        )
    except RuntimeError as e:
        raise e  # Re-raise custom runtime errors

    # 7. Assemble final artifact
    final_artifact = artifact_data.copy()
    final_artifact["metadata"]["checksum"] = checksum
    final_artifact["metadata"]["sign"] = signature
    final_artifact["metadata"]["build_timestamp"] = metadata_for_signing[
        "build_timestamp"
    ]
    final_artifact["metadata"]["createdBy"] = created_by

    # 8. Final validation of the completed artifact against the meta-meta-schema
    print("  - Performing final validation on signed artifact...")
    try:
        meta_meta_schema = load_and_resolve_schema(META_META_SCHEMA_FILE)
        validate(instance=final_artifact, schema=meta_meta_schema["spec"])
        print("  - [92m✓ Final artifact validation passed against meta-meta-schema.[0m")
    except (ValueError, RuntimeError, ValidationError) as e:
        raise RuntimeError(
            f"Final artifact validation failed against meta-meta-schema: {e}"
        )

    return final_artifact


def run_validation(args):
    """
    Runs offline validation on a single source schema.
    This command is for developers to check their work before release.
    """
    print("--- Running Schema Validation ---")
    source_file = args.file if args.file else CANONICAL_SOURCE_FILE
    print(f"  Validating and resolving {source_file}...")

    try:
        # Use the new 'smart' loader
        source_data = load_and_resolve_schema(source_file)
        validator_schema = _get_validator_schema(source_data)

        # The actual validation happens against the 'spec' of the validator
        validate(instance=source_data, schema=validator_schema["spec"])
        print(
            f"\n  [92m✓ Schema is valid against"
            f" {validator_schema['metadata']['name']}@"
            f"{validator_schema['metadata']['version']}[0m"
        )

    except (ValueError, RuntimeError, ValidationError) as e:
        print(f"\n  [91m✗ VALIDATION FAILED: {e}[0m")
        sys.exit(1)
    except Exception as e:
        print(f"\n  [91m✗ UNEXPECTED ERROR: {e}[0m")
        sys.exit(1)

    print("\nValidation successful.")


def run_release_dependency(args):
    """
    Releases a meta-schema or shared library schema to the dependencies directory.
    """
    print("--- Releasing Dependency Schema ---")
    source_file = args.source
    target_version = args.version

    print(f"Processing source: {source_file} for version: {target_version}")

    try:
        source_data = load_and_resolve_schema(source_file)

        # Ensure the version in the source file is a .dev version
        current_version = source_data.get("metadata", {}).get("version")
        if not current_version or not current_version.endswith(".dev"):
            raise ValueError(
                f"Source schema {source_file} must have a '.dev' version "
                f"in its metadata (e.g., v1.0.dev) to be released as a dependency."
            )

        # Ensure the target version is not a .dev version
        if target_version.endswith(".dev"):
            raise ValueError(
                f"Target version '{target_version}' cannot be a '.dev' version "
                f"for a dependency release."
            )

        # Ensure the source schema's name matches the expected output name
        schema_name = source_data.get("metadata", {}).get("name")
        if not schema_name:
            raise ValueError("Source schema is missing 'metadata.name'.")

        # Generate the signed artifact
        signed_artifact = _generate_signed_artifact(
            source_data, target_version, DEPENDENCIES_DIR
        )

        # Write the signed artifact to the dependencies directory
        output_filename = f"{schema_name}-{target_version}.yaml"
        output_path = os.path.join(DEPENDENCIES_DIR, output_filename)
        write_yaml(output_path, signed_artifact)
        print(f"\n[92m✓ Successfully released dependency schema to {output_path}[0m")

    except (ValueError, RuntimeError, ValidationError) as e:
        print(f"\n  [91m✗ RELEASE FAILED: {e}[0m")
        sys.exit(1)
    except Exception as e:
        print(f"\n  [91m✗ UNEXPECTED ERROR: {e}[0m")
        sys.exit(1)


def run_release_schema(args):
    """
    Releases an application-specific schema to the release directory.
    """
    print("--- Releasing Application Schema ---")
    source_file = args.source
    target_version = args.version

    print(f"Processing source: {source_file} for version: {target_version}")

    try:
        source_data = load_and_resolve_schema(source_file)

        # Ensure the version in the source file is a .dev version
        current_version = source_data.get("metadata", {}).get("version")
        if not current_version or not current_version.endswith(".dev"):
            raise ValueError(
                f"Source schema {source_file} must have a '.dev' version "
                f"in its metadata (e.g., v1.0.dev) to be released as an application schema."
            )

        # Ensure the target version is not a .dev version
        if target_version.endswith(".dev"):
            raise ValueError(
                f"Target version '{target_version}' cannot be a '.dev' version "
                f"for an application schema release."
            )

        # Ensure the source schema's name matches the expected output name
        schema_name = source_data.get("metadata", {}).get("name")
        if not schema_name:
            raise ValueError("Source schema is missing 'metadata.name'.")

        # Generate the signed artifact
        signed_artifact = _generate_signed_artifact(
            source_data, target_version, RELEASES_DIR
        )

        # Write the signed artifact to the releases directory
        output_filename = f"{schema_name}-{target_version}.yaml"
        output_path = os.path.join(RELEASES_DIR, output_filename)
        write_yaml(output_path, signed_artifact)
        print(f"\n[92m✓ Successfully released application schema to {output_path}[0m")

    except (ValueError, RuntimeError, ValidationError) as e:
        print(f"\n  [91m✗ RELEASE FAILED: {e}[0m")
        sys.exit(1)
    except Exception as e:
        print(f"\n  [91m✗ UNEXPECTED ERROR: {e}[0m")
        sys.exit(1)


def run_get_name(args):
    """
    Prints the schema name from the canonical source file.
    """
    try:
        source_data = load_and_resolve_schema(CANONICAL_SOURCE_FILE)
        schema_name = source_data.get("metadata", {}).get("name")
        if schema_name:
            print(schema_name)
        else:
            sys.exit(1)
    except Exception:
        sys.exit(1)


def main():
    """Main entrypoint for the script."""
    parser = argparse.ArgumentParser(description="Schema Compiler & Toolkit")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- 'validate' command ---
    validate_parser = subparsers.add_parser(
        "validate",
        help="Validates a source schema against the validator specified in"
        " its 'validatedBy' block.",
    )
    validate_parser.add_argument(
        "file",
        nargs="?",
        default=CANONICAL_SOURCE_FILE,
        help=f"Path to the source schema file to validate. "
        f"Defaults to '{CANONICAL_SOURCE_FILE}'.",
    )
    validate_parser.set_defaults(func=run_validation)

    # --- 'release-dependency' command ---
    release_dep_parser = subparsers.add_parser(
        "release-dependency",
        help="Releases a meta-schema or shared library schema to the dependencies directory.",
    )
    release_dep_parser.add_argument(
        "--source",
        required=True,
        help="Path to the source schema file (e.g., sources/cic-meta-schema.yaml).",
    )
    release_dep_parser.add_argument(
        "--version",
        required=True,
        help="The target release version (e.g., v1.0.0). Must not be a '.dev' version.",
    )
    release_dep_parser.set_defaults(func=run_release_dependency)

    # --- 'release-schema' command ---
    release_schema_parser = subparsers.add_parser(
        "release-schema",
        help="Releases an application-specific schema to the release directory.",
    )
    release_schema_parser.add_argument(
        "--source",
        required=True,
        help="Path to the source schema file (e.g., sources/postgres.yaml).",
    )
    release_schema_parser.add_argument(
        "--version",
        required=True,
        help="The target release version (e.g., v1.2.0). Must not be a '.dev' version.",
    )
    release_schema_parser.set_defaults(func=run_release_schema)

    # --- 'get-name' command ---
    get_name_parser = subparsers.add_parser(
        "get-name", help="Prints the schema name from the canonical source file."
    )
    get_name_parser.set_defaults(func=run_get_name)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
