import datetime
import logging
import sys
from pathlib import Path

import requests
import yaml

from .releaselib.exceptions import (
    ConfigurationError,
    GitStateError,
    ReleaseError,
    VaultServiceError,
)
from .schemalib.artifact import (
    build_signing_payload,
    compute_spec_checksum,
    generate_signed_artifact,
    parse_certificate_info,
)
from .schemalib.loader import load_and_resolve_schema, load_yaml, write_yaml
from .schemalib.validator import ValidationFailureError, get_validator_schema, run_validation

# Back-compat aliases for tests and external consumers
_parse_certificate_info = parse_certificate_info

__all__ = [
    "ReleaseManager",
    "ValidationFailureError",
    "load_and_resolve_schema",
    "load_yaml",
    "write_yaml",
    "_parse_certificate_info",
    "parse_certificate_info",
    "ConfigurationError",
    "GitStateError",
    "ReleaseError",
    "VaultServiceError",
]


class ReleaseManager:
    def __init__(
        self,
        config,
        git_service,
        vault_service,
        project_root: Path = Path("."),
        dry_run=False,
        logger=None,
    ):
        self.config = config
        self.git_service = git_service
        self.vault_service = vault_service
        self.project_root = project_root.resolve()
        self.dry_run = dry_run
        self.logger = logger if logger else logging.getLogger(__name__)

    def _path(self, relative_path):
        return self.project_root / relative_path

    def _check_base_branch_and_version(self, release_version: str):
        """Checks git branch and version validity."""
        component_name = self.config.get("component_name", "main")
        original_base_branch = self.git_service.get_current_branch()
        self.logger.info(
            f"✓ Starting release process for component '{component_name}' from branch '{original_base_branch}'."
        )
        return component_name, original_base_branch

    def _check_api_accessibility(self, api_url: str):
        """Checks if a given API URL is accessible."""
        self.logger.info(f"Checking API accessibility for: {api_url}")
        try:
            response = requests.get(api_url, timeout=5)
            response.raise_for_status()
            self.logger.info(
                f"✓ API '{api_url}' is accessible. Status: {response.status_code}"
            )
        except requests.exceptions.RequestException as e:
            self.logger.warning(f"API '{api_url}' is NOT accessible: {e}")
        sys.exit(0)

    def _validate_final_project_yaml(self):
        """Validates the project.yaml against the project.schema.yaml."""
        self.logger.info("Validating final project.yaml against schema...")
        try:
            schema_path = self._path(
                self.config.get("meta_schema_file", "project.schema.yaml")
            )
            schema = load_and_resolve_schema(schema_path)
            if schema is None:
                raise ConfigurationError(
                    f"Project schema file '{schema_path}' is empty."
                )
            project_yaml_path = self._path("project.yaml")
            instance = load_yaml(project_yaml_path)
            if instance is None:
                raise ConfigurationError(
                    f"Project YAML file '{project_yaml_path}' is empty."
                )
            run_validation(instance, schema)
            self.logger.info("✓ project.yaml is valid against the schema.")
        except (ValidationFailureError, ConfigurationError) as e:
            raise ValidationFailureError(f"Final project.yaml validation failed: {e}")
        except Exception as e:
            raise ReleaseError(
                f"An unexpected error occurred during project.yaml validation: {e}"
            )

    def _execute_developer_preparation_phase(
        self, release_version: str, component_name: str, original_base_branch: str
    ):
        """Handles the developer preparation phase: creates release branch, updates project.yaml, commits."""
        if self.git_service.is_dirty():
            raise GitStateError(
                "Uncommitted changes detected. Please commit or stash them before starting a release."
            )
        self.git_service.assert_clean_index()

        project_yaml_path = self._path("project.yaml")
        release_branch_name = (
            f"{component_name}/releases/v{release_version}"
            if component_name != "main"
            else f"releases/v{release_version}"
        )

        try:
            self.logger.info(
                f"Creating release branch: '{release_branch_name}' from '{original_base_branch}'"
            )
            if not self.dry_run:
                self.git_service.checkout(release_branch_name, create_new=True)
            self.logger.info(f"✓ Switched to release branch: '{release_branch_name}'")

            self.logger.info("Processing and validating source schema...")
            source_file = self._path(
                self.config.get("canonical_source_file", "sources/index.yaml")
            )
            source_data = load_and_resolve_schema(source_file)
            self.logger.info("✓ Source schema loaded and resolved.")

            checksum = compute_spec_checksum(source_data["spec"])
            self.logger.info(f"✓ Calculated spec checksum: {checksum[:12]}...")

            user_certificate = self.vault_service.get_certificate(
                self.config["vault_cert_mount"],
                self.config["vault_cert_secret_name"],
                self.config["vault_cert_secret_key"],
            )
            cic_root_ca_cert = self.vault_service.get_certificate(
                self.config["vault_cert_mount"],
                self.config.get("cic_root_ca_secret_name", "CICRootCA"),
                self.config["vault_cert_secret_key"],
            )
            self.logger.info("✓ User and CIC Root CA certificates obtained from Vault.")

            build_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
            schema_name = source_data.get("metadata", {}).get("name", "unknown")

            digest_b64 = build_signing_payload(
                name=schema_name,
                version=release_version,
                checksum=checksum,
                build_timestamp=build_timestamp,
            )
            signature = self.vault_service.sign(digest_b64, self.config["vault_key_name"])
            self.logger.info("✓ Project metadata signed successfully.")

            project_data = load_yaml(project_yaml_path) or {}
            metadata = {
                **project_data.get("metadata", {}),
                "version": release_version,
                "checksum": checksum,
                "sign": signature,
                "build_timestamp": build_timestamp,
                "createdBy": {
                    "name": None,
                    "email": None,
                    "certificate": user_certificate,
                    "issuer_certificate": cic_root_ca_cert,
                },
                "buildHash": "",
                "cicSign": "",
                "cicSignedCA": {"certificate": ""},
            }
            cert_name, cert_email = parse_certificate_info(user_certificate)
            metadata["createdBy"]["name"] = cert_name
            metadata["createdBy"]["email"] = cert_email
            self.logger.info(f"✓ Parsed user certificate: {cert_name} <{cert_email}>")

            project_data["metadata"] = metadata

            if self.dry_run:
                self.logger.info(
                    "[DRY-RUN] The following data would be written to project.yaml:"
                )
                self.logger.info(yaml.dump(project_data, sort_keys=False, indent=2))
            else:
                self.logger.info("Writing developer-stage metadata to project.yaml...")
                write_yaml(project_yaml_path, project_data)
                self.logger.info("✓ project.yaml updated for developer release step.")
                self.git_service.add(str(project_yaml_path))
                commit_message = (
                    f"release: Prepare {component_name} v{release_version} for build"
                )
                self.logger.info(f"Committing changes with message: '{commit_message}'")
                self.git_service.run(["git", "commit", "-m", commit_message])
                self.logger.info("✓ Developer release commit created successfully.")

            self.logger.info(
                f"✓ Release branch '{release_branch_name}' created. Proceed with build and finalization."
            )
            self.logger.info(
                f"ACTION REQUIRED: You are now on branch '{release_branch_name}'."
            )
            self.logger.info(
                "  1. Run your build process to generate artifacts and update 'buildHash' in project.yaml."
            )
            self.logger.info("  2. Commit the updated project.yaml to this branch.")
            self.logger.info("  3. Run 'make release VERSION=...' again to finalize.")

        except Exception as e:
            self.logger.critical(
                f"Release process failed during developer preparation: {e}",
                exc_info=True,
            )
            if not self.dry_run:
                try:
                    self.logger.warning(
                        f"Attempting to clean up release branch '{release_branch_name}'."
                    )
                    self.git_service.checkout(original_base_branch)
                    self.git_service.delete_branch(release_branch_name, force=True)
                    self.logger.info("✓ Release branch cleaned up.")
                except Exception as cleanup_e:
                    self.logger.critical(
                        f"Failed to clean up release branch: {cleanup_e}", exc_info=True
                    )
            raise ReleaseError(f"Release process failed: {e}") from e

    def _execute_finalization_phase(
        self,
        release_version: str,
        component_name: str,
        original_base_branch: str,
        release_branch_name: str,
    ):
        """Handles the finalization phase: validates project.yaml, commits, tags, merges, and cleans up."""
        project_yaml_path = self._path("project.yaml")
        main_branch = self.config.get("main_branch", "main")

        self.logger.info(
            f"--- Starting Finalization for v{release_version} on branch '{release_branch_name}' ---"
        )
        self._validate_final_project_yaml()
        self.logger.info(
            "✓ project.yaml is fully validated and ready for finalization."
        )

        if not self.dry_run:
            if self.git_service.is_dirty():
                self.logger.info(
                    "Committing pending changes to project.yaml (from build process)..."
                )
                self.git_service.add(str(project_yaml_path))
                self.git_service.run(
                    [
                        "git",
                        "commit",
                        "-m",
                        f"release: Finalize {component_name} v{release_version} build artifacts",
                    ]
                )
            else:
                self.logger.info(
                    "No pending changes to project.yaml detected. Assuming manual commit of build artifacts."
                )

            final_tag_name = f"{component_name}@v{release_version}"
            final_tag_message = f"Release {component_name} v{release_version}"
            self.logger.info(f"Creating final annotated tag: '{final_tag_name}'")
            self.git_service.run(
                ["git", "tag", "-a", final_tag_name, "-m", final_tag_message]
            )
            self.logger.info("✓ Final release tag created.")

            self.logger.info(f"Switching back to main branch: '{main_branch}'")
            self.git_service.checkout(main_branch)
            self.logger.info(f"Merging '{release_branch_name}' into '{main_branch}'")
            self.git_service.merge(
                release_branch_name,
                no_ff=True,
                message=f"Merge branch '{release_branch_name}' for release {release_version}",
            )
            self.logger.info(f"Deleting release branch: '{release_branch_name}'")
            self.git_service.delete_branch(release_branch_name)

        self.logger.info(
            f"✓ Release v{release_version} successfully finalized and merged into '{main_branch}'."
        )

    def _execute_schema_release(self, release_version: str, tier: str):
        """
        Schema-only release: validate source, generate signed artifact, write to output dir.
        tier: "dependency" -> dependencies/ directory
        tier: "application" -> release/ directory
        """
        output_dir = self._path(
            self.config.get("dependencies_dir", "dependencies")
            if tier == "dependency"
            else self.config.get("release_dir", "release")
        )
        dependencies_dir = self._path(self.config.get("dependencies_dir", "dependencies"))
        source_file = self._path(
            self.config.get("canonical_source_file", "sources/index.yaml")
        )

        self.logger.info(f"Loading source schema from {source_file}...")
        source_data = load_and_resolve_schema(source_file)

        validated_by = source_data.get("metadata", {}).get("validatedBy", {})
        validator_name = validated_by.get("name")
        validator_version = validated_by.get("version")

        if not validator_name or not validator_version:
            raise ConfigurationError(
                "Source schema is missing 'metadata.validatedBy.name' or 'version'."
            )

        self.logger.info(f"Fetching and verifying validator '{validator_name}@{validator_version}'...")
        validator_schema = get_validator_schema(
            validator_name, validator_version, source_data, dependencies_dir
        )
        run_validation(source_data, validator_schema)
        self.logger.info("✓ Source schema is valid.")

        checksum = compute_spec_checksum(source_data["spec"])
        build_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        schema_name = source_data.get("metadata", {}).get("name", "unknown")

        user_certificate = self.vault_service.get_certificate(
            self.config["vault_cert_mount"],
            self.config["vault_cert_secret_name"],
            self.config["vault_cert_secret_key"],
        )
        cic_root_ca_cert = self.vault_service.get_certificate(
            self.config["vault_cert_mount"],
            self.config.get("cic_root_ca_secret_name", "CICRootCA"),
            self.config["vault_cert_secret_key"],
        )

        digest_b64 = build_signing_payload(
            name=schema_name,
            version=release_version,
            checksum=checksum,
            build_timestamp=build_timestamp,
        )
        signature = self.vault_service.sign(digest_b64, self.config["vault_key_name"])
        self.logger.info("✓ Artifact signed successfully.")

        validator_checksum = compute_spec_checksum(validator_schema["spec"])
        artifact = generate_signed_artifact(
            spec=source_data["spec"],
            name=schema_name,
            version=release_version,
            checksum=checksum,
            build_timestamp=build_timestamp,
            developer_cert=user_certificate,
            issuer_cert=cic_root_ca_cert,
            signature=signature,
            validator_name=validator_name,
            validator_version=validator_version,
            validator_checksum=validator_checksum,
        )

        output_filename = f"{schema_name}-{release_version}.yaml"
        output_path = output_dir / output_filename

        if self.dry_run:
            self.logger.info(f"[DRY-RUN] Would write artifact to: {output_path}")
            self.logger.info(yaml.dump(artifact, sort_keys=False, indent=2))
        else:
            write_yaml(output_path, artifact)
            self.logger.info(f"✓ Artifact written to {output_path}")

    def run_release_close(self, release_version: str):
        """Orchestrates the release process based on the current Git branch and dry_run status."""
        component_name, original_base_branch = self._check_base_branch_and_version(
            release_version
        )
        if not self.vault_service:
            raise VaultServiceError("VaultService is not initialized.")

        main_branch = self.config.get("main_branch", "main")
        release_branch_name = (
            f"{component_name}/releases/v{release_version}"
            if component_name != "main"
            else f"releases/v{release_version}"
        )

        if self.dry_run:
            self.logger.info("[DRY-RUN] Simulating Developer Preparation Phase.")
            self._execute_developer_preparation_phase(
                release_version, component_name, original_base_branch
            )
        elif original_base_branch == main_branch:
            self._execute_developer_preparation_phase(
                release_version, component_name, original_base_branch
            )
        elif original_base_branch == release_branch_name:
            self._execute_finalization_phase(
                release_version,
                component_name,
                original_base_branch,
                release_branch_name,
            )
        else:
            raise GitStateError(
                f"Release command must be run from the main branch ('{main_branch}') "
                f"to start a new release, or from an existing release branch ('{release_branch_name}') "
                f"to finalize it. Currently on '{original_base_branch}'."
            )

    def _get_repo_type(self) -> str:
        return self.config.get("repo_type", "module")

    def _require_repo_type(self, command: str, required: str):
        """Raises ReleaseError if repo_type doesn't match the required type."""
        repo_type = self._get_repo_type()
        if repo_type != required:
            raise ReleaseError(
                f"Command '{command}' is only available for repo_type='{required}'. "
                f"This repo is configured as repo_type='{repo_type}'."
            )

    def run_release_dependency(self, release_version: str):
        """Releases a validator/meta schema into the dependencies/ directory."""
        self._require_repo_type("release-dependency", "schema")
        self.logger.info("--- Releasing Dependency Schema ---")
        self._execute_schema_release(release_version, tier="dependency")

    def run_release_schema(self, release_version: str):
        """Releases an application schema into the release/ directory."""
        self._require_repo_type("release-schema", "schema")
        self.logger.info("--- Releasing Application Schema ---")
        self._execute_schema_release(release_version, tier="application")

    def run_validation(self):
        """Runs offline validation on the canonical source schema (schema repos only)."""
        self._require_repo_type("validate", "schema")
        self.logger.info("--- Running Schema Validation ---")
        source_file = self._path(
            self.config.get("canonical_source_file", "sources/index.yaml")
        )
        dependencies_dir = self._path(self.config.get("dependencies_dir", "dependencies"))
        self.logger.info(f"Loading and resolving {source_file}...")

        try:
            source_data = load_and_resolve_schema(source_file)

            validated_by = source_data.get("metadata", {}).get("validatedBy", {})
            validator_name = validated_by.get("name")
            validator_version = validated_by.get("version")

            if not validator_name or not validator_version:
                raise ConfigurationError(
                    "Source schema is missing 'metadata.validatedBy.name' or 'version'."
                )

            validator_schema = get_validator_schema(
                validator_name, validator_version, source_data, dependencies_dir
            )
            run_validation(source_data, validator_schema)
            self.logger.info("✓ Validation successful.")

        except (ValidationFailureError, ConfigurationError, ValueError) as e:
            self.logger.critical(f"VALIDATION FAILED: {e}")
            raise ReleaseError("Schema validation failed.") from e
        except Exception as e:
            self.logger.critical(f"UNEXPECTED ERROR during validation: {e}")
            raise ReleaseError("An unexpected error occurred during validation.") from e