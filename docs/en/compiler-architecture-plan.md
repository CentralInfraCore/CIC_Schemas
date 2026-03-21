# Unified Compiler Architecture Plan

**Status:** Design Document — Implementation Target  
**Version:** 1.0  
**Branch:** schemas/devel  
**Language note:** The authoritative version of this document is the Hungarian edition (`docs/hu/compiler-architektura-terv.md`). This English version is a translation provided for international contributors.

---

## 1. Architecture Overview

### Purpose

The goal of this plan is to unify the ad-hoc, monolithic compiler found in `CIC-Schemas` (v18) with the clean, modular infrastructure of `base-repo`. The result is a single `compiler.py` + library stack that can drive all repository types (schema repos, workflow repos, module/relay repos) while preserving the best properties of both codebases.

### Design Principles

1. **`compiler.py` is a thin CLI dispatcher only.** All logic lives in libraries.
2. **`project.yaml` is the universal project manifest** for every repo type. It never contains a `spec` block.
3. **`spec` content lives in dedicated source files** (e.g., `sources/index.yaml` for schema repos).
4. **Configuration is data-driven** via `compiler_settings` in `project.yaml`. No hardcoded paths or constants.
5. **Dual-signature model**: developer signature (`sign`) + CIC central authority signature (`cicSign`/`cicSignedCA`). The central signing is currently performed by the temporary `finalize_release.py` script; the relay will replace this.
6. **Validator integrity is verified before use** (v18 pattern): a validator schema's checksum is verified against a known-good value before it is used to validate anything.
7. **Two-tier schema release**: `release-dependency` produces validator schemas into `dependencies/`; `release-schema` produces application schemas into `release/`.

### Module Map

```
tools/
├── compiler.py                   ← thin CLI (keep base-repo pattern)
├── infra.py                      ← ReleaseManager (refactored, delegates to schemalib)
├── finalize_release.py           ← TEMPORARY: to be deleted when relay is ready
│
├── releaselib/                   ← keep as-is (base-repo)
│   ├── __init__.py
│   ├── exceptions.py             ← ReleaseError hierarchy
│   ├── vault_service.py          ← VaultService (sign, get_certificate)
│   └── git_service.py            ← GitService (branch, commit, tag, merge)
│
└── schemalib/                    ← NEW: extracted/ported from v18 compiler.py
    ├── __init__.py
    ├── loader.py                 ← load_and_resolve_schema, convert_to_json_serializable
    ├── validator.py              ← get_validator_schema, integrity_check, run_jsonschema
    └── artifact.py              ← generate_signed_artifact, checksum, cert parsing
```

### Responsibility Matrix

| Module | Responsibility |
|---|---|
| `compiler.py` | CLI argument parsing, env-var reading, service instantiation, command dispatch |
| `infra.py::ReleaseManager` | Git workflow orchestration, project.yaml lifecycle, phase detection |
| `releaselib/vault_service.py` | Vault Transit signing, Vault KV certificate retrieval |
| `releaselib/git_service.py` | All subprocess git calls, branch/tag/merge operations |
| `releaselib/exceptions.py` | Exception hierarchy |
| `schemalib/loader.py` | YAML loading, `$ref` resolution, JSON-round-trip normalisation |
| `schemalib/validator.py` | Validator schema retrieval, integrity verification, jsonschema execution |
| `schemalib/artifact.py` | Signed artifact construction, checksum calculation, certificate parsing |

---

## 2. `schemalib/` Detailed Design

### 2.1 `schemalib/loader.py`

This module supersedes the current `load_and_resolve_schema()` in `infra.py`. The v18 version is strictly better because it performs a JSON round-trip that strips `JsonRef` proxy objects and normalises `datetime` instances before any further processing.

**Functions:**

```python
def convert_to_json_serializable(obj: Any) -> Any:
    """
    Recursively converts a Python object graph to one that is fully
    JSON-serialisable. Handles JsonRef proxies (by forcing resolution),
    datetime objects (to ISO-8601 strings), and other edge cases.
    Called internally after JsonRef.replace_refs().
    """

def load_and_resolve_schema(path: Path) -> dict:
    """
    Loads a YAML file, resolves all $ref references (including cross-file
    references using the file's directory as base URI), then performs a
    JSON round-trip via convert_to_json_serializable() to guarantee that
    the returned object contains only plain Python types.

    Raises:
        ConfigurationError: if the file is missing or YAML is malformed.
    Returns:
        dict: Fully resolved, JSON-serialisable document.
    """

def load_yaml(path: Path) -> Optional[dict]:
    """
    Loads a YAML file without $ref resolution. Returns None for empty files.
    Raises ConfigurationError on missing file or parse error.
    """

def write_yaml(path: Path, data: dict) -> None:
    """
    Atomically writes data to a YAML file using a temp-file + os.replace()
    pattern. Raises ReleaseError on I/O failure.
    """
```

**Data flow:**

```
YAML file on disk
    → yaml.safe_load()
    → JsonRef.replace_refs(base_uri=file_dir/)   # resolves $ref
    → convert_to_json_serializable()              # strips proxy types
    → json.loads(json.dumps(...))                 # round-trip normalisation
    → plain dict (ready for hashing / validation)
```

### 2.2 `schemalib/validator.py`

This module provides schema validation logic, ported from v18 where validation was embedded in the monolithic compiler.

**Functions:**

```python
def get_validator_schema(
    validator_name: str,
    validator_version: str,
    dependencies_dir: Path,
) -> dict:
    """
    Loads a validator schema from the dependencies/ directory.
    The expected file name pattern is: <name>-<version>.yaml

    Before returning, calls verify_validator_integrity() to ensure
    the schema has not been tampered with.

    Raises:
        ConfigurationError: if the file is not found.
        ValidationFailureError: if the integrity check fails.
    """

def verify_validator_integrity(
    schema: dict,
    expected_checksum: str,
) -> None:
    """
    Verifies the SHA-256 checksum of the validator schema's spec block
    against the expected value stored in the schema's own metadata.

    This is a security control: a tampered validator could silently
    accept invalid schemas.

    Raises:
        ValidationFailureError: if checksum does not match.
    """

def run_validation(
    instance: dict,
    validator_schema: dict,
) -> None:
    """
    Runs jsonschema.validate() against instance using validator_schema['spec'].
    Wraps JsonSchemaValidationError into ValidationFailureError for a
    consistent exception surface.

    Raises:
        ValidationFailureError: on validation failure.
    """
```

**Integrity check algorithm:**

1. Extract `spec` block from the validator schema dict.
2. Serialise to canonical JSON (`json.dumps(sort_keys=True, separators=(',', ':'))`).
3. SHA-256 hash the UTF-8 bytes.
4. Compare hex digest against `schema['metadata']['checksum']`.
5. Raise `ValidationFailureError` on mismatch.

### 2.3 `schemalib/artifact.py`

This module handles the construction of signed release artifacts. It encapsulates the logic currently spread between `infra.py::_execute_developer_preparation_phase()` and `finalize_release.py`.

**Functions:**

```python
def parse_certificate_info(pem_cert_data: str) -> tuple[str, str]:
    """
    Parses a PEM certificate using pyOpenSSL.
    Extracts Common Name and email address (from SubjectAltName or emailAddress).
    Returns: (name: str, email: str)
    Falls back to ("Unknown", "unknown@example.com") on parse error.
    """

def compute_spec_checksum(spec: dict) -> str:
    """
    Computes a canonical SHA-256 hex digest of the spec block.
    Uses json.dumps(sort_keys=True, separators=(',', ':')) for determinism.
    Returns: hex string (64 chars)
    """

def build_signing_payload(
    name: str,
    version: str,
    checksum: str,
    build_timestamp: str,
) -> str:
    """
    Constructs the base64-encoded SHA-256 digest of the canonical signing
    metadata dict. This is the input to VaultService.sign().
    Returns: base64 string (suitable for Vault's prehashed=True endpoint)
    """

def generate_signed_artifact(
    spec: dict,
    metadata_base: dict,
    release_version: str,
    build_timestamp: str,
    developer_cert: str,
    issuer_cert: str,
    signature: str,
) -> dict:
    """
    Assembles the complete release artifact dict. This is the structure
    written to release/<name>-<version>.yaml (for schema repos).

    The returned dict contains:
        metadata:
            name, version, checksum, sign, build_timestamp,
            createdBy: {name, email, certificate, issuer_certificate},
            buildHash: ""      # placeholder, filled by build step
            cicSign: ""        # placeholder, filled by relay/finalize_release.py
            cicSignedCA:
                certificate: ""  # placeholder
        spec: <the resolved spec dict>
    """
```

---

## 3. `compiler.py` Commands

### Command Set

| Command | Description | Applicable Repo Types |
|---|---|---|
| `validate` | Offline validation: load source files, resolve $refs, run jsonschema against validator schema (with integrity check) | Schema repos |
| `release` | Full Git-workflow release: prepare branch, sign, commit, finalize, tag, merge | All repo types |
| `release-dependency` | Schema-only: release a validator schema into `dependencies/` | Schema repos |
| `release-schema` | Schema-only: release an application schema into `release/` | Schema repos |
| `get-name` | Print the `metadata.name` value from the project manifest | All repo types |

### Argument Structure

```
compiler.py [--dry-run] [--verbose] [--debug]
            [--git-timeout N] [--vault-timeout N]
            <command> [command-args]

validate
    (no extra args)

release
    --version X.Y.Z         required

release-dependency
    --version X.Y.Z         required

release-schema
    --version X.Y.Z         required

get-name
    (no extra args)
```

### Command Dispatch Logic in `compiler.py::main()`

```python
manager = ReleaseManager(compiler_config, git_service, vault_service, ...)

match args.command:
    case "validate":
        manager.run_validation()

    case "release":
        manager.run_release_close(args.version)

    case "release-dependency":
        manager.run_release_dependency(args.version)

    case "release-schema":
        manager.run_release_schema(args.version)

    case "get-name":
        print(full_config["metadata"]["name"])
```

---

## 4. `project.yaml` Structure — Definitive Reference

### 4.1 Field Inventory

```yaml
# ── MANUAL fields (set by humans at project init, never touched by compiler) ──
metadata:
  name: string                # human-readable project name
  description: string         # one-sentence summary
  version: string | null      # SemVer; set to null before first release
  license: string             # SPDX identifier (e.g., CC-BY-NC-SA-4.0)
  main_branch: string         # Git main branch name (e.g., "main", "schemas")
  owner: string               # responsible team or individual
  tags: [string]              # optional classification tags
  maintenance:                # optional
    status: active | maintenance-only | deprecated | end-of-life
    supported_until: YYYY-MM-DD
  contacts:                   # optional
    - type: email | slack | msteams
      value: string
  links:                      # optional
    - name: string
      url: string

# ── AUTO-GENERATED fields (written by compiler during release) ──
  validatedBy:                # which validator schema was used
    name: string
    version: string
    checksum: string          # checksum of the validator schema's spec

  createdBy:                  # from Vault certificate
    name: string              # CN from certificate
    email: string             # email from SubjectAltName
    certificate: string       # full PEM (developer cert)
    issuer_certificate: string  # full PEM (CIC Root CA)

  build_timestamp: string     # ISO-8601 UTC, set at release time

  validity:                   # optional, set by compiler from compiler_settings
    from: string              # ISO-8601
    until: string             # ISO-8601

  checksum: string            # SHA-256 hex of canonical spec JSON
  sign: string                # vault:v1:... developer signature

  buildHash: string           # filled by build step (artifact checksum or git tree hash)
                              # empty string ("") until build step runs

  cicSign: string             # vault:v1:... CIC central authority signature
                              # empty string ("") until finalize_release.py / relay runs

  cicSignedCA:
    certificate: string       # PEM of the CIC signing CA cert
                              # empty string ("") until finalize_release.py / relay runs

# ── CONFIGURATION block (manual, never modified by compiler) ──
compiler_settings:
  component_name: string           # used in branch/tag naming
  main_branch: string              # target branch for merge-back
  canonical_source_file: string    # e.g., "sources/index.yaml"
  meta_schema_file: string         # e.g., "project.schema.yaml"
  meta_schemas_dir: string         # directory containing meta schemas
  source_dir: string               # root of schema source files
  dependencies_dir: string         # e.g., "dependencies/"
  release_dir: string              # e.g., "release/"
  vault_key_name: string           # developer signing key in Vault Transit
  cic_root_ca_key_name: string     # CIC CA key (for finalize_release)
  vault_cert_mount: string         # Vault KV mount for certificates
  vault_cert_secret_name: string   # secret name for developer cert
  vault_cert_secret_key: string    # key within the KV secret
  cic_root_ca_secret_name: string  # secret name for CIC Root CA cert
  validity_days: integer           # optional: how many days a release is valid
```

### 4.2 What `project.yaml` Never Contains

- A `spec` block. The `spec` lives in dedicated source files (`sources/index.yaml`, workflow YAMLs, etc.).
- Hardcoded file paths that differ per environment. All paths go in `compiler_settings`.

### 4.3 Lifecycle of Auto-Generated Fields

| Phase | Fields Written |
|---|---|
| Developer preparation (branch creation) | `version`, `checksum`, `sign`, `build_timestamp`, `createdBy`, `validatedBy`, `validity`, `buildHash: ""`, `cicSign: ""`, `cicSignedCA.certificate: ""` |
| Build step (CI pipeline) | `buildHash` (actual artifact checksum or tree hash) |
| CIC finalisation (finalize_release.py / relay) | `cicSign`, `cicSignedCA.certificate` |

---

## 5. Release Flow by Repository Type

### 5.1 Schema Repository (e.g., `CIC-Schemas`)

```
Developer on main branch
    │
    ├─ make validate
    │     └─ ReleaseManager.run_validation()
    │           ├─ schemalib/loader.load_and_resolve_schema(sources/index.yaml)
    │           ├─ schemalib/validator.get_validator_schema(dependencies/)  [+ integrity check]
    │           └─ schemalib/validator.run_validation(instance, validator_schema)
    │
    └─ make release-schema VERSION=1.0.0
          └─ ReleaseManager.run_release_schema("1.0.0")
                │
                ├─ PHASE 1: Developer Preparation (runs from main)
                │     ├─ git checkout -b schemas/releases/v1.0.0
                │     ├─ loader.load_and_resolve_schema(sources/index.yaml)
                │     ├─ validator.run_validation(spec, validator)
                │     ├─ artifact.compute_spec_checksum(spec)
                │     ├─ vault_service.get_certificate(developer_cert)
                │     ├─ vault_service.get_certificate(cic_root_ca_cert)
                │     ├─ artifact.parse_certificate_info(developer_cert) → name, email
                │     ├─ artifact.build_signing_payload(...) → digest_b64
                │     ├─ vault_service.sign(digest_b64, vault_key_name)
                │     ├─ artifact.generate_signed_artifact(...) → release_doc
                │     ├─ write release/<name>-<version>.yaml
                │     ├─ git add + git commit "release: Prepare ..."
                │     └─ [ACTION REQUIRED] message to developer
                │
                ├─ [Manual or CI: build step — sets buildHash in project.yaml]
                │
                └─ PHASE 2: Finalisation (runs from release branch)
                      ├─ validate project.yaml against project.schema.yaml
                      ├─ git commit (if dirty)
                      ├─ git tag schemas@v1.0.0
                      ├─ git checkout main
                      ├─ git merge --no-ff schemas/releases/v1.0.0
                      └─ git branch -d schemas/releases/v1.0.0

    [Separately, after merge:]
    └─ python -m tools.finalize_release project.yaml \
          --cic-vault-key cic-root-ca-key \
          --cic-cert-vault-path kv/data/secrets/CICRootCA:cert
          # Writes: cicSign, cicSignedCA.certificate into project.yaml
          # TEMPORARY: relay will do this automatically in the future
```

**Two-tier release distinction:**

- `release-dependency`: the output goes to `dependencies/<name>-<version>.yaml`. These are validator schemas used by other schema repos.
- `release-schema`: the output goes to `release/<name>-<version>.yaml`. These are application schemas for consumption by services.

The flow is identical; only the output directory and the `validatedBy` field differ.

### 5.2 Workflow Repository

```
Developer on main branch
    │
    └─ make release VERSION=1.0.0
          └─ ReleaseManager.run_release_close("1.0.0")
                │
                ├─ PHASE 1: Developer Preparation
                │     ├─ git checkout -b <component>/releases/v1.0.0
                │     ├─ [No spec loading — workflow files are source-of-truth]
                │     ├─ compute checksum of workflow source files
                │     ├─ vault_service.get_certificate + sign
                │     ├─ write project.yaml (metadata only, no spec)
                │     └─ git add + git commit
                │
                └─ PHASE 2: Finalisation (identical to schema repo)
```

Note: workflow repos do not use `schemalib/validator.py`. The `run_validation()` method returns early for workflow repos (controlled by `compiler_settings.repo_type: workflow`).

### 5.3 Module / Relay Repository

```
Developer on main branch
    │
    └─ make release VERSION=1.0.0
          └─ ReleaseManager.run_release_close("1.0.0")
                │
                ├─ PHASE 1: Developer Preparation
                │     ├─ git checkout -b <component>/releases/v1.0.0
                │     ├─ compute git tree hash (git write-tree)
                │     ├─ vault_service.sign(tree_hash_digest)
                │     ├─ write project.yaml:
                │     │     metadata.checksum = tree_hash
                │     │     metadata.sign = vault_signature
                │     │     metadata.buildHash = ""   # filled after Go build
                │     └─ git add + git commit
                │
                ├─ [CI: go build → produces binary; writes binary checksum to buildHash]
                │
                └─ PHASE 2: Finalisation (git tag + merge)
```

Module repos do not produce YAML artifacts. `buildHash` is the SHA-256 of the compiled Go binary (or a manifest of multiple binaries).

---

## 6. `infra.py` Refactor Plan

### 6.1 What Stays in `infra.py`

- `ReleaseManager` class and its `__init__` signature (unchanged).
- Git workflow methods: `_check_base_branch_and_version`, `_execute_developer_preparation_phase`, `_execute_finalization_phase`, `run_release_close`.
- `_validate_final_project_yaml()` (validates project.yaml against project.schema.yaml).
- `write_yaml()` utility (or move to `schemalib/loader.py` — either is acceptable).

### 6.2 What Moves to `schemalib/`

| Current location in `infra.py` | Moves to |
|---|---|
| `load_and_resolve_schema()` (simple version) | `schemalib/loader.py` (replace with v18 robust version) |
| `load_yaml()` | `schemalib/loader.py` |
| `_parse_certificate_info()` | `schemalib/artifact.py` |
| `to_canonical_json()` | `schemalib/artifact.py` |
| `get_sha256_hex()` | `schemalib/artifact.py` |
| inline signing payload construction (in `_execute_developer_preparation_phase`) | `schemalib/artifact.build_signing_payload()` |
| inline artifact assembly (in `_execute_developer_preparation_phase`) | `schemalib/artifact.generate_signed_artifact()` |
| `run_validation()` stub | `schemalib/validator.py` (full implementation) |

### 6.3 New Methods Added to `ReleaseManager`

```python
def run_release_dependency(self, release_version: str) -> None:
    """
    Schema-only release targeting dependencies/ directory.
    Delegates to _execute_schema_release(release_version, tier="dependency").
    """

def run_release_schema(self, release_version: str) -> None:
    """
    Schema-only release targeting release/ directory.
    Delegates to _execute_schema_release(release_version, tier="application").
    """

def _execute_schema_release(self, release_version: str, tier: str) -> None:
    """
    Internal: performs validate → artifact generation → write file.
    tier: "dependency" → dependencies/ directory
    tier: "application" → release/ directory
    """
```

### 6.4 New `run_validation()` Implementation

The current `run_validation()` in `infra.py` is a stub. The full implementation:

```python
def run_validation(self) -> None:
    source_file = self._path(self.config.get("canonical_source_file", "sources/index.yaml"))
    source_data = schemalib.loader.load_and_resolve_schema(source_file)
    spec = source_data["spec"]

    validated_by = source_data.get("metadata", {}).get("validatedBy", {})
    validator_name = validated_by.get("name")
    validator_version = validated_by.get("version")
    expected_checksum = validated_by.get("checksum")

    if validator_name and validator_version:
        dependencies_dir = self._path(self.config.get("dependencies_dir", "dependencies"))
        validator_schema = schemalib.validator.get_validator_schema(
            validator_name, validator_version, dependencies_dir
        )
        schemalib.validator.verify_validator_integrity(validator_schema, expected_checksum)
        schemalib.validator.run_validation(spec, validator_schema)
    else:
        self.logger.warning("No validatedBy configured — skipping jsonschema validation.")
```

### 6.5 `finalize_release.py` Migration Path

`finalize_release.py` is a **temporary script**. Its logical responsibilities are permanent:

1. Verify `checksum == buildHash` (build integrity gate).
2. Embed `cicSignedCA.certificate`.
3. Sign the final document with the CIC key (`cicSign`).
4. Write back to `project.yaml`.

**Migration plan:**
- When the relay is operational, steps 1–4 above become a relay API call triggered from `ReleaseManager._execute_finalization_phase()`.
- The call is gated by `compiler_settings.cic_relay_url` being set.
- If not set (local/dev mode), `finalize_release.py` can still be called manually.
- Once relay is stable and all repos migrated, `finalize_release.py` is deleted.
- The `cicSign`/`cicSignedCA` fields in `project.yaml` remain permanent fixtures.

---

## 7. Implementation Order

The implementation should proceed in the following sequence to minimise risk and allow incremental testing.

### Step 1 — Create `schemalib/` skeleton

Create `tools/schemalib/__init__.py` with empty exports. This unblocks import changes in subsequent steps.

**Files:** `tools/schemalib/__init__.py`

### Step 2 — Port `loader.py`

Move `load_yaml`, `write_yaml` from `infra.py` to `schemalib/loader.py`.  
Replace the simple `load_and_resolve_schema()` in `infra.py` with the v18 robust version (including `convert_to_json_serializable()` + JSON round-trip).  
Update `infra.py` imports to use `schemalib.loader`.  
Update all existing tests to import from the new location.

**Files:** `tools/schemalib/loader.py`, `tools/infra.py`

### Step 3 — Port `artifact.py`

Move `_parse_certificate_info`, `to_canonical_json`, `get_sha256_hex` from `infra.py` to `schemalib/artifact.py`.  
Add `compute_spec_checksum()`, `build_signing_payload()`, `generate_signed_artifact()`.  
Update `infra.py` to use `schemalib.artifact`.

**Files:** `tools/schemalib/artifact.py`, `tools/infra.py`

### Step 4 — Implement `validator.py`

Implement `get_validator_schema()`, `verify_validator_integrity()`, `run_validation()` in `schemalib/validator.py`.  
Replace the `run_validation()` stub in `ReleaseManager` with the full implementation that calls `schemalib.validator`.

**Files:** `tools/schemalib/validator.py`, `tools/infra.py`

### Step 5 — Add `release-dependency` and `release-schema` commands

Add `run_release_dependency()`, `run_release_schema()`, `_execute_schema_release()` to `ReleaseManager`.  
Add the two new subparsers to `compiler.py`.  
Add the dispatch cases in `main()`.

**Files:** `tools/compiler.py`, `tools/infra.py`

### Step 6 — Add `get-name` command

Add the `get-name` subparser and dispatch in `compiler.py::main()`. One line of logic.

**Files:** `tools/compiler.py`

### Step 7 — Add `repo_type` routing

Add `compiler_settings.repo_type: schema | workflow | module` to `project.yaml` (and `project.schema.yaml`).  
Gate `run_validation()` and schema artifact generation behind `repo_type == "schema"`.

**Files:** `tools/infra.py`, `project.schema.yaml`

### Step 8 — Write tests for `schemalib/`

Add test files under `tests/test_tools/` for each new module:
- `test_schemalib_loader.py`
- `test_schemalib_validator.py`
- `test_schemalib_artifact.py`

Target: minimum 85% coverage on `schemalib/`.

### Step 9 — Update documentation

Update `docs/en/workflow.md` and `docs/hu/workflow.md` to reflect the new command set.  
Update `docs/en/architecture.md` to reference `schemalib/`.

### Step 10 — Mark `finalize_release.py` for deletion

Add a prominent `# DEPRECATED: Use relay API when available.` comment block.  
Track relay readiness as a separate milestone; delete on relay GA.

---

## Appendix A: Exception Hierarchy (Unchanged)

```
ReleaseError (base)
├── GitStateError
├── GitServiceError
├── VersionMismatchError
├── ConfigurationError
├── VaultServiceError
├── ManualInterventionRequired
└── ValidationFailureError   ← defined in infra.py, should move to releaselib/exceptions.py
```

## Appendix B: Environment Variables

| Variable | Used by | Purpose |
|---|---|---|
| `VAULT_ADDR` | `compiler.py` | Vault server URL |
| `VAULT_TOKEN` | `compiler.py` | Vault authentication token |
| `VAULT_CACERT` | `compiler.py` | Path to Vault CA certificate file |
| `CIC_VAULT_TOKEN_FILE` | `compiler.py` | Path to file containing Vault token (default: `/var/run/secrets/vault-token`) |