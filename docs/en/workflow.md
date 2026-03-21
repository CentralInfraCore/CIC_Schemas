# Developer Workflow

This document outlines the typical workflows for interacting with the schema framework, from initial setup to creating a new release.

## First-Time Setup

Before you begin, ensure you have the following prerequisites installed on your host machine:
- `docker`
- `docker-compose`
- `make`
- `git`

Follow these steps to initialize the project after cloning the repository:

1.  **Start the Vault Signing Agent:**
    This project requires a running Vault instance for signing release artifacts. A helper script is provided to run a temporary, local Vault server for development.

    ```sh
    # This needs to be run from the project root in a separate terminal
    ./tools/vault-sign-agent.sh -k /path/to/your/key.pem -c /path/to/your/cert.crt --root-ca-file /path/to/your/CICRootCA.crt
    ```
    This agent will remain running in the background.

2.  **Install Python Dependencies:**
    This command compiles the `requirements.in` file and installs all necessary Python packages into a local `./p_venv` directory, which is used as a cache by the Docker container.

    ```sh
    make infra.deps
    ```

3.  **Build Docker Images:**
    Build the necessary Docker images for the `setup` and `builder` services.

    ```sh
    make build
    ```

4.  **Start the Development Container:**
    This starts the `builder` container in the background.

    ```sh
    make up
    ```

5.  **Initialize Git Hooks:**
    This script sets up the `commit-msg` Git hook, which automatically signs your commits using the running Vault agent.

    ```sh
    make repo.init
    ```

Your environment is now fully configured and ready for development.

## Compiler Commands Reference

The compiler (`python -m tools.compiler`) is the central tool. Available commands depend on the `repo_type` set in `compiler_settings` of `project.yaml`.

| Command | Repo type | Description |
|---|---|---|
| `validate` | `schema` | Offline validation of source schema against its declared validator (with integrity check). |
| `release --version X.Y.Z` | all | Full Git-workflow release: branch, sign, commit, tag, merge. |
| `release-dependency --version X.Y.Z` | `schema` | Release a validator/meta schema into `dependencies/`. |
| `release-schema --version X.Y.Z` | `schema` | Release an application schema into `release/`. |
| `get-name` | all | Print `metadata.name` from `project.yaml`. |

Global flags available on all commands:

```
--dry-run         Simulate all actions without writing or committing anything.
--verbose / -v    Show INFO-level log messages.
--debug   / -d    Show DEBUG-level log messages (most verbose).
--git-timeout N   Git subprocess timeout in seconds (default: 60).
--vault-timeout N Vault API timeout in seconds (default: 10).
```

## Day-to-Day Development

This is the typical cycle you will follow when modifying or creating schemas.

1.  **Modify a Schema:**
    Make your desired changes to a schema file located in the `sources/` directory.

2.  **Run Validation (schema repos only):**
    Before creating a release, validate your changes. The `validate` command loads the source schema, resolves all `$ref` references, verifies the validator's integrity, and runs jsonschema validation.

    ```sh
    make validate
    # or with verbose output:
    make validate VERBOSE=1
    ```

3.  **Run Tests:**
    To ensure the tooling itself is working correctly, run the `pytest` suite.

    ```sh
    make test
    ```

4.  **Commit Your Changes:**
    When you are ready, commit your changes. The `commit-msg` hook will automatically run, connect to your local Vault agent, and append a signing block to your commit message.

    ```sh
    git add .
    git commit -m "feat: Update schema with new properties"
    ```

## Creating a Release

### Schema Repositories

Schema repos support two release commands depending on what kind of artifact is produced:

- **`release-dependency`** — produces a signed validator schema into `dependencies/`. Used for meta-schemas consumed by other repos.
- **`release-schema`** — produces a signed application schema into `release/`. Used for schemas consumed by services.

```sh
# Release a validator schema (e.g., template-schema)
make release-dependency VERSION=v1.0.0

# Release an application schema (e.g., postgresql)
make release-schema VERSION=v1.0.0
```

The compiler will:
1. Load and validate the source schema from `sources/index.yaml`.
2. Verify the validator schema's integrity (checksum).
3. Compute a SHA-256 checksum of the `spec` block.
4. Fetch your signing certificate and the CIC Root CA from Vault.
5. Sign the artifact metadata with your Vault key.
6. Write the signed artifact to `dependencies/` or `release/`.

### All Repository Types (Git workflow release)

The `release` command runs the full Git workflow: creates a release branch, signs and commits `project.yaml`, then waits for the build step before finalizing.

**Phase 1 — Developer preparation (run from main branch):**

```sh
make release VERSION=1.0.0
```

This creates a release branch (e.g., `base/releases/v1.0.0`), signs the project metadata, and commits `project.yaml`. You are then prompted to run the build process.

**Phase 2 — Finalization (run from the release branch):**

After the build step has updated `buildHash` in `project.yaml`:

```sh
make release VERSION=1.0.0
```

The compiler detects the release branch and runs finalization: validates `project.yaml`, creates an annotated tag, merges back to main, and deletes the release branch.

**CIC Central Signing (optional post-step):**

After finalization, the CIC authority can apply a second signature:

```sh
python -m tools.finalize_release project.yaml \
  --cic-vault-key cic-root-ca-key \
  --cic-cert-vault-path kv/data/secrets/CICRootCA:cert
```

This fills in `cicSign` and `cicSignedCA.certificate` in `project.yaml`.

> **Note:** `finalize_release.py` is a temporary script. This step will be automated by the relay infrastructure in a future release.

### Dry Run Mode

All release commands support `--dry-run` (or `DRY_RUN=1` via make). No files are written, no Git operations are performed, and Vault calls return placeholder values.

```sh
make release VERSION=1.0.0 DRY_RUN=1
make release-schema VERSION=v1.0.0 DRY_RUN=1
```