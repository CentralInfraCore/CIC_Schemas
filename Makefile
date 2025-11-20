# Makefile for Schema Development Environment

.PHONY: all help up down shell validate test repo.init infra.deps infra.coverage infra.clean fmt lint check typecheck build release-dependency release-schema

# Default to showing help
all: help

# --- Configuration Variables ---
# The Python command to be executed inside the container
PYTHON_CMD = python3 tools/compiler.py

# Get current git branch name and sanitize it for Docker project name
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "local")
COMPOSE_PROJECT_NAME := $(shell echo "$(GIT_BRANCH)" | sed 's|/|-|g')

# Export this variable for all docker compose commands
export COMPOSE_PROJECT_NAME

# =============================================================================
# Container Lifecycle Management
# =============================================================================

up:
	@echo "--- Starting development environment in the background (Project: $(COMPOSE_PROJECT_NAME)) ---"
	@export POSTGRES_VERSION=$(shell yq '.dependencies.postgres' dependency.yaml) && \
	docker compose up -d

down:
	@echo "--- Stopping development environment (Project: $(COMPOSE_PROJECT_NAME)) ---"
	@docker compose down -v

shell:
	@echo "--- Opening a shell into the running builder container (Project: $(COMPOSE_PROJECT_NAME)) ---"
	@docker compose exec builder bash

build:
	@echo "--- Building Docker images (Project: $(COMPOSE_PROJECT_NAME)) ---"
	@docker compose build

# =============================================================================
# Main Development Tasks
# =============================================================================

validate:
	@echo "--- Validating current schema against its declared validator (Project: $(COMPOSE_PROJECT_NAME)) ---"
	@docker compose exec builder $(PYTHON_CMD) validate

test:
	@echo "--- Running pytest for the compiler infrastructure (Project: $(COMPOSE_PROJECT_NAME)) ---"
	@docker compose exec builder python3 -m pytest --cov=tools --cov-report=term-missing tests/

fmt:
	@echo "--- Formatting Python code with Black and Isort (Project: $(COMPOSE_PROJECT_NAME)) ---"
	@docker compose exec builder python3 -m black --exclude p_venv .
	@docker compose exec builder python3 -m isort --skip-glob "p_venv/*" .

lint:
	@echo "--- Linting Python code with Ruff (Project: $(COMPOSE_PROJECT_NAME)) ---"
	@docker compose exec builder python3 -m ruff check .
	@echo "--- Linting YAML files with yamllint (Project: $(COMPOSE_PROJECT_NAME)) ---"
	@docker compose exec builder python3 -m yamllint .

typecheck:
	@echo "--- Running static type checking with MyPy (Project: $(COMPOSE_PROJECT_NAME)) ---"
	@docker compose exec builder python3 -m mypy --exclude p_venv .

security:
	@echo "--- Running security checks with Bandit ---"
	@docker compose exec builder python3 -m bandit -r tools

check: fmt lint typecheck security
	@echo "--- Running all code quality checks (format, lint, typecheck) (Project: $(COMPOSE_PROJECT_NAME)) ---"

# =============================================================================
# Release Management
# =============================================================================

release-dependency: check
	@if [ -z "$(VERSION)" ]; then echo "[ERROR] VERSION is required. Usage: make release-dependency VERSION=v1.0.0"; exit 1; fi
	@echo "--- Releasing Dependency Schema version $(VERSION) (Project: $(COMPOSE_PROJECT_NAME)) ---"
	@GIT_AUTHOR_NAME="$(shell git config user.name)" GIT_AUTHOR_EMAIL="$(shell git config user.email)" docker compose exec builder bash tools/release.sh dependency $(VERSION)

release-schema: check
	@if [ -z "$(VERSION)" ]; then echo "[ERROR] VERSION is required. Usage: make release-schema VERSION=v1.0.0"; exit 1; fi
	@echo "--- Releasing Application Schema version $(VERSION) (Project: $(COMPOSE_PROJECT_NAME)) ---"
	@GIT_AUTHOR_NAME="$(shell git config user.name)" GIT_AUTHOR_EMAIL="$(shell git config user.email)" docker compose exec builder bash tools/release.sh schema $(VERSION)

# =============================================================================
# Repository Setup
# =============================================================================

repo.init:
	@echo "--- Initializing repository hooks (Project: $(COMPOSE_PROJECT_NAME)) ---"
	@sh tools/init-hooks.sh

# =============================================================================
# Infrastructure & Maintenance Tasks
# =============================================================================

infra.deps:
	@echo "--- Initializing Python dependencies into ./p_venv cache (Project: $(COMPOSE_PROJECT_NAME)) ---"
	@docker compose run --rm setup

infra.coverage:
	@echo "--- Generating HTML coverage report (Project: $(COMPOSE_PROJECT_NAME)) ---"
	@docker compose exec builder python3 -m pytest --cov=tools --cov-report=html
	@echo "HTML coverage report generated in ./htmlcov/index.html"

infra.clean:
	@echo "--- Cleaning up all generated files and caches (Project: $(COMPOSE_PROJECT_NAME)) ---"
	@docker compose down -v --remove-orphans
	@rm -rf ./p_venv
	@rm -f ./requirements.txt
	@rm -rf ./htmlcov

# =============================================================================
# Help
# =============================================================================

help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Container Lifecycle:"
	@echo "  up            Start the development container in the background."
	@echo "  down          Stop and remove the development container."
	@echo "  shell         Open an interactive shell into the running container."
	@echo "  build         Build Docker images."
	@echo ""
	@echo "Main Tasks:"
	@echo "  validate      Validate current schema (schemas/index.yaml) against its declared validator."
	@echo "  test          Run pytest for the compiler infrastructure code."
	@echo "  fmt           Format Python code with Black and Isort."
	@echo "  lint          Lint Python code with Ruff and YAML files with yamllint."
	@echo "  typecheck     Run static type checking with MyPy."
	@echo "  check         Run all code quality checks (fmt, lint, typecheck)."
	@echo ""
	@echo "Release Management:"
	@echo "  release-dependency VERSION=<version>  Release a meta-schema or shared library to the 'dependencies' directory."
	@echo "  release-schema VERSION=<version>      Release an application-specific schema to the 'release' directory."
	@echo ""
	@echo "Repository Setup:"
	@echo "  repo.init     Set up the Git hooks for this repository (pre-commit, commit-msg)."
	@echo ""
	@echo "Infrastructure & Maintenance:"
	@echo "  infra.deps    (Re)generate requirements.txt and install dependencies into the cache."
	@echo "  infra.coverage Generate HTML coverage report."
	@echo "  infra.clean   Remove all generated files, caches, and stopped containers."
