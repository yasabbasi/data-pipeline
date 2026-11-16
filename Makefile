.DEFAULT_GOAL := help
.PHONY: help install check lint test format

SQLFLUFF := uv run sqlfluff
SQLFLUFF_CONFIG := --ignore-local-config --config pyproject.toml
YAML_RULES = {document-start: disable, line-length: disable, truthy: {check-keys: false}, \
	comments: {min-spaces-from-content: 1}, indentation: {spaces: 2}}

help: ## List the targets
	@awk -F ':.*## ' '/^[a-z-]+:.*## / {printf "%-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install the locked dependencies
	uv sync --locked

check: lint test ## Lint and test; run it before you finish

lint: ## Check style, types, SQL, Markdown, YAML and JSON
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy
	$(SQLFLUFF) lint $(SQLFLUFF_CONFIG) --dialect duckdb sql/views
	$(SQLFLUFF) lint $(SQLFLUFF_CONFIG) --dialect bigquery sql/profile
	uv run pymarkdown scan --recurse --exclude .venv --exclude .worktrees .
	uv run yamllint --strict -d '{extends: default, rules: $(YAML_RULES)}' .github metrics.yaml
	uv run python scripts/lint_json.py

test: ## Run the tests, which use synthetic fixtures and no network
	uv run pytest

format: ## Fix style and formatting
	uv run ruff format .
	uv run ruff check --fix .
	$(SQLFLUFF) fix $(SQLFLUFF_CONFIG) --dialect duckdb sql/views
	$(SQLFLUFF) fix $(SQLFLUFF_CONFIG) --dialect bigquery sql/profile
