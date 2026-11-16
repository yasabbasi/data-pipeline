.DEFAULT_GOAL := help
.PHONY: help install check lint test format demo run sample

PIPELINE := uv run data-pipeline
DEMO := --db data/demo/warehouse.duckdb --runs-dir data/demo/runs --output-dir data/demo/output
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

demo: ## Run the whole pipeline twice on synthetic fixtures, offline, into data/demo/
	$(PIPELINE) run $(DEMO) --as-of 2026-01-21 --full-refresh
	$(PIPELINE) run $(DEMO) --as-of 2026-01-31
	$(PIPELINE) show --output-dir data/demo/output --days 10

run: ## Run against live BigQuery into data/output/ (needs gcloud credentials)
	$(PIPELINE) run --source bigquery --full-refresh
	$(PIPELINE) show

sample: ## Rebuild the committed sample outputs from the fixtures
	uv run python -m data_pipeline.sample
