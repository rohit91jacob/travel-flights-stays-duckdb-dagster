# Developer entry points. Everything runs through uv; `make help` lists the targets.

SHELL := /bin/bash
.DEFAULT_GOAL := help

UV ?= uv
DAGSTER_HOME ?= $(CURDIR)/dagster_home
DEFS := -m travel_pipeline.definitions
FLIGHTS_RANGE := {"dagster/asset_partition_range_start": "2022-02-11", "dagster/asset_partition_range_end": "2022-03-31"}
STAYS_RANGE := {"dagster/asset_partition_range_start": "2013-06-24", "dagster/asset_partition_range_end": "2017-09-14"}

export DAGSTER_HOME

.PHONY: help install fixtures dbt-parse lint format test test-e2e coverage validate dev \
        ingest load-flights load-stays transform pipeline docker-build docker-up docker-down clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install: ## Install the project and dev tools into .venv
	$(UV) sync --all-groups --frozen

fixtures: ## Regenerate the synthetic CI fixtures in tests/fixtures/kaggle
	$(UV) run python scripts/generate_fixtures.py

dbt-parse: ## Build dbt/target/manifest.json (needed before Dagster can load)
	cd dbt && $(UV) run dbt parse --profiles-dir . --target $${DBT_TARGET:-dev}

lint: ## ruff + sqlfluff
	$(UV) run ruff check .
	$(UV) run ruff format --check .
	$(UV) run sqlfluff lint dbt/models dbt/tests dbt/macros

format: ## Auto-fix formatting
	$(UV) run ruff format .
	$(UV) run ruff check --fix .
	$(UV) run sqlfluff fix dbt/models dbt/tests

test: ## Unit tests (no dbt build)
	$(UV) run pytest -m "not e2e"

test-e2e: ## Every job end-to-end on the synthetic fixtures
	$(UV) run pytest -m e2e

coverage: ## All tests with a coverage report
	$(UV) run pytest --cov --cov-report=term-missing

validate: ## Load the Dagster definitions exactly as a deployment would
	$(UV) run dagster definitions validate $(DEFS)

dev: ## Dagster UI + daemon on http://localhost:3000
	$(UV) run dagster dev $(DEFS) -p 3000

ingest: ## Land the Kaggle datasets and reload the listings snapshot
	$(UV) run dagster job execute $(DEFS) -j ingest_job

load-flights: ## Load every flights partition in a single run
	$(UV) run dagster job execute $(DEFS) -j flights_load_job --tags '$(FLIGHTS_RANGE)'

load-stays: ## Load every stays partition in a single run
	$(UV) run dagster job execute $(DEFS) -j stays_load_job --tags '$(STAYS_RANGE)'

transform: ## dbt source freshness + dbt build
	$(UV) run dagster job execute $(DEFS) -j transform_job

pipeline: ingest load-flights load-stays transform ## Full refresh: ingest -> loads -> transform

docker-build: ## Build the image
	docker compose build

docker-up: ## Start Postgres, code location, webserver and daemon
	docker compose up -d --wait

docker-down: ## Stop the stack (volumes are kept)
	docker compose down

clean: ## Remove local data, Dagster state and dbt artifacts
	rm -rf data dbt/target dbt/logs .pytest_cache .ruff_cache
	find dagster_home -mindepth 1 ! -name dagster.yaml -exec rm -rf {} +
