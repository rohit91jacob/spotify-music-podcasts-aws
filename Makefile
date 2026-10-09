# Developer entrypoints. `make help` lists targets. Settings come from .env when present.
-include .env
export

UV ?= uv
FIXTURES ?= tests/fixtures/bronze
FIXTURE_DATA ?= data/fixtures
FIXTURE_DATE ?= 2026-10-09
TF_DIR ?= infra

.DEFAULT_GOAL := help
.PHONY: help install lint format test test-unit test-e2e test-integration fixtures-run run ingest \
        replay sink glue load dbt-build dbt-docs dbt-compile-athena verify report sql asl \
        tf-fmt tf-validate tf-lint checkov iac pre-commit clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

install: ## Create the virtualenv from uv.lock (dev group included)
	$(UV) sync --frozen

lint: ## Ruff lint + format check
	$(UV) run ruff check .
	$(UV) run ruff format --check .

format: ## Auto-fix lint and format
	$(UV) run ruff check --fix .
	$(UV) run ruff format .

test-unit: ## Fast unit tests (moto in-process, no network)
	$(UV) run pytest tests/unit

test-e2e: ## End-to-end pipeline on the committed fixtures, incl. rerun/idempotency
	$(UV) run pytest -m e2e tests/e2e

test-integration: ## Firehose/S3/Iceberg round trip against MUSICPODS_AWS_ENDPOINT_URL (LocalStack)
	$(UV) run pytest -m integration tests/integration

test: ## Everything except the LocalStack integration test and live downloads
	$(UV) run pytest -m "not integration and not live" --cov

fixtures-run: ## Whole pipeline (Step Functions definition, run locally) on the fixtures
	$(UV) run musicpods --data-dir $(FIXTURE_DATA) run --from-dir $(FIXTURES) --run-date $(FIXTURE_DATE) --dbt-target ci

run: ## Whole pipeline on live data (downloads; MusicBrainz is paced at 1 req/s)
	$(UV) run musicpods run

ingest: ## Download every credential-free source into bronze (key-gated ones if keys are set)
	$(UV) run musicpods ingest

replay: ## Replay new listens into Kinesis Data Firehose (moto unless an endpoint is set)
	$(UV) run musicpods replay

sink: ## Commit Firehose output to Iceberg raw.listen_events
	$(UV) run musicpods sink

glue: ## Glue-style batch jobs: bronze -> DQ gate -> Iceberg raw tables
	$(UV) run musicpods glue

load: ## Expose the Iceberg raw tables to DuckDB
	$(UV) run musicpods load

dbt-build: ## dbt build (models + tests) on DuckDB
	$(UV) run musicpods dbt build

dbt-docs: ## dbt docs generate (target/ in dbt/)
	$(UV) run musicpods dbt docs generate

dbt-compile-athena: ## Compile every model for the Athena target (no AWS needed)
	$(UV) run --extra athena musicpods dbt compile --target athena

verify: ## Reconcile outputs with figures the sources publish
	$(UV) run musicpods verify

report: ## Write the static results site to ./site (open site/index.html)
	$(UV) run musicpods report --out site

sql: ## Example query: this week's top artist momentum
	$(UV) run musicpods sql "SELECT week_start, artist_name, listens, prev_week_listens, wow_change FROM marts.mart_artist_momentum WHERE is_comparable_week ORDER BY week_start DESC, momentum_rank LIMIT 10"

asl: ## Validate the Step Functions definition
	$(UV) run musicpods asl validate

tf-fmt: ## terraform fmt -check
	terraform -chdir=$(TF_DIR) fmt -check -recursive

tf-validate: ## terraform init -backend=false + validate
	terraform -chdir=$(TF_DIR) init -backend=false -input=false
	terraform -chdir=$(TF_DIR) validate

tf-lint: ## tflint (aws ruleset)
	cd $(TF_DIR) && tflint --init && tflint --recursive

checkov: ## checkov static analysis of infra/
	uvx --from checkov checkov -d $(TF_DIR) --framework terraform --quiet --compact

iac: tf-fmt tf-validate tf-lint checkov ## All infrastructure checks

pre-commit: ## Run every pre-commit hook on all files
	$(UV) run pre-commit run --all-files

clean: ## Remove local caches and build output (not data/)
	rm -rf .pytest_cache .ruff_cache .coverage coverage.xml htmlcov dbt/target dbt/logs site
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
