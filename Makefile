.DEFAULT_GOAL := help
COMPOSE := docker compose -f deploy/docker/docker-compose.yml

.PHONY: help install check fmt lint type test test-int test-chaos migrate dev-up dev-down dev-logs seed rehearse demo clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "};{printf "  \033[36m%-12s\033[0m %s\n",$$1,$$2}'

install: ## Install dependencies and pre-commit hooks
	uv sync --all-extras
	uv run pre-commit install

check: lint type test ## Lint, typecheck and unit test (what CI runs)

fmt: ## Format and autofix
	uv run ruff format .
	uv run ruff check --fix .

lint: ## Lint without fixing
	uv run ruff check .
	uv run ruff format --check .

type: ## Typecheck (strict)
	uv run mypy gantry

test: ## Unit tests
	uv run pytest

migrate: ## Apply metadata store migrations
	uv run alembic upgrade head

test-int: dev-up migrate ## Integration tests against the local stack
	uv run pytest -m integration

test-chaos: dev-up migrate ## Fault-injection tests, including a real kill -9
	PYTHONPATH=tests/integration/helpers uv run pytest -m chaos

dev-up: ## Start the local stack (postgres x3, kafka, debezium, temporal, prometheus)
	$(COMPOSE) up -d --wait

dev-down: ## Stop the stack and remove volumes
	$(COMPOSE) down -v

dev-logs: ## Tail stack logs
	$(COMPOSE) logs -f

seed: ## Seed the source database with synthetic orders (ROWS=1000000)
	uv run python -m gantry.cli.main seed --rows $${ROWS:-1000000}

rehearse: dev-up migrate ## Full end-to-end rehearsal, timed (ROWS=4000000)
	uv run python scripts/rehearsal.py --rows $${ROWS:-4000000} --reset

demo: dev-up migrate ## The engine-success / Gantry-failure case, on real data
	uv run python scripts/guarantee_boundary.py

clean: ## Remove build and cache artifacts
	rm -rf .pytest_cache .mypy_cache .ruff_cache dist build
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
