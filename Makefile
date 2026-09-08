.DEFAULT_GOAL := help

.PHONY: help install check fmt lint type test build clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "};{printf "  \033[36m%-10s\033[0m %s\n",$$1,$$2}'

install: ## Install development dependencies
	uv sync

check: lint type test build ## Run all local and CI checks

fmt: ## Format and autofix source and tests
	uv run ruff format .
	uv run ruff check --fix .

lint: ## Check formatting and lint rules
	uv run ruff check .
	uv run ruff format --check .

type: ## Run strict static type checking
	uv run mypy gantry tests

test: ## Run the test suite
	uv run pytest --cov=gantry --cov-report=term-missing

build: ## Build source and wheel distributions
	uv run python -m build

clean: ## Remove generated build and cache artifacts
	rm -rf .coverage .pytest_cache .mypy_cache .ruff_cache build dist
