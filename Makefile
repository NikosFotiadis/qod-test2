.PHONY: install
install: ## Install the poetry environment with all non-optional dependencies.
	@echo "🚀 Creating virtual environment"
	@poetry config virtualenvs.in-project true
	@poetry config virtualenvs.prefer-active-python true
	@poetry install --only main
	@poetry self add poetry-dotenv-plugin

.PHONY: install-aws
install-aws: install ## Installs dependencies for interacting with AWS
	@echo "🚀 Installing AWS dependencies"
	@poetry install --only aws

.PHONY: install-mlflow
install-mlflow: install ## Installs dependencies for interacting with MLFlow
	@echo "🚀 Installing MLFlow dependencies"
	@poetry install --only mlflow

.PHONY: install-dev
install-dev: install ## Installs dev dependencies
	@echo "🚀 Installing dev dependencies"
	@poetry install --only dev

.PHONY: install-test
install-test: install ## Installs test dependencies
	@echo "🚀 Installing test dependencies"
	@poetry install --only test

.PHONY: install-typing
install-typing: install ## Installs typing dependencies
	@echo "🚀 Installing typing dependencies"
	@poetry install --only typing

.PHONY: install-all
install-all: ## Installs all project dependencies
	@echo "🚀 Installing ALL dependencies"
	@poetry install

.PHONY: install-hooks
install-hooks: install-dev ## Install hooks
	@poetry run pre-commit install

.PHONY: check
check: ## Run code quality tools.
	@echo "🚀 Checking Poetry lock file consistency with 'pyproject.toml': Running poetry lock --check"
	@poetry check --lock
	@echo "🚀 Linting code: Running pre-commit"
	@poetry run pre-commit run --all-files
	@echo "🚀 Static type checking: Running mypy"
	@poetry run mypy

.PHONY: clean
clean: ## Clean intermediate files
	@echo "🚀 Cleaning up.."
	@rm -rf .mypy_cache
	@rm -rf .ruff_cache
	@rm -rf .coverage
	@rm -rf coverage.xml
	@rm -rf .pytest_cache
	@rm -rf dist

.PHONY: test
test: ## Test the code with pytest
	@echo "🚀 Testing code: Running pytest"
	@poetry run pytest -rP --cov --cov-config=pyproject.toml --cov-report=xml

.PHONY: build
build: clean-build ## Build wheel file using poetry
	@echo "🚀 Creating wheel file"
	@poetry build -v

.PHONY: clean-build
clean-build: ## clean build artifacts
	@rm -rf dist

.PHONY: docs-test
docs-test: ## Test if documentation can be built without warnings or errors
	@poetry run mkdocs build -s

.PHONY: docs
docs: ## Build and serve the documentation
	@poetry run mkdocs serve

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
