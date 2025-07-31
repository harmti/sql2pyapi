.PHONY: help test test-unit test-integration test-system lint format check install clean dev-install

# Default target
help:
	@echo "Available commands:"
	@echo "  help              Show this help message"
	@echo "  install           Install the package in development mode"
	@echo "  dev-install       Install development dependencies"
	@echo "  test              Run all tests"
	@echo "  test-unit         Run unit tests only"
	@echo "  test-integration  Run integration tests only"
	@echo "  test-system       Run system tests only"
	@echo "  lint              Run linting (ruff check)"
	@echo "  format            Run formatting (ruff format)"
	@echo "  check             Run both linting and formatting checks"
	@echo "  fix               Fix linting and formatting issues"
	@echo "  clean             Clean up temporary files"
	@echo "  pre-commit-install Install pre-commit hooks"
	@echo "  pre-commit-run    Run pre-commit on all files"

# Installation
install:
	uv pip install -e .

dev-install:
	uv pip install -e ".[dev]"

# Testing
test:
	uv run pytest

test-unit:
	uv run pytest tests/unit

test-integration:
	uv run pytest tests/integration

test-system:
	uv run pytest tests/system

# Linting and formatting
lint:
	uv run ruff check .

format:
	uv run ruff format .

check:
	uv run ruff check .
	uv run ruff format --check .

fix:
	uv run ruff check --fix .
	uv run ruff format .

# Pre-commit
pre-commit-install:
	uv run pre-commit install

pre-commit-run:
	uv run pre-commit run --all-files

# Cleanup
clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf build/
	rm -rf dist/
	rm -rf .pytest_cache/
	rm -rf .ruff_cache/

# Development workflow
setup: dev-install pre-commit-install
	@echo "Development environment setup complete!"
	@echo "Run 'make check' to verify everything is working."

ci-check: check test
	@echo "CI checks passed!"