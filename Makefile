.PHONY: help setup test lint clean install-uv

# Default target
help:
	@echo "Snowflake Customer 360 Analytics Platform - Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  make setup          - Create virtual environment and install dependencies"
	@echo "  make test           - Run pytest test suite"
	@echo "  make lint           - Run code linting with ruff"
	@echo "  make format         - Format code with black"
	@echo "  make clean          - Remove cache files and temporary artifacts"
	@echo "  make install-uv     - Install UV package manager"
	@echo ""

# Install UV package manager if not already installed
install-uv:
	@which uv > /dev/null || (echo "Installing UV..." && pip install uv)

# Set up virtual environment and install dependencies
setup: install-uv
	@echo "Creating virtual environment with UV..."
	uv venv
	@echo "Installing dependencies..."
	uv pip install -e ".[dev]"
	@echo ""
	@echo "Setup complete! Activate your environment with:"
	@echo "  source .venv/bin/activate  (Linux/macOS)"
	@echo "  .venv\\Scripts\\activate     (Windows)"

# Run tests
test:
	@echo "Running pytest test suite..."
	uv run pytest tests/ -v

# Run linting
lint:
	@echo "Running ruff linter..."
	uv run ruff check .

# Format code
format:
	@echo "Formatting code with black..."
	uv run black .
	@echo "Sorting imports with ruff..."
	uv run ruff check --select I --fix .

# Clean up cache and temporary files
clean:
	@echo "Cleaning up cache and temporary files..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true
	find . -type f -name ".coverage" -delete 2>/dev/null || true
	rm -rf dbt_customer_analytics/target dbt_customer_analytics/logs
	rm -rf .terraform terraform/*.tfstate terraform/*.tfstate.backup
	@echo "Clean complete!"

# Quick install using requirements.txt (fallback for non-UV users)
install-pip:
	@echo "Installing dependencies with pip..."
	pip install -r requirements.txt
	@echo "Installation complete!"
