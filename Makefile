.PHONY: help setup test lint clean install-uv run-all generate-customers upload-customers load-customers generate-transactions run-dbt validate-data

# Configuration - Set these or pass as arguments
BUCKET ?= snowflake-customer-analytics-data-demo
SNOWFLAKE_PROFILE ?= default
CUSTOMER_COUNT ?= 50000
SEED ?= 42

# Default target
help:
	@echo "Snowflake Customer 360 Analytics Platform - Makefile"
	@echo ""
	@echo "=== Development Commands ==="
	@echo "  make setup          - Create virtual environment and install dependencies"
	@echo "  make test           - Run pytest test suite"
	@echo "  make lint           - Run code linting with ruff"
	@echo "  make format         - Format code with black"
	@echo "  make clean          - Remove cache files and temporary artifacts"
	@echo "  make install-uv     - Install UV package manager"
	@echo ""
	@echo "=== Pipeline Commands (Local Fallback) ==="
	@echo "  make run-all              - Run complete pipeline locally (ONE COMMAND)"
	@echo "  make generate-customers   - Generate synthetic customer data"
	@echo "  make upload-customers     - Upload customer CSV to S3"
	@echo "  make load-customers       - Load customers from S3 to Snowflake"
	@echo "  make generate-transactions- Generate transaction data in Snowflake"
	@echo "  make run-dbt              - Run dbt transformations"
	@echo "  make validate-data        - Run data quality checks"
	@echo ""
	@echo "=== Configuration ==="
	@echo "  BUCKET=$(BUCKET)"
	@echo "  CUSTOMER_COUNT=$(CUSTOMER_COUNT)"
	@echo "  SEED=$(SEED)"
	@echo ""
	@echo "Examples:"
	@echo "  make run-all BUCKET=my-bucket CUSTOMER_COUNT=10000"
	@echo "  make generate-customers CUSTOMER_COUNT=5000 SEED=123"
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

# ============================================================================
# PIPELINE COMMANDS - Local Fallback Execution
# ============================================================================

# Run entire pipeline locally in one command (FALLBACK if Snowflake Tasks fail)
run-all: generate-customers upload-customers load-customers generate-transactions run-dbt validate-data
	@echo ""
	@echo "========================================="
	@echo "✓ Complete pipeline executed successfully"
	@echo "========================================="
	@echo ""
	@echo "Next steps:"
	@echo "  1. Check data: snowsql -c $(SNOWFLAKE_PROFILE) -q 'SELECT COUNT(*) FROM CUSTOMER_ANALYTICS.BRONZE.RAW_CUSTOMERS;'"
	@echo "  2. View results in Streamlit app"
	@echo "  3. Run EDA: snowsql -c $(SNOWFLAKE_PROFILE) -f snowflake/eda/04_delta_analysis.sql"

# Step 1: Generate customer data locally
generate-customers:
	@echo "→ Generating $(CUSTOMER_COUNT) customers with seed $(SEED)..."
	@mkdir -p data
	uv run python -m data_generation generate-customers \
		--count $(CUSTOMER_COUNT) \
		--seed $(SEED) \
		--output data/customers.csv
	@echo "✓ Generated customers saved to data/customers.csv"

# Step 2: Upload to S3
upload-customers:
	@echo "→ Uploading customers to S3 bucket: $(BUCKET)..."
	@if [ -z "$(BUCKET)" ]; then \
		echo "ERROR: BUCKET not set. Usage: make upload-customers BUCKET=your-bucket-name"; \
		exit 1; \
	fi
	@if [ ! -f data/customers.csv ]; then \
		echo "ERROR: data/customers.csv not found. Run 'make generate-customers' first."; \
		exit 1; \
	fi
	uv run python -m data_generation upload-customers \
		--file data/customers.csv \
		--bucket $(BUCKET)
	@echo "✓ Uploaded to s3://$(BUCKET)/customers/customers.csv"

# Step 3: Load customers into Snowflake
load-customers:
	@echo "→ Loading customers to Snowflake..."
	@if ! command -v snowsql &> /dev/null; then \
		echo "ERROR: snowsql not found. Install from https://docs.snowflake.com/en/user-guide/snowsql-install-config"; \
		exit 1; \
	fi
	snowsql -c $(SNOWFLAKE_PROFILE) -f snowflake/load/load_customers_bulk.sql
	@echo "✓ Customers loaded to BRONZE.RAW_CUSTOMERS"

# Step 4: Generate transactions in Snowflake
generate-transactions:
	@echo "→ Generating transactions in Snowflake..."
	snowsql -c $(SNOWFLAKE_PROFILE) -f snowflake/data_generation/generate_transactions.sql
	@echo "✓ Transactions generated in BRONZE.BRONZE_TRANSACTIONS"

# Step 5: Run dbt transformations
run-dbt:
	@echo "→ Running dbt transformations..."
	@if [ ! -d "dbt_customer_analytics" ]; then \
		echo "ERROR: dbt_customer_analytics directory not found"; \
		exit 1; \
	fi
	cd dbt_customer_analytics && dbt run --profiles-dir profile
	@echo "✓ dbt models built successfully"

# Step 6: Run data validation
validate-data:
	@echo "→ Running data quality checks..."
	snowsql -c $(SNOWFLAKE_PROFILE) -f snowflake/eda/03_post_generation_validation.sql
	@echo "✓ Data validation complete"

# ============================================================================
# Individual Pipeline Steps (for granular control)
# ============================================================================

# Generate and load customers (combined)
load-all-customers: generate-customers upload-customers load-customers
	@echo "✓ Customer data pipeline complete"

# Run baseline EDA before generation
run-baseline-eda:
	@echo "→ Running baseline EDA..."
	snowsql -c $(SNOWFLAKE_PROFILE) -f snowflake/eda/01_baseline_metrics.sql
	snowsql -c $(SNOWFLAKE_PROFILE) -f snowflake/eda/02_pre_generation_eda.sql
	@echo "✓ Baseline EDA complete"

# Run delta analysis after generation
run-delta-analysis:
	@echo "→ Running delta analysis..."
	snowsql -c $(SNOWFLAKE_PROFILE) -f snowflake/eda/04_delta_analysis.sql
	snowsql -c $(SNOWFLAKE_PROFILE) -f snowflake/eda/05_telemetry_tracking.sql
	@echo "✓ Delta analysis complete"

# Full EDA workflow
run-full-eda: run-baseline-eda generate-transactions validate-data run-delta-analysis
	@echo "✓ Complete EDA workflow finished"

# ============================================================================
# Demo/Testing Commands
# ============================================================================

# Quick test run with small dataset
test-pipeline:
	@echo "→ Running test pipeline with 1,000 customers..."
	$(MAKE) run-all CUSTOMER_COUNT=1000 SEED=999
	@echo "✓ Test pipeline complete"

# Clean generated data
clean-data:
	@echo "Cleaning generated data files..."
	rm -f data/customers.csv
	rm -f data/transactions.csv
	@echo "✓ Data files removed"
