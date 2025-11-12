# Snowflake Customer 360 Analytics Platform
## Implementation Prompts for Code-Generation LLM

**Project Overview:** Building a Customer 360 Analytics Platform on Snowflake demonstrating Data Engineering, Analytics, Applications, and AI/ML pillars through a post-acquisition credit card customer integration scenario.

**Approach:** Test-Driven Development with incremental, safe progress. Each prompt builds on previous work with no orphaned code.

---

## Phase 1: Foundation & Infrastructure Setup

### Iteration 1.1: Project Structure & Configuration

**Context:** Starting from scratch. Need basic project structure, version control setup, and configuration management for a Snowflake data platform demo project.

**Prerequisites:**
- Python 3.10+ installed
- Git installed
- AWS CLI configured
- Snowflake trial account credentials available

**Prompt 1.1:**

```
Create the foundational project structure for the Snowflake Customer 360 Analytics Platform with the following requirements - Project Structure will us UV packaging manager:

PROJECT STRUCTURE:
```
snowflake-customer-analytics/
├── README.md                           # Project overview and quick start
├── .gitignore                          # Ignore secrets, cache, temp files
├── .env.example                        # Template for environment variables
├── requirements.txt                    # Python dependencies
├── terraform/                          # AWS infrastructure
├── snowflake/                          # Snowflake SQL scripts
│   └── setup/
├── dbt_customer_analytics/             # dbt project (empty structure for now)
├── data_generation/                    # Synthetic data generators
├── ml/                                 # ML model scripts
├── semantic_layer/                     # Cortex Analyst config
├── streamlit/                          # Streamlit app
└── tests/                              # Test suite
    ├── unit/
    ├── integration/
    ├── performance/
    └── data_quality/
```

REQUIREMENTS:

1. **README.md** should include:
   - Project title and description (Customer 360 Analytics for post-acquisition scenario)
   - Prerequisites list
   - Quick start instructions (placeholder)
   - Architecture overview (brief, 2-3 sentences)
   - Project structure explanation

2. **.gitignore** should exclude:
   - Python cache (__pycache__, *.pyc, .pytest_cache)
   - Environment files (.env, secrets.toml)
   - Terraform state files (*.tfstate, *.tfstate.backup, .terraform/)
   - IDE files (.vscode/, .idea/)
   - Data files (*.csv, *.parquet - except fixtures)
   - dbt artifacts (dbt_project/target/, dbt_project/logs/)

3. **.env.example** should have placeholders for:
   - Snowflake credentials (SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_ROLE)
   - AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)
   - S3 bucket name (S3_BUCKET_NAME)

4. **requirements.txt** should include - this will be done with uv packaged manager:
   - snowflake-connector-python[pandas]
   - dbt-snowflake
   - faker
   - pandas
   - pytest
   - pytest-cov
   - python-dotenv
   - boto3
   - tenacity (for retry logic)

5. Create empty __init__.py files in:
   - data_generation/
   - tests/unit/
   - tests/integration/

6. Create a simple **Makefile** with targets:
   - `setup`: Create virtual environment and install dependencies
   - `test`: Run pytest
   - `lint`: Run basic Python linting (placeholder)
   - `clean`: Remove cache and temp files

TESTING REQUIREMENTS:
- Create tests/unit/test_project_structure.py that verifies:
  - All required directories exist
  - All required files exist (README, .gitignore, requirements.txt, .env.example)
  - requirements.txt is valid (can be parsed)
  - .env.example contains all required environment variable placeholders

OUTPUT:
- All files and directories created
- Test passes with `pytest tests/unit/test_project_structure.py -v`
- Instructions for running `make setup` to initialize environment
```

---

### Iteration 1.2: AWS Infrastructure Foundation (Terraform)

**Context:** Project structure is ready. Now set up AWS infrastructure (S3, IAM) using Terraform before Snowflake integration.

**Prerequisites:**
- Iteration 1.1 complete
- AWS account with permissions to create S3, IAM, SNS, SQS

**Prompt 1.2:**

```
Create Terraform configuration to provision AWS infrastructure for Snowflake data ingestion with the following requirements:

CONTEXT:
- We need an S3 bucket to store customer data and transaction files
- Snowflake will access S3 via IAM role (no long-term credentials)
- S3 event notifications will trigger Snowpipe via SNS/SQS

REQUIREMENTS:

1. **terraform/variables.tf**:
   - Define variables:
     - project_name (default: "snowflake-customer-analytics")
     - environment (default: "demo")
     - aws_region (default: "us-east-1")
     - snowflake_account_id (no default, must be provided)
     - snowflake_external_id (no default, must be provided)

2. **terraform/main.tf**:
   - Configure AWS provider using aws_region variable
   - Configure backend (local for now, comment explaining how to switch to S3 backend)

3. **terraform/s3.tf**:
   - Create S3 bucket with name: "${var.project_name}-data-${var.environment}"
   - Enable versioning
   - Enable server-side encryption (SSE-S3)
   - Add lifecycle rule to transition old files to Glacier after 90 days
   - Block public access (all 4 settings)
   - Create folder structure using null_resource:
     - customers/
     - transactions/historical/
     - transactions/streaming/

4. **terraform/iam.tf**:
   - Create IAM role "snowflake-s3-access-role" with:
     - Trust relationship allowing Snowflake AWS account (using var.snowflake_account_id and var.snowflake_external_id)
     - Policy allowing:
       - s3:GetObject, s3:GetObjectVersion on bucket/*
       - s3:ListBucket on bucket
       - s3:GetBucketLocation on bucket
   - Output the role ARN

5. **terraform/outputs.tf**:
   - Output: s3_bucket_name
   - Output: s3_bucket_arn
   - Output: iam_role_arn
   - Output: iam_role_name

6. **terraform/README.md**:
   - Explain what infrastructure is created
   - Provide initialization steps:
     1. Get Snowflake account ID and external ID from Snowflake (placeholder instructions)
     2. Create terraform.tfvars with required variables
     3. Run terraform init
     4. Run terraform plan
     5. Run terraform apply
   - Note: SNS/SQS for Snowpipe will be added in next iteration

7. **terraform/terraform.tfvars.example**:
   - Template file with example values (not tracked in git)

TESTING REQUIREMENTS:
- Create tests/integration/test_terraform_config.sh that:
  - Validates Terraform configuration (terraform validate)
  - Checks that terraform plan runs without errors (using dummy variables)
  - Does NOT apply changes (testing only)

- Create tests/unit/test_terraform_variables.py that:
  - Parses terraform/variables.tf
  - Verifies all required variables are defined
  - Verifies default values are set where appropriate

DOCUMENTATION:
- Update main README.md with section "Phase 1: AWS Infrastructure Setup"
- Link to terraform/README.md

OUTPUT:
- Terraform configuration complete but NOT applied
- Tests pass
- Clear instructions for manual terraform apply (after getting Snowflake IDs)
- Note in README: "SNS/SQS for Snowpipe event notifications will be added after Snowflake storage integration is created"
```

---

### Iteration 1.3: Snowflake Foundation Setup

**Context:** AWS infrastructure is defined (not yet applied). Now create Snowflake database objects, schemas, and roles.

**Prerequisites:**
- Iteration 1.2 complete
- Snowflake trial account created
- Snowflake credentials in .env file

**Prompt 1.3:**

```
Create Snowflake initialization scripts to set up database, schemas, and role-based access control with the following requirements:

CONTEXT:
- We're building a medallion architecture (Bronze/Silver/Gold layers)
- Need roles for Data Engineers, Marketing Managers, and Data Analysts
- Will create storage integration and stages in next iteration (after Terraform apply)

REQUIREMENTS:

1. **snowflake/setup/00_environment_check.sql**:
   - Check current Snowflake account and user
   - Display available warehouses
   - Display current role and grants
   - This helps verify connectivity before proceeding

2. **snowflake/setup/01_create_database_schemas.sql**:
   - Create database: CUSTOMER_ANALYTICS
   - Create schemas:
     - BRONZE (raw data landing zone)
     - SILVER (cleaned, deduplicated data)
     - GOLD (analytics-ready dimensional models)
     - OBSERVABILITY (pipeline metadata, DQ metrics)
   - Add comments to each schema explaining purpose
   - Set default warehouse to COMPUTE_WH (assumes it exists)

3. **snowflake/setup/02_create_roles_grants.sql**:
   - Create role: DATA_ENGINEER
     - Grant: ALL privileges on database CUSTOMER_ANALYTICS
     - Grant: USAGE on warehouse COMPUTE_WH
   - Create role: MARKETING_MANAGER
     - Grant: USAGE on database and GOLD schema
     - Grant: SELECT on all tables in GOLD schema
     - Grant: SELECT on future tables in GOLD schema
     - Grant: USAGE on warehouse COMPUTE_WH
     - DENY: Access to BRONZE and SILVER schemas
   - Create role: DATA_ANALYST
     - Grant: USAGE on database and all schemas
     - Grant: SELECT on all tables in all schemas
     - Grant: SELECT on future tables in all schemas
     - Grant: USAGE on warehouse COMPUTE_WH
   - Add comments explaining each role's purpose

4. **snowflake/setup/03_create_observability_tables.sql**:
   - Create table: OBSERVABILITY.PIPELINE_RUN_METADATA
     - run_id STRING PRIMARY KEY
     - run_timestamp TIMESTAMP
     - status STRING (STARTED, SUCCESS, FAILED)
     - models_run INT
     - models_failed INT
     - error_message STRING
   - Create table: OBSERVABILITY.DATA_QUALITY_METRICS
     - run_id STRING
     - run_timestamp TIMESTAMP
     - layer STRING (bronze, silver, gold)
     - table_name STRING
     - check_type STRING (duplicate, null, referential, etc.)
     - records_checked INT
     - records_failed INT
     - failure_rate FLOAT
     - failure_details VARIANT (JSON)
   - Create table: OBSERVABILITY.LAYER_RECORD_COUNTS
     - run_id STRING
     - run_timestamp TIMESTAMP
     - model_name STRING
     - record_count INT

5. **snowflake/run_setup.sh**:
   - Bash script that runs all SQL files in order using SnowSQL
   - Load environment variables from .env
   - Run each SQL file with error handling
   - Print success/failure for each step
   - Exit on first error
   - Usage: ./snowflake/run_setup.sh

6. **snowflake/README.md**:
   - Explain the setup process
   - List all SQL scripts and what they do
   - Provide manual execution instructions (if not using run_setup.sh)
   - Document role hierarchy and permissions

TESTING REQUIREMENTS:

- Create tests/integration/test_snowflake_setup.py:
  - Use snowflake-connector-python to connect
  - Verify database CUSTOMER_ANALYTICS exists
  - Verify all 4 schemas exist (BRONZE, SILVER, GOLD, OBSERVABILITY)
  - Verify all 3 roles exist (DATA_ENGINEER, MARKETING_MANAGER, DATA_ANALYST)
  - Verify observability tables exist and have correct columns
  - Test role permissions:
    - Switch to MARKETING_MANAGER role → verify cannot query BRONZE schema
    - Switch to DATA_ANALYST role → verify can query all schemas
  - Use pytest fixtures for connection management

- Create tests/unit/test_sql_syntax.py:
  - Parse each SQL file
  - Check for basic syntax errors (unmatched quotes, semicolons, etc.)
  - Verify comments exist for each major object

DOCUMENTATION:
- Update main README.md with "Phase 1: Snowflake Foundation Setup"
- Add connection test instructions

OUTPUT:
- All SQL scripts created
- run_setup.sh is executable (chmod +x)
- Tests pass with `pytest tests/integration/test_snowflake_setup.py -v`
- Clear documentation for running setup
- Note: Storage integration and stages will be created in next iteration after Terraform apply
```

---

## Phase 2: Data Generation & Ingestion

### Iteration 2.1: Customer Data Generator

**Context:** Infrastructure is ready. Now build synthetic data generator for customer dimension (50K customers).

**Prerequisites:**
- Iteration 1.3 complete
- Python environment set up with faker, pandas

**Prompt 2.1:**

```
Create a Python-based synthetic customer data generator with test-driven development approach:

CONTEXT:
- Generating 50,000 customers for a credit card portfolio
- 5 customer segments with different characteristics
- Realistic demographic data using Faker library
- Output to CSV for bulk load to S3/Snowflake

REQUIREMENTS:

1. **data_generation/config.py**:
   - Define constants:
     - SEGMENTS (dict with segment names and percentages)
       - High-Value Travelers: 15%
       - Stable Mid-Spenders: 40%
       - Budget-Conscious: 25%
       - Declining: 10%
       - New & Growing: 10%
     - SEGMENT_SPEND_RANGES (dict with monthly spend ranges per segment)
     - CARD_TYPES = ['Standard', 'Premium']
     - EMPLOYMENT_STATUSES = ['Employed', 'Self-Employed', 'Retired', 'Unemployed']
     - US_STATES (list of state abbreviations)

2. **data_generation/customer_generator.py**:
   - Function: generate_customers(n: int, seed: int = 42) -> pd.DataFrame
     - Use Faker with seed for reproducibility
     - Generate n customers with columns:
       - customer_id: "CUST" + 8-digit zero-padded number
       - first_name, last_name (Faker)
       - email (Faker, consistent with name)
       - age: random 22-75
       - state: random from US_STATES
       - city: Faker city
       - employment_status: random from EMPLOYMENT_STATUSES
       - card_type: 'Premium' for 30% of High-Value Travelers, else 'Standard'
       - credit_limit: random 5000-50000 (rounded to nearest 1000)
       - account_open_date: random date between 5 years ago and 2 years ago
       - customer_segment: assigned based on SEGMENTS distribution
       - decline_type: 'gradual' or 'sudden' for Declining segment only, else NULL
   - Return DataFrame

   - Function: validate_customer_data(df: pd.DataFrame) -> Dict[str, Any]
     - Check: No null values in required fields
     - Check: customer_id unique and correct format
     - Check: Segment distribution within 5% of target
     - Check: Email format valid
     - Check: Credit limits in valid range
     - Return dict with validation results and statistics

   - Function: save_to_csv(df: pd.DataFrame, filepath: str) -> None
     - Save DataFrame to CSV with headers
     - Print confirmation with row count

3. **data_generation/cli.py**:
   - Click-based CLI with command: generate-customers
   - Arguments:
     - --count (default 50000): Number of customers to generate
     - --output (default 'customers.csv'): Output file path
     - --seed (default 42): Random seed for reproducibility
   - Workflow:
     1. Generate customers
     2. Validate data
     3. Print validation results
     4. Save to CSV if validation passes
     5. Exit with error code if validation fails

4. **data_generation/__main__.py**:
   - Entry point to run CLI: `python -m data_generation`

TESTING REQUIREMENTS:

- Create tests/unit/test_customer_generator.py:

  Test Class: TestCustomerGeneration

  - test_generates_correct_row_count():
    - Generate 1000 customers
    - Assert len(df) == 1000

  - test_customer_id_format():
    - Generate 100 customers
    - Assert all customer_ids match regex ^CUST\d{8}$
    - Assert all customer_ids are unique

  - test_customer_id_sequential():
    - Generate 100 customers
    - Assert customer_ids are CUST00000001 through CUST00000100

  - test_segment_distribution():
    - Generate 10000 customers
    - Calculate segment distribution
    - Assert each segment percentage within 5% of target

  - test_no_null_required_fields():
    - Generate 100 customers
    - Check required fields: customer_id, email, state, card_type, credit_limit
    - Assert no nulls

  - test_credit_limit_ranges():
    - Generate 1000 customers
    - Assert min credit_limit >= 5000
    - Assert max credit_limit <= 50000
    - Assert all credit_limits are multiples of 1000

  - test_email_format():
    - Generate 100 customers
    - Assert all emails contain '@'
    - Assert all emails contain '.'

  - test_reproducibility():
    - Generate 100 customers with seed=42
    - Generate 100 customers with seed=42 again
    - Assert DataFrames are identical

  - test_decline_type_only_for_declining_segment():
    - Generate 1000 customers
    - Get customers where customer_segment == 'Declining'
    - Assert all have decline_type in ['gradual', 'sudden']
    - Get customers where customer_segment != 'Declining'
    - Assert all have decline_type == NULL

  Test Class: TestCustomerValidation

  - test_validation_passes_for_valid_data():
    - Generate 100 customers
    - Run validate_customer_data()
    - Assert validation['is_valid'] == True

  - test_validation_fails_for_duplicate_ids():
    - Create DataFrame with duplicate customer_id
    - Run validate_customer_data()
    - Assert validation['is_valid'] == False

  - test_validation_fails_for_null_required_fields():
    - Create DataFrame with null email
    - Run validate_customer_data()
    - Assert validation['is_valid'] == False

INTEGRATION TEST:

- Create tests/integration/test_customer_generation_e2e.py:
  - test_cli_generates_valid_file():
    - Run CLI command to generate 1000 customers to temp file
    - Assert file exists
    - Load CSV
    - Assert 1000 rows
    - Run validation
    - Assert validation passes

DOCUMENTATION:

- Create data_generation/README.md:
  - Explain customer generation logic
  - Provide usage examples
  - Document segment definitions
  - Show sample CLI commands

- Update main README.md with "Phase 2: Data Generation"

OUTPUT:
- All code files created
- All tests pass with `pytest tests/unit/test_customer_generator.py -v`
- CLI works: `python -m data_generation generate-customers --count 1000 --output test_customers.csv`
- CSV file is created with valid data
- Code is well-documented with docstrings
```

---

### Iteration 2.2: Snowflake Storage Integration & S3 Upload

**Context:** Customer generator is ready. Now apply Terraform to create AWS infrastructure and connect Snowflake to S3.

**Prerequisites:**
- Iteration 2.1 complete
- Iteration 1.2 Terraform config ready
- AWS and Snowflake credentials configured

**Prompt 2.2:**

```
Create scripts to apply Terraform, establish Snowflake-S3 integration, and upload customer data with the following requirements:

CONTEXT:
- Terraform will create S3 bucket and IAM role
- Snowflake needs storage integration to access S3 securely
- Then we'll upload generated customer CSV to S3

REQUIREMENTS:

1. **terraform/deploy.sh**:
   - Bash script to apply Terraform configuration
   - Steps:
     1. Check if terraform.tfvars exists, error if not
     2. Run terraform init
     3. Run terraform validate
     4. Run terraform plan -out=tfplan
     5. Prompt user to review plan
     6. Run terraform apply tfplan
     7. Run terraform output -json > outputs.json
     8. Print success message with outputs
   - Make executable with error handling

2. **terraform/get_snowflake_ids.sql**:
   - SQL script to help get Snowflake account identifiers needed for Terraform
   - Query: SELECT CURRENT_ACCOUNT() AS account_id;
   - Query: SELECT SYSTEM$GET_AWS_SNS_IAM_POLICY('<S3_BUCKET_ARN>') AS iam_policy;
   - Instructions in comments explaining how to use output for terraform.tfvars

3. **snowflake/setup/04_create_storage_integration.sql**:
   - Create storage integration pointing to S3 bucket
   - Use placeholders for IAM role ARN (to be filled after Terraform apply)
   - Query storage integration to get AWS IAM User ARN and External ID
   - Print instructions to update Terraform variables with these values
   - Add comments explaining the trust relationship setup

4. **snowflake/setup/05_create_stages.sql**:
   - Create external stage: CUSTOMER_STAGE
     - URL: s3://<BUCKET>/customers/
     - Storage integration reference
     - File format: CSV with SKIP_HEADER=1
   - Create external stage: TRANSACTION_STAGE_HISTORICAL
     - URL: s3://<BUCKET>/transactions/historical/
     - Storage integration reference
     - File format: CSV with SKIP_HEADER=1
   - Create external stage: TRANSACTION_STAGE_STREAMING
     - URL: s3://<BUCKET>/transactions/streaming/
     - Storage integration reference
     - File format: CSV with SKIP_HEADER=1
   - Test stages: LIST @CUSTOMER_STAGE;

5. **data_generation/s3_uploader.py**:
   - Function: upload_to_s3(
       local_file: str,
       s3_bucket: str,
       s3_key: str,
       aws_profile: Optional[str] = None
     ) -> bool
     - Use boto3 to upload file
     - Add retry logic with tenacity (3 retries, exponential backoff)
     - Return True on success, False on failure
     - Log progress

   - Function: upload_customers_to_s3(
       csv_file: str,
       s3_bucket: str
     ) -> bool
     - Upload to customers/ folder
     - Return success status

6. **data_generation/cli.py** (extend):
   - Add new command: upload-customers
   - Arguments:
     - --file (required): CSV file to upload
     - --bucket (required): S3 bucket name
   - Use upload_customers_to_s3 function
   - Print success/failure message

7. **scripts/setup_end_to_end.sh**:
   - Master script orchestrating full setup:
     1. Echo "Step 1: Generating customer data..."
     2. Run customer generator
     3. Echo "Step 2: Applying Terraform..."
     4. Run terraform/deploy.sh
     5. Echo "Step 3: Creating Snowflake storage integration..."
     6. Prompt user to update 04_create_storage_integration.sql with IAM role ARN
     7. Run Snowflake setup scripts 04 and 05
     8. Echo "Step 4: Uploading customer data to S3..."
     9. Run upload-customers command
     10. Echo "Setup complete!"
   - Make executable

TESTING REQUIREMENTS:

- Create tests/integration/test_s3_upload.py:
  - test_upload_to_s3_success():
    - Create small temp CSV file
    - Upload to test S3 bucket
    - Verify file exists in S3
    - Delete test file from S3

  - test_upload_with_retry():
    - Mock boto3 to fail twice then succeed
    - Verify retry logic works
    - Verify eventual success

- Create tests/integration/test_storage_integration.py:
  - test_snowflake_can_list_s3_stage():
    - After setup, run LIST @CUSTOMER_STAGE
    - Verify command succeeds (stage accessible)

  - test_snowflake_can_see_uploaded_file():
    - Upload test file to S3
    - Run LIST @CUSTOMER_STAGE
    - Verify file appears in listing

DOCUMENTATION:

- Create docs/SETUP_GUIDE.md:
  - Step-by-step instructions for full setup
  - Prerequisites checklist
  - Terraform → Snowflake → S3 workflow explanation
  - Troubleshooting common issues:
    - IAM trust relationship errors
    - Storage integration permissions
    - S3 access denied

- Update main README.md:
  - Link to docs/SETUP_GUIDE.md
  - Add "Quick Setup" section with one-liner (setup_end_to_end.sh)

OUTPUT:
- Terraform applied successfully (S3 bucket and IAM role created)
- Snowflake storage integration created and verified
- Snowflake stages created and accessible
- Customer CSV uploaded to S3
- Tests pass: pytest tests/integration/test_s3_upload.py tests/integration/test_storage_integration.py -v
- Clear documentation for setup process
```

---

### Iteration 2.3: Bronze Layer - Customer Bulk Load

**Context:** S3 integration is working. Now load customer data from S3 to Snowflake Bronze layer.

**Prerequisites:**
- Iteration 2.2 complete
- Customer CSV in S3
- Snowflake stages configured

**Prompt 2.3:**

```
Create Bronze layer table for customers and implement bulk load from S3 with testing:

CONTEXT:
- Bronze layer stores raw data exactly as received
- Bulk load using COPY INTO for historical customer data
- No transformations at this layer
- Capture metadata (ingestion timestamp, source file)

REQUIREMENTS:

1. **snowflake/setup/06_create_bronze_tables.sql**:
   - Create table: BRONZE.BRONZE_CUSTOMERS
     - customer_id STRING
     - first_name STRING
     - last_name STRING
     - email STRING
     - age INT
     - state STRING
     - city STRING
     - employment_status STRING
     - card_type STRING
     - credit_limit NUMBER(10,2)
     - account_open_date DATE
     - customer_segment STRING
     - decline_type STRING
     - ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
     - source_file STRING
     - _metadata_file_row_number INT (Snowflake auto-generated)
   - Add table comment explaining Bronze layer purpose
   - Do NOT add primary key or constraints (Bronze is raw)

2. **snowflake/load/load_customers_bulk.sql**:
   - COPY INTO BRONZE.BRONZE_CUSTOMERS
   - FROM @CUSTOMER_ANALYTICS.BRONZE.CUSTOMER_STAGE
   - FILE_FORMAT = (TYPE='CSV' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
   - PATTERN = '.*customers\.csv'
   - ON_ERROR = 'ABORT_STATEMENT' (fail fast for bulk loads)
   - Add source_file using METADATA$FILENAME
   - After load, query to show summary:
     - Total rows loaded
     - Distinct customer count
     - Rows per segment

3. **snowflake/load/verify_customer_load.sql**:
   - Query to validate loaded data:
     - Count total rows
     - Check for null customer_ids
     - Check for duplicate customer_ids
     - Segment distribution
     - Date range of account_open_dates
   - Return validation results as table

4. **data_generation/cli.py** (extend):
   - Add command: load-customers-to-snowflake
   - Workflow:
     1. Connect to Snowflake
     2. Run 06_create_bronze_tables.sql (if table doesn't exist)
     3. Run load_customers_bulk.sql
     4. Run verify_customer_load.sql
     5. Print validation results
     6. Exit with error if validation fails

5. **snowflake/load/README.md**:
   - Explain bulk load process
   - Document COPY INTO options
   - Explain error handling (ABORT_STATEMENT vs CONTINUE)
   - Provide manual load instructions

TESTING REQUIREMENTS:

- Create tests/integration/test_customer_bulk_load.py:

  - test_bronze_table_created():
    - Query INFORMATION_SCHEMA.TABLES
    - Verify BRONZE_CUSTOMERS exists in BRONZE schema
    - Verify table has expected columns

  - test_bulk_load_executes_without_error():
    - Truncate BRONZE_CUSTOMERS (if exists)
    - Run load_customers_bulk.sql
    - Assert no SQL errors raised

  - test_all_customers_loaded():
    - Count rows in CSV file
    - Count rows in BRONZE_CUSTOMERS
    - Assert counts match

  - test_no_duplicate_customer_ids():
    - Query: SELECT customer_id, COUNT(*) FROM BRONZE_CUSTOMERS GROUP BY customer_id HAVING COUNT(*) > 1
    - Assert result is empty

  - test_no_null_customer_ids():
    - Query: SELECT COUNT(*) FROM BRONZE_CUSTOMERS WHERE customer_id IS NULL
    - Assert count == 0

  - test_metadata_fields_populated():
    - Query sample rows
    - Assert ingestion_timestamp is not null
    - Assert source_file contains 'customers.csv'

  - test_segment_distribution():
    - Query segment counts
    - Calculate percentages
    - Assert High-Value Travelers is ~15% (+/- 5%)
    - Assert Stable Mid-Spenders is ~40% (+/- 5%)
    - (Similar for other segments)

  - test_data_types_correct():
    - Query: SELECT * FROM BRONZE_CUSTOMERS LIMIT 1
    - Verify age is integer
    - Verify credit_limit is numeric
    - Verify account_open_date is date type

LOGGING & OBSERVABILITY:

- Update load_customers_bulk.sql to log to observability table:
  ```sql
  INSERT INTO OBSERVABILITY.LAYER_RECORD_COUNTS
  SELECT
      'BULK_LOAD_CUSTOMERS' AS run_id,
      CURRENT_TIMESTAMP() AS run_timestamp,
      'BRONZE.BRONZE_CUSTOMERS' AS model_name,
      COUNT(*) AS record_count
  FROM BRONZE.BRONZE_CUSTOMERS;
  ```

DOCUMENTATION:

- Update main README.md with "Data Loading - Customer Bulk Load"
- Document expected row count (50,000)
- Add troubleshooting section for common load errors

OUTPUT:
- Bronze table created
- Customer data loaded from S3 to Snowflake
- All tests pass: pytest tests/integration/test_customer_bulk_load.py -v
- Observability table updated with row count
- Clear success message with loaded row count
```

---

### Iteration 2.4: Transaction Data Generator (Snowflake SQL-based)

**Context:** Customers are loaded in Bronze. Now generate 13.5M transactions using Snowflake's GENERATOR() function for scale.

**Prerequisites:**
- Iteration 2.3 complete
- Customers in BRONZE.BRONZE_CUSTOMERS

**Prompt 2.4:**

```
Create Snowflake SQL script to generate synthetic transaction data at scale (13.5M rows) with segment-specific spending patterns:

CONTEXT:
- 50,000 customers × ~750 transactions per customer over 18 months = 13.5M transactions
- Use Snowflake GENERATOR() for performance at scale
- Apply segment-specific spending patterns and decline trajectories
- Generate directly in Snowflake (no Python), output to S3 for bulk load

REQUIREMENTS:

1. **snowflake/data_generation/generate_transactions.sql**:

   Part A: Create date spine (18 months, daily granularity)
   ```sql
   CREATE OR REPLACE TEMP TABLE date_spine AS
   SELECT
       DATEADD('day', SEQ4(), DATEADD('month', -18, CURRENT_DATE())) AS transaction_date,
       DATEDIFF('month', DATEADD('month', -18, CURRENT_DATE()), DATEADD('day', SEQ4(), DATEADD('month', -18, CURRENT_DATE()'))) AS month_num
   FROM TABLE(GENERATOR(ROWCOUNT => 540));  -- 18 months * 30 days
   ```

   Part B: Determine monthly transaction volume per customer by segment
   ```sql
   CREATE OR REPLACE TEMP TABLE customer_monthly_volume AS
   SELECT
       c.customer_id,
       c.customer_segment,
       c.decline_type,
       d.transaction_date,
       d.month_num,
       CASE c.customer_segment
           WHEN 'High-Value Travelers' THEN UNIFORM(40, 80, RANDOM())
           WHEN 'Stable Mid-Spenders' THEN UNIFORM(20, 40, RANDOM())
           WHEN 'Budget-Conscious' THEN UNIFORM(15, 30, RANDOM())
           WHEN 'Declining' THEN UNIFORM(20, 40, RANDOM())
           WHEN 'New & Growing' THEN UNIFORM(25, 50, RANDOM())
       END AS monthly_transactions
   FROM BRONZE.BRONZE_CUSTOMERS c
   CROSS JOIN (SELECT DISTINCT transaction_date, month_num FROM date_spine WHERE DAY(transaction_date) = 1) d;
   ```

   Part C: Expand to individual transactions
   ```sql
   CREATE OR REPLACE TEMP TABLE transactions_expanded AS
   SELECT
       cmv.customer_id,
       cmv.customer_segment,
       cmv.decline_type,
       cmv.month_num,
       DATEADD('day', UNIFORM(0, 28, RANDOM()), cmv.transaction_date) AS transaction_date,
       ROW_NUMBER() OVER (PARTITION BY cmv.customer_id, cmv.transaction_date ORDER BY RANDOM()) AS txn_seq
   FROM customer_monthly_volume cmv
   CROSS JOIN TABLE(GENERATOR(ROWCOUNT => 100))  -- Max transactions per customer per month
   WHERE txn_seq <= cmv.monthly_transactions;  -- Filter to actual monthly volume
   ```

   Part D: Generate transaction details with segment-specific patterns
   ```sql
   CREATE OR REPLACE TEMP TABLE transactions_with_details AS
   SELECT
       'TXN' || LPAD(ROW_NUMBER() OVER (ORDER BY transaction_date, customer_id), 10, '0') AS transaction_id,
       customer_id,
       transaction_date,

       -- Transaction amount varies by segment and applies decline pattern
       CASE customer_segment
           WHEN 'High-Value Travelers' THEN
               ROUND(UNIFORM(50, 500, RANDOM()), 2)
           WHEN 'Stable Mid-Spenders' THEN
               ROUND(UNIFORM(30, 150, RANDOM()), 2)
           WHEN 'Budget-Conscious' THEN
               ROUND(UNIFORM(10, 80, RANDOM()), 2)
           WHEN 'Declining' THEN
               CASE decline_type
                   WHEN 'gradual' THEN
                       -- Linear decline: 10% reduction per month after month 12
                       ROUND(UNIFORM(30, 150, RANDOM()) * GREATEST(0.4, 1 - ((month_num - 12) * 0.1)), 2)
                   WHEN 'sudden' THEN
                       -- Sudden drop: 60% reduction after month 16
                       ROUND(UNIFORM(30, 150, RANDOM()) * IFF(month_num < 16, 1.0, 0.4), 2)
               END
           WHEN 'New & Growing' THEN
               -- 5% growth per month
               ROUND(UNIFORM(20, 100, RANDOM()) * (1 + month_num * 0.05), 2)
       END AS transaction_amount,

       -- Merchant name (simplified)
       'Merchant_' || UNIFORM(1, 1000, RANDOM()) AS merchant_name,

       -- Merchant category varies by segment
       CASE customer_segment
           WHEN 'High-Value Travelers' THEN
               ARRAY_CONSTRUCT('Travel', 'Dining', 'Hotels', 'Airlines')[UNIFORM(0, 3, RANDOM())]
           WHEN 'Budget-Conscious' THEN
               ARRAY_CONSTRUCT('Grocery', 'Gas', 'Utilities')[UNIFORM(0, 2, RANDOM())]
           ELSE
               ARRAY_CONSTRUCT('Retail', 'Dining', 'Entertainment', 'Grocery', 'Gas', 'Travel', 'Healthcare', 'Utilities')[UNIFORM(0, 7, RANDOM())]
       END AS merchant_category,

       -- Transaction channel
       ARRAY_CONSTRUCT('Online', 'In-Store', 'Mobile')[UNIFORM(0, 2, RANDOM())] AS channel

   FROM transactions_expanded
   WHERE transaction_amount > 0;  -- Filter out any negative amounts from decline logic
   ```

   Part E: Unload to S3 for bulk load
   ```sql
   COPY INTO @TRANSACTION_STAGE_HISTORICAL/transactions_historical.csv
   FROM transactions_with_details
   FILE_FORMAT = (TYPE='CSV' COMPRESSION='GZIP')
   HEADER = TRUE
   OVERWRITE = TRUE
   MAX_FILE_SIZE = 104857600;  -- 100MB files

   -- Query to show summary
   SELECT
       COUNT(*) AS total_transactions,
       COUNT(DISTINCT customer_id) AS unique_customers,
       ROUND(AVG(transaction_amount), 2) AS avg_amount,
       MIN(transaction_date) AS earliest_date,
       MAX(transaction_date) AS latest_date
   FROM transactions_with_details;
   ```

2. **snowflake/data_generation/README.md**:
   - Explain why we use Snowflake for transaction generation (scale, performance)
   - Document transaction volume per segment
   - Explain decline patterns (gradual vs sudden)
   - Provide execution instructions

3. **snowflake/data_generation/run_transaction_generation.sh**:
   - Bash script to execute generate_transactions.sql
   - Print start time
   - Execute SQL script
   - Print end time and duration
   - Print summary statistics
   - Make executable

TESTING REQUIREMENTS:

- Create tests/integration/test_transaction_generation.py:

  - test_transaction_generation_completes():
    - Run generate_transactions.sql
    - Assert no SQL errors

  - test_transaction_volume_reasonable():
    - Count rows in transactions_with_details temp table
    - Assert count is between 10M and 15M (target: 13.5M with variance)

  - test_all_customers_have_transactions():
    - Query: SELECT COUNT(DISTINCT customer_id) FROM transactions_with_details
    - Assert count == 50000

  - test_transaction_ids_unique():
    - Query: SELECT transaction_id, COUNT(*) FROM transactions_with_details GROUP BY transaction_id HAVING COUNT(*) > 1
    - Assert result is empty

  - test_transaction_amounts_positive():
    - Query: SELECT COUNT(*) FROM transactions_with_details WHERE transaction_amount <= 0
    - Assert count == 0

  - test_date_range_correct():
    - Query min and max transaction_date
    - Assert range is approximately 18 months
    - Assert no future dates

  - test_declining_segment_shows_decline():
    - Get customers from Declining segment
    - Calculate monthly average spend
    - Assert spend in month 18 < spend in month 1 by at least 30%

  - test_high_value_travelers_spend_more():
    - Calculate avg transaction amount for High-Value Travelers
    - Calculate avg transaction amount for Budget-Conscious
    - Assert High-Value avg > Budget-Conscious avg

  - test_file_exported_to_s3():
    - Run LIST @TRANSACTION_STAGE_HISTORICAL
    - Assert transactions_historical.csv files exist

- Create tests/unit/test_transaction_sql_syntax.py:
  - test_sql_file_parses():
    - Read generate_transactions.sql
    - Check for balanced parentheses
    - Check for proper semicolons
    - Verify no obvious syntax errors

PERFORMANCE TESTING:

- Create tests/performance/test_transaction_generation_performance.py:
  - test_generation_completes_within_time_limit():
    - Run generation script
    - Assert completes within 15 minutes (adjust based on warehouse size)

DOCUMENTATION:

- Update main README.md with "Transaction Data Generation (13.5M rows)"
- Document expected execution time
- Add note about warehouse sizing for performance

OUTPUT:
- SQL script created and tested
- Transaction generation completes successfully
- ~13.5M transactions generated with realistic patterns
- Files exported to S3 in compressed CSV format
- All tests pass: pytest tests/integration/test_transaction_generation.py -v
- Clear documentation for running generation script
```

---

### Iteration 2.5: Bronze Layer - Transaction Bulk Load

**Context:** Transactions generated and exported to S3. Now load to Bronze layer similar to customers.

**Prerequisites:**
- Iteration 2.4 complete
- Transaction CSV files in S3

**Prompt 2.5:**

```
Create Bronze layer table for transactions and implement bulk load from S3 with validation:

CONTEXT:
- Loading 13.5M transactions from S3
- Bronze layer captures raw transaction data
- Include metadata for lineage and debugging

REQUIREMENTS:

1. **snowflake/setup/07_create_bronze_transaction_table.sql**:
   - Create table: BRONZE.BRONZE_TRANSACTIONS
     - transaction_id STRING
     - customer_id STRING
     - transaction_date TIMESTAMP
     - transaction_amount NUMBER(10,2)
     - merchant_name STRING
     - merchant_category STRING
     - channel STRING
     - ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
     - source_file STRING
     - _metadata_file_row_number INT
   - Add table comment
   - Consider adding clustering key on transaction_date for performance (optional, can be added later)

2. **snowflake/load/load_transactions_bulk.sql**:
   - Use transactional approach for validation:
     ```sql
     BEGIN TRANSACTION;

     COPY INTO BRONZE.BRONZE_TRANSACTIONS
     FROM @TRANSACTION_STAGE_HISTORICAL
     FILE_FORMAT = (TYPE='CSV' SKIP_HEADER=1 COMPRESSION='GZIP')
     PATTERN = '.*transactions_historical.*\.csv.*'
     ON_ERROR = 'ABORT_STATEMENT'
     SOURCE_FILE = METADATA$FILENAME;

     -- Validate row count matches expected
     LET expected_rows := 13500000;  -- Approximate
     LET actual_rows := (SELECT COUNT(*) FROM BRONZE.BRONZE_TRANSACTIONS);

     IF ($actual_rows < $expected_rows * 0.9 OR $actual_rows > $expected_rows * 1.1) THEN
         ROLLBACK;
         RAISE EXCEPTION 'Row count out of expected range. Expected ~13.5M, got ' || $actual_rows;
     ELSE
         COMMIT;
     END IF;
     ```

3. **snowflake/load/verify_transaction_load.sql**:
   - Validation queries:
     ```sql
     -- Summary statistics
     SELECT
         COUNT(*) AS total_rows,
         COUNT(DISTINCT transaction_id) AS unique_transactions,
         COUNT(DISTINCT customer_id) AS unique_customers,
         MIN(transaction_date) AS earliest_date,
         MAX(transaction_date) AS latest_date,
         ROUND(AVG(transaction_amount), 2) AS avg_amount,
         ROUND(SUM(transaction_amount), 2) AS total_volume
     FROM BRONZE.BRONZE_TRANSACTIONS;

     -- Check for duplicates
     SELECT
         'Duplicate transaction_ids' AS check_name,
         COUNT(*) AS issue_count
     FROM (
         SELECT transaction_id
         FROM BRONZE.BRONZE_TRANSACTIONS
         GROUP BY transaction_id
         HAVING COUNT(*) > 1
     );

     -- Check for nulls in critical fields
     SELECT
         'Null transaction_ids' AS check_name,
         COUNT(*) AS issue_count
     FROM BRONZE.BRONZE_TRANSACTIONS
     WHERE transaction_id IS NULL;

     SELECT
         'Null customer_ids' AS check_name,
         COUNT(*) AS issue_count
     FROM BRONZE.BRONZE_TRANSACTIONS
     WHERE customer_id IS NULL;

     -- Verify all customers have transactions
     SELECT
         'Customers without transactions' AS check_name,
         COUNT(*) AS issue_count
     FROM BRONZE.BRONZE_CUSTOMERS c
     WHERE NOT EXISTS (
         SELECT 1 FROM BRONZE.BRONZE_TRANSACTIONS t
         WHERE t.customer_id = c.customer_id
     );

     -- Transaction volume by month
     SELECT
         DATE_TRUNC('month', transaction_date) AS month,
         COUNT(*) AS transaction_count,
         ROUND(AVG(transaction_amount), 2) AS avg_amount
     FROM BRONZE.BRONZE_TRANSACTIONS
     GROUP BY DATE_TRUNC('month', transaction_date)
     ORDER BY month;
     ```

4. **scripts/load_all_bronze.sh**:
   - Master script to load all Bronze data:
     ```bash
     #!/bin/bash
     set -e

     echo "Loading Bronze layer data..."

     echo "Step 1: Creating Bronze tables..."
     snowsql -f snowflake/setup/06_create_bronze_tables.sql
     snowsql -f snowflake/setup/07_create_bronze_transaction_table.sql

     echo "Step 2: Loading customers..."
     snowsql -f snowflake/load/load_customers_bulk.sql
     snowsql -f snowflake/load/verify_customer_load.sql

     echo "Step 3: Loading transactions..."
     snowsql -f snowflake/load/load_transactions_bulk.sql
     snowsql -f snowflake/load/verify_transaction_load.sql

     echo "Bronze layer load complete!"
     ```

TESTING REQUIREMENTS:

- Create tests/integration/test_transaction_bulk_load.py:

  - test_bronze_transaction_table_created():
    - Verify table exists with expected columns

  - test_transaction_load_completes():
    - Run load_transactions_bulk.sql
    - Assert no errors

  - test_expected_row_count():
    - Count rows in BRONZE_TRANSACTIONS
    - Assert count between 10M and 15M

  - test_no_duplicate_transaction_ids():
    - Query for duplicates
    - Assert none found

  - test_all_customers_represented():
    - Count distinct customer_ids in transactions
    - Assert equals 50000

  - test_referential_integrity():
    - Query for transactions with customer_ids not in BRONZE_CUSTOMERS
    - Assert none found

  - test_date_range_valid():
    - Assert no NULL transaction_dates
    - Assert no future dates
    - Assert date range is ~18 months

  - test_transaction_amounts_valid():
    - Assert all amounts > 0
    - Assert reasonable max amount (< $10,000)

  - test_metadata_populated():
    - Assert ingestion_timestamp not null
    - Assert source_file contains 'transactions_historical'

PERFORMANCE TESTING:

- Create tests/performance/test_transaction_load_performance.py:
  - test_load_completes_within_time_limit():
    - Time the bulk load operation
    - Assert completes within 10 minutes (adjust based on warehouse)

  - test_query_performance_on_large_table():
    - Run aggregation query on 13.5M rows
    - Assert completes within 30 seconds

OBSERVABILITY:

- Log to OBSERVABILITY.LAYER_RECORD_COUNTS after load:
  ```sql
  INSERT INTO OBSERVABILITY.LAYER_RECORD_COUNTS
  SELECT
      'BULK_LOAD_TRANSACTIONS_' || CURRENT_TIMESTAMP()::STRING AS run_id,
      CURRENT_TIMESTAMP() AS run_timestamp,
      'BRONZE.BRONZE_TRANSACTIONS' AS model_name,
      COUNT(*) AS record_count
  FROM BRONZE.BRONZE_TRANSACTIONS;
  ```

DOCUMENTATION:

- Update main README.md with "Transaction Bulk Load (13.5M rows)"
- Document expected load time and row count
- Add troubleshooting for large data loads

OUTPUT:
- Bronze transactions table created
- 13.5M transactions loaded successfully
- All validation checks pass
- All tests pass: pytest tests/integration/test_transaction_bulk_load.py -v
- Observability logged
- Clear summary statistics printed
```

---

## Phase 3: dbt Transformations (Silver & Gold Layers)

### Iteration 3.1: dbt Project Setup & Silver Layer Foundation

**Context:** Bronze layer is loaded with raw data. Now set up dbt project for transformations.

**Prerequisites:**
- Iteration 2.5 complete
- dbt-snowflake installed
- Bronze layer populated

**Prompt 3.1:**

```
Initialize dbt project and create Silver layer staging models with data quality checks:

CONTEXT:
- dbt will transform Bronze → Silver → Gold
- Silver layer: cleaned, deduplicated, validated data
- Set up sources, tests, and documentation framework

REQUIREMENTS:

1. **dbt_customer_analytics/dbt_project.yml**:
   ```yaml
   name: 'customer_analytics'
   version: '1.0.0'
   config-version: 2

   profile: 'customer_analytics'

   model-paths: ["models"]
   test-paths: ["tests"]
   seed-paths: ["seeds"]
   macro-paths: ["macros"]

   target-path: "target"
   clean-targets:
     - "target"
     - "dbt_packages"

   models:
     customer_analytics:
       +materialized: table
       +schema: silver  # Default to silver schema

       staging:
         +materialized: view
         +schema: silver

       intermediate:
         +materialized: view
         +schema: silver

       marts:
         +materialized: table
         +schema: gold

   on-run-start:
     - "{{ log('dbt run started at ' ~ run_started_at, info=True) }}"
     - "INSERT INTO {{ target.database }}.OBSERVABILITY.PIPELINE_RUN_METADATA (run_id, run_timestamp, status) VALUES ('{{ invocation_id }}', '{{ run_started_at }}', 'STARTED')"

   on-run-end:
     - "{{ log('dbt run completed', info=True) }}"
     - "{% set status = 'SUCCESS' if results | selectattr('status', 'equalto', 'error') | list | length == 0 else 'FAILED' %}"
     - "INSERT INTO {{ target.database }}.OBSERVABILITY.PIPELINE_RUN_METADATA (run_id, run_timestamp, status, models_run, models_failed)
        VALUES ('{{ invocation_id }}', CURRENT_TIMESTAMP(), '{{ status }}', {{ results | length }}, {{ results | selectattr('status', 'equalto', 'error') | list | length }})"
   ```

2. **dbt_customer_analytics/profiles.yml** (for local dev):
   ```yaml
   customer_analytics:
     target: dev
     outputs:
       dev:
         type: snowflake
         account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
         user: "{{ env_var('SNOWFLAKE_USER') }}"
         password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
         role: DATA_ENGINEER
         database: CUSTOMER_ANALYTICS
         warehouse: COMPUTE_WH
         schema: silver
         threads: 4
         client_session_keep_alive: False
   ```

3. **dbt_customer_analytics/packages.yml**:
   ```yaml
   packages:
     - package: dbt-labs/dbt_utils
       version: 1.1.1
   ```

4. **dbt_customer_analytics/models/staging/_staging_sources.yml**:
   ```yaml
   version: 2

   sources:
     - name: bronze
       database: CUSTOMER_ANALYTICS
       schema: BRONZE
       tables:
         - name: bronze_customers
           description: "Raw customer data loaded from S3"
           columns:
             - name: customer_id
               description: "Unique customer identifier"
               tests:
                 - not_null

         - name: bronze_transactions
           description: "Raw transaction data (13.5M rows)"
           columns:
             - name: transaction_id
               description: "Unique transaction identifier"
             - name: customer_id
               description: "References bronze_customers"
   ```

5. **dbt_customer_analytics/models/staging/stg_customers.sql**:
   ```sql
   {{
       config(
           materialized='view'
       )
   }}

   WITH source AS (
       SELECT * FROM {{ source('bronze', 'bronze_customers') }}
   ),

   cleaned AS (
       SELECT
           -- IDs
           customer_id,

           -- Demographics
           TRIM(first_name) AS first_name,
           TRIM(last_name) AS last_name,
           LOWER(TRIM(email)) AS email,  -- Normalize email
           age,
           UPPER(TRIM(state)) AS state,  -- Normalize state
           TRIM(city) AS city,
           employment_status,

           -- Account details
           card_type,
           credit_limit,
           account_open_date,
           customer_segment,
           decline_type,

           -- Metadata
           ingestion_timestamp,
           source_file

       FROM source
   )

   SELECT * FROM cleaned
   ```

6. **dbt_customer_analytics/models/staging/stg_customers.yml**:
   ```yaml
   version: 2

   models:
     - name: stg_customers
       description: "Cleaned and normalized customer data from Bronze layer"
       columns:
         - name: customer_id
           description: "Unique customer identifier"
           tests:
             - unique
             - not_null

         - name: email
           description: "Customer email (normalized to lowercase)"
           tests:
             - not_null

         - name: state
           description: "US state abbreviation (normalized to uppercase)"
           tests:
             - not_null

         - name: credit_limit
           description: "Credit limit in USD"
           tests:
             - not_null
             - dbt_utils.accepted_range:
                 min_value: 5000
                 max_value: 50000
   ```

7. **dbt_customer_analytics/models/staging/stg_transactions.sql**:
   ```sql
   {{
       config(
           materialized='incremental',
           unique_key='transaction_id',
           on_schema_change='fail'
       )
   }}

   WITH source AS (
       SELECT * FROM {{ source('bronze', 'bronze_transactions') }}

       {% if is_incremental() %}
       -- Only process new records
       WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
       {% endif %}
   ),

   deduplicated AS (
       SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY transaction_id
               ORDER BY ingestion_timestamp DESC
           ) AS row_num
       FROM source
   ),

   -- Log duplicates to observability (if any found)
   duplicates AS (
       SELECT
           '{{ invocation_id }}' AS run_id,
           CURRENT_TIMESTAMP() AS run_timestamp,
           'silver' AS layer,
           'stg_transactions' AS table_name,
           'duplicate' AS check_type,
           COUNT(*) AS records_checked,
           SUM(CASE WHEN row_num > 1 THEN 1 ELSE 0 END) AS records_failed,
           ROUND(SUM(CASE WHEN row_num > 1 THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 4) AS failure_rate
       FROM deduplicated
   ),

   cleaned AS (
       SELECT
           -- IDs
           transaction_id,
           customer_id,

           -- Transaction details
           transaction_date,
           transaction_amount,
           TRIM(merchant_name) AS merchant_name,
           COALESCE(TRIM(merchant_category), 'Uncategorized') AS merchant_category,  -- Handle nulls
           channel,

           -- Metadata
           ingestion_timestamp,
           source_file

       FROM deduplicated
       WHERE row_num = 1  -- Keep only first occurrence
   )

   -- Insert duplicate stats into observability table
   -- (This is a simplified version; in production, use a macro or post-hook)

   SELECT * FROM cleaned
   ```

8. **dbt_customer_analytics/models/staging/stg_transactions.yml**:
   ```yaml
   version: 2

   models:
     - name: stg_transactions
       description: "Cleaned and deduplicated transactions from Bronze layer"
       columns:
         - name: transaction_id
           description: "Unique transaction identifier"
           tests:
             - unique
             - not_null

         - name: customer_id
           description: "References stg_customers"
           tests:
             - not_null
             - relationships:
                 to: ref('stg_customers')
                 field: customer_id

         - name: transaction_amount
           description: "Transaction amount in USD"
           tests:
             - not_null
             - dbt_utils.expression_is_true:
                 expression: "> 0"

         - name: merchant_category
           description: "Merchant category (Uncategorized if null)"
           tests:
             - not_null
   ```

9. **dbt_customer_analytics/README.md**:
   - Project overview
   - Setup instructions (dbt deps, dbt seed, dbt run, dbt test)
   - Model documentation
   - Folder structure explanation

TESTING REQUIREMENTS:

- Create tests/integration/test_dbt_setup.py:

  - test_dbt_project_compiles():
    - Run `dbt compile`
    - Assert no compilation errors

  - test_dbt_dependencies_install():
    - Run `dbt deps`
    - Assert dbt_utils package installed

  - test_sources_accessible():
    - Run `dbt run-operation test_source --args '{name: bronze, table: bronze_customers}'`
    - Assert source query succeeds

  - test_staging_models_build():
    - Run `dbt run --models staging`
    - Assert stg_customers and stg_transactions created

  - test_staging_model_tests_pass():
    - Run `dbt test --models staging`
    - Assert all tests pass (unique, not_null, relationships, accepted_range)

  - test_deduplication_works():
    - Insert duplicate transaction into Bronze (manual test data)
    - Run dbt run for stg_transactions
    - Assert only one record in Silver

  - test_incremental_load_works():
    - Run dbt run for stg_transactions (initial load)
    - Insert new transactions into Bronze
    - Run dbt run again (incremental)
    - Assert new records added, no duplicates

  - test_observability_logging():
    - Run dbt run
    - Query OBSERVABILITY.PIPELINE_RUN_METADATA
    - Assert run_id logged with status 'SUCCESS'

DOCUMENTATION:

- Generate dbt docs:
  ```bash
  dbt docs generate
  dbt docs serve
  ```

- Update main README.md with "dbt Transformations - Silver Layer"

OUTPUT:
- dbt project initialized and configured
- Sources defined and accessible
- Staging models (stg_customers, stg_transactions) created
- Deduplication logic working
- All dbt tests pass: `dbt test --models staging`
- Observability hooks logging runs
- dbt documentation generated and viewable
```

---

### Iteration 3.2: Gold Layer - Dimensional Model (Customers)

**Context:** Silver layer is ready with cleaned data. Now build Gold layer dimensional model starting with customer dimension.

**Prerequisites:**
- Iteration 3.1 complete
- Silver layer models tested

**Prompt 3.2:**

```
Create Gold layer customer dimension with SCD Type 2 tracking for card_type and credit_limit changes:

CONTEXT:
- Gold layer uses dimensional modeling (star schema)
- Track history of card_type and credit_limit changes (SCD Type 2)
- Other attributes are Type 1 (overwrite)
- This is the foundation for Customer 360 analytics

REQUIREMENTS:

1. **dbt_customer_analytics/models/marts/core/dim_customer.sql**:
   ```sql
   {{
       config(
           materialized='table',
           unique_key='customer_key',
           schema='gold'
       )
   }}

   {% if is_incremental() %}

   -- INCREMENTAL LOGIC: Implement SCD Type 2
   WITH current_dimension AS (
       SELECT * FROM {{ this }}
       WHERE is_current = TRUE
   ),

   source_data AS (
       SELECT
           customer_id,
           first_name,
           last_name,
           email,
           age,
           state,
           city,
           employment_status,
           card_type,
           credit_limit,
           account_open_date,
           customer_segment,
           decline_type
       FROM {{ ref('stg_customers') }}
   ),

   -- Detect changes in SCD Type 2 attributes
   changes AS (
       SELECT
           s.customer_id,
           s.card_type AS new_card_type,
           s.credit_limit AS new_credit_limit,
           c.card_type AS old_card_type,
           c.credit_limit AS old_credit_limit,
           CASE
               WHEN c.customer_id IS NULL THEN 'NEW'
               WHEN s.card_type != c.card_type OR s.credit_limit != c.credit_limit THEN 'CHANGED'
               ELSE 'NO_CHANGE'
           END AS change_type
       FROM source_data s
       LEFT JOIN current_dimension c ON s.customer_id = c.customer_id
   ),

   -- Expire old records
   expired_records AS (
       UPDATE {{ this }}
       SET
           valid_to = CURRENT_DATE() - 1,
           is_current = FALSE,
           updated_timestamp = CURRENT_TIMESTAMP()
       WHERE customer_id IN (
           SELECT customer_id FROM changes WHERE change_type = 'CHANGED'
       )
       AND is_current = TRUE
   ),

   -- Insert new and changed records
   new_records AS (
       SELECT
           {{ dbt_utils.generate_surrogate_key(['s.customer_id', 'CURRENT_TIMESTAMP()']) }} AS customer_key,
           s.customer_id,
           s.first_name,
           s.last_name,
           s.email,
           s.age,
           s.state,
           s.city,
           s.employment_status,
           s.card_type,
           s.credit_limit,
           s.account_open_date,
           s.customer_segment,
           s.decline_type,
           CURRENT_DATE() AS valid_from,
           NULL AS valid_to,
           TRUE AS is_current,
           CURRENT_TIMESTAMP() AS created_timestamp,
           CURRENT_TIMESTAMP() AS updated_timestamp
       FROM source_data s
       JOIN changes c ON s.customer_id = c.customer_id
       WHERE c.change_type IN ('NEW', 'CHANGED')
   )

   SELECT * FROM new_records

   {% else %}

   -- FULL REFRESH: Initial load, all records are current
   SELECT
       {{ dbt_utils.generate_surrogate_key(['customer_id', 'account_open_date']) }} AS customer_key,
       customer_id,
       first_name,
       last_name,
       email,
       age,
       state,
       city,
       employment_status,
       card_type,
       credit_limit,
       account_open_date,
       customer_segment,
       decline_type,
       account_open_date AS valid_from,
       NULL AS valid_to,
       TRUE AS is_current,
       CURRENT_TIMESTAMP() AS created_timestamp,
       CURRENT_TIMESTAMP() AS updated_timestamp
   FROM {{ ref('stg_customers') }}

   {% endif %}
   ```

2. **dbt_customer_analytics/models/marts/core/dim_customer.yml**:
   ```yaml
   version: 2

   models:
     - name: dim_customer
       description: "Customer dimension with SCD Type 2 tracking for card_type and credit_limit"
       columns:
         - name: customer_key
           description: "Surrogate key (unique for each version of customer)"
           tests:
             - unique
             - not_null

         - name: customer_id
           description: "Natural key (same customer can have multiple rows)"
           tests:
             - not_null

         - name: card_type
           description: "Card product type (tracked via SCD Type 2)"
           tests:
             - not_null
             - accepted_values:
                 values: ['Standard', 'Premium']

         - name: credit_limit
           description: "Credit limit (tracked via SCD Type 2)"
           tests:
             - not_null

         - name: valid_from
           description: "Start date of this version"
           tests:
             - not_null

         - name: is_current
           description: "Flag indicating current version of customer"
           tests:
             - not_null
   ```

3. **dbt_customer_analytics/tests/assert_scd_type_2_integrity.sql**:
   ```sql
   -- Custom test: Verify SCD Type 2 integrity
   -- Each customer should have exactly one current record

   SELECT
       customer_id,
       SUM(CASE WHEN is_current = TRUE THEN 1 ELSE 0 END) AS current_count
   FROM {{ ref('dim_customer') }}
   GROUP BY customer_id
   HAVING current_count != 1
   ```

4. **dbt_customer_analytics/macros/test_scd_type_2_no_gaps.sql**:
   ```sql
   {% macro test_scd_type_2_no_gaps(model, customer_id_column) %}

   -- Test: Verify no date gaps in SCD Type 2 history
   -- valid_to of one record should equal valid_from of next record (minus 1 day)

   WITH customer_history AS (
       SELECT
           {{ customer_id_column }},
           valid_from,
           valid_to,
           LEAD(valid_from) OVER (
               PARTITION BY {{ customer_id_column }}
               ORDER BY valid_from
           ) AS next_valid_from
       FROM {{ model }}
       WHERE valid_to IS NOT NULL  -- Exclude current records
   )

   SELECT *
   FROM customer_history
   WHERE valid_to != next_valid_from - INTERVAL '1 day'

   {% endmacro %}
   ```

TESTING REQUIREMENTS:

- Create tests/integration/test_dim_customer.py:

  - test_dim_customer_created():
    - Run `dbt run --models dim_customer`
    - Assert table created in GOLD schema

  - test_all_customers_represented():
    - Count distinct customer_ids in dim_customer
    - Assert equals 50,000

  - test_each_customer_has_one_current_record():
    - Run custom test assert_scd_type_2_integrity
    - Assert no rows returned

  - test_scd_type_2_initial_load():
    - After initial dbt run, all records should have is_current = TRUE
    - Assert count where is_current = TRUE equals total customer count

  - test_scd_type_2_change_detection():
    - Update card_type for a customer in Bronze layer
    - Run dbt run --models +dim_customer (staging + dimension)
    - Query dim_customer for that customer_id
    - Assert 2 records exist (one historical, one current)
    - Assert old record has is_current = FALSE and valid_to populated
    - Assert new record has is_current = TRUE and valid_to = NULL

  - test_scd_type_1_attributes_update():
    - Update first_name for a customer in Bronze (Type 1 change)
    - Run dbt run
    - Assert customer still has only 1 record (no history tracking for Type 1)
    - Assert first_name is updated

  - test_surrogate_key_generation():
    - Query dim_customer
    - Assert customer_key is unique across all records
    - Assert customer_key format is consistent

DOCUMENTATION:

- Add detailed model documentation to dim_customer.yml explaining:
  - SCD Type 2 attributes
  - SCD Type 1 attributes
  - How to query for current vs historical records
  - Example queries

- Update main README.md with "Gold Layer - Customer Dimension (SCD Type 2)"

OUTPUT:
- dim_customer table created in GOLD schema
- SCD Type 2 logic working for card_type and credit_limit changes
- All tests pass: `dbt test --models dim_customer`
- Custom SCD Type 2 integrity tests pass
- Clear documentation on querying dimension with history
```

---

### Iteration 3.3: Gold Layer - Fact Table (Transactions)

**Context:** Customer dimension is ready. Now create fact table for transactions.

**Prerequisites:**
- Iteration 3.2 complete
- dim_customer tested

**Prompt 3.3:**

```
Create Gold layer fact table for transactions linked to customer dimension:

CONTEXT:
- Fact table at transaction grain (13.5M rows)
- Foreign key to dim_customer using customer_key (current version)
- Include date_key for future date dimension
- Optimize for query performance with clustering

REQUIREMENTS:

1. **dbt_customer_analytics/models/marts/core/dim_date.sql**:
   ```sql
   {{
       config(
           materialized='table',
           schema='gold'
       )
   }}

   -- Generate date dimension for transaction date range
   WITH date_spine AS (
       SELECT
           DATEADD('day', SEQ4(), DATEADD('month', -18, CURRENT_DATE())) AS date_day
       FROM TABLE(GENERATOR(ROWCOUNT => 550))  -- 18 months + buffer
   )

   SELECT
       TO_NUMBER(TO_CHAR(date_day, 'YYYYMMDD')) AS date_key,
       date_day AS date,
       YEAR(date_day) AS year,
       QUARTER(date_day) AS quarter,
       MONTH(date_day) AS month,
       MONTHNAME(date_day) AS month_name,
       DAY(date_day) AS day_of_month,
       DAYOFWEEK(date_day) AS day_of_week,
       DAYNAME(date_day) AS day_name,
       WEEKOFYEAR(date_day) AS week_of_year,
       CASE WHEN DAYOFWEEK(date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
   FROM date_spine
   ```

2. **dbt_customer_analytics/models/marts/core/dim_merchant_category.sql**:
   ```sql
   {{
       config(
           materialized='table',
           schema='gold'
       )
   }}

   -- Simple dimension for merchant categories
   WITH categories AS (
       SELECT DISTINCT merchant_category
       FROM {{ ref('stg_transactions') }}
   )

   SELECT
       ROW_NUMBER() OVER (ORDER BY merchant_category) AS category_key,
       merchant_category AS category_name,
       CASE merchant_category
           WHEN 'Travel' THEN 'Leisure'
           WHEN 'Dining' THEN 'Leisure'
           WHEN 'Hotels' THEN 'Leisure'
           WHEN 'Airlines' THEN 'Leisure'
           WHEN 'Entertainment' THEN 'Leisure'
           WHEN 'Grocery' THEN 'Necessities'
           WHEN 'Gas' THEN 'Necessities'
           WHEN 'Utilities' THEN 'Necessities'
           WHEN 'Healthcare' THEN 'Necessities'
           ELSE 'Other'
       END AS category_group
   FROM categories
   ```

3. **dbt_customer_analytics/models/marts/core/fact_transaction.sql**:
   ```sql
   {{
       config(
           materialized='incremental',
           unique_key='transaction_key',
           schema='gold',
           cluster_by=['transaction_date']  -- Optimize for time-based queries
       )
   }}

   SELECT
       {{ dbt_utils.generate_surrogate_key(['t.transaction_id']) }} AS transaction_key,
       t.transaction_id,

       -- Foreign keys
       c.customer_key,
       TO_NUMBER(TO_CHAR(t.transaction_date, 'YYYYMMDD')) AS date_key,
       cat.category_key AS merchant_category_key,

       -- Transaction attributes
       t.transaction_date,
       t.transaction_amount,
       t.merchant_name,
       t.channel,

       -- Metadata
       t.ingestion_timestamp,
       t.source_file

   FROM {{ ref('stg_transactions') }} t

   -- Join to current customer dimension record
   LEFT JOIN {{ ref('dim_customer') }} c
       ON t.customer_id = c.customer_id
       AND c.is_current = TRUE

   -- Join to merchant category dimension
   LEFT JOIN {{ ref('dim_merchant_category') }} cat
       ON t.merchant_category = cat.category_name

   {% if is_incremental() %}
   WHERE t.ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
   {% endif %}
   ```

4. **dbt_customer_analytics/models/marts/core/schema.yml**:
   ```yaml
   version: 2

   models:
     - name: dim_date
       description: "Date dimension for time-based analysis"
       columns:
         - name: date_key
           description: "Surrogate key (YYYYMMDD format)"
           tests:
             - unique
             - not_null

     - name: dim_merchant_category
       description: "Merchant category dimension"
       columns:
         - name: category_key
           description: "Surrogate key"
           tests:
             - unique
             - not_null
         - name: category_name
           description: "Category name (matches transactions)"
           tests:
             - unique
             - not_null

     - name: fact_transaction
       description: "Transaction fact table (13.5M rows)"
       columns:
         - name: transaction_key
           description: "Surrogate key"
           tests:
             - unique
             - not_null

         - name: transaction_id
           description: "Natural key"
           tests:
             - unique
             - not_null

         - name: customer_key
           description: "FK to dim_customer"
           tests:
             - not_null
             - relationships:
                 to: ref('dim_customer')
                 field: customer_key

         - name: date_key
           description: "FK to dim_date"
           tests:
             - relationships:
                 to: ref('dim_date')
                 field: date_key

         - name: merchant_category_key
           description: "FK to dim_merchant_category"
           tests:
             - not_null
             - relationships:
                 to: ref('dim_merchant_category')
                 field: category_key

         - name: transaction_amount
           description: "Transaction amount in USD"
           tests:
             - not_null
             - dbt_utils.expression_is_true:
                 expression: "> 0"
   ```

TESTING REQUIREMENTS:

- Create tests/integration/test_fact_transaction.py:

  - test_dimensional_model_builds():
    - Run `dbt run --models marts.core`
    - Assert all dimension and fact tables created

  - test_fact_table_row_count():
    - Count rows in fact_transaction
    - Assert approximately 13.5M rows

  - test_all_fk_relationships_valid():
    - Run `dbt test --models fact_transaction --select test_type:relationships`
    - Assert all FK relationship tests pass

  - test_no_orphan_transactions():
    - Query for transactions with NULL customer_key
    - Assert none found

  - test_clustering_applied():
    - Query INFORMATION_SCHEMA to check clustering key
    - Assert transaction_date is clustering key

  - test_star_schema_query_performance():
    - Run sample star schema query joining fact to all dimensions
    - Assert completes within reasonable time (<10 seconds)
    - Example query:
      ```sql
      SELECT
          c.customer_segment,
          cat.category_group,
          d.year,
          d.month,
          COUNT(*) AS transaction_count,
          SUM(f.transaction_amount) AS total_amount
      FROM fact_transaction f
      JOIN dim_customer c ON f.customer_key = c.customer_key
      JOIN dim_merchant_category cat ON f.merchant_category_key = cat.category_key
      JOIN dim_date d ON f.date_key = d.date_key
      WHERE d.year = 2024
      GROUP BY c.customer_segment, cat.category_group, d.year, d.month
      ORDER BY total_amount DESC;
      ```

  - test_incremental_load_fact_table():
    - Insert new transactions into Silver layer
    - Run `dbt run --models fact_transaction`
    - Assert new transactions added to fact table
    - Assert no duplicates

DOCUMENTATION:

- Add star schema ERD to docs (can be ASCII art or mermaid diagram)
- Document join patterns and best practices
- Update main README.md with "Gold Layer - Star Schema Complete"

OUTPUT:
- Complete star schema in GOLD layer (dim_customer, dim_date, dim_merchant_category, fact_transaction)
- All tables built successfully
- All dbt tests pass: `dbt test --models marts.core`
- Referential integrity validated
- Query performance acceptable
- Clear documentation of dimensional model
```

---

### Iteration 3.4: Gold Layer - Customer Segmentation

**Context:** Star schema is complete. Now implement customer segmentation logic with rolling 90-day window.

**Prerequisites:**
- Iteration 3.3 complete
- Star schema tested

**Prompt 3.4:**

```
Create customer segmentation model that classifies customers into 5 segments based on spending patterns:

CONTEXT:
- 5 segments: High-Value Travelers, Stable Mid-Spenders, Budget-Conscious, Declining, New & Growing
- Use rolling 90-day window for dynamic recalculation
- Initial assignment based on full transaction history, then monthly updates

REQUIREMENTS:

1. **dbt_customer_analytics/models/marts/customer_analytics/customer_segments.sql**:
   ```sql
   {{
       config(
           materialized='table',
           schema='gold'
       )
   }}

   WITH customer_spending AS (
       SELECT
           c.customer_id,
           c.customer_key,

           -- Overall metrics (all-time)
           COUNT(f.transaction_key) AS total_transactions,
           SUM(f.transaction_amount) AS lifetime_value,
           AVG(f.transaction_amount) AS avg_transaction_value,
           MIN(f.transaction_date) AS first_transaction_date,
           MAX(f.transaction_date) AS last_transaction_date,

           -- Rolling 90-day metrics
           SUM(CASE
               WHEN f.transaction_date >= DATEADD('day', -90, CURRENT_DATE())
               THEN f.transaction_amount ELSE 0
           END) AS spend_last_90_days,

           SUM(CASE
               WHEN f.transaction_date >= DATEADD('day', -180, CURRENT_DATE())
                    AND f.transaction_date < DATEADD('day', -90, CURRENT_DATE())
               THEN f.transaction_amount ELSE 0
           END) AS spend_prior_90_days,

           -- Category analysis
           SUM(CASE
               WHEN cat.category_name IN ('Travel', 'Airlines', 'Hotels')
               THEN f.transaction_amount ELSE 0
           END) / NULLIF(SUM(f.transaction_amount), 0) * 100 AS travel_spend_pct,

           SUM(CASE
               WHEN cat.category_name IN ('Grocery', 'Gas', 'Utilities')
               THEN f.transaction_amount ELSE 0
           END) / NULLIF(SUM(f.transaction_amount), 0) * 100 AS necessities_spend_pct,

           -- Tenure
           DATEDIFF('month', MIN(f.transaction_date), CURRENT_DATE()) AS tenure_months

       FROM {{ ref('dim_customer') }} c
       LEFT JOIN {{ ref('fact_transaction') }} f
           ON c.customer_key = f.customer_key
       LEFT JOIN {{ ref('dim_merchant_category') }} cat
           ON f.merchant_category_key = cat.category_key
       WHERE c.is_current = TRUE
       GROUP BY c.customer_id, c.customer_key
   ),

   spending_trends AS (
       SELECT
           *,
           -- Calculate month-over-month change
           CASE
               WHEN spend_prior_90_days > 0
               THEN ((spend_last_90_days - spend_prior_90_days) / spend_prior_90_days) * 100
               ELSE 0
           END AS spend_change_pct,

           -- Monthly average
           spend_last_90_days / 3 AS avg_monthly_spend

       FROM customer_spending
   ),

   segment_assignment AS (
       SELECT
           *,
           CASE
               -- High-Value Travelers: High spend + travel focus
               WHEN avg_monthly_spend >= 5000
                    AND travel_spend_pct >= 25
               THEN 'High-Value Travelers'

               -- Declining: Significant spend drop
               WHEN spend_change_pct <= -30
                    AND spend_prior_90_days >= 2000
               THEN 'Declining'

               -- New & Growing: Recent customers with growth
               WHEN tenure_months <= 6
                    AND spend_change_pct >= 50
               THEN 'New & Growing'

               -- Budget-Conscious: Low spend + necessity focus
               WHEN avg_monthly_spend < 1500
                    AND necessities_spend_pct >= 60
               THEN 'Budget-Conscious'

               -- Stable Mid-Spenders: Default for consistent behavior
               ELSE 'Stable Mid-Spenders'

           END AS customer_segment,

           CURRENT_DATE() AS segment_assigned_date

       FROM spending_trends
   )

   SELECT * FROM segment_assignment
   ```

2. **dbt_customer_analytics/models/marts/customer_analytics/customer_segments.yml**:
   ```yaml
   version: 2

   models:
     - name: customer_segments
       description: "Customer segmentation based on spending patterns (rolling 90-day window)"
       columns:
         - name: customer_id
           description: "Customer identifier"
           tests:
             - unique
             - not_null

         - name: customer_segment
           description: "Assigned segment"
           tests:
             - not_null
             - accepted_values:
                 values: ['High-Value Travelers', 'Stable Mid-Spenders', 'Budget-Conscious', 'Declining', 'New & Growing']

         - name: avg_monthly_spend
           description: "Average monthly spend (last 90 days / 3)"
           tests:
             - not_null

         - name: spend_change_pct
           description: "Percentage change from prior 90 days to last 90 days"

         - name: lifetime_value
           description: "Total spending all-time"
           tests:
             - not_null
   ```

3. **dbt_customer_analytics/tests/assert_segment_distribution.sql**:
   ```sql
   -- Test: Verify segment distribution is reasonable
   -- Each segment should have at least 5% of customers

   WITH segment_counts AS (
       SELECT
           customer_segment,
           COUNT(*) AS customer_count,
           COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS percentage
       FROM {{ ref('customer_segments') }}
       GROUP BY customer_segment
   )

   SELECT
       customer_segment,
       percentage
   FROM segment_counts
   WHERE percentage < 5.0  -- Flag if any segment has < 5%
   ```

4. **dbt_customer_analytics/macros/recalculate_segments.sql**:
   ```sql
   {% macro recalculate_segments() %}
   -- Macro to refresh customer segments (run monthly)

   {{ log("Recalculating customer segments with rolling 90-day window...", info=True) }}

   {% set query %}
   -- Truncate and reload (full refresh pattern for segments)
   TRUNCATE TABLE {{ target.database }}.GOLD.CUSTOMER_SEGMENTS;

   INSERT INTO {{ target.database }}.GOLD.CUSTOMER_SEGMENTS
   SELECT * FROM {{ ref('customer_segments') }};
   {% endset %}

   {% do run_query(query) %}

   {{ log("Segment recalculation complete", info=True) }}

   {% endmacro %}
   ```

TESTING REQUIREMENTS:

- Create tests/integration/test_customer_segmentation.py:

  - test_customer_segments_builds():
    - Run `dbt run --models customer_segments`
    - Assert table created

  - test_all_customers_assigned_segment():
    - Query for customers with NULL segment
    - Assert none found

  - test_segment_distribution():
    - Query segment counts
    - Assert each segment has at least 5% of customers
    - Assert total percentages sum to 100%

  - test_high_value_travelers_criteria():
    - Query customers in High-Value Travelers segment
    - Assert all have avg_monthly_spend >= 5000
    - Assert all have travel_spend_pct >= 25

  - test_declining_segment_has_negative_growth():
    - Query Declining segment
    - Assert all have spend_change_pct <= -30

  - test_segment_recalculation():
    - Initial run: Record segment distribution
    - Update transaction data (add more transactions for some customers)
    - Run recalculate_segments macro
    - Assert segment distribution changed
    - Assert some customers moved segments

  - test_rolling_window_calculation():
    - Verify spend_last_90_days only includes transactions from last 90 days
    - Verify spend_prior_90_days covers days 91-180

PERFORMANCE TESTING:

- test_segmentation_query_performance():
  - Run customer_segments model
  - Assert completes within 2 minutes (for 50K customers, 13.5M transactions)

DOCUMENTATION:

- Create dbt docs page explaining segmentation logic
- Add segment definitions and criteria to customer_segments.yml
- Update main README.md with "Customer Segmentation - 5 Segments"

OUTPUT:
- customer_segments table created in GOLD
- All 50K customers assigned to segments
- Segment distribution matches expectations (~15% High-Value, ~40% Stable, etc.)
- All tests pass: `dbt test --models customer_segments`
- Macro for monthly recalculation documented
```

---

### Iteration 3.5: Gold Layer - Aggregate Marts (Metrics)

**Context:** Segmentation is working. Now create aggregate marts for key business metrics (CLV, MoM Spend Change, ATV, Customer 360).

**Prerequisites:**
- Iteration 3.4 complete
- Customer segments tested

**Prompt 3.5:**

```
Create aggregate mart models for business metrics used in dashboards:

CONTEXT:
- Pre-aggregate metrics for dashboard performance
- 3 hero metrics: CLV, MoM Spend Change %, ATV
- Create denormalized Customer 360 profile mart
- All marts in customer_analytics and marketing folders

REQUIREMENTS:

1. **dbt_customer_analytics/models/marts/marketing/metric_customer_ltv.sql**:
   ```sql
   {{
       config(
           materialized='table',
           schema='gold'
       )
   }}

   -- Customer Lifetime Value: Total spend over all time
   SELECT
       c.customer_id,
       c.customer_key,
       seg.customer_segment,
       SUM(f.transaction_amount) AS lifetime_value,
       COUNT(f.transaction_key) AS total_transactions,
       MIN(f.transaction_date) AS first_transaction_date,
       MAX(f.transaction_date) AS last_transaction_date,
       DATEDIFF('day', MIN(f.transaction_date), MAX(f.transaction_date)) AS customer_age_days,
       CURRENT_DATE() AS metric_calculated_date
   FROM {{ ref('dim_customer') }} c
   LEFT JOIN {{ ref('fact_transaction') }} f
       ON c.customer_key = f.customer_key
   LEFT JOIN {{ ref('customer_segments') }} seg
       ON c.customer_id = seg.customer_id
   WHERE c.is_current = TRUE
   GROUP BY c.customer_id, c.customer_key, seg.customer_segment
   ```

2. **dbt_customer_analytics/models/marts/marketing/metric_mom_spend_change.sql**:
   ```sql
   {{
       config(
           materialized='table',
           schema='gold'
       )
   }}

   -- Month-over-Month Spend Change Percentage
   WITH monthly_spend AS (
       SELECT
           c.customer_id,
           DATE_TRUNC('month', f.transaction_date) AS month,
           SUM(f.transaction_amount) AS monthly_spend
       FROM {{ ref('dim_customer') }} c
       LEFT JOIN {{ ref('fact_transaction') }} f
           ON c.customer_key = f.customer_key
       WHERE c.is_current = TRUE
       GROUP BY c.customer_id, DATE_TRUNC('month', f.transaction_date)
   )

   SELECT
       customer_id,
       month,
       monthly_spend,
       LAG(monthly_spend) OVER (
           PARTITION BY customer_id
           ORDER BY month
       ) AS prior_month_spend,
       CASE
           WHEN LAG(monthly_spend) OVER (PARTITION BY customer_id ORDER BY month) > 0
           THEN ((monthly_spend - LAG(monthly_spend) OVER (PARTITION BY customer_id ORDER BY month))
                 / LAG(monthly_spend) OVER (PARTITION BY customer_id ORDER BY month)) * 100
           ELSE NULL
       END AS mom_change_pct,
       CURRENT_DATE() AS metric_calculated_date
   FROM monthly_spend
   ```

3. **dbt_customer_analytics/models/marts/marketing/metric_avg_transaction_value.sql**:
   ```sql
   {{
       config(
           materialized='table',
           schema='gold'
       )
   }}

   -- Average Transaction Value per customer
   SELECT
       c.customer_id,
       c.customer_key,
       seg.customer_segment,
       AVG(f.transaction_amount) AS avg_transaction_value,
       STDDEV(f.transaction_amount) AS transaction_value_stddev,
       MIN(f.transaction_amount) AS min_transaction_value,
       MAX(f.transaction_amount) AS max_transaction_value,
       CURRENT_DATE() AS metric_calculated_date
   FROM {{ ref('dim_customer') }} c
   LEFT JOIN {{ ref('fact_transaction') }} f
       ON c.customer_key = f.customer_key
   LEFT JOIN {{ ref('customer_segments') }} seg
       ON c.customer_id = seg.customer_id
   WHERE c.is_current = TRUE
   GROUP BY c.customer_id, c.customer_key, seg.customer_segment
   ```

4. **dbt_customer_analytics/models/marts/customer_analytics/customer_360_profile.sql**:
   ```sql
   {{
       config(
           materialized='table',
           schema='gold'
       )
   }}

   -- Denormalized Customer 360 view for application
   SELECT
       c.customer_id,
       c.first_name || ' ' || c.last_name AS full_name,
       c.email,
       c.age,
       c.state,
       c.city,
       c.employment_status,
       c.card_type,
       c.credit_limit,
       c.account_open_date,

       -- Segmentation
       seg.customer_segment,
       seg.segment_assigned_date,

       -- Lifetime metrics
       ltv.lifetime_value,
       ltv.total_transactions,
       ltv.customer_age_days,

       -- Average transaction value
       atv.avg_transaction_value,

       -- Recent activity
       seg.spend_last_90_days,
       seg.spend_change_pct,
       seg.last_transaction_date,
       DATEDIFF('day', seg.last_transaction_date, CURRENT_DATE()) AS days_since_last_transaction,

       -- Category preferences (top 3 categories by spend)
       seg.travel_spend_pct,
       seg.necessities_spend_pct,

       -- Placeholder for churn risk score (will add in ML iteration)
       NULL AS churn_risk_score,
       CAST(NULL AS STRING) AS churn_risk_category,

       CURRENT_DATE() AS profile_updated_date

   FROM {{ ref('dim_customer') }} c
   JOIN {{ ref('customer_segments') }} seg
       ON c.customer_id = seg.customer_id
   JOIN {{ ref('metric_customer_ltv') }} ltv
       ON c.customer_id = ltv.customer_id
   JOIN {{ ref('metric_avg_transaction_value') }} atv
       ON c.customer_id = atv.customer_id
   WHERE c.is_current = TRUE
   ```

5. **dbt_customer_analytics/models/marts/schema.yml** (extend):
   ```yaml
   version: 2

   models:
     - name: metric_customer_ltv
       description: "Customer Lifetime Value calculation"
       columns:
         - name: customer_id
           tests:
             - unique
             - not_null
         - name: lifetime_value
           tests:
             - not_null

     - name: metric_mom_spend_change
       description: "Month-over-month spend change percentage"
       columns:
         - name: customer_id
           tests:
             - not_null
         - name: month
           tests:
             - not_null

     - name: metric_avg_transaction_value
       description: "Average transaction value per customer"
       columns:
         - name: customer_id
           tests:
             - unique
             - not_null
         - name: avg_transaction_value
           tests:
             - not_null

     - name: customer_360_profile
       description: "Denormalized Customer 360 view for dashboards"
       columns:
         - name: customer_id
           tests:
             - unique
             - not_null
         - name: full_name
           tests:
             - not_null
         - name: lifetime_value
           tests:
             - not_null
   ```

TESTING REQUIREMENTS:

- Create tests/integration/test_aggregate_marts.py:

  - test_all_marts_build():
    - Run `dbt run --models marts`
    - Assert all metric tables created

  - test_metric_customer_ltv():
    - Assert all 50K customers have LTV calculated
    - Assert LTV > 0 for customers with transactions
    - Test specific customer: verify LTV = SUM of their transactions

  - test_metric_mom_spend_change():
    - Assert monthly records exist for 18 months
    - Assert mom_change_pct calculated correctly
    - Test edge case: First month has NULL prior_month_spend

  - test_metric_avg_transaction_value():
    - Assert ATV > 0 for all customers
    - Verify calculation: ATV = SUM(amount) / COUNT(transactions)

  - test_customer_360_profile():
    - Assert all 50K customers in profile
    - Assert no NULL required fields
    - Test join integrity: All metrics present for each customer
    - Assert churn_risk_score is NULL (placeholder for ML iteration)

  - test_metrics_refresh():
    - Add new transactions to fact table
    - Re-run mart models
    - Assert metrics updated (LTV increased for affected customers)

PERFORMANCE TESTING:

- test_mart_query_performance():
  - Query customer_360_profile (SELECT * WHERE customer_id = 'CUST00012345')
  - Assert < 1 second response time
  - Query aggregate: SELECT segment, AVG(lifetime_value) FROM customer_360_profile GROUP BY segment
  - Assert < 3 seconds

DOCUMENTATION:

- Add metric definitions to schema.yml with business context
- Document calculation formulas
- Update main README.md with "Aggregate Marts - Business Metrics"

OUTPUT:
- 4 aggregate mart tables created in GOLD
- All metric calculations validated
- customer_360_profile ready for dashboard consumption
- All tests pass: `dbt test --models marts`
- Query performance meets targets
```

---

## Phase 4: ML & Semantic Layer

### Iteration 4.1: Churn Training Data Preparation

**Context:** Analytics marts are complete. Now prepare training data for churn prediction ML model.

**Prerequisites:**
- Iteration 3.5 complete
- customer_360_profile tested

**Prompt 4.1:**

```
Create training dataset for churn prediction model with labeled examples:

CONTEXT:
- Churn definition: No transactions for 60+ days OR spend < 30% of baseline
- Need features: spending trends, frequency, recency, demographics
- Train on months 1-15, validate on months 16-18
- Generate labels based on actual behavior in synthetic data

REQUIREMENTS:

1. **snowflake/ml/01_create_churn_labels.sql**:
   ```sql
   -- Create churn labels based on actual customer behavior
   CREATE OR REPLACE TABLE GOLD.CHURN_LABELS AS
   WITH customer_baseline AS (
       -- Calculate baseline spend (first 12 months)
       SELECT
           c.customer_id,
           AVG(monthly_spend) AS baseline_avg_spend
       FROM {{ ref('dim_customer') }} c
       JOIN (
           SELECT
               customer_id,
               DATE_TRUNC('month', transaction_date) AS month,
               SUM(transaction_amount) AS monthly_spend
           FROM {{ ref('fact_transaction') }} f
           JOIN {{ ref('dim_customer') }} c2 ON f.customer_key = c2.customer_key
           WHERE c2.is_current = TRUE
             AND transaction_date < DATEADD('month', -6, CURRENT_DATE())  -- Months 1-12
           GROUP BY customer_id, DATE_TRUNC('month', transaction_date)
       ) m ON c.customer_id = m.customer_id
       WHERE c.is_current = TRUE
       GROUP BY c.customer_id
   ),

   recent_behavior AS (
       -- Analyze last 3 months (months 16-18)
       SELECT
           c.customer_id,
           MAX(f.transaction_date) AS last_transaction_date,
           AVG(monthly_spend) AS recent_avg_spend
       FROM {{ ref('dim_customer') }} c
       LEFT JOIN (
           SELECT
               c2.customer_id,
               DATE_TRUNC('month', f.transaction_date) AS month,
               SUM(f.transaction_amount) AS monthly_spend,
               f.transaction_date
           FROM {{ ref('fact_transaction') }} f
           JOIN {{ ref('dim_customer') }} c2 ON f.customer_key = c2.customer_key
           WHERE c2.is_current = TRUE
             AND f.transaction_date >= DATEADD('month', -3, CURRENT_DATE())  -- Last 3 months
           GROUP BY c2.customer_id, DATE_TRUNC('month', f.transaction_date), f.transaction_date
       ) m ON c.customer_id = m.customer_id
       WHERE c.is_current = TRUE
       GROUP BY c.customer_id
   )

   SELECT
       b.customer_id,
       b.baseline_avg_spend,
       r.recent_avg_spend,
       r.last_transaction_date,
       DATEDIFF('day', r.last_transaction_date, CURRENT_DATE()) AS days_since_last_transaction,
       CASE
           WHEN r.recent_avg_spend IS NULL OR r.recent_avg_spend = 0
           THEN -100.0
           ELSE ((r.recent_avg_spend - b.baseline_avg_spend) / b.baseline_avg_spend) * 100
       END AS spend_change_pct,

       -- Churn label (TRUE if churned)
       CASE
           WHEN r.last_transaction_date IS NULL
                OR DATEDIFF('day', r.last_transaction_date, CURRENT_DATE()) > 60
           THEN TRUE
           WHEN r.recent_avg_spend < (b.baseline_avg_spend * 0.30)
           THEN TRUE
           ELSE FALSE
       END AS churned

   FROM customer_baseline b
   LEFT JOIN recent_behavior r ON b.customer_id = r.customer_id;
   ```

2. **snowflake/ml/02_create_training_features.sql**:
   ```sql
   -- Create feature table for ML model
   CREATE OR REPLACE TABLE GOLD.ML_TRAINING_DATA AS
   SELECT
       cp.customer_id,

       -- Demographics (features)
       cp.age,
       cp.state,
       cp.card_type,
       cp.credit_limit,
       cp.employment_status,

       -- Spending behavior (features)
       cp.lifetime_value,
       cp.avg_transaction_value,
       cp.total_transactions,
       cp.days_since_last_transaction,
       seg.spend_change_pct,
       seg.travel_spend_pct,
       seg.necessities_spend_pct,
       seg.avg_monthly_spend,

       -- Derived features
       CASE
           WHEN cp.total_transactions > 0
           THEN cp.lifetime_value / cp.total_transactions
           ELSE 0
       END AS avg_spend_per_transaction,

       CASE
           WHEN cp.credit_limit > 0
           THEN (seg.spend_last_90_days / 3) / cp.credit_limit
           ELSE 0
       END AS credit_utilization,

       DATEDIFF('month', cp.account_open_date, CURRENT_DATE()) AS tenure_months,

       -- Target variable
       labels.churned

   FROM {{ ref('customer_360_profile') }} cp
   JOIN {{ ref('customer_segments') }} seg ON cp.customer_id = seg.customer_id
   JOIN GOLD.CHURN_LABELS labels ON cp.customer_id = labels.customer_id
   WHERE labels.baseline_avg_spend IS NOT NULL;  -- Filter customers with sufficient history
   ```

3. **snowflake/ml/validate_training_data.sql**:
   ```sql
   -- Validation queries for training data

   -- Check 1: Row count
   SELECT 'Total training examples' AS check_name, COUNT(*) AS value
   FROM GOLD.ML_TRAINING_DATA;

   -- Check 2: Class balance
   SELECT
       'Class distribution' AS check_name,
       churned,
       COUNT(*) AS count,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
   FROM GOLD.ML_TRAINING_DATA
   GROUP BY churned;

   -- Check 3: Null features
   SELECT
       'Null avg_monthly_spend' AS check_name,
       COUNT(*) AS value
   FROM GOLD.ML_TRAINING_DATA
   WHERE avg_monthly_spend IS NULL;

   -- Check 4: Feature ranges
   SELECT
       'Feature statistics' AS check_name,
       ROUND(AVG(avg_monthly_spend), 2) AS avg_monthly_spend,
       ROUND(AVG(credit_utilization), 2) AS avg_credit_util,
       ROUND(AVG(tenure_months), 2) AS avg_tenure
   FROM GOLD.ML_TRAINING_DATA;
   ```

4. **snowflake/ml/README.md**:
   - Explain churn definition (60 days OR 30% decline)
   - Document feature engineering rationale
   - Describe training/validation split approach
   - List all features with descriptions

TESTING REQUIREMENTS:

- Create tests/integration/test_churn_training_data.py:

  - test_churn_labels_created():
    - Run 01_create_churn_labels.sql
    - Assert CHURN_LABELS table created
    - Assert has rows for most customers

  - test_churn_label_logic():
    - Query customers with no recent transactions
    - Assert labeled as churned = TRUE
    - Query stable customers (spending normally)
    - Assert labeled as churned = FALSE

  - test_training_features_created():
    - Run 02_create_training_features.sql
    - Assert ML_TRAINING_DATA table created
    - Assert all expected columns present

  - test_no_null_features():
    - Query for null values in critical features
    - Assert no nulls in: avg_monthly_spend, credit_limit, age, tenure_months

  - test_class_balance():
    - Query class distribution
    - Assert churned = TRUE is between 8-15% (realistic imbalance)
    - Assert churned = FALSE is majority class

  - test_feature_ranges():
    - Assert avg_monthly_spend > 0
    - Assert credit_utilization between 0 and 1
    - Assert tenure_months >= 0

  - test_sufficient_training_examples():
    - Count total rows
    - Assert >= 1000 training examples minimum

DOCUMENTATION:

- Create ML model card documenting:
  - Problem statement
  - Target variable definition
  - Feature list with descriptions
  - Expected class distribution
- Update main README.md with "ML Training Data Preparation"

OUTPUT:
- CHURN_LABELS table with labeled customers
- ML_TRAINING_DATA table with features ready for model training
- All validation checks pass
- Class balance reasonable (8-15% positive class)
- No null critical features
- Clear documentation of churn definition and features
```

---

### Iteration 4.2: Cortex ML Model Training & Predictions

**Context:** Training data is ready. Now train Cortex ML classification model and generate predictions.

**Prerequisites:**
- Iteration 4.1 complete
- ML_TRAINING_DATA validated

**Prompt 4.2:**

```
Train Snowflake Cortex ML model for churn prediction and apply to all customers:

CONTEXT:
- Use Cortex ML CLASSIFICATION for binary churn prediction
- Train on prepared features, evaluate with F1 score
- Apply predictions to all active customers
- Store results in customer_360_profile

REQUIREMENTS:

1. **snowflake/ml/03_train_churn_model.sql**:
   ```sql
   -- Train Cortex ML classification model

   -- Pre-training validation
   CALL VALIDATE_TRAINING_DATA();  -- Stored procedure from previous iteration

   -- Drop existing model if retraining
   DROP SNOWFLAKE.ML.CLASSIFICATION IF EXISTS CHURN_MODEL;

   -- Train model
   CREATE SNOWFLAKE.ML.CLASSIFICATION CHURN_MODEL(
       INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'GOLD.ML_TRAINING_DATA'),
       TARGET_COLNAME => 'churned',
       CONFIG_OBJECT => {
           'EVALUATION_METRIC': 'F1',
           'ON_ERROR': 'SKIP_ROW'
       }
   );

   -- Display model evaluation metrics
   SELECT * FROM TABLE(CHURN_MODEL!SHOW_EVALUATION_METRICS());

   -- Show feature importance
   SELECT * FROM TABLE(CHURN_MODEL!SHOW_GLOBAL_EVALUATION_METRICS());
   ```

2. **snowflake/ml/04_validate_model_performance.sql**:
   ```sql
   -- Validate model performance meets minimum thresholds

   WITH model_metrics AS (
       SELECT * FROM TABLE(CHURN_MODEL!SHOW_EVALUATION_METRICS())
   )

   SELECT
       CASE
           WHEN (SELECT F1_SCORE FROM model_metrics) < 0.50
           THEN 'FAIL: F1 score too low'
           WHEN (SELECT PRECISION FROM model_metrics) < 0.60
           THEN 'FAIL: Precision too low'
           WHEN (SELECT RECALL FROM model_metrics) < 0.40
           THEN 'FAIL: Recall too low'
           ELSE 'PASS: Model performance acceptable'
       END AS validation_result,
       (SELECT F1_SCORE FROM model_metrics) AS f1_score,
       (SELECT PRECISION FROM model_metrics) AS precision,
       (SELECT RECALL FROM model_metrics) AS recall;
   ```

3. **snowflake/ml/05_apply_predictions.sql**:
   ```sql
   -- Apply model predictions to all active customers

   CREATE OR REPLACE TABLE GOLD.CHURN_PREDICTIONS AS
   WITH customer_features AS (
       SELECT
           customer_id,
           age,
           state,
           card_type,
           credit_limit,
           employment_status,
           lifetime_value,
           avg_transaction_value,
           total_transactions,
           days_since_last_transaction,
           spend_change_pct,
           travel_spend_pct,
           necessities_spend_pct,
           avg_monthly_spend,
           -- Derived features (same as training)
           CASE
               WHEN total_transactions > 0
               THEN lifetime_value / total_transactions
               ELSE 0
           END AS avg_spend_per_transaction,
           CASE
               WHEN credit_limit > 0
               THEN (spend_last_90_days / 3) / credit_limit
               ELSE 0
           END AS credit_utilization,
           DATEDIFF('month', account_open_date, CURRENT_DATE()) AS tenure_months
       FROM {{ ref('customer_360_profile') }} cp
       JOIN {{ ref('customer_segments') }} seg ON cp.customer_id = seg.customer_id
   )

   SELECT
       customer_id,
       CHURN_MODEL!PREDICT(
           OBJECT_CONSTRUCT(
               'age', age,
               'card_type', card_type,
               'credit_limit', credit_limit,
               'employment_status', employment_status,
               'lifetime_value', lifetime_value,
               'avg_transaction_value', avg_transaction_value,
               'total_transactions', total_transactions,
               'days_since_last_transaction', days_since_last_transaction,
               'spend_change_pct', spend_change_pct,
               'travel_spend_pct', travel_spend_pct,
               'necessities_spend_pct', necessities_spend_pct,
               'avg_monthly_spend', avg_monthly_spend,
               'avg_spend_per_transaction', avg_spend_per_transaction,
               'credit_utilization', credit_utilization,
               'tenure_months', tenure_months
           )
       ) AS prediction_result,
       prediction_result['churned']::BOOLEAN AS predicted_churn,
       prediction_result['probability']::FLOAT * 100 AS churn_risk_score,
       CURRENT_DATE() AS prediction_date
   FROM customer_features;
   ```

4. **dbt_customer_analytics/models/marts/customer_analytics/customer_360_profile.sql** (update):
   ```sql
   -- Add churn predictions to Customer 360 profile
   -- (Replace NULL placeholders added in Iteration 3.5)

   {{
       config(
           materialized='table',
           schema='gold'
       )
   }}

   SELECT
       cp.*,
       pred.churn_risk_score,
       CASE
           WHEN pred.churn_risk_score >= 70 THEN 'High Risk'
           WHEN pred.churn_risk_score >= 40 THEN 'Medium Risk'
           ELSE 'Low Risk'
       END AS churn_risk_category
   FROM (
       -- Original Customer 360 logic from Iteration 3.5
       SELECT ...
   ) cp
   LEFT JOIN GOLD.CHURN_PREDICTIONS pred
       ON cp.customer_id = pred.customer_id;
   ```

5. **snowflake/ml/stored_procedures.sql**:
   ```sql
   -- Stored procedure for model retraining workflow
   CREATE OR REPLACE PROCEDURE RETRAIN_CHURN_MODEL()
   RETURNS STRING
   LANGUAGE SQL
   AS
   $$
   BEGIN
       -- Step 1: Refresh training data
       CALL SYSTEM$LOG('INFO', 'Refreshing training data...');
       EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE GOLD.ML_TRAINING_DATA AS SELECT * FROM ...' ;

       -- Step 2: Validate data
       LET validation_result := (CALL VALIDATE_TRAINING_DATA());
       IF (validation_result != 'PASS') THEN
           RETURN 'ERROR: Training data validation failed';
       END IF;

       -- Step 3: Train model
       CALL SYSTEM$LOG('INFO', 'Training model...');
       DROP SNOWFLAKE.ML.CLASSIFICATION IF EXISTS CHURN_MODEL;
       CREATE SNOWFLAKE.ML.CLASSIFICATION CHURN_MODEL(...);

       -- Step 4: Validate model performance
       LET f1_score := (SELECT F1_SCORE FROM TABLE(CHURN_MODEL!SHOW_EVALUATION_METRICS()));
       IF (f1_score < 0.50) THEN
           RETURN 'ERROR: Model F1 score below threshold: ' || f1_score;
       END IF;

       -- Step 5: Apply predictions
       CALL SYSTEM$LOG('INFO', 'Applying predictions...');
       EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE GOLD.CHURN_PREDICTIONS AS SELECT ...';

       RETURN 'SUCCESS: Model retrained with F1 score ' || f1_score;
   EXCEPTION
       WHEN OTHER THEN
           RETURN 'ERROR: ' || SQLERRM;
   END;
   $$;
   ```

TESTING REQUIREMENTS:

- Create tests/integration/test_churn_model.py:

  - test_model_trains_successfully():
    - Run 03_train_churn_model.sql
    - Assert model created without errors

  - test_model_performance_acceptable():
    - Query model metrics
    - Assert F1 score >= 0.50
    - Assert precision >= 0.60
    - Assert recall >= 0.40

  - test_predictions_generated():
    - Run 05_apply_predictions.sql
    - Assert CHURN_PREDICTIONS table has 50K rows (all customers)
    - Assert no NULL churn_risk_scores

  - test_churn_risk_score_distribution():
    - Query churn_risk_score distribution
    - Assert scores between 0 and 100
    - Assert reasonable spread (not all clustered)

  - test_high_risk_customers_make_sense():
    - Query High Risk customers (score >= 70)
    - Assert most have negative spend_change_pct
    - Assert most have high days_since_last_transaction

  - test_customer_360_updated_with_predictions():
    - Run dbt run --models customer_360_profile
    - Assert churn_risk_score column populated (not NULL)
    - Assert churn_risk_category assigned

  - test_model_retraining_procedure():
    - Call RETRAIN_CHURN_MODEL()
    - Assert returns 'SUCCESS'
    - Assert new predictions differ from old (model updated)

DOCUMENTATION:

- Create ML model card:
  - Model type: Cortex ML Binary Classification
  - Target variable: Churned (TRUE/FALSE)
  - Features: List all 17 features
  - Performance: F1, Precision, Recall scores
  - Interpretation: Churn risk score (0-100 scale)
  - Retraining schedule: Monthly
  - Limitations: Based on 18-month historical data, may not generalize to different time periods

- Update main README.md with "ML Model - Churn Prediction"

OUTPUT:
- CHURN_MODEL trained successfully
- F1 score >= 0.50 (acceptable performance)
- All 50K customers scored with churn_risk_score
- customer_360_profile updated with predictions
- All tests pass
- Retraining procedure documented and tested
```

---

### Iteration 4.3: Semantic Layer for Cortex Analyst

**Context:** Data model and ML predictions are complete. Now create semantic layer YAML for natural language queries.

**Prerequisites:**
- Iteration 4.2 complete
- customer_360_profile with churn scores

**Prompt 4.3:**

```
Create comprehensive semantic model for Snowflake Cortex Analyst to enable natural language queries:

CONTEXT:
- Cortex Analyst requires YAML semantic model defining tables, metrics, dimensions, relationships
- Enable business users to ask questions in plain English
- Cover customer profiles, transactions, segments, and metrics

REQUIREMENTS:

1. **semantic_layer/semantic_model.yaml**:
   ```yaml
   name: customer_analytics_semantic_model
   description: "Semantic layer for Customer 360 credit card analytics"

   # Base Tables
   tables:
     # Customer 360 Profile (main table)
     - name: customer_360_profile
       base_table: CUSTOMER_ANALYTICS.GOLD.CUSTOMER_360_PROFILE
       description: "Complete customer profile with demographics, spending, and churn risk"

       dimensions:
         - name: customer_id
           type: string
           description: "Unique customer identifier (format: CUST########)"
           synonyms: ["customer number", "account id", "customer code"]

         - name: full_name
           type: string
           description: "Customer full name"
           synonyms: ["name", "customer name"]

         - name: state
           type: string
           description: "US state of residence (two-letter code)"
           synonyms: ["location", "region", "state code"]

         - name: city
           type: string
           description: "City of residence"

         - name: customer_segment
           type: string
           description: "Customer segment based on spending patterns"
           synonyms: ["segment", "customer type", "category"]
           allowed_values:
             - "High-Value Travelers"
             - "Stable Mid-Spenders"
             - "Budget-Conscious"
             - "Declining"
             - "New & Growing"

         - name: card_type
           type: string
           description: "Credit card product type"
           synonyms: ["card product", "card tier"]
           allowed_values: ["Standard", "Premium"]

         - name: employment_status
           type: string
           description: "Current employment status"
           allowed_values: ["Employed", "Self-Employed", "Retired", "Unemployed"]

         - name: age
           type: number
           description: "Customer age in years"

         - name: churn_risk_category
           type: string
           description: "Churn risk level from ML model"
           synonyms: ["risk level", "churn category", "retention risk"]
           allowed_values: ["Low Risk", "Medium Risk", "High Risk"]

       metrics:
         - name: lifetime_value
           type: number
           aggregation: sum
           description: "Total amount spent by customer over all time"
           synonyms: ["LTV", "total spend", "total spending", "customer value"]
           format: "currency"

         - name: avg_transaction_value
           type: number
           aggregation: avg
           description: "Average amount per transaction"
           synonyms: ["ATV", "average purchase", "typical transaction"]
           format: "currency"

         - name: total_transactions
           type: number
           aggregation: sum
           description: "Total number of transactions"
           synonyms: ["transaction count", "number of purchases"]

         - name: churn_risk_score
           type: number
           aggregation: avg
           description: "ML-predicted churn risk probability (0-100 scale)"
           synonyms: ["risk score", "churn probability", "retention score"]

         - name: spend_last_90_days
           type: number
           aggregation: sum
           description: "Total spending in last 90 days"
           synonyms: ["recent spend", "quarterly spend"]
           format: "currency"

         - name: spend_change_pct
           type: number
           aggregation: avg
           description: "Percentage change in spending (last 90 vs prior 90 days)"
           synonyms: ["spending trend", "spend growth", "spending change"]
           format: "percentage"

         - name: days_since_last_transaction
           type: number
           aggregation: avg
           description: "Days since customer's last transaction"
           synonyms: ["recency", "last activity", "days inactive"]

         - name: credit_limit
           type: number
           aggregation: avg
           description: "Customer credit limit"
           format: "currency"

         - name: customer_count
           type: number
           aggregation: count
           description: "Count of customers"
           synonyms: ["number of customers", "customer total"]

     # Transaction Fact Table (for detailed queries)
     - name: fact_transaction
       base_table: CUSTOMER_ANALYTICS.GOLD.FACT_TRANSACTION
       description: "Individual credit card transactions"

       dimensions:
         - name: transaction_date
           type: date
           description: "Date and time of transaction"
           synonyms: ["transaction time", "purchase date"]

         - name: merchant_name
           type: string
           description: "Merchant where transaction occurred"
           synonyms: ["store", "vendor", "merchant"]

         - name: channel
           type: string
           description: "Transaction channel"
           allowed_values: ["Online", "In-Store", "Mobile"]

       metrics:
         - name: transaction_amount
           type: number
           aggregation: sum
           description: "Transaction dollar amount"
           synonyms: ["purchase amount", "spend", "amount"]
           format: "currency"

         - name: transaction_count
           type: number
           aggregation: count
           description: "Number of transactions"
           synonyms: ["transaction total", "purchase count"]

     # Merchant Category Dimension
     - name: dim_merchant_category
       base_table: CUSTOMER_ANALYTICS.GOLD.DIM_MERCHANT_CATEGORY
       description: "Merchant category classification"

       dimensions:
         - name: category_name
           type: string
           description: "Merchant category"
           synonyms: ["category", "merchant type", "spending category"]
           allowed_values:
             - "Travel"
             - "Dining"
             - "Hotels"
             - "Airlines"
             - "Grocery"
             - "Gas"
             - "Utilities"
             - "Healthcare"
             - "Entertainment"
             - "Retail"

         - name: category_group
           type: string
           description: "High-level category grouping"
           allowed_values: ["Leisure", "Necessities", "Other"]

   # Relationships between tables
   relationships:
     - from_table: fact_transaction
       to_table: customer_360_profile
       join_key: customer_id
       join_type: many_to_one
       description: "Transactions belong to customers"

     - from_table: fact_transaction
       to_table: dim_merchant_category
       join_key: merchant_category_key
       join_type: many_to_one
       description: "Transactions have merchant categories"

   # Sample questions for testing
   sample_questions:
     # Customer queries
     - "What is the average spend of customers in California?"
     - "Show me customers in the Northeast spending over $5K/month who don't have premium cards"
     - "How many High-Value Travelers are there?"
     - "Which customers have a lifetime value over $100,000?"

     # Churn queries
     - "Which acquired customers are at highest risk of churning this month?"
     - "Show me the top 10 customers by churn risk score"
     - "How many customers are in the High Risk churn category?"
     - "What is the average churn risk score for the Declining segment?"

     # Spending trend queries
     - "Show me spending trends in the travel category over the last 6 months"
     - "Which merchant categories are most popular among high-income customers?"
     - "What's the average transaction value for Premium cardholders?"

     # Segment queries
     - "Compare lifetime value across customer segments"
     - "Show me Budget-Conscious customers who increased spending"
     - "How many customers in each segment have premium cards?"

     # Time-based queries
     - "What was total spending in Q4 2024?"
     - "Show monthly transaction volume trends"
     - "Which customers haven't transacted in over 60 days?"

   # Query optimization hints
   optimization:
     - table: customer_360_profile
       recommended_filters: ["customer_segment", "state", "churn_risk_category"]
       clustering_keys: ["customer_id"]

     - table: fact_transaction
       recommended_filters: ["transaction_date"]
       clustering_keys: ["transaction_date"]
   ```

2. **semantic_layer/test_semantic_model.sql**:
   ```sql
   -- Test semantic model with Cortex Analyst
   -- These queries simulate natural language questions

   -- Test 1: Simple aggregation
   SELECT
       customer_segment,
       AVG(lifetime_value) AS avg_ltv
   FROM CUSTOMER_ANALYTICS.GOLD.CUSTOMER_360_PROFILE
   GROUP BY customer_segment;

   -- Test 2: Filtering
   SELECT
       full_name,
       state,
       lifetime_value
   FROM CUSTOMER_ANALYTICS.GOLD.CUSTOMER_360_PROFILE
   WHERE state IN ('CA', 'NY', 'TX')
     AND lifetime_value > 100000
   ORDER BY lifetime_value DESC
   LIMIT 10;

   -- Test 3: Churn risk query
   SELECT
       customer_segment,
       churn_risk_category,
       COUNT(*) AS customer_count
   FROM CUSTOMER_ANALYTICS.GOLD.CUSTOMER_360_PROFILE
   GROUP BY customer_segment, churn_risk_category;

   -- Test 4: Join with transactions
   SELECT
       cat.category_name,
       COUNT(f.transaction_key) AS transaction_count,
       SUM(f.transaction_amount) AS total_spend
   FROM CUSTOMER_ANALYTICS.GOLD.FACT_TRANSACTION f
   JOIN CUSTOMER_ANALYTICS.GOLD.DIM_MERCHANT_CATEGORY cat
       ON f.merchant_category_key = cat.category_key
   WHERE f.transaction_date >= DATEADD('month', -6, CURRENT_DATE())
   GROUP BY cat.category_name
   ORDER BY total_spend DESC;
   ```

3. **semantic_layer/README.md**:
   - Explain semantic layer purpose (enable natural language queries)
   - Document table relationships
   - List all metrics and dimensions
   - Provide example natural language questions
   - Explain how to deploy semantic model to Snowflake

4. **semantic_layer/deploy_semantic_model.sh**:
   ```bash
   #!/bin/bash
   # Deploy semantic model to Snowflake for Cortex Analyst

   # Upload YAML to Snowflake stage
   snowsql -q "PUT file://semantic_model.yaml @CUSTOMER_ANALYTICS.GOLD.SEMANTIC_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

   # Register semantic model with Cortex Analyst
   snowsql -q "
   CREATE OR REPLACE CORTEX SEARCH SERVICE customer_analytics_semantic
     ON semantic_model
     WAREHOUSE = COMPUTE_WH
     TARGET_LAG = '1 minute'
     AS SELECT * FROM @CUSTOMER_ANALYTICS.GOLD.SEMANTIC_STAGE/semantic_model.yaml;
   "

   echo "Semantic model deployed successfully"
   ```

TESTING REQUIREMENTS:

- Create tests/integration/test_semantic_layer.py:

  - test_semantic_model_valid_yaml():
    - Parse semantic_model.yaml
    - Assert valid YAML syntax
    - Assert all required keys present (tables, relationships, sample_questions)

  - test_all_tables_exist():
    - For each table in semantic model
    - Query INFORMATION_SCHEMA to verify table exists in Snowflake

  - test_all_metrics_calculable():
    - For each metric defined
    - Run sample query with that metric
    - Assert returns results

  - test_relationships_valid():
    - For each relationship
    - Query join between tables
    - Assert foreign key columns exist and joinable

  - test_sample_questions_answerable():
    - For each sample question
    - Manually convert to SQL (or use Cortex Analyst if available)
    - Run query
    - Assert returns meaningful results

  - test_cortex_analyst_integration():
    - If Cortex Analyst available in environment
    - Submit natural language query: "What is the average lifetime value?"
    - Assert receives SQL query response
    - Assert SQL executes successfully

DOCUMENTATION:

- Create user guide for natural language queries
- Document supported question types
- Provide tips for phrasing questions
- List example questions by use case (churn, segmentation, trends)
- Update main README.md with "Semantic Layer - Cortex Analyst"

OUTPUT:
- semantic_model.yaml created with 30+ metrics and dimensions
- All base tables, relationships documented
- Sample questions provided and tested
- Semantic model validated (YAML syntax, table references)
- Deploy script ready for Cortex Analyst registration
- Clear user documentation for asking questions
```

---

## Phase 5: Streamlit Application

### Iteration 5.1: Streamlit Foundation + Segment Explorer Tab

**Context:** Data platform is complete (data, ML, semantic layer). Now build Streamlit app starting with foundation and first tab.

**Prerequisites:**
- Iteration 4.3 complete
- All data marts and ML predictions ready

**Prompt 5.1:**

```
Create Streamlit application foundation with connection management, navigation, and Segment Explorer tab:

CONTEXT:
- Building multi-tab Streamlit app in Snowflake (SiS)
- First tab: Segment Explorer for marketing managers
- Need secure connection, session state, error handling

REQUIREMENTS:

1. **streamlit/app.py** (main application):
   ```python
   import streamlit as st
   import snowflake.connector
   from snowflake.connector.errors import DatabaseError, ProgrammingError
   import pandas as pd
   import plotly.express as px
   import plotly.graph_objects as go
   from datetime import datetime
   import os

   # Page configuration
   st.set_page_config(
       page_title="Customer 360 Analytics",
       page_icon="📊",
       layout="wide",
       initial_sidebar_state="expanded"
   )

   # ============= CONNECTION MANAGEMENT =============

   @st.cache_resource
   def get_snowflake_connection():
       """Create cached Snowflake connection"""
       try:
           conn = snowflake.connector.connect(
               account=os.getenv('SNOWFLAKE_ACCOUNT'),
               user=os.getenv('SNOWFLAKE_USER'),
               password=os.getenv('SNOWFLAKE_PASSWORD'),
               warehouse='COMPUTE_WH',
               database='CUSTOMER_ANALYTICS',
               schema='GOLD',
               role='DATA_ANALYST',
               client_session_keep_alive=True
           )
           return conn
       except Exception as e:
           st.error(f"Failed to connect to Snowflake: {e}")
           st.stop()

   def execute_query(query, params=None):
       """Execute Snowflake query with error handling"""
       conn = get_snowflake_connection()

       try:
           cursor = conn.cursor()
           cursor.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 60")

           if params:
               cursor.execute(query, params)
           else:
               cursor.execute(query)

           # Fetch results with size limit
           results = cursor.fetchmany(10000)
           columns = [desc[0] for desc in cursor.description]
           df = pd.DataFrame(results, columns=columns)

           cursor.close()
           return df

       except ProgrammingError as e:
           st.error(f"Query error: {e}")
           return pd.DataFrame()
       except DatabaseError as e:
           if "timeout" in str(e).lower():
               st.warning("Query timed out. Try filtering to a smaller dataset.")
           else:
               st.error(f"Database error: {e}")
           return pd.DataFrame()
       except Exception as e:
           st.error(f"Unexpected error: {e}")
           return pd.DataFrame()

   # ============= HEADER =============

   st.title("📊 Customer 360 Analytics Platform")
   st.markdown("**Post-Acquisition Credit Card Customer Intelligence**")
   st.markdown("---")

   # ============= SIDEBAR NAVIGATION =============

   with st.sidebar:
       st.header("Navigation")
       page = st.radio(
           "Select View",
           ["Segment Explorer", "Customer 360", "AI Assistant", "Campaign Performance"],
           index=0
       )

       st.markdown("---")
       st.markdown("### Platform Info")
       st.info(f"**Database:** CUSTOMER_ANALYTICS")
       st.info(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")

   # ============= MAIN CONTENT =============

   if page == "Segment Explorer":
       from tabs import segment_explorer
       segment_explorer.render(execute_query)

   elif page == "Customer 360":
       st.info("Customer 360 tab - Coming in next iteration")

   elif page == "AI Assistant":
       st.info("AI Assistant tab - Coming in next iteration")

   elif page == "Campaign Performance":
       st.info("Campaign Performance tab - Coming in next iteration")
   ```

2. **streamlit/tabs/segment_explorer.py**:
   ```python
   import streamlit as st
   import pandas as pd
   import plotly.express as px
   from datetime import datetime

   def render(execute_query):
       """Render Segment Explorer tab"""

       st.header("🎯 Customer Segment Explorer")
       st.markdown("Identify and export customer segments for targeted marketing campaigns")

       # ========== FILTERS ==========
       st.subheader("Filters")

       col1, col2, col3 = st.columns(3)

       with col1:
           # Segment filter
           segments = st.multiselect(
               "Customer Segment",
               ["High-Value Travelers", "Stable Mid-Spenders", "Budget-Conscious", "Declining", "New & Growing"],
               default=["High-Value Travelers", "Declining"]
           )

       with col2:
           # State filter
           states_query = "SELECT DISTINCT state FROM CUSTOMER_360_PROFILE ORDER BY state"
           states_df = execute_query(states_query)

           if not states_df.empty:
               all_states = ["All"] + states_df['STATE'].tolist()
               selected_states = st.multiselect("State", all_states, default=["All"])
           else:
               selected_states = ["All"]

       with col3:
           # Churn risk filter
           churn_risk = st.selectbox(
               "Churn Risk",
               ["All", "High Risk", "Medium Risk", "Low Risk"]
           )

       # Additional filters (expandable)
       with st.expander("Advanced Filters"):
           col1, col2 = st.columns(2)

           with col1:
               min_ltv = st.number_input("Min Lifetime Value ($)", min_value=0, value=0, step=1000)

           with col2:
               card_type = st.selectbox("Card Type", ["All", "Standard", "Premium"])

       # ========== BUILD QUERY ==========
       st.markdown("---")

       if st.button("Apply Filters", type="primary"):
           # Build WHERE clause
           where_clauses = []

           if segments:
               segments_str = "', '".join(segments)
               where_clauses.append(f"customer_segment IN ('{segments_str}')")

           if "All" not in selected_states:
               states_str = "', '".join(selected_states)
               where_clauses.append(f"state IN ('{states_str}')")

           if churn_risk != "All":
               where_clauses.append(f"churn_risk_category = '{churn_risk}'")

           if min_ltv > 0:
               where_clauses.append(f"lifetime_value >= {min_ltv}")

           if card_type != "All":
               where_clauses.append(f"card_type = '{card_type}'")

           where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

           # Execute query
           query = f"""
           SELECT
               customer_id,
               full_name,
               email,
               state,
               city,
               customer_segment,
               card_type,
               lifetime_value,
               avg_transaction_value,
               churn_risk_category,
               churn_risk_score,
               days_since_last_transaction
           FROM CUSTOMER_360_PROFILE
           WHERE {where_clause}
           ORDER BY lifetime_value DESC
           LIMIT 5000
           """

           with st.spinner("Loading customer data..."):
               df = execute_query(query)

           if not df.empty:
               # Store in session state
               st.session_state['filtered_customers'] = df
           else:
               st.warning("No customers match the selected filters.")

       # ========== DISPLAY RESULTS ==========

       if 'filtered_customers' in st.session_state:
           df = st.session_state['filtered_customers']

           # Summary metrics
           st.subheader("📈 Summary Metrics")
           col1, col2, col3, col4 = st.columns(4)

           with col1:
               st.metric("Customers", f"{len(df):,}")

           with col2:
               st.metric("Total LTV", f"${df['LIFETIME_VALUE'].sum():,.0f}")

           with col3:
               st.metric("Avg LTV", f"${df['LIFETIME_VALUE'].mean():,.0f}")

           with col4:
               avg_risk = df['CHURN_RISK_SCORE'].mean()
               st.metric("Avg Churn Risk", f"{avg_risk:.1f}%")

           # Visualizations
           st.subheader("📊 Segment Analysis")

           col1, col2 = st.columns(2)

           with col1:
               # Segment distribution pie chart
               segment_counts = df['CUSTOMER_SEGMENT'].value_counts().reset_index()
               segment_counts.columns = ['Segment', 'Count']

               fig_pie = px.pie(
                   segment_counts,
                   values='Count',
                   names='Segment',
                   title='Customer Segment Distribution'
               )
               st.plotly_chart(fig_pie, use_container_width=True)

           with col2:
               # Churn risk distribution
               risk_counts = df['CHURN_RISK_CATEGORY'].value_counts().reset_index()
               risk_counts.columns = ['Risk Level', 'Count']

               fig_bar = px.bar(
                   risk_counts,
                   x='Risk Level',
                   y='Count',
                   title='Churn Risk Distribution',
                   color='Risk Level',
                   color_discrete_map={'Low Risk': 'green', 'Medium Risk': 'orange', 'High Risk': 'red'}
               )
               st.plotly_chart(fig_bar, use_container_width=True)

           # LTV by segment
           segment_ltv = df.groupby('CUSTOMER_SEGMENT')['LIFETIME_VALUE'].agg(['mean', 'sum']).reset_index()

           fig_ltv = px.bar(
               segment_ltv,
               x='CUSTOMER_SEGMENT',
               y='sum',
               title='Total LTV by Segment',
               labels={'sum': 'Total LTV', 'CUSTOMER_SEGMENT': 'Segment'},
               text_auto='.2s'
           )
           st.plotly_chart(fig_ltv, use_container_width=True)

           # Customer data table
           st.subheader("👥 Customer List")

           # Format columns for display
           display_df = df.copy()
           display_df['LIFETIME_VALUE'] = display_df['LIFETIME_VALUE'].apply(lambda x: f"${x:,.0f}")
           display_df['AVG_TRANSACTION_VALUE'] = display_df['AVG_TRANSACTION_VALUE'].apply(lambda x: f"${x:,.0f}")
           display_df['CHURN_RISK_SCORE'] = display_df['CHURN_RISK_SCORE'].apply(lambda x: f"{x:.1f}%")

           st.dataframe(
               display_df,
               use_container_width=True,
               height=400
           )

           # Export functionality
           st.subheader("📥 Export Segment")

           csv = df.to_csv(index=False)

           st.download_button(
               label="Download as CSV",
               data=csv,
               file_name=f"customer_segment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
               mime="text/csv",
               type="primary"
           )

           st.info("💡 **Coming soon:** Direct export to Salesforce, HubSpot, and Google Ads")
   ```

3. **streamlit/requirements.txt**:
   ```
   streamlit==1.30.0
   snowflake-connector-python[pandas]==3.5.0
   pandas==2.1.4
   plotly==5.18.0
   python-dotenv==1.0.0
   ```

4. **streamlit/.env.example**:
   ```
   SNOWFLAKE_ACCOUNT=your_account.snowflakecomputing.com
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_PASSWORD=your_password
   ```

5. **streamlit/README.md**:
   - Setup instructions for local development
   - Instructions for deploying to Streamlit in Snowflake
   - Tab descriptions
   - Feature list

TESTING REQUIREMENTS:

- Create tests/integration/test_streamlit_segment_explorer.py:

  - test_streamlit_app_runs_locally():
    - Start Streamlit app in subprocess
    - Check if app is accessible on localhost
    - Verify no startup errors

  - test_snowflake_connection():
    - Import connection function
    - Test connection succeeds
    - Verify can query CUSTOMER_360_PROFILE

  - test_segment_filter_query():
    - Call execute_query with segment filter
    - Assert returns DataFrame
    - Assert has expected columns

  - test_export_csv():
    - Create sample DataFrame
    - Convert to CSV
    - Assert CSV has headers
    - Assert CSV has data rows

  - test_error_handling():
    - Mock Snowflake connection to fail
    - Verify error message displayed gracefully
    - Verify app doesn't crash

DOCUMENTATION:

- Screenshot placeholders for UI (to be added after implementation)
- User guide for Segment Explorer tab
- Document filter options and use cases
- Update main README.md with "Streamlit App - Segment Explorer"

OUTPUT:
- Streamlit app runs locally
- Segment Explorer tab fully functional
- Connection to Snowflake working
- Filters apply correctly
- Visualizations render
- CSV export works
- Error handling graceful
- Ready for deployment to Streamlit in Snowflake
```

---

### Iteration 5.2: Customer 360 Deep Dive Tab

**Context:** Segment Explorer tab complete. Now build individual customer profile view for customer service and account managers to investigate specific customers.

**Prerequisites:**
- Iteration 5.1 complete (Streamlit foundation working)
- GOLD.CUSTOMER_360_PROFILE table populated
- GOLD.FCT_TRANSACTIONS table populated

**Prompt 5.2:**

```
Create Customer 360 Deep Dive tab for detailed individual customer analysis:

CONTEXT:
- Tab for customer service reps and account managers
- Deep dive into single customer: profile, transaction history, trends
- Interactive charts showing spending patterns over time
- Transaction-level detail with filtering and search

REQUIREMENTS:

1. **streamlit/tabs/customer_360.py** (new tab module):
   ```python
   import streamlit as st
   import pandas as pd
   import plotly.express as px
   import plotly.graph_objects as go
   from datetime import datetime, timedelta


   def render_customer_360_tab(conn):
       """
       Render Customer 360 Deep Dive tab.

       Features:
       - Customer search (by ID, name, email)
       - Profile summary with key metrics
       - Spending trends over time
       - Transaction history table with filters
       - Category breakdown pie chart
       - Alerts for churn risk and unusual activity
       """
       st.title("🔍 Customer 360 Deep Dive")
       st.markdown("Detailed customer profile and transaction analysis")

       # ========== CUSTOMER SEARCH ==========
       st.subheader("🔎 Find Customer")

       search_method = st.radio(
           "Search by:",
           ["Customer ID", "Name", "Email"],
           horizontal=True
       )

       if search_method == "Customer ID":
           customer_id = st.number_input(
               "Enter Customer ID",
               min_value=1,
               max_value=100000,
               value=1,
               step=1
           )

           query = f"""
               SELECT *
               FROM GOLD.CUSTOMER_360_PROFILE
               WHERE customer_id = {customer_id}
           """

       elif search_method == "Name":
           name_search = st.text_input("Enter customer name (partial match)")

           if not name_search:
               st.info("Enter a name to search")
               return

           query = f"""
               SELECT *
               FROM GOLD.CUSTOMER_360_PROFILE
               WHERE LOWER(full_name) LIKE LOWER('%{name_search}%')
               LIMIT 20
           """

       else:  # Email
           email_search = st.text_input("Enter email (partial match)")

           if not email_search:
               st.info("Enter an email to search")
               return

           query = f"""
               SELECT *
               FROM GOLD.CUSTOMER_360_PROFILE
               WHERE LOWER(email) LIKE LOWER('%{email_search}%')
               LIMIT 20
           """

       # Execute search
       if st.button("Search", type="primary"):
           with st.spinner("Searching..."):
               cursor = conn.cursor()
               cursor.execute(query)
               results = cursor.fetchall()
               columns = [desc[0] for desc in cursor.description]
               cursor.close()

               if results:
                   df_results = pd.DataFrame(results, columns=columns)
                   st.session_state['search_results'] = df_results
               else:
                   st.warning("No customers found")
                   return

       # Display search results (if multiple)
       if 'search_results' in st.session_state:
           df_results = st.session_state['search_results']

           if len(df_results) > 1:
               st.subheader(f"Found {len(df_results)} customers")

               # Let user select
               selected_idx = st.selectbox(
                   "Select customer:",
                   range(len(df_results)),
                   format_func=lambda i: f"{df_results.iloc[i]['FULL_NAME']} ({df_results.iloc[i]['EMAIL']})"
               )

               customer = df_results.iloc[selected_idx]
           else:
               customer = df_results.iloc[0]

           # Store selected customer
           st.session_state['selected_customer'] = customer

       # ========== CUSTOMER PROFILE ==========

       if 'selected_customer' not in st.session_state:
           st.info("👆 Search for a customer to view their profile")
           return

       customer = st.session_state['selected_customer']
       customer_id = customer['CUSTOMER_ID']

       st.markdown("---")
       st.subheader("👤 Customer Profile")

       # Profile header
       col1, col2, col3 = st.columns([2, 1, 1])

       with col1:
           st.markdown(f"### {customer['FULL_NAME']}")
           st.markdown(f"**Email:** {customer['EMAIL']}")
           st.markdown(f"**Location:** {customer['CITY']}, {customer['STATE']}")
           st.markdown(f"**Segment:** {customer['CUSTOMER_SEGMENT']}")

       with col2:
           st.metric("Card Type", customer['CARD_TYPE'])
           st.metric("Credit Limit", f"${customer['CREDIT_LIMIT']:,.0f}")

       with col3:
           # Churn risk alert
           risk_score = customer['CHURN_RISK_SCORE'] if pd.notna(customer['CHURN_RISK_SCORE']) else 0
           risk_category = customer['CHURN_RISK_CATEGORY'] if pd.notna(customer['CHURN_RISK_CATEGORY']) else 'Unknown'

           if risk_category == 'High Risk':
               st.error(f"⚠️ High Churn Risk\n{risk_score:.1f}%")
           elif risk_category == 'Medium Risk':
               st.warning(f"⚡ Medium Churn Risk\n{risk_score:.1f}%")
           else:
               st.success(f"✅ Low Churn Risk\n{risk_score:.1f}%")

       # ========== KEY METRICS ==========

       st.subheader("📊 Key Metrics")

       col1, col2, col3, col4 = st.columns(4)

       with col1:
           st.metric("Lifetime Value", f"${customer['LIFETIME_VALUE']:,.0f}")

       with col2:
           st.metric("Avg Transaction", f"${customer['AVG_TRANSACTION_VALUE']:,.0f}")

       with col3:
           spend_90d = customer['SPEND_LAST_90_DAYS'] if pd.notna(customer['SPEND_LAST_90_DAYS']) else 0
           st.metric("Spend (90d)", f"${spend_90d:,.0f}")

       with col4:
           days_since_last = customer['DAYS_SINCE_LAST_TRANSACTION']
           st.metric("Days Since Last Txn", f"{days_since_last}")

       # Spending trend
       col1, col2 = st.columns(2)

       with col1:
           spend_change = customer['SPEND_CHANGE_PCT'] if pd.notna(customer['SPEND_CHANGE_PCT']) else 0
           delta_color = "normal" if spend_change >= 0 else "inverse"
           st.metric(
               "Spend Change (MoM)",
               f"{spend_change:+.1f}%",
               delta=f"{spend_change:+.1f}%",
               delta_color=delta_color
           )

       with col2:
           avg_monthly = customer['AVG_MONTHLY_SPEND'] if pd.notna(customer['AVG_MONTHLY_SPEND']) else 0
           st.metric("Avg Monthly Spend", f"${avg_monthly:,.0f}")

       # ========== TRANSACTION HISTORY ==========

       st.markdown("---")
       st.subheader("💳 Transaction History")

       # Fetch transactions
       txn_query = f"""
           SELECT
               t.transaction_date,
               t.merchant_name,
               c.category_name,
               c.category_group,
               t.transaction_amount,
               t.channel,
               t.status
           FROM GOLD.FCT_TRANSACTIONS t
           JOIN GOLD.DIM_MERCHANT_CATEGORY c ON t.merchant_category_key = c.category_key
           WHERE t.customer_id = {customer_id}
           ORDER BY t.transaction_date DESC
           LIMIT 1000
       """

       cursor = conn.cursor()
       cursor.execute(txn_query)
       txn_results = cursor.fetchall()
       txn_columns = [desc[0] for desc in cursor.description]
       cursor.close()

       if not txn_results:
           st.warning("No transactions found for this customer")
           return

       df_txns = pd.DataFrame(txn_results, columns=txn_columns)

       # Transaction filters
       col1, col2, col3 = st.columns(3)

       with col1:
           # Date range filter
           date_range = st.selectbox(
               "Time Period",
               ["Last 30 days", "Last 90 days", "Last 6 months", "All time"]
           )

           if date_range == "Last 30 days":
               cutoff_date = datetime.now() - timedelta(days=30)
           elif date_range == "Last 90 days":
               cutoff_date = datetime.now() - timedelta(days=90)
           elif date_range == "Last 6 months":
               cutoff_date = datetime.now() - timedelta(days=180)
           else:
               cutoff_date = datetime.min

           df_txns_filtered = df_txns[df_txns['TRANSACTION_DATE'] >= cutoff_date]

       with col2:
           # Category filter
           categories = ["All"] + sorted(df_txns['CATEGORY_NAME'].unique().tolist())
           selected_category = st.selectbox("Category", categories)

           if selected_category != "All":
               df_txns_filtered = df_txns_filtered[df_txns_filtered['CATEGORY_NAME'] == selected_category]

       with col3:
           # Status filter
           statuses = ["All"] + sorted(df_txns['STATUS'].unique().tolist())
           selected_status = st.selectbox("Status", statuses)

           if selected_status != "All":
               df_txns_filtered = df_txns_filtered[df_txns_filtered['STATUS'] == selected_status]

       # ========== VISUALIZATIONS ==========

       st.subheader("📈 Spending Trends")

       col1, col2 = st.columns(2)

       with col1:
           # Daily spending over time
           df_daily = df_txns_filtered.groupby('TRANSACTION_DATE')['TRANSACTION_AMOUNT'].sum().reset_index()
           df_daily = df_daily.sort_values('TRANSACTION_DATE')

           fig_trend = px.line(
               df_daily,
               x='TRANSACTION_DATE',
               y='TRANSACTION_AMOUNT',
               title='Daily Spending Over Time',
               labels={'TRANSACTION_AMOUNT': 'Amount ($)', 'TRANSACTION_DATE': 'Date'}
           )
           fig_trend.update_traces(line_color='#1f77b4', line_width=2)
           st.plotly_chart(fig_trend, use_container_width=True)

       with col2:
           # Category breakdown pie chart
           df_category = df_txns_filtered.groupby('CATEGORY_NAME')['TRANSACTION_AMOUNT'].sum().reset_index()
           df_category = df_category.sort_values('TRANSACTION_AMOUNT', ascending=False)

           fig_category = px.pie(
               df_category,
               values='TRANSACTION_AMOUNT',
               names='CATEGORY_NAME',
               title='Spending by Category'
           )
           st.plotly_chart(fig_category, use_container_width=True)

       # ========== TRANSACTION TABLE ==========

       st.subheader("📋 Transaction Details")

       # Summary stats
       col1, col2, col3, col4 = st.columns(4)

       with col1:
           st.metric("Total Transactions", f"{len(df_txns_filtered):,}")

       with col2:
           st.metric("Total Spend", f"${df_txns_filtered['TRANSACTION_AMOUNT'].sum():,.2f}")

       with col3:
           st.metric("Avg Transaction", f"${df_txns_filtered['TRANSACTION_AMOUNT'].mean():,.2f}")

       with col4:
           approved_pct = (df_txns_filtered['STATUS'] == 'approved').sum() / len(df_txns_filtered) * 100
           st.metric("Approval Rate", f"{approved_pct:.1f}%")

       # Transaction table
       display_df = df_txns_filtered.copy()
       display_df['TRANSACTION_DATE'] = pd.to_datetime(display_df['TRANSACTION_DATE']).dt.strftime('%Y-%m-%d')
       display_df['TRANSACTION_AMOUNT'] = display_df['TRANSACTION_AMOUNT'].apply(lambda x: f"${x:,.2f}")

       st.dataframe(
           display_df,
           use_container_width=True,
           height=400
       )

       # Export
       st.download_button(
           label="📥 Download Transaction History (CSV)",
           data=df_txns_filtered.to_csv(index=False),
           file_name=f"customer_{customer_id}_transactions_{datetime.now().strftime('%Y%m%d')}.csv",
           mime="text/csv"
       )
   ```

2. **Update streamlit/app.py** (add Customer 360 tab):
   ```python
   # Add import at top
   from tabs.customer_360 import render_customer_360_tab

   # In main() function, add new tab:
   tab1, tab2, tab3, tab4 = st.tabs([
       "📊 Segment Explorer",
       "🔍 Customer 360",
       "🤖 AI Assistant",
       "📈 Campaign Performance"
   ])

   with tab1:
       render_segment_explorer_tab(conn)

   with tab2:
       render_customer_360_tab(conn)

   with tab3:
       st.info("🚧 Coming in Iteration 5.3")

   with tab4:
       st.info("🚧 Coming in Iteration 5.4")
   ```

TESTING REQUIREMENTS:

- Create tests/integration/test_customer_360_tab.py:

  - test_customer_search_by_id():
    - Search for known customer_id
    - Verify customer profile loads
    - Assert key fields present (name, email, segment)

  - test_customer_search_by_name():
    - Search with partial name match
    - Verify multiple results returned
    - Assert search_results stored in session state

  - test_transaction_history_query():
    - Fetch transactions for customer
    - Assert DataFrame returned
    - Verify columns: transaction_date, merchant_name, transaction_amount

  - test_transaction_filters():
    - Apply date range filter (last 30 days)
    - Apply category filter (e.g., "Travel")
    - Assert filtered DataFrame has correct subset

  - test_spending_trend_chart():
    - Group transactions by date
    - Create line chart figure
    - Assert figure has data traces

  - test_category_breakdown():
    - Group by category_name
    - Create pie chart
    - Assert all categories represented

  - test_profile_metrics():
    - Verify lifetime_value displayed
    - Verify churn_risk_score displayed
    - Assert metrics formatted correctly ($, %)

  - test_export_transaction_csv():
    - Convert transaction DataFrame to CSV
    - Assert CSV has headers
    - Assert CSV has transaction rows

DOCUMENTATION:

- Add Customer 360 tab section to streamlit/README.md
- Document search methods (ID, name, email)
- Document transaction filters and visualizations
- Screenshot placeholders for Customer 360 UI
- Update main README.md with Customer 360 features

OUTPUT:
- Customer 360 tab fully functional
- Customer search works (ID, name, email)
- Profile displays all key metrics
- Transaction history loads with filters
- Spending trend chart renders
- Category breakdown pie chart renders
- CSV export works
- Churn risk alerts display correctly
- Integration tests pass
```

---

### Iteration 5.3: AI Assistant Tab (Cortex Analyst Integration)

**Context:** Customer 360 tab complete. Now integrate Cortex Analyst semantic layer for natural language queries.

**Prerequisites:**
- Iteration 5.2 complete
- Semantic layer deployed (semantic_model.yaml in Snowflake stage)
- Cortex Analyst enabled in Snowflake account

**Prompt 5.3:**

```
Create AI Assistant tab integrating Snowflake Cortex Analyst for natural language analytics:

CONTEXT:
- Enable business users to ask questions in plain English
- Leverage semantic_model.yaml (deployed in Iteration 4.3)
- Display generated SQL and results
- Provide suggested questions by use case

REQUIREMENTS:

1. **streamlit/tabs/ai_assistant.py** (new tab module):
   ```python
   import streamlit as st
   import pandas as pd
   from datetime import datetime


   # Suggested questions organized by use case
   SUGGESTED_QUESTIONS = {
       "Churn Analysis": [
           "Which customers are at highest risk of churning?",
           "What is the average churn risk score by segment?",
           "Show me High-Value Travelers with high churn risk",
           "Which states have the highest churn risk?",
       ],
       "Customer Segmentation": [
           "How many customers are in each segment?",
           "Compare lifetime value across segments",
           "Which segments have Premium cards?",
           "Show me Declining segment customers in California",
       ],
       "Spending Trends": [
           "What is the total spending in the last 90 days?",
           "Show spending trends in travel over last 6 months",
           "Which customers increased spending the most?",
           "What is the average transaction value by card type?",
       ],
       "Geographic Analysis": [
           "What is the average lifetime value by state?",
           "Which states have the most Premium cardholders?",
           "Show me customer distribution across states",
           "Compare spending between California and Texas",
       ],
       "Campaign Targeting": [
           "Show me customers eligible for retention campaigns",
           "Which Premium cardholders are at medium or high risk?",
           "Find customers with declining spend in the last 90 days",
           "Show high-value customers with low recent activity",
       ]
   }


   def call_cortex_analyst(conn, question: str) -> dict:
       """
       Call Snowflake Cortex Analyst to answer natural language question.

       Args:
           conn: Snowflake connection
           question: Natural language question

       Returns:
           dict with keys: sql, results, error
       """
       try:
           cursor = conn.cursor()

           # Call Cortex Analyst function
           # NOTE: Exact syntax may vary based on Snowflake version
           # Consult Snowflake Cortex Analyst documentation

           analyst_query = f"""
               SELECT SNOWFLAKE.CORTEX.ANALYST(
                   '{question}',
                   'CUSTOMER_ANALYTICS.GOLD.SEMANTIC_STAGE/semantic_model.yaml'
               ) AS response
           """

           cursor.execute(analyst_query)
           response = cursor.fetchone()[0]

           # Parse response (format may vary)
           # Assuming response contains generated SQL
           generated_sql = response.get('sql', '')

           # Execute generated SQL
           if generated_sql:
               cursor.execute(generated_sql)
               results = cursor.fetchall()
               columns = [desc[0] for desc in cursor.description]
               df = pd.DataFrame(results, columns=columns)

               cursor.close()

               return {
                   'sql': generated_sql,
                   'results': df,
                   'error': None
               }
           else:
               cursor.close()
               return {
                   'sql': None,
                   'results': None,
                   'error': 'No SQL generated'
               }

       except Exception as e:
           return {
               'sql': None,
               'results': None,
               'error': str(e)
           }


   def render_ai_assistant_tab(conn):
       """
       Render AI Assistant tab with Cortex Analyst integration.

       Features:
       - Natural language question input
       - Suggested questions by category
       - Generated SQL display
       - Results table
       - Query history
       """
       st.title("🤖 AI Assistant")
       st.markdown("Ask questions about your customers in plain English")

       # ========== SUGGESTED QUESTIONS ==========

       st.subheader("💡 Suggested Questions")

       # Category selector
       selected_category = st.selectbox(
           "Browse by category:",
           list(SUGGESTED_QUESTIONS.keys())
       )

       # Display suggested questions as clickable buttons
       st.markdown(f"**{selected_category}:**")

       cols = st.columns(2)
       for idx, question in enumerate(SUGGESTED_QUESTIONS[selected_category]):
           with cols[idx % 2]:
               if st.button(question, key=f"suggested_{selected_category}_{idx}"):
                   st.session_state['current_question'] = question

       st.markdown("---")

       # ========== QUESTION INPUT ==========

       st.subheader("❓ Ask Your Question")

       # Text input for custom question
       default_question = st.session_state.get('current_question', '')

       question = st.text_area(
           "Enter your question:",
           value=default_question,
           height=100,
           placeholder="e.g., Which customers spent more than $10,000 in the last 90 days?"
       )

       col1, col2, col3 = st.columns([1, 1, 4])

       with col1:
           ask_button = st.button("🚀 Ask", type="primary")

       with col2:
           clear_button = st.button("🔄 Clear")

       if clear_button:
           st.session_state['current_question'] = ''
           st.session_state.pop('last_response', None)
           st.rerun()

       # ========== QUERY EXECUTION ==========

       if ask_button and question:
           with st.spinner("🤔 Thinking..."):
               response = call_cortex_analyst(conn, question)
               st.session_state['last_response'] = response
               st.session_state['last_question'] = question

               # Add to history
               if 'query_history' not in st.session_state:
                   st.session_state['query_history'] = []

               st.session_state['query_history'].append({
                   'timestamp': datetime.now(),
                   'question': question,
                   'response': response
               })

       # ========== DISPLAY RESULTS ==========

       if 'last_response' in st.session_state:
           response = st.session_state['last_response']
           question = st.session_state.get('last_question', '')

           st.markdown("---")
           st.subheader("📊 Results")

           if response['error']:
               st.error(f"❌ Error: {response['error']}")

               st.info("""
               **Troubleshooting Tips:**
               - Rephrase your question to be more specific
               - Use terms from the semantic model (segment, state, churn risk, etc.)
               - Try one of the suggested questions above
               - Ensure Cortex Analyst is enabled in your Snowflake account
               """)

           else:
               # Display question
               st.markdown(f"**Question:** {question}")

               # Display generated SQL
               with st.expander("🔍 View Generated SQL", expanded=False):
                   st.code(response['sql'], language='sql')

               # Display results
               df = response['results']

               if df is not None and not df.empty:
                   st.success(f"✅ Found {len(df)} results")

                   # Summary metrics (if applicable)
                   if len(df) < 20 and len(df.columns) <= 5:
                       # Display as cards for small result sets
                       cols = st.columns(min(len(df.columns), 4))

                       for idx, col_name in enumerate(df.columns[:4]):
                           with cols[idx]:
                               if pd.api.types.is_numeric_dtype(df[col_name]):
                                   value = df[col_name].iloc[0] if len(df) == 1 else df[col_name].sum()
                                   if col_name.lower() in ['lifetime_value', 'total_spend', 'amount']:
                                       st.metric(col_name, f"${value:,.0f}")
                                   else:
                                       st.metric(col_name, f"{value:,.0f}")

                   # Results table
                   st.dataframe(df, use_container_width=True, height=400)

                   # Export
                   st.download_button(
                       label="📥 Download Results (CSV)",
                       data=df.to_csv(index=False),
                       file_name=f"cortex_analyst_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                       mime="text/csv"
                   )

               else:
                   st.warning("No results found")

       # ========== QUERY HISTORY ==========

       if st.session_state.get('query_history'):
           st.markdown("---")
           st.subheader("📜 Query History")

           history = st.session_state['query_history']

           # Display last 5 queries
           for idx, item in enumerate(reversed(history[-5:])):
               with st.expander(f"{item['timestamp'].strftime('%H:%M:%S')} - {item['question'][:50]}..."):
                   st.markdown(f"**Question:** {item['question']}")

                   if item['response']['error']:
                       st.error(f"Error: {item['response']['error']}")
                   else:
                       st.code(item['response']['sql'], language='sql')

                       if item['response']['results'] is not None:
                           st.dataframe(item['response']['results'], use_container_width=True)

       # ========== HELP SECTION ==========

       st.markdown("---")

       with st.expander("ℹ️ How to Use AI Assistant"):
           st.markdown("""
           **Tips for asking questions:**

           1. **Be specific:** Instead of "Show customers", try "Show customers in California with high churn risk"

           2. **Use domain terms:** The AI understands:
              - Customer segments: High-Value Travelers, Declining, New & Growing, Budget-Conscious, Stable Mid-Spenders
              - Churn risk: High Risk, Medium Risk, Low Risk
              - Card types: Standard, Premium
              - Metrics: lifetime value, churn risk score, spend last 90 days

           3. **Time periods:** Specify timeframes like "last 30 days", "last 90 days", "last 6 months"

           4. **Comparisons:** Ask to "compare" segments, states, or time periods

           5. **Filters:** Combine multiple criteria: "Premium cardholders in Texas with declining spend"

           **Powered by Snowflake Cortex Analyst**
           """)
   ```

2. **Update streamlit/app.py** (add AI Assistant tab):
   ```python
   # Add import at top
   from tabs.ai_assistant import render_ai_assistant_tab

   # Update tabs in main():
   with tab3:
       render_ai_assistant_tab(conn)
   ```

3. **Fallback for testing without Cortex Analyst**:

   If Cortex Analyst is not yet available, create a mock implementation:

   ```python
   # In ai_assistant.py, add fallback function:

   def call_cortex_analyst_mock(conn, question: str) -> dict:
       """
       Mock Cortex Analyst for testing when Cortex Analyst not available.
       Maps common questions to pre-written SQL.
       """
       question_lower = question.lower()

       # Map questions to SQL
       if 'highest risk' in question_lower and 'churn' in question_lower:
           sql = """
               SELECT customer_id, full_name, email, customer_segment,
                      churn_risk_score, churn_risk_category
               FROM GOLD.CUSTOMER_360_PROFILE
               WHERE churn_risk_category = 'High Risk'
               ORDER BY churn_risk_score DESC
               LIMIT 100
           """

       elif 'customers in each segment' in question_lower:
           sql = """
               SELECT customer_segment, COUNT(*) AS customer_count
               FROM GOLD.CUSTOMER_360_PROFILE
               GROUP BY customer_segment
               ORDER BY customer_count DESC
           """

       elif 'lifetime value' in question_lower and 'segment' in question_lower:
           sql = """
               SELECT customer_segment,
                      AVG(lifetime_value) AS avg_ltv,
                      COUNT(*) AS customer_count
               FROM GOLD.CUSTOMER_360_PROFILE
               GROUP BY customer_segment
               ORDER BY avg_ltv DESC
           """

       else:
           return {
               'sql': None,
               'results': None,
               'error': 'Question not recognized by mock. Try a suggested question.'
           }

       # Execute SQL
       try:
           cursor = conn.cursor()
           cursor.execute(sql)
           results = cursor.fetchall()
           columns = [desc[0] for desc in cursor.description]
           df = pd.DataFrame(results, columns=columns)
           cursor.close()

           return {
               'sql': sql,
               'results': df,
               'error': None
           }
       except Exception as e:
           return {
               'sql': sql,
               'results': None,
               'error': str(e)
           }
   ```

TESTING REQUIREMENTS:

- Create tests/integration/test_ai_assistant_tab.py:

  - test_suggested_questions_display():
    - Verify SUGGESTED_QUESTIONS dictionary populated
    - Assert all categories present
    - Assert each category has questions

  - test_cortex_analyst_mock():
    - Call call_cortex_analyst_mock with known question
    - Assert SQL returned
    - Assert results DataFrame returned
    - Verify no errors

  - test_question_execution():
    - Submit question via mock
    - Verify response stored in session_state
    - Assert results displayed

  - test_query_history():
    - Execute multiple questions
    - Verify query_history populated
    - Assert timestamps recorded

  - test_csv_export():
    - Generate results DataFrame
    - Convert to CSV
    - Assert CSV has data

  - test_error_handling():
    - Submit invalid question
    - Verify error message displayed
    - Assert app doesn't crash

DOCUMENTATION:

- Add AI Assistant section to streamlit/README.md
- Document suggested questions and categories
- Document Cortex Analyst integration
- Screenshot placeholders for AI Assistant UI
- Update main README.md with AI Assistant features
- Document fallback mock for testing

OUTPUT:
- AI Assistant tab functional
- Suggested questions display by category
- Question input and execution works
- Generated SQL displayed in expander
- Results table renders
- Query history tracks recent questions
- CSV export works
- Error handling graceful
- Mock fallback works for testing
- Ready for Cortex Analyst integration
```

---

### Iteration 5.4: Campaign Performance Simulator Tab

**Context:** AI Assistant complete. Now build campaign ROI simulator for marketing managers to model retention campaigns.

**Prerequisites:**
- Iteration 5.3 complete
- GOLD.CUSTOMER_360_PROFILE with churn predictions available
- Customer segments defined

**Prompt 5.4:**

```
Create Campaign Performance Simulator tab for marketing ROI analysis:

CONTEXT:
- Marketing managers need to simulate retention campaign performance
- Target high-risk customers with incentive offers
- Calculate ROI based on retention rate assumptions and customer LTV
- Allow scenario modeling with different parameters

REQUIREMENTS:

1. **streamlit/tabs/campaign_simulator.py** (new tab module):
   ```python
   import streamlit as st
   import pandas as pd
   import plotly.express as px
   import plotly.graph_objects as go
   from datetime import datetime


   def calculate_campaign_roi(
       target_customers: pd.DataFrame,
       incentive_per_customer: float,
       expected_retention_rate: float,
       campaign_cost_per_customer: float
   ) -> dict:
       """
       Calculate ROI for retention campaign.

       Args:
           target_customers: DataFrame of customers to target
           incentive_per_customer: $ incentive offered (e.g., $50 statement credit)
           expected_retention_rate: % of customers expected to be retained (0-100)
           campaign_cost_per_customer: $ cost to run campaign per customer

       Returns:
           dict with ROI metrics
       """
       num_customers = len(target_customers)
       total_ltv = target_customers['LIFETIME_VALUE'].sum()
       avg_ltv = target_customers['LIFETIME_VALUE'].mean()

       # Costs
       total_incentive_cost = num_customers * incentive_per_customer
       total_campaign_cost = num_customers * campaign_cost_per_customer
       total_cost = total_incentive_cost + total_campaign_cost

       # Expected retention
       expected_retained_customers = int(num_customers * (expected_retention_rate / 100))

       # Assume retained customers continue spending at current rate
       # Use avg_monthly_spend * 12 months as proxy for annual value
       expected_retained_value = expected_retained_customers * avg_ltv * 0.20  # 20% of LTV as annual value

       # ROI calculation
       net_benefit = expected_retained_value - total_cost
       roi_pct = (net_benefit / total_cost * 100) if total_cost > 0 else 0

       return {
           'num_customers': num_customers,
           'total_cost': total_cost,
           'incentive_cost': total_incentive_cost,
           'campaign_cost': total_campaign_cost,
           'expected_retained_customers': expected_retained_customers,
           'expected_retained_value': expected_retained_value,
           'net_benefit': net_benefit,
           'roi_pct': roi_pct,
           'cost_per_retained_customer': total_cost / expected_retained_customers if expected_retained_customers > 0 else 0
       }


   def render_campaign_simulator_tab(conn):
       """
       Render Campaign Performance Simulator tab.

       Features:
       - Target audience selector (segment, churn risk)
       - Campaign parameter inputs (incentive, retention rate, costs)
       - ROI calculation and visualization
       - Scenario comparison
       - Export campaign target list
       """
       st.title("📈 Campaign Performance Simulator")
       st.markdown("Model retention campaign ROI and target audiences")

       # ========== TARGET AUDIENCE ==========

       st.subheader("🎯 Define Target Audience")

       col1, col2, col3 = st.columns(3)

       with col1:
           # Segment filter
           segment_options = st.multiselect(
               "Customer Segments",
               ["High-Value Travelers", "Declining", "New & Growing", "Budget-Conscious", "Stable Mid-Spenders"],
               default=["Declining"]
           )

       with col2:
           # Churn risk filter
           churn_risk_options = st.multiselect(
               "Churn Risk Levels",
               ["High Risk", "Medium Risk", "Low Risk"],
               default=["High Risk", "Medium Risk"]
           )

       with col3:
           # Card type filter
           card_type_options = st.multiselect(
               "Card Types",
               ["Standard", "Premium"],
               default=["Standard", "Premium"]
           )

       # Advanced filters
       with st.expander("🔧 Advanced Filters"):
           col1, col2 = st.columns(2)

           with col1:
               min_ltv = st.number_input(
                   "Min Lifetime Value ($)",
                   min_value=0,
                   value=5000,
                   step=1000
               )

           with col2:
               min_churn_score = st.number_input(
                   "Min Churn Risk Score",
                   min_value=0,
                   max_value=100,
                   value=40,
                   step=5
               )

       # Build query
       if st.button("🔍 Find Target Audience", type="primary"):
           where_clauses = []

           if segment_options:
               segments_str = "', '".join(segment_options)
               where_clauses.append(f"customer_segment IN ('{segments_str}')")

           if churn_risk_options:
               risk_str = "', '".join(churn_risk_options)
               where_clauses.append(f"churn_risk_category IN ('{risk_str}')")

           if card_type_options:
               card_str = "', '".join(card_type_options)
               where_clauses.append(f"card_type IN ('{card_str}')")

           where_clauses.append(f"lifetime_value >= {min_ltv}")
           where_clauses.append(f"churn_risk_score >= {min_churn_score}")

           where_clause = " AND ".join(where_clauses)

           query = f"""
               SELECT
                   customer_id,
                   full_name,
                   email,
                   customer_segment,
                   churn_risk_category,
                   churn_risk_score,
                   card_type,
                   lifetime_value,
                   avg_monthly_spend,
                   spend_last_90_days,
                   state
               FROM GOLD.CUSTOMER_360_PROFILE
               WHERE {where_clause}
               ORDER BY churn_risk_score DESC, lifetime_value DESC
           """

           with st.spinner("Finding target audience..."):
               cursor = conn.cursor()
               cursor.execute(query)
               results = cursor.fetchall()
               columns = [desc[0] for desc in cursor.description]
               cursor.close()

               if results:
                   df_targets = pd.DataFrame(results, columns=columns)
                   st.session_state['target_customers'] = df_targets
                   st.success(f"✅ Found {len(df_targets):,} customers matching criteria")
               else:
                   st.warning("No customers match the selected criteria")
                   return

       # ========== CAMPAIGN PARAMETERS ==========

       if 'target_customers' not in st.session_state:
           st.info("👆 Define target audience to begin campaign simulation")
           return

       df_targets = st.session_state['target_customers']

       st.markdown("---")
       st.subheader("💰 Campaign Parameters")

       col1, col2, col3 = st.columns(3)

       with col1:
           incentive = st.number_input(
               "Incentive per Customer ($)",
               min_value=0,
               max_value=500,
               value=50,
               step=10,
               help="Statement credit or reward offered to retain customer"
           )

       with col2:
           retention_rate = st.slider(
               "Expected Retention Rate (%)",
               min_value=0,
               max_value=100,
               value=30,
               step=5,
               help="% of targeted customers expected to be retained"
           )

       with col3:
           campaign_cost = st.number_input(
               "Campaign Cost per Customer ($)",
               min_value=0,
               max_value=100,
               value=5,
               step=1,
               help="Email, SMS, and operational costs per customer"
           )

       # ========== ROI CALCULATION ==========

       st.markdown("---")
       st.subheader("📊 Campaign ROI Analysis")

       roi_results = calculate_campaign_roi(
           df_targets,
           incentive,
           retention_rate,
           campaign_cost
       )

       # Display key metrics
       col1, col2, col3, col4 = st.columns(4)

       with col1:
           st.metric("Target Customers", f"{roi_results['num_customers']:,}")

       with col2:
           st.metric("Total Cost", f"${roi_results['total_cost']:,.0f}")

       with col3:
           st.metric("Expected Retained", f"{roi_results['expected_retained_customers']:,}")

       with col4:
           roi_color = "normal" if roi_results['roi_pct'] >= 0 else "inverse"
           st.metric(
               "ROI",
               f"{roi_results['roi_pct']:.1f}%",
               delta=f"${roi_results['net_benefit']:,.0f}",
               delta_color=roi_color
           )

       # Detailed breakdown
       col1, col2 = st.columns(2)

       with col1:
           st.markdown("**💸 Cost Breakdown**")
           cost_data = pd.DataFrame({
               'Category': ['Incentives', 'Campaign Operations'],
               'Cost': [roi_results['incentive_cost'], roi_results['campaign_cost']]
           })

           fig_cost = px.pie(
               cost_data,
               values='Cost',
               names='Category',
               title='Campaign Cost Breakdown'
           )
           st.plotly_chart(fig_cost, use_container_width=True)

       with col2:
           st.markdown("**📈 Expected Value**")
           st.metric("Retained Customer Value", f"${roi_results['expected_retained_value']:,.0f}")
           st.metric("Cost per Retained Customer", f"${roi_results['cost_per_retained_customer']:,.0f}")
           st.metric("Net Benefit", f"${roi_results['net_benefit']:,.0f}")

       # ========== SENSITIVITY ANALYSIS ==========

       st.markdown("---")
       st.subheader("🔬 Sensitivity Analysis")

       st.markdown("See how ROI changes with different retention rates:")

       # Calculate ROI for range of retention rates
       retention_range = range(10, 81, 10)
       sensitivity_results = []

       for rate in retention_range:
           result = calculate_campaign_roi(df_targets, incentive, rate, campaign_cost)
           sensitivity_results.append({
               'Retention Rate (%)': rate,
               'ROI (%)': result['roi_pct'],
               'Net Benefit ($)': result['net_benefit']
           })

       df_sensitivity = pd.DataFrame(sensitivity_results)

       fig_sensitivity = go.Figure()

       fig_sensitivity.add_trace(go.Scatter(
           x=df_sensitivity['Retention Rate (%)'],
           y=df_sensitivity['ROI (%)'],
           mode='lines+markers',
           name='ROI',
           line=dict(color='blue', width=3),
           marker=dict(size=8)
       ))

       # Add zero line
       fig_sensitivity.add_hline(y=0, line_dash="dash", line_color="gray")

       fig_sensitivity.update_layout(
           title='ROI vs Retention Rate',
           xaxis_title='Retention Rate (%)',
           yaxis_title='ROI (%)',
           hovermode='x'
       )

       st.plotly_chart(fig_sensitivity, use_container_width=True)

       # Breakeven analysis
       breakeven_rate = None
       for rate in range(1, 101):
           result = calculate_campaign_roi(df_targets, incentive, rate, campaign_cost)
           if result['roi_pct'] >= 0:
               breakeven_rate = rate
               break

       if breakeven_rate:
           st.info(f"💡 **Breakeven Point:** Campaign breaks even at {breakeven_rate}% retention rate")
       else:
           st.warning("⚠️ Campaign does not break even at any retention rate up to 100%")

       # ========== TARGET LIST ==========

       st.markdown("---")
       st.subheader("📋 Target Customer List")

       # Top customers by churn risk
       st.markdown(f"**Top 10 Highest Risk Customers (of {len(df_targets):,} total)**")

       display_df = df_targets.head(10).copy()
       display_df['LIFETIME_VALUE'] = display_df['LIFETIME_VALUE'].apply(lambda x: f"${x:,.0f}")
       display_df['AVG_MONTHLY_SPEND'] = display_df['AVG_MONTHLY_SPEND'].apply(lambda x: f"${x:,.0f}")

       st.dataframe(display_df, use_container_width=True)

       # Export full list
       st.download_button(
           label="📥 Download Full Target List (CSV)",
           data=df_targets.to_csv(index=False),
           file_name=f"campaign_targets_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
           mime="text/csv",
           type="primary"
       )

       # ========== RECOMMENDATIONS ==========

       st.markdown("---")

       with st.expander("💡 Campaign Recommendations"):
           st.markdown(f"""
           **Based on your target audience ({roi_results['num_customers']:,} customers):**

           ✅ **Recommended Actions:**
           - Current ROI: **{roi_results['roi_pct']:.1f}%**
           - Target retention breakeven: **{breakeven_rate}%**
           - Focus on customers with churn risk score > 60
           - Personalize incentives based on customer segment

           📧 **Campaign Messaging:**
           - Emphasize benefits of staying (rewards, benefits)
           - Highlight exclusive offers for loyal customers
           - Create urgency with limited-time offers

           ⏰ **Timing:**
           - Deploy within 7 days for high-risk customers
           - Follow up after 2 weeks
           - Monitor spend changes in next 30 days

           📊 **Success Metrics:**
           - Track retention rate (target: {retention_rate}%+)
           - Monitor spend increase among retained customers
           - Calculate actual ROI vs projected
           """)
   ```

2. **Update streamlit/app.py** (add Campaign Simulator tab):
   ```python
   # Add import at top
   from tabs.campaign_simulator import render_campaign_simulator_tab

   # Update tabs in main():
   with tab4:
       render_campaign_simulator_tab(conn)
   ```

TESTING REQUIREMENTS:

- Create tests/integration/test_campaign_simulator.py:

  - test_target_audience_query():
    - Build query with filters (segment, churn risk)
    - Execute query
    - Assert DataFrame returned with expected columns

  - test_calculate_campaign_roi():
    - Create sample customer DataFrame
    - Call calculate_campaign_roi with parameters
    - Assert ROI dict returned
    - Verify calculations (total_cost, expected_retained_value, roi_pct)

  - test_roi_calculation_logic():
    - Test with known inputs
    - Assert total_cost = (incentive + campaign_cost) * num_customers
    - Assert expected_retained = num_customers * retention_rate / 100
    - Assert roi_pct calculated correctly

  - test_sensitivity_analysis():
    - Generate sensitivity data for retention rates 10-80%
    - Assert DataFrame has correct shape
    - Verify ROI increases with retention rate

  - test_breakeven_calculation():
    - Find retention rate where ROI >= 0
    - Assert breakeven rate found
    - Verify ROI negative below breakeven, positive above

  - test_export_target_list():
    - Create target DataFrame
    - Convert to CSV
    - Assert CSV has headers and data

  - test_campaign_recommendations():
    - Generate recommendations based on ROI
    - Verify recommendations displayed

DOCUMENTATION:

- Add Campaign Simulator section to streamlit/README.md
- Document ROI calculation methodology
- Document campaign parameters and assumptions
- Screenshot placeholders for Campaign Simulator UI
- Update main README.md with Campaign Simulator features
- Document use cases for marketing managers
- Create completion summary: docs/prompt_5_completion_summary.md

OUTPUT:
- Campaign Simulator tab fully functional
- Target audience selection works
- ROI calculation accurate
- Sensitivity analysis chart renders
- Breakeven analysis displayed
- Target list export works
- Campaign recommendations generated
- All integration tests pass
- Phase 5 (Streamlit Application) complete
- Full end-to-end platform ready for deployment
```

---

## End of Phase 5 Prompts

All 4 iterations of Phase 5 (Streamlit Application) are now complete:
- ✅ 5.1: Streamlit Foundation + Segment Explorer Tab
- ✅ 5.2: Customer 360 Deep Dive Tab
- ✅ 5.3: AI Assistant Tab (Cortex Analyst Integration)
- ✅ 5.4: Campaign Performance Simulator Tab

The Snowflake Customer 360 Analytics Platform is now fully specified from data generation through application deployment.