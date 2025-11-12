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
Create the foundational project structure for the Snowflake Customer 360 Analytics Platform with the following requirements - Project Structure will us UV package manager:

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
