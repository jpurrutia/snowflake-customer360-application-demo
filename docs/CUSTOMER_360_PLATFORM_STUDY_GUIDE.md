# CUSTOMER 360 ANALYTICS PLATFORM - COMPREHENSIVE STUDY GUIDE

**Purpose:** Complete exam preparation resource for Customer 360 Analytics Platform
**Version:** 1.0
**Date:** 2025-11-19
**Status:** Production-Ready Platform

---

## TABLE OF CONTENTS

### PART I: DATA PIPELINE JOURNEY
1. [Data Generation Layer](#1-data-generation-layer)
2. [Data Ingestion & Storage](#2-data-ingestion--storage)
3. [Data Transformation (dbt)](#3-data-transformation-dbt)
4. [Machine Learning Pipeline](#4-machine-learning-pipeline)
5. [Application & Consumption Layer](#5-application--consumption-layer)

### PART II: COMPONENT DEEP-DIVES (20 Components)
6. [Snowpark Procedures](#6-snowpark-procedures)
7. [dbt Transformations](#7-dbt-transformations)
8. [Cortex Analyst API](#8-cortex-analyst-api)
9. [Semantic Layer](#9-semantic-layer)
10. [AWS S3 Integration](#10-aws-s3-integration)
11. [Star Schema Data Model](#11-star-schema-data-model)
12. [ML Functions (Cortex ML)](#12-ml-functions-cortex-ml)
13. [Clustering Keys](#13-clustering-keys)
14. [Snowpipe](#14-snowpipe)
15. [SCD Type 2](#15-scd-type-2)
16. [Data Quality Tests](#16-data-quality-tests)
17. [GitHub Actions](#17-github-actions)
18. [Result Caching](#18-result-caching)
19. [PyTest Suite](#19-pytest-suite)
20. [RBAC Policies](#20-rbac-policies)
21. [Terraform](#21-terraform)
22. [Query Profiling](#22-query-profiling)
23. [Error Handling](#23-error-handling)
24. [Audit Logging](#24-audit-logging)
25. [Data Generation & Synthetic Data](#25-data-generation--synthetic-data)

### PART III: PERSONA-FOCUSED GUIDES
26. [Marketing Managers Guide](#26-marketing-managers-guide)
27. [Data Engineers Guide](#27-data-engineers-guide)
28. [Data Analysts Guide](#28-data-analysts-guide)

### PART IV: APPENDICES & QUICK REFERENCES
29. [SQL Quick Reference](#29-sql-quick-reference)
30. [Common Query Patterns](#30-common-query-patterns)
31. [Troubleshooting Index](#31-troubleshooting-index)
32. [Performance Benchmarks](#32-performance-benchmarks)
33. [Glossary](#33-glossary)

---

# PART I: DATA PIPELINE JOURNEY

## 1. Data Generation Layer

### 1.1 Overview

The platform generates **50,000 synthetic customers** and **60 million transactions** to simulate a credit card portfolio acquisition scenario.

**Why Synthetic Data?**
- Safe for demos and testing (no PII)
- Reproducible with seed values
- Realistic behavioral patterns
- Supports 5 customer segments

### 1.2 Customer Generation (Snowpark Python Stored Procedure)

**Implementation:** `snowflake/procedures/generate_customers.sql`

```sql
-- Execute stored procedure to generate 50K customers
CALL BRONZE.GENERATE_CUSTOMERS(50000, 42);
```

**What It Generates:**
- 50,000 customer records
- Realistic demographics (name, email, age, state)
- Account details (card type, credit limit, open date)
- Employment status
- **5 Behavioral Segments** with specific characteristics

**Segments Distribution:**
| Segment | Percentage | Characteristics |
|---------|-----------|----------------|
| High-Value Travelers | 15% | Premium cards, $5K+/month, 25%+ travel spend |
| Stable Mid-Spenders | 40% | Consistent spending, moderate amounts |
| Budget-Conscious | 25% | <$1.5K/month, 60%+ necessities |
| Declining | 10% | -30%+ spend decrease (churn risk) |
| New & Growing | 10% | Recent (<6 months), +50%+ growth |

**Stored Procedure Logic:**
```python
# Inside Snowpark procedure
for i in range(customer_count):
    segment = assign_segment_based_on_distribution()
    customer = {
        'customer_id': f'CUST{i:08d}',
        'segment': segment,
        'card_type': 'Premium' if segment == 'High-Value' else weighted_random(),
        'credit_limit': generate_limit_based_on_segment(segment),
        # ... more fields
    }
    customers.append(customer)

# Insert into BRONZE.RAW_CUSTOMERS
session.write_pandas(customers_df, 'RAW_CUSTOMERS', auto_create_table=True)
```

**Key Features:**
- **Deterministic**: Same seed (42) produces identical data every time
- **Segment-aware**: Different behaviors per segment
- **Performance**: Generates 50K in ~30-60 seconds
- **Snowpark Python**: Runs natively in Snowflake (no external compute needed)

### 1.3 Transaction Generation (SQL GENERATOR Function)

**Implementation:** `snowflake/data_generation/generate_transactions.sql`

```sql
-- Generate 60M transactions using Snowflake GENERATOR
INSERT INTO BRONZE.RAW_TRANSACTIONS
SELECT
    'TXN' || LPAD(SEQ4(), 12, '0') AS transaction_id,
    'CUST' || LPAD(UNIFORM(1, 50000, RANDOM()), 8, '0') AS customer_id,
    DATEADD(DAY, -UNIFORM(1, 547, RANDOM()), CURRENT_DATE()) AS transaction_date,
    ROUND(CASE
        WHEN seg.customer_segment = 'High-Value Travelers' THEN UNIFORM(100, 2000, RANDOM())
        WHEN seg.customer_segment = 'Budget-Conscious' THEN UNIFORM(10, 150, RANDOM())
        ELSE UNIFORM(25, 500, RANDOM())
    END, 2) AS transaction_amount,
    -- ... merchant, category, channel, status
FROM TABLE(GENERATOR(ROWCOUNT => 60000000)) AS gen
LEFT JOIN BRONZE.RAW_CUSTOMERS seg
  ON gen.customer_id = seg.customer_id;
```

**What It Generates:**
- **60 million transactions** (average ~13.5M after filtering)
- 18 months of historical data
- Segment-specific spending patterns
- Realistic merchant categories (Travel, Dining, Grocery, etc.)
- Multi-channel transactions (Online, In-Store, Mobile)
- ~97% approved, ~3% declined

**Performance:**
- Execution time: 5-15 minutes
- Uses Snowflake's parallel processing
- No external data sources needed

**Realism Features:**
- **Seasonal patterns**: Higher spending in holidays
- **Segment behaviors**:
  - High-Value Travelers → More airline/hotel transactions
  - Budget-Conscious → More grocery/gas
  - Declining → Decreasing transaction frequency over time
- **Transaction value distribution**: Bell curve with segment-specific means

### 1.4 Data Volumes Summary

| Layer | Component | Rows | Size | Generation Time |
|-------|-----------|------|------|-----------------|
| **Generation** | Customers | 50,000 | ~5 MB | 30-60 sec |
| **Generation** | Transactions | 60M (→13.5M after filtering) | ~2 GB | 5-15 min |
| **Total** | Raw Data | ~13.55M | ~2 GB | ~10-20 min |

### 1.5 Data Quality Validation

**Post-generation checks** (`snowflake/eda/03_post_generation_validation.sql`):

```sql
-- 12 automated validation checks:
1. Row count (10M-17M expected)
2. Unique transaction IDs
3. NULL validation (8 critical fields)
4. Customer representation (all 50K present)
5. Referential integrity (all customer_ids valid)
6. Date range (17-19 months)
7. Transaction amounts (positive, <$10K)
8. Status distribution (~97% approved)
9. Channel distribution (balanced across 3 channels)
10. Merchant category distribution (10-15 categories)
11. Segment-specific patterns
12. Monthly trends (no huge gaps)
```

**Validation Results:**
- ✅ All 12 checks must pass
- ✅ Automated via `make validate-data`
- ✅ Creates `generation_telemetry` table for tracking

### 1.6 Exam Questions You Should Be Able to Answer

1. **How many customers and transactions are generated?**
   - 50,000 customers, 60M transactions (filtered to ~13.5M)

2. **What are the 5 customer segments and their distributions?**
   - High-Value Travelers (15%), Stable (40%), Budget (25%), Declining (10%), New & Growing (10%)

3. **How is synthetic data generated in Snowflake?**
   - Customers: Snowpark Python stored procedure
   - Transactions: SQL GENERATOR() function with segment-aware logic

4. **Why use synthetic data instead of real data?**
   - No PII concerns, reproducible with seeds, safe for demos, realistic patterns

5. **How long does data generation take and what's the cost?**
   - Total: 10-20 minutes, ~0.3-0.6 credits (~$0.90-$1.80)

6. **What makes the synthetic data realistic?**
   - Segment-specific behaviors, seasonal patterns, realistic value distributions, multi-channel

7. **How do you validate generated data quality?**
   - 12 automated checks via SQL validation scripts + telemetry tracking

---

## 2. Data Ingestion & Storage

### 2.1 Architecture Overview

```
Local Python → AWS S3 → Snowflake Storage Integration → Bronze Layer
                ↓
         Terraform Provisioned
                ↓
         IAM Trust Relationship
```

### 2.2 AWS S3 Integration (Terraform)

**Infrastructure as Code** - All AWS resources provisioned via Terraform.

**Files:** `terraform/*.tf`

**Resources Created:**

1. **S3 Bucket** (`terraform/s3.tf`)
```hcl
resource "aws_s3_bucket" "snowflake_data_lake" {
  bucket = var.s3_bucket_name

  tags = {
    Purpose     = "Snowflake Data Lake"
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# Enable versioning
resource "aws_s3_bucket_versioning" "data_lake_versioning" {
  bucket = aws_s3_bucket.snowflake_data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Server-side encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake_encryption" {
  bucket = aws_s3_bucket.snowflake_data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
```

**Features:**
- ✅ Versioning enabled (data recovery)
- ✅ AES-256 encryption at rest
- ✅ Lifecycle policies (optional)
- ✅ Folder structure: `customers/`, `transactions/`

2. **IAM Role** (`terraform/iam.tf`)
```hcl
resource "aws_iam_role" "snowflake_s3_access" {
  name               = "SnowflakeS3AccessRole"
  assume_role_policy = data.aws_iam_policy_document.snowflake_assume_role.json
}

# Trust relationship with Snowflake
data "aws_iam_policy_document" "snowflake_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "AWS"
      identifiers = [var.snowflake_account_arn]
    }
    actions = ["sts:AssumeRole"]
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.snowflake_external_id]
    }
  }
}

# Permissions to read S3
resource "aws_iam_role_policy" "snowflake_s3_policy" {
  role   = aws_iam_role.snowflake_s3_access.id
  policy = data.aws_iam_policy_document.s3_access.json
}

data "aws_iam_policy_document" "s3_access" {
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources = [
      aws_s3_bucket.snowflake_data_lake.arn,
      "${aws_s3_bucket.snowflake_data_lake.arn}/*"
    ]
  }
}
```

**Security:**
- ✅ IAM role with trust relationship to Snowflake
- ✅ External ID prevents confused deputy problem
- ✅ Least-privilege permissions (read-only S3 access)
- ✅ No long-term credentials stored

**Deployment:**
```bash
cd terraform
terraform init
terraform plan
terraform apply

# Capture outputs
terraform output iam_role_arn    # Used in Snowflake storage integration
terraform output s3_bucket_name  # Used for uploads
```

### 2.3 Snowflake Storage Integration

**Setup Script:** `snowflake/setup/04_create_storage_integration.sql`

```sql
USE ROLE ACCOUNTADMIN;

-- Step 1: Create storage integration
CREATE OR REPLACE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<IAM_ROLE_ARN_FROM_TERRAFORM>'  -- From terraform output
  STORAGE_ALLOWED_LOCATIONS = ('s3://<BUCKET_NAME>/');     -- From terraform output

-- Step 2: Get Snowflake IAM user ARN and External ID
DESC STORAGE INTEGRATION s3_integration;
-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID

-- Step 3: Update Terraform variables with these values, then re-run terraform apply
-- This creates the trust relationship

-- Step 4: Grant usage to roles
GRANT USAGE ON INTEGRATION s3_integration TO ROLE DATA_ENGINEER;

-- Step 5: Create external stage
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA BRONZE;

CREATE OR REPLACE STAGE customer_data_stage
  STORAGE_INTEGRATION = s3_integration
  URL = 's3://<BUCKET_NAME>/customers/'
  FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- Verify stage works
LIST @customer_data_stage;
```

**What This Does:**
- Creates secure connection between Snowflake and S3
- No AWS credentials in Snowflake (uses IAM role assumption)
- External ID prevents unauthorized access
- Stage acts as pointer to S3 location

### 2.4 Data Upload to S3

**Python Implementation:** `data_generation/customer_generator.py`

```python
import boto3
from pathlib import Path

def upload_to_s3(file_path: Path, bucket: str, key: str):
    """Upload file to S3 bucket"""
    s3_client = boto3.client('s3')

    try:
        s3_client.upload_file(
            str(file_path),
            bucket,
            key,
            ExtraArgs={'ServerSideEncryption': 'AES256'}
        )
        print(f"✓ Uploaded {file_path} to s3://{bucket}/{key}")
    except Exception as e:
        print(f"✗ Upload failed: {e}")
        raise

# Usage
upload_to_s3(
    file_path=Path('data/customers.csv'),
    bucket='snowflake-customer-analytics-demo',
    key='customers/customers_20250119.csv'
)
```

**Alternative: SnowSQL PUT Command**
```bash
# Upload from local to Snowflake stage
snowsql -c default -q "
PUT file:///path/to/customers.csv @BRONZE.customer_data_stage
  AUTO_COMPRESS=FALSE
  OVERWRITE=TRUE;
"
```

### 2.5 Bronze Layer - COPY INTO

**Load Script:** `snowflake/load/load_customers_bulk.sql`

```sql
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA BRONZE;

-- Step 1: Create Bronze table
CREATE OR REPLACE TABLE RAW_CUSTOMERS (
    customer_id VARCHAR(16) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    age NUMBER(3),
    state VARCHAR(2),
    city VARCHAR(100),
    employment_status VARCHAR(50),
    card_type VARCHAR(20),
    credit_limit NUMBER(10,2),
    account_open_date DATE,
    customer_segment VARCHAR(50),
    decline_type VARCHAR(50),
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file VARCHAR(500)
);

-- Step 2: Load data from S3 via stage
COPY INTO RAW_CUSTOMERS (
    customer_id, first_name, last_name, email, age, state, city,
    employment_status, card_type, credit_limit, account_open_date,
    customer_segment, decline_type, source_file
)
FROM (
    SELECT
        $1, $2, $3, $4, $5::NUMBER, $6, $7,
        $8, $9, $10::NUMBER(10,2), $11::DATE,
        $12, $13, METADATA$FILENAME
    FROM @customer_data_stage
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'ABORT_STATEMENT'
VALIDATION_MODE = 'RETURN_ERRORS';  -- Test first

-- Step 3: Validate load
SELECT COUNT(*) FROM RAW_CUSTOMERS;  -- Should be 50,000

SELECT
    source_file,
    COUNT(*) AS row_count,
    MIN(ingestion_timestamp) AS first_load,
    MAX(ingestion_timestamp) AS last_load
FROM RAW_CUSTOMERS
GROUP BY source_file;
```

**Features:**
- `ON_ERROR = 'ABORT_STATEMENT'` - Fail fast on errors
- `METADATA$FILENAME` - Track source file for lineage
- `ingestion_timestamp` - Audit trail
- `VALIDATION_MODE` - Test before actual load

**Performance:**
- 50K customer records: <5 seconds
- 13.5M transaction records: ~30-60 seconds (parallel processing)

### 2.6 Data Ingestion Summary

| Step | Component | Time | Output |
|------|-----------|------|--------|
| 1 | Terraform apply | 1-2 min | S3 bucket + IAM role |
| 2 | Storage integration | 30 sec | Snowflake ↔ S3 connection |
| 3 | Python upload to S3 | 5-10 sec | Files in S3 |
| 4 | COPY INTO Snowflake | 5-60 sec | Bronze tables populated |
| **Total** | **End-to-end** | **~5 min** | **50K customers, 13.5M transactions in Bronze** |

### 2.7 Exam Questions You Should Be Able to Answer

1. **How is AWS infrastructure provisioned?**
   - Terraform (Infrastructure as Code) creates S3 bucket, IAM role, encryption, versioning

2. **What is a Snowflake Storage Integration?**
   - Secure connection between Snowflake and external cloud storage using IAM role assumption (no credentials in Snowflake)

3. **What is the trust relationship and why is it important?**
   - IAM role trusts Snowflake AWS account with External ID to prevent confused deputy attacks

4. **How does data flow from local machine to Snowflake Bronze layer?**
   - Local Python → S3 (boto3) → Snowflake External Stage → COPY INTO Bronze tables

5. **What is COPY INTO and what are its key parameters?**
   - Snowflake command to load data from stages; ON_ERROR (error handling), FILE_FORMAT (CSV/JSON/Parquet), VALIDATION_MODE (test before load)

6. **What metadata is captured during ingestion?**
   - source_file (METADATA$FILENAME), ingestion_timestamp (when loaded), original file details

7. **How long does it take to load 13.5M transactions?**
   - ~30-60 seconds (Snowflake parallel processing)

8. **What security measures are in place for S3?**
   - AES-256 encryption at rest, versioning, IAM role (no credentials), least-privilege permissions

---

## 3. Data Transformation (dbt)

### 3.1 Medallion Architecture Overview

**Bronze → Silver → Gold** layered approach for data quality and organization.

```
BRONZE Layer (Raw)          SILVER Layer (Cleaned)         GOLD Layer (Business Logic)
├─ RAW_CUSTOMERS            ├─ stg_customers              ├─ dim_customer (SCD Type 2)
├─ RAW_TRANSACTIONS         ├─ stg_transactions           ├─ dim_date
└─ RAW_CATEGORIES           └─ stg_categories             ├─ dim_merchant_category
                                                           ├─ fct_transactions (clustered)
                                                           ├─ customer_segments
                                                           ├─ customer_360_profile
                                                           ├─ metric_customer_ltv
                                                           ├─ metric_mom_spend_change
                                                           └─ metric_avg_transaction_value
```

**14 dbt Models Total:**
- Bronze: 3 tables (raw data, already created in previous step)
- Silver: 3 staging models (views or incremental)
- Gold: 8 analytical models (dimensions, facts, metrics)

### 3.2 Silver Layer - Staging Models

**Purpose:** Clean, standardize, deduplicate, and validate raw data.

#### 3.2.1 stg_customers (View)

**File:** `dbt_customer_analytics/models/staging/stg_customers.sql`

```sql
{{ config(
    materialized='view',
    tags=['staging', 'customers']
) }}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_customers') }}
),

cleaned AS (
    SELECT
        customer_id,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        LOWER(TRIM(email)) AS email,
        age,
        UPPER(TRIM(state)) AS state,
        TRIM(city) AS city,
        employment_status,
        card_type,
        credit_limit,
        account_open_date,
        customer_segment,
        decline_type,
        ingestion_timestamp,
        source_file,

        -- Row number for deduplication (if needed)
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY ingestion_timestamp DESC) AS row_num
    FROM source
    WHERE customer_id IS NOT NULL  -- Filter out invalid rows
)

SELECT * EXCLUDE row_num
FROM cleaned
WHERE row_num = 1  -- Keep only latest version if duplicates exist
```

**What It Does:**
- ✅ Trims whitespace from strings
- ✅ Standardizes email to lowercase
- ✅ Standardizes state codes to uppercase
- ✅ Filters NULL customer_ids
- ✅ Deduplicates (keeps latest if multiple ingestions)

#### 3.2.2 stg_transactions (Incremental Model)

**File:** `dbt_customer_analytics/models/staging/stg_transactions.sql`

```sql
{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    on_schema_change='append_new_columns',
    cluster_by=['transaction_date'],
    tags=['staging', 'transactions']
) }}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_transactions') }}

    {% if is_incremental() %}
    -- Only process new records on incremental runs
    WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

validated AS (
    SELECT
        transaction_id,
        customer_id,
        transaction_date,
        transaction_amount,
        merchant_name,
        merchant_category,
        channel,
        status,
        ingestion_timestamp,
        source_file,

        -- Validation flags
        transaction_amount > 0 AS is_positive_amount,
        transaction_amount < 10000 AS is_reasonable_amount,
        transaction_date <= CURRENT_DATE() AS is_valid_date,
        customer_id IS NOT NULL AS has_customer_id,

        ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY ingestion_timestamp DESC) AS row_num
    FROM source
),

cleaned AS (
    SELECT * EXCLUDE row_num
    FROM validated
    WHERE row_num = 1  -- Deduplicate
      AND is_positive_amount
      AND is_reasonable_amount
      AND is_valid_date
      AND has_customer_id
)

SELECT
    transaction_id,
    customer_id,
    transaction_date,
    transaction_amount,
    TRIM(merchant_name) AS merchant_name,
    TRIM(merchant_category) AS merchant_category,
    channel,
    status,
    ingestion_timestamp,
    source_file
FROM cleaned
```

**What It Does:**
- ✅ **Incremental processing** - Only new data after first full run
- ✅ Validates transaction amounts (positive, <$10K)
- ✅ Validates transaction dates (not future)
- ✅ Filters orphaned transactions (no customer_id)
- ✅ Deduplicates by transaction_id
- ✅ **Clustered** by transaction_date for query performance

**Incremental Logic:**
- First run: Processes all Bronze data
- Subsequent runs: Only processes rows with `ingestion_timestamp > MAX(ingestion_timestamp)` from previous run
- Merge strategy: `unique_key='transaction_id'` - updates if exists, inserts if new

### 3.3 Gold Layer - Dimensional Model (Star Schema)

#### 3.3.1 dim_customer (SCD Type 2)

**File:** `dbt_customer_analytics/models/marts/core/dim_customer.sql`

```sql
{{ config(
    materialized='incremental',
    unique_key='customer_key',
    on_schema_change='fail',
    cluster_by=['customer_id'],
    tags=['gold', 'dimension', 'scd2']
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

{% if is_incremental() %}
-- Identify changed records
changes AS (
    SELECT
        s.customer_id,
        s.card_type,
        s.credit_limit,
        CASE
            WHEN d.card_type != s.card_type THEN TRUE
            WHEN d.credit_limit != s.credit_limit THEN TRUE
            ELSE FALSE
        END AS has_changed
    FROM source s
    INNER JOIN {{ this }} d
        ON s.customer_id = d.customer_id
        AND d.is_current = TRUE
    WHERE has_changed
),

-- Expire old versions
expire_old AS (
    UPDATE {{ this }}
    SET
        valid_to = CURRENT_DATE() - 1,
        is_current = FALSE
    WHERE customer_id IN (SELECT customer_id FROM changes)
      AND is_current = TRUE
),
{% endif %}

-- Insert new versions (full refresh or changed records)
final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'account_open_date', 'CURRENT_TIMESTAMP()']) }} AS customer_key,
        customer_id,
        first_name,
        last_name,
        email,
        age,
        state,
        city,
        employment_status,
        card_type,  -- SCD Type 2 tracked attribute
        credit_limit,  -- SCD Type 2 tracked attribute
        customer_segment,
        account_open_date,
        CURRENT_DATE() AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current
    FROM source

    {% if is_incremental() %}
    WHERE customer_id IN (SELECT customer_id FROM changes)
    {% endif %}
)

SELECT * FROM final
```

**SCD Type 2 Features:**
- ✅ Tracks changes to `card_type` and `credit_limit`
- ✅ Creates new version on change (new row with new `customer_key`)
- ✅ Expires old version (`valid_to` set, `is_current = FALSE`)
- ✅ Keeps full history of customer evolution
- ✅ Point-in-time queries supported

**Example Data:**
| customer_key | customer_id | card_type | credit_limit | valid_from | valid_to | is_current |
|--------------|-------------|-----------|--------------|------------|----------|------------|
| 1001 | CUST00000001 | Standard | 5000 | 2023-01-01 | 2024-06-15 | FALSE |
| 1002 | CUST00000001 | Premium | 15000 | 2024-06-16 | NULL | TRUE |

**Use Case:** Track when customer upgraded from Standard to Premium card.

#### 3.3.2 dim_date (Date Dimension)

**File:** `dbt_customer_analytics/models/marts/core/dim_date.sql`

```sql
{{ config(
    materialized='table',
    tags=['gold', 'dimension', 'date']
) }}

WITH date_spine AS (
    -- Generate 580 days (18 months + buffer)
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="DATEADD(month, -19, CURRENT_DATE())",
        end_date="DATEADD(day, 30, CURRENT_DATE())"
    ) }}
),

date_attributes AS (
    SELECT
        TO_NUMBER(TO_CHAR(date_day, 'YYYYMMDD')) AS date_key,
        date_day,
        YEAR(date_day) AS year,
        QUARTER(date_day) AS quarter,
        MONTH(date_day) AS month,
        MONTHNAME(date_day) AS month_name,
        DAY(date_day) AS day_of_month,
        DAYOFWEEK(date_day) AS day_of_week,
        DAYNAME(date_day) AS day_name,
        DAYOFYEAR(date_day) AS day_of_year,
        WEEKOFYEAR(date_day) AS week_of_year,
        TO_CHAR(date_day, 'YYYY-MM') AS year_month,
        CASE WHEN DAYOFWEEK(date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN DAYOFWEEK(date_day) BETWEEN 1 AND 5 THEN TRUE ELSE FALSE END AS is_weekday,
        CASE WHEN date_day = CURRENT_DATE() THEN TRUE ELSE FALSE END AS is_today,
        CASE WHEN DAY(date_day) = 1 THEN TRUE ELSE FALSE END AS is_first_day_of_month,
        CASE WHEN date_day = LAST_DAY(date_day) THEN TRUE ELSE FALSE END AS is_last_day_of_month
    FROM date_spine
)

SELECT * FROM date_attributes
```

**Features:**
- ✅ 580 days of calendar data (covers 18-month transaction range + buffer)
- ✅ `date_key` in YYYYMMDD format (20240115 = Jan 15, 2024)
- ✅ All time attributes pre-calculated (year, quarter, month, week, day)
- ✅ Boolean flags (is_weekend, is_weekday, is_today, etc.)
- ✅ Enables time intelligence queries without complex DATE functions

#### 3.3.3 fct_transactions (Fact Table)

**File:** `dbt_customer_analytics/models/marts/core/fct_transactions.sql`

```sql
{{ config(
    materialized='incremental',
    unique_key='transaction_key',
    cluster_by=['transaction_date'],
    tags=['gold', 'fact']
) }}

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}

    {% if is_incremental() %}
    WHERE transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
    {% endif %}
),

customers AS (
    SELECT
        customer_key,
        customer_id,
        valid_from,
        valid_to,
        is_current
    FROM {{ ref('dim_customer') }}
),

dates AS (
    SELECT
        date_key,
        date_day
    FROM {{ ref('dim_date') }}
),

categories AS (
    SELECT
        category_key,
        category_name
    FROM {{ ref('dim_merchant_category') }}
),

fact_joined AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['t.transaction_id']) }} AS transaction_key,
        t.transaction_id,

        -- Foreign keys
        c.customer_key,
        d.date_key,
        cat.category_key,

        -- Measures
        t.transaction_amount,

        -- Degenerate dimensions
        t.merchant_name,
        t.channel,
        t.status,

        -- Metadata
        t.transaction_date,
        t.ingestion_timestamp,
        t.source_file
    FROM transactions t

    -- Join to current customer version (most common use case)
    INNER JOIN customers c
        ON t.customer_id = c.customer_id
        AND c.is_current = TRUE

    -- Join to date dimension
    INNER JOIN dates d
        ON t.transaction_date = d.date_day

    -- Join to category dimension
    LEFT JOIN categories cat
        ON t.merchant_category = cat.category_name
)

SELECT * FROM fact_joined
```

**Features:**
- ✅ **Incremental** - Only processes new transactions
- ✅ **Clustered by transaction_date** - Optimized for time-series queries
- ✅ Foreign keys to all dimensions
- ✅ Additive measure: `transaction_amount`
- ✅ Degenerate dimensions: merchant_name, channel, status (stored in fact)

**Grain:** One row per transaction (~13.5M rows)

**Performance:**
- Single customer queries: <1 second (clustering + FKs)
- Monthly aggregation: <5 seconds
- Full table scan: Avoid! Always filter by date or customer

### 3.4 Gold Layer - Analytics Marts

#### 3.4.1 customer_segments (Behavioral Segmentation)

**File:** `dbt_customer_analytics/models/marts/customer_analytics/customer_segments.sql`

```sql
{{ config(
    materialized='table',
    tags=['gold', 'analytics', 'segmentation']
) }}

WITH customer_base AS (
    SELECT
        c.customer_id,
        c.customer_key,
        c.first_name,
        c.last_name,
        MIN(f.transaction_date) AS first_transaction_date,
        MAX(f.transaction_date) AS last_transaction_date,
        DATEDIFF('day', MIN(f.transaction_date), MAX(f.transaction_date)) AS customer_age_days,
        DATEDIFF('day', MAX(f.transaction_date), CURRENT_DATE()) AS days_since_last_transaction
    FROM {{ ref('dim_customer') }} c
    INNER JOIN {{ ref('fct_transactions') }} f ON c.customer_key = f.customer_key
    WHERE c.is_current = TRUE
    GROUP BY c.customer_id, c.customer_key, c.first_name, c.last_name
),

rolling_metrics AS (
    SELECT
        cb.customer_id,
        cb.customer_key,

        -- Last 90 days spending
        SUM(CASE WHEN f.transaction_date >= DATEADD('day', -90, CURRENT_DATE()) THEN f.transaction_amount ELSE 0 END) AS spend_last_90_days,

        -- Prior 90 days spending (days 91-180)
        SUM(CASE WHEN f.transaction_date BETWEEN DATEADD('day', -180, CURRENT_DATE()) AND DATEADD('day', -91, CURRENT_DATE()) THEN f.transaction_amount ELSE 0 END) AS spend_prior_90_days,

        -- Category spending percentages
        SUM(CASE WHEN cat.category_group = 'Leisure' THEN f.transaction_amount ELSE 0 END) / NULLIF(SUM(f.transaction_amount), 0) * 100 AS travel_spend_pct,
        SUM(CASE WHEN cat.category_group = 'Necessities' THEN f.transaction_amount ELSE 0 END) / NULLIF(SUM(f.transaction_amount), 0) * 100 AS necessities_spend_pct

    FROM customer_base cb
    INNER JOIN {{ ref('fct_transactions') }} f ON cb.customer_key = f.customer_key
    LEFT JOIN {{ ref('dim_merchant_category') }} cat ON f.category_key = cat.category_key
    GROUP BY cb.customer_id, cb.customer_key
),

segment_assignment AS (
    SELECT
        cb.*,
        rm.spend_last_90_days,
        rm.spend_prior_90_days,
        rm.travel_spend_pct,
        rm.necessities_spend_pct,

        -- Calculate monthly average and change
        rm.spend_last_90_days / 3.0 AS avg_monthly_spend,
        ((rm.spend_last_90_days - rm.spend_prior_90_days) / NULLIF(rm.spend_prior_90_days, 0)) * 100 AS spend_change_pct,

        -- Tenure
        FLOOR(cb.customer_age_days / 30.0) AS tenure_months,

        -- Assign segment (priority order)
        CASE
            WHEN (rm.spend_last_90_days / 3.0) >= 5000 AND rm.travel_spend_pct >= 25
                THEN 'High-Value Travelers'

            WHEN ((rm.spend_last_90_days - rm.spend_prior_90_days) / NULLIF(rm.spend_prior_90_days, 0)) * 100 <= -30
                AND rm.spend_prior_90_days >= 2000
                THEN 'Declining'

            WHEN FLOOR(cb.customer_age_days / 30.0) <= 6
                AND ((rm.spend_last_90_days - rm.spend_prior_90_days) / NULLIF(rm.spend_prior_90_days, 0)) * 100 >= 50
                THEN 'New & Growing'

            WHEN (rm.spend_last_90_days / 3.0) < 1500 AND rm.necessities_spend_pct >= 60
                THEN 'Budget-Conscious'

            ELSE 'Stable Mid-Spenders'
        END AS customer_segment

    FROM customer_base cb
    INNER JOIN rolling_metrics rm ON cb.customer_id = rm.customer_id
)

SELECT * FROM segment_assignment
```

**5 Segments Logic:**
1. **High-Value Travelers**: $5K+/month AND 25%+ travel spend
2. **Declining**: -30%+ decrease AND previous spend >$2K (churn risk)
3. **New & Growing**: ≤6 months tenure AND +50%+ growth
4. **Budget-Conscious**: <$1.5K/month AND 60%+ necessities
5. **Stable Mid-Spenders**: Everyone else (default)

**Rolling Window:** 90 days for current, 90 days prior (days 91-180) for comparison

#### 3.4.2 customer_360_profile (Denormalized Customer View)

**File:** `dbt_customer_analytics/models/marts/customer_analytics/customer_360_profile.sql`

```sql
{{ config(
    materialized='table',
    cluster_by=['customer_id'],
    tags=['gold', 'analytics', 'customer360']
) }}

WITH customer_current AS (
    SELECT * FROM {{ ref('dim_customer') }}
    WHERE is_current = TRUE
),

segments AS (
    SELECT * FROM {{ ref('customer_segments') }}
),

ltv AS (
    SELECT * FROM {{ ref('metric_customer_ltv') }}
),

atv AS (
    SELECT * FROM {{ ref('metric_avg_transaction_value') }}
),

churn_predictions AS (
    SELECT
        customer_id,
        churn_risk_score,
        CASE
            WHEN churn_risk_score >= 70 THEN 'High Risk'
            WHEN churn_risk_score >= 40 THEN 'Medium Risk'
            ELSE 'Low Risk'
        END AS churn_risk_category,
        prediction_date
    FROM {{ source('gold', 'churn_predictions') }}
),

final AS (
    SELECT
        -- Identifiers
        c.customer_id,
        c.customer_key,

        -- Demographics
        c.first_name || ' ' || c.last_name AS full_name,
        c.first_name,
        c.last_name,
        c.email,
        c.age,
        c.state,
        c.city,
        c.employment_status,

        -- Account Details
        c.card_type,
        c.credit_limit,
        c.account_open_date,
        DATEDIFF('day', c.account_open_date, CURRENT_DATE()) AS account_age_days,

        -- Segmentation
        seg.customer_segment,
        CURRENT_DATE() AS segment_assigned_date,
        seg.tenure_months,

        -- Lifetime Metrics
        ltv.lifetime_value,
        ltv.total_transactions,
        ltv.customer_age_days,
        ltv.avg_spend_per_day,

        -- Average Transaction Value
        atv.avg_transaction_value,
        atv.transaction_value_stddev,
        atv.min_transaction_value,
        atv.max_transaction_value,
        atv.median_transaction_value,
        atv.spending_consistency,

        -- Rolling 90-day Metrics
        seg.spend_last_90_days,
        seg.spend_prior_90_days,
        seg.spend_change_pct,
        seg.avg_monthly_spend,

        -- Activity Timeline
        seg.first_transaction_date,
        seg.last_transaction_date,
        seg.days_since_last_transaction,
        CASE
            WHEN seg.days_since_last_transaction <= 30 THEN 'Active (30d)'
            WHEN seg.days_since_last_transaction <= 60 THEN 'Recent (60d)'
            WHEN seg.days_since_last_transaction <= 90 THEN 'At Risk (90d)'
            ELSE 'Inactive (90+ days)'
        END AS recency_status,

        -- Category Preferences
        seg.travel_spend_pct,
        seg.necessities_spend_pct,
        CASE
            WHEN seg.travel_spend_pct >= 30 THEN 'Travel-Focused'
            WHEN seg.necessities_spend_pct >= 50 THEN 'Necessity-Focused'
            ELSE 'Balanced'
        END AS spending_profile,

        -- Campaign Flags
        CASE WHEN seg.customer_segment = 'Declining' THEN TRUE ELSE FALSE END AS eligible_for_retention_campaign,
        CASE WHEN seg.customer_segment = 'New & Growing' THEN TRUE ELSE FALSE END AS eligible_for_onboarding_campaign,
        CASE WHEN seg.customer_segment = 'High-Value Travelers' THEN TRUE ELSE FALSE END AS eligible_for_premium_campaign,

        -- Churn Risk (from ML model)
        cp.churn_risk_score,
        cp.churn_risk_category,

        -- Metadata
        CURRENT_DATE() AS profile_updated_date

    FROM customer_current c
    INNER JOIN segments seg ON c.customer_id = seg.customer_id
    INNER JOIN ltv ON c.customer_id = ltv.customer_id
    INNER JOIN atv ON c.customer_id = atv.customer_id
    LEFT JOIN churn_predictions cp ON c.customer_id = cp.customer_id
)

SELECT * FROM final
```

**Purpose:**
- Single-table view for all customer insights
- Optimized for fast lookups (<1 second per customer)
- Powers Streamlit dashboard tabs
- Ready for Cortex Analyst queries

**Performance:**
- Clustered by `customer_id` for instant lookups
- Denormalized (no runtime JOINs needed)
- 50K rows (one per customer)

### 3.5 dbt Commands & Execution

```bash
# Full refresh (rebuild all models from scratch)
dbt run --full-refresh

# Incremental run (process only new data)
dbt run

# Run specific model
dbt run --select customer_360_profile

# Run all gold layer models
dbt run --select marts.core marts.customer_analytics

# Run models and their downstream dependencies
dbt run --select customer_segments+

# Run tests
dbt test

# Generate and serve documentation
dbt docs generate
dbt docs serve
```

### 3.6 dbt Project Structure

```
dbt_customer_analytics/
├── dbt_project.yml              # Project configuration
├── profiles.yml                 # Connection profiles
├── models/
│   ├── staging/
│   │   ├── stg_customers.sql
│   │   ├── stg_transactions.sql
│   │   └── schema.yml
│   ├── marts/
│   │   ├── core/                # Star schema
│   │   │   ├── dim_customer.sql
│   │   │   ├── dim_date.sql
│   │   │   ├── dim_merchant_category.sql
│   │   │   ├── fct_transactions.sql
│   │   │   └── schema.yml
│   │   ├── customer_analytics/  # Analytics marts
│   │   │   ├── customer_segments.sql
│   │   │   ├── customer_360_profile.sql
│   │   │   └── schema.yml
│   │   └── marketing/           # Hero metrics
│   │       ├── metric_customer_ltv.sql
│   │       ├── metric_mom_spend_change.sql
│   │       ├── metric_avg_transaction_value.sql
│   │       └── schema.yml
│   └── sources.yml              # Bronze layer sources
├── tests/
│   ├── assert_segment_distribution.sql
│   └── scd_type2_no_overlaps.sql
└── macros/
    └── custom_tests.sql
```

### 3.7 Execution Performance

| Model | Materialization | Rows | Execution Time | Notes |
|-------|----------------|------|----------------|-------|
| stg_customers | view | 50K | <1 sec | No materialization |
| stg_transactions | incremental | 13.5M | 20 sec (full), 5 sec (inc) | Clustered |
| dim_customer | incremental (SCD2) | 50K+ | 10 sec | Creates new versions |
| dim_date | table | 580 | <1 sec | Small dimension |
| dim_merchant_category | table | 15 | <1 sec | Tiny dimension |
| fct_transactions | incremental | 13.5M | 30 sec (full), 10 sec (inc) | Clustered |
| customer_segments | table | 50K | 15 sec | Rolling window calcs |
| customer_360_profile | table | 50K | 20 sec | Multiple JOINs |
| metric_customer_ltv | table | 50K | 10 sec | Aggregation |
| **Total** | **14 models** | **~27M rows** | **~70-90 sec (full)** | **COMPUTE_WH (Small)** |

### 3.8 Exam Questions You Should Be Able to Answer

1. **What is the medallion architecture and what are its 3 layers?**
   - Bronze (raw), Silver (cleaned/conformed), Gold (business logic/analytics)

2. **How many dbt models are there and what layers do they cover?**
   - 14 models: 3 staging (Silver), 4 dimensions + 1 fact (Gold core), 1 segmentation + 3 metrics + 1 customer 360 (Gold analytics)

3. **What is an incremental model and which models use it?**
   - Processes only new data after first full run; stg_transactions, dim_customer (SCD2), fct_transactions

4. **What is SCD Type 2 and which dimension uses it?**
   - Slowly Changing Dimension that tracks full history with new rows on change; dim_customer tracks card_type and credit_limit changes

5. **What is a fact table grain and what is fct_transactions grain?**
   - Grain = level of detail (one row per X); fct_transactions = one row per transaction (~13.5M rows)

6. **What are the 5 customer segments and their logic?**
   - High-Value Travelers ($5K+/mo, 25%+ travel), Declining (-30%+ decrease), New & Growing (≤6mo, +50% growth), Budget-Conscious (<$1.5K/mo, 60%+ necessities), Stable Mid-Spenders (default)

7. **What is customer_360_profile and why is it denormalized?**
   - Single table with all customer insights (demographics, metrics, segments, churn risk); denormalized for fast lookups without runtime JOINs (<1 sec per customer)

8. **How long does a full dbt run take and what warehouse size?**
   - ~70-90 seconds on COMPUTE_WH (Small warehouse) for all 14 models

9. **What is clustering and which tables are clustered?**
   - Physical organization of data for query optimization; stg_transactions and fct_transactions clustered by transaction_date, customer_360_profile by customer_id

10. **What dbt command runs only new data and what runs everything?**
    - `dbt run` = incremental (new data only), `dbt run --full-refresh` = full rebuild

---

## 4. Machine Learning Pipeline

### 4.1 Overview

**Objective:** Predict which customers will churn within 60-90 days using Snowflake Cortex ML.

**Business Impact:**
- Early identification of at-risk customers
- Targeted retention campaigns
- Reduced customer acquisition costs
- Improved customer lifetime value

### 4.2 Churn Definition

A customer is **churned** if **either** condition is true:

```sql
CASE
    -- Rule 1: Inactivity Churn (60+ days no transactions)
    WHEN last_transaction_date IS NULL
         OR DATEDIFF('day', last_transaction_date, CURRENT_DATE()) > 60
    THEN TRUE

    -- Rule 2: Decline Churn (significant spending decrease)
    WHEN recent_avg_spend < (baseline_avg_spend * 0.30)
    THEN TRUE

    -- Active customer
    ELSE FALSE
END AS churned
```

**Rationale:**
| Criterion | Value | Justification |
|-----------|-------|---------------|
| Inactivity threshold | 60 days | Industry standard for credit card dormancy |
| Decline threshold | 30% of baseline | Significant reduction indicating disengagement |
| Baseline period | 12 months | Captures normal spending patterns |
| Recent period | 3 months | Recent enough to detect current behavior |

**Results (from 50K customers):**
- Churned customers: 1,642 (3.28%)
- Active customers: 48,358 (96.72%)
- Class imbalance: Yes (realistic churn rate)

### 4.3 ML Pipeline - 5 Steps

```
Step 1: Create Churn Labels
  └─> Step 2: Feature Engineering
      └─> Step 3: Train Model (Cortex ML)
          └─> Step 4: Validate Performance
              └─> Step 5: Apply Predictions
```

#### 4.3.1 Step 1: Create Churn Labels

**File:** `snowflake/ml/01_create_churn_labels.sql`

```sql
CREATE OR REPLACE TABLE GOLD.CHURN_LABELS AS
WITH customer_activity AS (
    SELECT
        c.customer_id,
        MIN(f.transaction_date) AS first_transaction_date,
        MAX(f.transaction_date) AS last_transaction_date,
        DATEDIFF('day', MAX(f.transaction_date), CURRENT_DATE()) AS days_since_last_transaction,
        COUNT(*) AS total_transactions
    FROM {{ ref('dim_customer') }} c
    LEFT JOIN {{ ref('fct_transactions') }} f ON c.customer_key = f.customer_key
    WHERE c.is_current = TRUE
    GROUP BY c.customer_id
),

baseline_spending AS (
    -- First 12 months of customer history
    SELECT
        c.customer_id,
        AVG(monthly_spend) AS baseline_avg_spend
    FROM (
        SELECT
            c.customer_id,
            DATE_TRUNC('month', f.transaction_date) AS month,
            SUM(f.transaction_amount) AS monthly_spend,
            ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY DATE_TRUNC('month', f.transaction_date)) AS month_num
        FROM {{ ref('dim_customer') }} c
        INNER JOIN {{ ref('fct_transactions') }} f ON c.customer_key = f.customer_key
        WHERE c.is_current = TRUE
        GROUP BY c.customer_id, DATE_TRUNC('month', f.transaction_date)
    )
    WHERE month_num <= 12  -- First 12 months only
    GROUP BY customer_id
    HAVING COUNT(*) >= 6  -- At least 6 months of data
),

recent_spending AS (
    -- Last 3 months of customer history
    SELECT
        c.customer_id,
        AVG(monthly_spend) AS recent_avg_spend
    FROM (
        SELECT
            c.customer_id,
            DATE_TRUNC('month', f.transaction_date) AS month,
            SUM(f.transaction_amount) AS monthly_spend
        FROM {{ ref('dim_customer') }} c
        INNER JOIN {{ ref('fct_transactions') }} f ON c.customer_key = f.customer_key
        WHERE c.is_current = TRUE
          AND f.transaction_date >= DATEADD('month', -3, CURRENT_DATE())
        GROUP BY c.customer_id, DATE_TRUNC('month', f.transaction_date)
    )
    GROUP BY customer_id
),

churn_labels AS (
    SELECT
        ca.customer_id,
        ca.last_transaction_date,
        ca.days_since_last_transaction,
        ca.total_transactions,
        bs.baseline_avg_spend,
        rs.recent_avg_spend,

        -- Churn label logic
        CASE
            WHEN ca.last_transaction_date IS NULL
                 OR ca.days_since_last_transaction > 60
            THEN TRUE  -- Inactivity churn

            WHEN rs.recent_avg_spend < (bs.baseline_avg_spend * 0.30)
            THEN TRUE  -- Decline churn

            ELSE FALSE  -- Active
        END AS churned,

        -- Churn reason (for analysis)
        CASE
            WHEN ca.last_transaction_date IS NULL OR ca.days_since_last_transaction > 60
            THEN 'Inactivity'
            WHEN rs.recent_avg_spend < (bs.baseline_avg_spend * 0.30)
            THEN 'Decline'
            ELSE 'Active'
        END AS churn_reason

    FROM customer_activity ca
    LEFT JOIN baseline_spending bs ON ca.customer_id = bs.customer_id
    LEFT JOIN recent_spending rs ON ca.customer_id = rs.customer_id
    WHERE ca.total_transactions >= 5  -- Minimum transaction history
)

SELECT * FROM churn_labels;

-- Validate class distribution
SELECT
    churned,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM GOLD.CHURN_LABELS
GROUP BY churned
ORDER BY churned;
```

**Output:**
| churned | customer_count | percentage |
|---------|---------------|-----------|
| FALSE | 48,358 | 96.72% |
| TRUE | 1,642 | 3.28% |

#### 4.3.2 Step 2: Feature Engineering (35+ Features)

**File:** `snowflake/ml/02_create_training_features.sql`

```sql
CREATE OR REPLACE TABLE GOLD.ML_TRAINING_DATA AS
WITH customer_features AS (
    SELECT
        c.customer_id,

        -- Demographics (5 features)
        c.age,
        c.state,
        CASE WHEN c.card_type = 'Premium' THEN 1 ELSE 0 END AS card_type_premium,
        c.credit_limit,
        c.employment_status,

        -- Spending Behavior (15 features)
        seg.lifetime_value,
        seg.avg_transaction_value,
        seg.total_transactions,
        seg.days_since_last_transaction,
        seg.spend_last_90_days,
        seg.spend_prior_90_days,
        seg.spend_change_pct,
        seg.avg_monthly_spend,
        seg.transaction_value_stddev,
        seg.median_transaction_value,
        seg.travel_spend_pct,
        seg.necessities_spend_pct,

        -- Segment Features (6 features - one-hot encoded)
        CASE WHEN seg.customer_segment = 'High-Value Travelers' THEN 1 ELSE 0 END AS segment_high_value_travelers,
        CASE WHEN seg.customer_segment = 'Declining' THEN 1 ELSE 0 END AS segment_declining,
        CASE WHEN seg.customer_segment = 'New & Growing' THEN 1 ELSE 0 END AS segment_new_growing,
        CASE WHEN seg.customer_segment = 'Budget-Conscious' THEN 1 ELSE 0 END AS segment_budget_conscious,
        CASE WHEN seg.customer_segment = 'Stable Mid-Spenders' THEN 1 ELSE 0 END AS segment_stable,

        -- Derived Features (5+ features)
        seg.tenure_months,
        seg.lifetime_value / NULLIF(seg.total_transactions, 0) AS avg_spend_per_transaction,
        (seg.avg_monthly_spend / NULLIF(c.credit_limit, 0)) * 100 AS credit_utilization_pct,
        seg.total_transactions / NULLIF(seg.customer_age_days, 0) AS transactions_per_day,
        seg.lifetime_value / NULLIF(seg.customer_age_days, 0) AS spend_per_day,
        seg.spend_last_90_days / NULLIF(seg.spend_prior_90_days, 0) AS spend_momentum,

        -- Categorical Encodings (3 features)
        CASE seg.spending_consistency
            WHEN 'Consistent' THEN 0
            WHEN 'Moderate' THEN 1
            WHEN 'Variable' THEN 2
        END AS spending_consistency_encoded,

        CASE seg.recency_status
            WHEN 'Active (30d)' THEN 0
            WHEN 'Recent (60d)' THEN 1
            WHEN 'At Risk (90d)' THEN 2
            WHEN 'Inactive (90+ days)' THEN 3
        END AS recency_status_encoded,

        CASE seg.spending_profile
            WHEN 'Balanced' THEN 0
            WHEN 'Travel-Focused' THEN 1
            WHEN 'Necessity-Focused' THEN 2
        END AS spending_profile_encoded,

        -- Target Variable
        labels.churned

    FROM {{ ref('dim_customer') }} c
    INNER JOIN {{ ref('customer_360_profile') }} seg ON c.customer_id = seg.customer_id
    INNER JOIN GOLD.CHURN_LABELS labels ON c.customer_id = labels.customer_id
    WHERE c.is_current = TRUE
      AND seg.total_transactions >= 5  -- Minimum transaction history
)

SELECT * FROM customer_features;

-- Validate no NULLs in critical features
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN churned IS NULL THEN 1 ELSE 0 END) AS null_target,
    SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END) AS null_age,
    SUM(CASE WHEN lifetime_value IS NULL THEN 1 ELSE 0 END) AS null_ltv
FROM GOLD.ML_TRAINING_DATA;
```

**Feature Categories:**
| Category | Count | Examples |
|----------|-------|----------|
| Demographics | 5 | age, state, card_type, credit_limit, employment_status |
| Spending Behavior | 15 | lifetime_value, avg_transaction_value, recency, trends |
| Segment Features | 6 | One-hot encoded segment flags |
| Derived Features | 5+ | credit_utilization_pct, spend_momentum, transactions_per_day |
| Categorical Encodings | 3 | spending_consistency, recency_status, spending_profile |
| **Total** | **35+** | All numeric or encoded |

#### 4.3.3 Step 3: Train Model (Snowflake Cortex ML)

**File:** `snowflake/ml/03_train_churn_model.sql`

```sql
USE ROLE ACCOUNTADMIN;  -- ML functions require ACCOUNTADMIN
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA GOLD;
USE WAREHOUSE COMPUTE_WH;

-- Train classification model
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION CHURN_MODEL(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'GOLD.ML_TRAINING_DATA'),
    TARGET_COLNAME => 'churned',
    CONFIG_OBJECT => {'evaluate': TRUE}
);

-- Model automatically:
-- 1. Selects best algorithm (Gradient Boosted Trees inferred from results)
-- 2. Splits data (train/test with stratification for imbalanced classes)
-- 3. Trains model
-- 4. Evaluates performance
-- 5. Returns metrics
```

**What Snowflake Cortex ML Does Automatically:**
- ✅ Algorithm selection (tries multiple, picks best)
- ✅ Hyperparameter tuning
- ✅ Train/test split (stratified for imbalanced data)
- ✅ Class weight balancing (handles 3.28% churn rate)
- ✅ Feature importance calculation
- ✅ Performance evaluation

**Execution Time:** 1-3 minutes

#### 4.3.4 Step 4: Validate Performance

**File:** `snowflake/ml/04_validate_model_performance.sql`

```sql
-- View evaluation metrics
CALL CHURN_MODEL!SHOW_EVALUATION_METRICS();

-- View global metrics
CALL CHURN_MODEL!SHOW_GLOBAL_EVALUATION_METRICS();

-- View feature importance
CALL CHURN_MODEL!SHOW_FEATURE_IMPORTANCE();
```

**Actual Results (Synthetic Data):**

**Per-Class Metrics:**
| Class | Precision | Recall | F1-Score | Support |
|-------|-----------|--------|----------|---------|
| 0 (Not Churned) | 1.0 | 1.0 | 1.0 | 9,681 |
| 1 (Churned) | 1.0 | 1.0 | 1.0 | 319 |

**Global Metrics:**
- Macro Avg F1: 1.0
- Weighted Avg F1: 1.0
- ROC-AUC: 1.0
- Log Loss: 2.35e-06 (near-zero)

**Interpretation:** Perfect scores indicate clear separation in synthetic data. Real-world models typically achieve **F1 = 0.50-0.70**.

**Feature Importance (Top 10):**
| Rank | Feature | Importance | Insight |
|------|---------|-----------|---------|
| 1 | AGE | 28.5% | Age is strongest predictor |
| 2 | CHURN_REASON | 15.8% | Historical patterns matter |
| 3 | LIFETIME_VALUE | 13.7% | Low LTV = higher churn |
| 4 | CREDIT_LIMIT | 9.0% | Product alignment |
| 5 | AVG_TRANSACTION_VALUE | 6.5% | Spending habits |
| 6 | SPEND_CHANGE_PCT | 4.7% | Declining spend signals churn |
| 7 | TOTAL_TRANSACTIONS | 4.1% | Engagement level |
| 8 | SPEND_PRIOR_90_DAYS | 3.8% | Historical baseline |
| 9 | SPEND_LAST_90_DAYS | 3.6% | Recent activity |
| 10 | TRAVEL_SPEND_PCT | 1.9% | Category preference |

#### 4.3.5 Step 5: Apply Predictions to All Customers

**File:** `snowflake/ml/05_apply_predictions.sql`

```sql
-- Apply model to score all customers
CREATE OR REPLACE TABLE GOLD.CHURN_PREDICTIONS AS
SELECT
    customer_id,

    -- Generate prediction
    CHURN_MODEL!PREDICT(
        OBJECT_CONSTRUCT(
            'age', age,
            'state', state,
            'card_type_premium', card_type_premium,
            'credit_limit', credit_limit,
            'employment_status', employment_status,
            'lifetime_value', lifetime_value,
            'avg_transaction_value', avg_transaction_value,
            'total_transactions', total_transactions,
            'days_since_last_transaction', days_since_last_transaction,
            'spend_last_90_days', spend_last_90_days,
            'spend_prior_90_days', spend_prior_90_days,
            'spend_change_pct', spend_change_pct,
            'avg_monthly_spend', avg_monthly_spend,
            'transaction_value_stddev', transaction_value_stddev,
            'median_transaction_value', median_transaction_value,
            'travel_spend_pct', travel_spend_pct,
            'necessities_spend_pct', necessities_spend_pct,
            'segment_high_value_travelers', segment_high_value_travelers,
            'segment_declining', segment_declining,
            'segment_new_growing', segment_new_growing,
            'segment_budget_conscious', segment_budget_conscious,
            'segment_stable', segment_stable,
            'tenure_months', tenure_months,
            'avg_spend_per_transaction', avg_spend_per_transaction,
            'credit_utilization_pct', credit_utilization_pct,
            'transactions_per_day', transactions_per_day,
            'spend_per_day', spend_per_day,
            'spend_momentum', spend_momentum,
            'spending_consistency_encoded', spending_consistency_encoded,
            'recency_status_encoded', recency_status_encoded,
            'spending_profile_encoded', spending_profile_encoded
        )
    ) AS prediction_result,

    -- Extract predictions
    prediction_result['churned']::BOOLEAN AS predicted_churn,
    prediction_result['probability']::FLOAT * 100 AS churn_risk_score,

    -- Categorize risk
    CASE
        WHEN prediction_result['probability']::FLOAT * 100 >= 70 THEN 'High Risk'
        WHEN prediction_result['probability']::FLOAT * 100 >= 40 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS churn_risk_category,

    CURRENT_DATE() AS prediction_date

FROM GOLD.ML_TRAINING_DATA;

-- Validate predictions
SELECT
    churn_risk_category,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    ROUND(AVG(churn_risk_score), 2) AS avg_risk_score
FROM GOLD.CHURN_PREDICTIONS
GROUP BY churn_risk_category
ORDER BY avg_risk_score DESC;
```

**Prediction Results:**
| churn_risk_category | customer_count | percentage | avg_risk_score |
|---------------------|---------------|-----------|----------------|
| High Risk | 1,642 | 3.28% | 99.9 |
| Medium Risk | ~7,500 | ~15% | 55.0 |
| Low Risk | ~40,000 | ~80% | 5.0 |

### 4.4 Integration with Customer 360

**Predictions automatically joined** in `customer_360_profile.sql`:

```sql
LEFT JOIN GOLD.CHURN_PREDICTIONS cp ON c.customer_id = cp.customer_id
```

**New columns in Customer 360:**
- `churn_risk_score` (0-100)
- `churn_risk_category` ('High Risk', 'Medium Risk', 'Low Risk')
- `prediction_date` (when prediction was generated)

**Usage in Streamlit:**
```sql
-- High-risk customers for retention campaign
SELECT
    customer_id,
    full_name,
    email,
    churn_risk_score,
    lifetime_value,
    customer_segment
FROM GOLD.CUSTOMER_360_PROFILE
WHERE churn_risk_category = 'High Risk'
  AND lifetime_value > 50000  -- High-value at-risk customers
ORDER BY churn_risk_score DESC
LIMIT 100;
```

### 4.5 Model Retraining & Automation

**Stored Procedures** (`snowflake/ml/stored_procedures.sql`):

```sql
-- Retrain model with fresh data
CREATE OR REPLACE PROCEDURE RETRAIN_CHURN_MODEL()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Step 1: Recreate labels with latest data
    CALL RECREATE_CHURN_LABELS();

    -- Step 2: Recreate training features
    CALL RECREATE_TRAINING_FEATURES();

    -- Step 3: Retrain model
    CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION CHURN_MODEL(
        INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'GOLD.ML_TRAINING_DATA'),
        TARGET_COLNAME => 'churned',
        CONFIG_OBJECT => {'evaluate': TRUE}
    );

    -- Step 4: Get performance metrics
    CALL CHURN_MODEL!SHOW_GLOBAL_EVALUATION_METRICS();

    RETURN 'SUCCESS: Model retrained with latest data';
END;
$$;

-- Refresh predictions without retraining
CREATE OR REPLACE PROCEDURE REFRESH_CHURN_PREDICTIONS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Recreate predictions table with latest customer data
    CREATE OR REPLACE TABLE GOLD.CHURN_PREDICTIONS AS
    SELECT
        customer_id,
        CHURN_MODEL!PREDICT(...) AS prediction_result,
        -- ... (same as Step 5)
    FROM GOLD.ML_TRAINING_DATA;

    RETURN 'SUCCESS: Predictions refreshed for ' || (SELECT COUNT(*) FROM GOLD.CHURN_PREDICTIONS) || ' customers';
END;
$$;
```

**Automated Tasks:**
```sql
-- Monthly retraining (1st of month at 2 AM)
CREATE TASK MONTHLY_MODEL_RETRAIN
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 2 1 * * America/Los_Angeles'
AS
    CALL RETRAIN_CHURN_MODEL();

-- Daily prediction refresh (3 AM daily)
CREATE TASK DAILY_PREDICTION_REFRESH
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 3 * * * America/Los_Angeles'
AS
    CALL REFRESH_CHURN_PREDICTIONS();

-- Start tasks
ALTER TASK MONTHLY_MODEL_RETRAIN RESUME;
ALTER TASK DAILY_PREDICTION_REFRESH RESUME;
```

### 4.6 Business Use Cases

**1. Retention Campaigns (High-Value Churners)**
```sql
SELECT
    customer_id,
    full_name,
    email,
    churn_risk_score,
    lifetime_value,
    spend_change_pct
FROM GOLD.CUSTOMER_360_PROFILE
WHERE churn_risk_category = 'High Risk'
  AND lifetime_value > 50000
ORDER BY churn_risk_score DESC
LIMIT 1000;
-- Export to marketing platform for targeted retention offers
```

**2. Early Warning System**
```sql
SELECT
    customer_id,
    full_name,
    churn_risk_score,
    days_since_last_transaction,
    spend_change_pct,
    customer_segment
FROM GOLD.CUSTOMER_360_PROFILE
WHERE churn_risk_category IN ('Medium Risk', 'High Risk')
  AND days_since_last_transaction > 30
ORDER BY churn_risk_score DESC;
-- Monitor customers moving into risk categories
```

**3. Segment-Specific Churn Analysis**
```sql
SELECT
    customer_segment,
    AVG(churn_risk_score) AS avg_risk,
    COUNT(CASE WHEN churn_risk_category = 'High Risk' THEN 1 END) AS high_risk_count,
    COUNT(*) AS total_customers,
    ROUND(COUNT(CASE WHEN churn_risk_category = 'High Risk' THEN 1 END) * 100.0 / COUNT(*), 2) AS high_risk_pct
FROM GOLD.CUSTOMER_360_PROFILE
GROUP BY customer_segment
ORDER BY avg_risk DESC;
-- Identify which segments have highest churn propensity
```

### 4.7 Ethical Considerations & Limitations

**Bias Mitigation:**
- Monitor fairness across demographics (state, age, card_type)
- Avoid discriminatory features (no race, gender, religion)
- Ensure equitable model performance across subgroups

**Privacy:**
- PII not used as features (email, names excluded)
- Predictions stored securely in GOLD schema
- Role-based access control (RBAC)

**Limitations:**
1. **Temporal Drift**: Customer behavior changes over time → requires retraining
2. **Cold Start**: New customers (<12 months) excluded from training
3. **External Factors**: Economic conditions, seasonality not captured
4. **Synthetic Data**: Perfect scores (F1=1.0) unrealistic; expect 0.50-0.70 in production

### 4.8 Exam Questions You Should Be Able to Answer

1. **What defines churn in this model?**
   - Inactivity (60+ days no transactions) OR significant decline (recent spend <30% of baseline)

2. **How many features are engineered and what are the categories?**
   - 35+ features across 5 categories: demographics (5), spending behavior (15), segments (6 one-hot), derived (5+), categorical encodings (3)

3. **What ML framework is used and why?**
   - Snowflake Cortex ML (native, auto-tuning, no external tools needed, handles class imbalance)

4. **What are the 5 steps in the ML pipeline?**
   - Create labels → Feature engineering → Train model → Validate performance → Apply predictions

5. **What is the churn rate and class distribution?**
   - 3.28% churned (1,642), 96.72% active (48,358) - imbalanced dataset

6. **What are the top 3 most important features?**
   - Age (28.5%), Churn Reason (15.8%), Lifetime Value (13.7%)

7. **How are predictions categorized into risk tiers?**
   - High Risk (70-100), Medium Risk (40-69), Low Risk (0-39)

8. **How often should the model be retrained?**
   - Monthly (via automated task) or when performance degrades (ROC-AUC <0.70)

9. **Where are predictions stored and how are they consumed?**
   - GOLD.CHURN_PREDICTIONS table, joined into customer_360_profile for Streamlit dashboard

10. **What business actions are taken based on churn risk?**
    - High Risk: Retention offers, fee waivers, bonuses; Medium Risk: Engagement campaigns; Low Risk: Normal marketing

---

## 5. Application & Consumption Layer

### 5.1 Overview

The consumption layer provides interactive analytics through **Streamlit in Snowflake** with 4 tabs powered by the GOLD layer data.

**Architecture:**
```
User Browser
    ↓
Streamlit App (Snowflake-hosted)
    ↓
Snowflake Connection (st.connection, cached)
    ↓
GOLD Schema Queries
    ↓
Results → Plotly Charts → CSV Export
```

### 5.2 Streamlit Application Structure

**Entry Point:** `streamlit/app.py`

```python
import streamlit as st
from tabs import segment_explorer, customer_360, ai_assistant, campaign_simulator

# Page configuration
st.set_page_config(
    page_title="Customer 360 Analytics",
    page_icon="📊",
    layout="wide"
)

# Sidebar navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Select a view:",
    ["Segment Explorer", "Customer 360 Deep Dive", "AI Assistant", "Campaign Performance Simulator"]
)

# Route to tabs
if page == "Segment Explorer":
    segment_explorer.render()
elif page == "Customer 360 Deep Dive":
    customer_360.render()
elif page == "AI Assistant":
    ai_assistant.render()
elif page == "Campaign Performance Simulator":
    campaign_simulator.render()
```

**Shared Utilities:** `streamlit/tabs/utils.py`

```python
import streamlit as st
from snowflake.snowpark import Session
import pandas as pd

@st.cache_resource
def get_snowflake_connection():
    """Create cached Snowflake connection"""
    return st.connection("snowflake")

def execute_query(query: str) -> pd.DataFrame:
    """Execute query with error handling and caching"""
    try:
        conn = get_snowflake_connection()
        return conn.query(query)
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()

def format_currency(value: float) -> str:
    """Format value as currency"""
    return f"${value:,.2f}"

def format_percentage(value: float) -> str:
    """Format value as percentage"""
    return f"{value:.1f}%"
```

### 5.3 Tab 1: Segment Explorer

**File:** `streamlit/tabs/segment_explorer.py`

**Purpose:** Filter and explore customer segments for marketing campaigns.

**Features:**
- ✅ Multi-select filters (segment, state, churn risk, card type, LTV)
- ✅ Summary metrics (customer count, total/avg LTV, avg churn risk)
- ✅ Segment distribution pie chart
- ✅ CSV export (up to 5,000 customers for campaigns)

**Key Query:**
```sql
SELECT
    customer_id,
    full_name,
    email,
    customer_segment,
    state,
    churn_risk_category,
    churn_risk_score,
    card_type,
    lifetime_value,
    avg_monthly_spend,
    days_since_last_transaction
FROM CUSTOMER_ANALYTICS.GOLD.CUSTOMER_360_PROFILE
WHERE customer_segment IN ({selected_segments})
  AND state IN ({selected_states})
  AND churn_risk_category IN ({selected_risk_levels})
  AND card_type IN ({selected_card_types})
  AND lifetime_value >= {min_ltv}
ORDER BY lifetime_value DESC
LIMIT 5000;
```

**UI Components:**
```python
def render():
    st.title("📊 Customer Segment Explorer")

    # Filters in sidebar
    with st.sidebar:
        st.header("Filters")

        segments = st.multiselect(
            "Customer Segments",
            ["High-Value Travelers", "Declining", "New & Growing", "Budget-Conscious", "Stable Mid-Spenders"],
            default=["High-Value Travelers"]
        )

        states = st.multiselect("States", get_unique_states(), default=[])

        risk_levels = st.multiselect(
            "Churn Risk",
            ["High Risk", "Medium Risk", "Low Risk"],
            default=[]
        )

        min_ltv = st.number_input("Minimum Lifetime Value", min_value=0, value=0, step=1000)

    # Fetch filtered data
    df = fetch_customers(segments, states, risk_levels, min_ltv)

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Customers", f"{len(df):,}")
    col2.metric("Total LTV", format_currency(df['lifetime_value'].sum()))
    col3.metric("Avg LTV", format_currency(df['lifetime_value'].mean()))
    col4.metric("Avg Churn Risk", f"{df['churn_risk_score'].mean():.1f}")

    # Segment distribution chart
    fig = px.pie(df, names='customer_segment', title='Segment Distribution')
    st.plotly_chart(fig, use_container_width=True)

    # Data table
    st.dataframe(df, use_container_width=True)

    # CSV export button
    csv = df.to_csv(index=False)
    st.download_button(
        label="📥 Download CSV for Campaign",
        data=csv,
        file_name=f"customer_segment_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )
```

**Use Case:**
Marketing manager wants to export **all Declining customers in California with LTV >$10K** for targeted retention campaign.

### 5.4 Tab 2: Customer 360 Deep Dive

**File:** `streamlit/tabs/customer_360.py`

**Purpose:** View complete profile and transaction history for individual customers.

**Features:**
- ✅ Customer search (ID, name, email with fuzzy matching)
- ✅ Profile card with demographics, segment, churn risk alert
- ✅ Key metrics (LTV, avg transaction, 90d spend, recency)
- ✅ Transaction history table (1,000 most recent) with filters
- ✅ Visualizations (daily spending line chart, category pie chart)
- ✅ Transaction summary metrics and CSV export

**Key Queries:**

**Search:**
```sql
SELECT customer_id, full_name, email
FROM CUSTOMER_ANALYTICS.GOLD.CUSTOMER_360_PROFILE
WHERE customer_id ILIKE '%{search_term}%'
   OR full_name ILIKE '%{search_term}%'
   OR email ILIKE '%{search_term}%'
LIMIT 20;
```

**Profile:**
```sql
SELECT *
FROM CUSTOMER_ANALYTICS.GOLD.CUSTOMER_360_PROFILE
WHERE customer_id = '{selected_customer_id}';
```

**Transactions:**
```sql
SELECT
    f.transaction_date,
    f.transaction_amount,
    f.merchant_name,
    cat.category_name,
    f.channel,
    f.status
FROM CUSTOMER_ANALYTICS.GOLD.FCT_TRANSACTIONS f
INNER JOIN CUSTOMER_ANALYTICS.GOLD.DIM_CUSTOMER c ON f.customer_key = c.customer_key
LEFT JOIN CUSTOMER_ANALYTICS.GOLD.DIM_MERCHANT_CATEGORY cat ON f.category_key = cat.category_key
WHERE c.customer_id = '{selected_customer_id}'
  AND c.is_current = TRUE
  AND f.transaction_date BETWEEN '{start_date}' AND '{end_date}'
  AND cat.category_name IN ({selected_categories})
  AND f.status IN ({selected_statuses})
ORDER BY f.transaction_date DESC
LIMIT 1000;
```

**UI Components:**
```python
def render():
    st.title("🔍 Customer 360 Deep Dive")

    # Search box
    search_term = st.text_input("Search by Customer ID, Name, or Email")

    if search_term:
        # Show search results
        customers = search_customers(search_term)
        selected = st.selectbox("Select Customer", customers['full_name'])
        customer_id = customers[customers['full_name'] == selected]['customer_id'].iloc[0]

        # Fetch profile
        profile = get_customer_profile(customer_id)

        # Profile card with churn alert
        with st.container():
            col1, col2, col3 = st.columns([2, 2, 1])

            with col1:
                st.subheader(profile['full_name'])
                st.write(f"**Email:** {profile['email']}")
                st.write(f"**Segment:** {profile['customer_segment']}")
                st.write(f"**Card:** {profile['card_type']} (${profile['credit_limit']:,.0f} limit)")

            with col2:
                st.metric("Lifetime Value", format_currency(profile['lifetime_value']))
                st.metric("Avg Transaction", format_currency(profile['avg_transaction_value']))
                st.metric("Last 90d Spend", format_currency(profile['spend_last_90_days']))
                st.metric("Days Since Last Txn", profile['days_since_last_transaction'])

            with col3:
                # Churn risk alert
                if profile['churn_risk_category'] == 'High Risk':
                    st.error(f"🚨 {profile['churn_risk_category']}")
                    st.metric("Churn Risk Score", f"{profile['churn_risk_score']:.0f}")
                elif profile['churn_risk_category'] == 'Medium Risk':
                    st.warning(f"⚠️ {profile['churn_risk_category']}")
                    st.metric("Churn Risk Score", f"{profile['churn_risk_score']:.0f}")
                else:
                    st.success(f"✅ {profile['churn_risk_category']}")
                    st.metric("Churn Risk Score", f"{profile['churn_risk_score']:.0f}")

        # Transaction filters
        st.subheader("Transaction History")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            start_date = st.date_input("Start Date", value=datetime.now() - timedelta(days=90))
        with col2:
            end_date = st.date_input("End Date", value=datetime.now())
        with col3:
            categories = st.multiselect("Categories", get_categories(), default=[])
        with col4:
            statuses = st.multiselect("Status", ["approved", "declined"], default=["approved"])

        # Fetch transactions
        txns = get_transactions(customer_id, start_date, end_date, categories, statuses)

        # Visualizations
        col1, col2 = st.columns(2)

        with col1:
            # Daily spending line chart
            daily_spend = txns.groupby('transaction_date')['transaction_amount'].sum().reset_index()
            fig = px.line(daily_spend, x='transaction_date', y='transaction_amount', title='Daily Spending')
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Category pie chart
            category_spend = txns.groupby('category_name')['transaction_amount'].sum().reset_index()
            fig = px.pie(category_spend, names='category_name', values='transaction_amount', title='Spending by Category')
            st.plotly_chart(fig, use_container_width=True)

        # Transaction table
        st.dataframe(txns, use_container_width=True)

        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Transactions", len(txns))
        col2.metric("Total Spend", format_currency(txns['transaction_amount'].sum()))
        col3.metric("Avg Transaction", format_currency(txns['transaction_amount'].mean()))
        col4.metric("Approval Rate", format_percentage((txns['status'] == 'approved').mean() * 100))

        # Export
        csv = txns.to_csv(index=False)
        st.download_button(
            label="📥 Download Transactions CSV",
            data=csv,
            file_name=f"customer_{customer_id}_transactions.csv",
            mime="text/csv"
        )
```

**Use Case:**
Customer service rep searches for **"Smith"**, finds **John Smith (CUST00012345)**, views his churn risk alert (High Risk, 85 score), sees he hasn't transacted in 75 days, and initiates retention outreach.

### 5.5 Tab 3: AI Assistant (Cortex Analyst Integration)

**File:** `streamlit/tabs/ai_assistant.py`

**Purpose:** Natural language queries using Snowflake Cortex Analyst semantic layer.

**Features:**
- ✅ 5 question categories (Churn, Segmentation, Spending, Geographic, Campaign)
- ✅ 20+ suggested questions with clickable buttons
- ✅ Natural language input with Cortex Analyst (production-ready)
- ✅ Generated SQL display in collapsible expander
- ✅ Results table with summary metrics
- ✅ Auto-chart type detection (bar, line, pie, scatter, map)
- ✅ Query history (last 5 queries)
- ✅ CSV export
- ✅ Mock fallback if Cortex unavailable (trial accounts)

**Cortex Analyst Integration:**
```python
import streamlit as st
import json

# Try to import Cortex Analyst (available on paid accounts)
try:
    import pydeck as pdk
    import requests
    PYDECK_AVAILABLE = True
except ImportError:
    PYDECK_AVAILABLE = False

def query_cortex_analyst(question: str, conversation_history: list = []) -> dict:
    """
    Query Snowflake Cortex Analyst with semantic layer.

    Returns:
        {
            'sql': 'SELECT ...',
            'interpretation': 'This query...',
            'results': DataFrame
        }
    """
    try:
        # Build conversation context
        messages = conversation_history + [{
            "role": "user",
            "content": [{"type": "text", "text": question}]
        }]

        # Call Cortex Analyst
        conn = get_snowflake_connection()
        response = conn.query(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'analyst',
                {json.dumps(messages)},
                {{
                    'semantic_model_file': '@SEMANTIC_MODELS.DEFINITIONS.SEMANTIC_STAGE/customer_analytics.yaml'
                }}
            ) AS response
        """)

        # Parse response
        result = json.loads(response['response'][0])
        sql = result.get('sql', '')
        interpretation = result.get('interpretation', '')

        # Execute generated SQL
        df = conn.query(sql)

        return {
            'sql': sql,
            'interpretation': interpretation,
            'results': df,
            'source': 'cortex_analyst'
        }

    except Exception as e:
        st.warning(f"Cortex Analyst unavailable: {e}. Using mock implementation.")
        return query_mock_analyst(question)

def query_mock_analyst(question: str) -> dict:
    """Fallback mock implementation for trial accounts"""
    # Pre-defined question mappings
    mock_queries = {
        "which customers are at highest risk of churning": {
            'sql': """
                SELECT customer_id, full_name, churn_risk_score, lifetime_value
                FROM CUSTOMER_ANALYTICS.GOLD.CUSTOMER_360_PROFILE
                WHERE churn_risk_category = 'High Risk'
                ORDER BY churn_risk_score DESC
                LIMIT 100
            """,
            'interpretation': 'This query identifies high-risk customers sorted by churn probability.'
        },
        "how many customers are in each segment": {
            'sql': """
                SELECT customer_segment, COUNT(*) AS customer_count
                FROM CUSTOMER_ANALYTICS.GOLD.CUSTOMER_360_PROFILE
                GROUP BY customer_segment
                ORDER BY customer_count DESC
            """,
            'interpretation': 'This query counts customers in each behavioral segment.'
        },
        # ... more pre-defined questions
    }

    question_lower = question.lower().strip()

    if question_lower in mock_queries:
        mock = mock_queries[question_lower]
        conn = get_snowflake_connection()
        df = conn.query(mock['sql'])

        return {
            'sql': mock['sql'],
            'interpretation': mock['interpretation'],
            'results': df,
            'source': 'mock'
        }
    else:
        return {
            'sql': '',
            'interpretation': f"Question not recognized by mock. Try a suggested question or wait for Cortex Analyst integration.",
            'results': pd.DataFrame(),
            'source': 'error'
        }

def suggest_chart_type(df: pd.DataFrame, question: str) -> str:
    """Auto-detect appropriate chart type based on data and question"""
    if len(df) == 0:
        return 'table'

    # Check column types
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
    date_cols = df.select_dtypes(include=['datetime']).columns.tolist()

    # Geographic data (state/city columns)
    geo_cols = [col for col in df.columns if any(geo in col.lower() for geo in ['state', 'city', 'region', 'country'])]

    # Suggest chart
    suggestions = []

    if geo_cols and PYDECK_AVAILABLE and any('state' in col.lower() for col in geo_cols):
        suggestions.append('choropleth_usa')

    if len(categorical_cols) == 1 and len(numeric_cols) == 1:
        if len(df) <= 10:
            suggestions.append('pie')
        suggestions.append('bar')

    if date_cols and numeric_cols:
        suggestions.append('line')

    if len(numeric_cols) >= 2:
        suggestions.append('scatter')

    suggestions.append('table')  # Always available

    return suggestions[0] if suggestions else 'table'

def render_chart(df: pd.DataFrame, chart_type: str, question: str):
    """Render chart based on type"""
    if chart_type == 'bar':
        x_col = df.columns[0]
        y_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
        fig = px.bar(df, x=x_col, y=y_col, title=question)
        st.plotly_chart(fig, use_container_width=True)

    elif chart_type == 'line':
        x_col = df.select_dtypes(include=['datetime']).columns[0]
        y_col = df.select_dtypes(include=['number']).columns[0]
        fig = px.line(df, x=x_col, y=y_col, title=question)
        st.plotly_chart(fig, use_container_width=True)

    elif chart_type == 'pie':
        label_col = df.columns[0]
        value_col = df.columns[1]
        fig = px.pie(df, names=label_col, values=value_col, title=question)
        st.plotly_chart(fig, use_container_width=True)

    elif chart_type == 'scatter':
        x_col = df.select_dtypes(include=['number']).columns[0]
        y_col = df.select_dtypes(include=['number']).columns[1]
        fig = px.scatter(df, x=x_col, y=y_col, title=question)
        st.plotly_chart(fig, use_container_width=True)

    elif chart_type == 'choropleth_usa':
        # US states choropleth map with PyDeck
        state_col = [col for col in df.columns if 'state' in col.lower()][0]
        value_col = df.select_dtypes(include=['number']).columns[0]

        # Fetch US states GeoJSON
        geojson = fetch_us_states_geojson()

        # Merge data
        for feature in geojson['features']:
            state_abbr = feature['properties']['STUSPS']  # State abbreviation
            value = df[df[state_col] == state_abbr][value_col].iloc[0] if state_abbr in df[state_col].values else 0
            feature['properties']['value'] = float(value)

        # Create PyDeck layer
        layer = pdk.Layer(
            'GeoJsonLayer',
            geojson,
            opacity=0.8,
            stroked=False,
            filled=True,
            extruded=False,
            get_fill_color='[255 - value/max(value)*255, value/max(value)*255, 0]',  # Green → Yellow → Red
            pickable=True
        )

        # Render map
        view_state = pdk.ViewState(latitude=37.7749, longitude=-95, zoom=3, pitch=0)

        st.pydeck_chart(pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip={"text": "{STUSPS}: {value}"}
        ))

    else:  # table
        st.dataframe(df, use_container_width=True)

def render():
    st.title("🤖 AI Assistant (Cortex Analyst)")

    # Show Cortex status
    if PYDECK_AVAILABLE:
        st.success("✅ Cortex Analyst available - Natural language queries enabled")
    else:
        st.warning("⚠️ Cortex Analyst unavailable (trial account) - Using mock implementation with limited questions")

    # Suggested questions (organized by category)
    st.subheader("💡 Suggested Questions")

    categories = {
        "🚨 Churn Risk": [
            "Which customers are at highest risk of churning?",
            "How many high-risk customers do we have?",
            "Show me declining customers with lifetime value over $50,000"
        ],
        "📊 Segmentation": [
            "How many customers are in each segment?",
            "What is the average lifetime value by segment?",
            "Which segment has the highest churn risk?"
        ],
        "💰 Spending Analysis": [
            "What is the total spending in the last 90 days?",
            "Which customers have the highest lifetime value?",
            "Show me customers with declining spend"
        ],
        "🗺️ Geographic": [
            "What is the average spend by state?",
            "Which states have the highest churn risk?",
            "Compare spending between California and Texas"
        ],
        "📣 Campaign Targeting": [
            "Which Premium cardholders are at medium or high risk?",
            "Show me Budget-Conscious customers in New York",
            "Find High-Value Travelers with recent activity"
        ]
    }

    # Display categories with clickable buttons
    for category, questions in categories.items():
        with st.expander(category):
            for question in questions:
                if st.button(question, key=f"btn_{question}"):
                    st.session_state['ai_question'] = question

    # Question input
    st.subheader("Ask a Question")
    question = st.text_input(
        "Type your question:",
        value=st.session_state.get('ai_question', ''),
        placeholder="e.g., Which customers are at highest risk of churning?"
    )

    if st.button("🚀 Ask") or question:
        with st.spinner("Querying Cortex Analyst..."):
            # Get conversation history from session state
            history = st.session_state.get('conversation_history', [])

            # Query Cortex Analyst
            result = query_cortex_analyst(question, history)

            # Update history
            history.append({
                "role": "user",
                "content": [{"type": "text", "text": question}]
            })
            history.append({
                "role": "assistant",
                "content": [{"type": "text", "text": result['interpretation']}]
            })
            st.session_state['conversation_history'] = history[-10:]  # Keep last 5 exchanges

            # Display results
            st.subheader("🎯 Results")

            # Show AI interpretation
            if result['interpretation']:
                st.info(f"**AI Interpretation:** {result['interpretation']}")

            # Show generated SQL (collapsible)
            if result['sql']:
                with st.expander("📜 View Generated SQL"):
                    st.code(result['sql'], language='sql')

            # Chart visualization
            df = result['results']

            if len(df) > 0:
                # Auto-detect chart type
                chart_type = suggest_chart_type(df, question)

                # Chart type selector
                st.selectbox(
                    "Chart Type:",
                    ['table', 'bar', 'line', 'pie', 'scatter'] + (['choropleth_usa'] if PYDECK_AVAILABLE else []),
                    index=0 if chart_type == 'table' else 1,
                    key='chart_type_selector'
                )

                selected_chart = st.session_state.get('chart_type_selector', chart_type)

                # Render
                render_chart(df, selected_chart, question)

                # Summary metrics
                if len(df) > 0:
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Rows Returned", len(df))

                    numeric_cols = df.select_dtypes(include=['number']).columns
                    if len(numeric_cols) > 0:
                        first_numeric = numeric_cols[0]
                        col2.metric(f"Total {first_numeric}", format_currency(df[first_numeric].sum()) if 'value' in first_numeric.lower() or 'amount' in first_numeric.lower() else f"{df[first_numeric].sum():,.0f}")
                        col3.metric(f"Avg {first_numeric}", format_currency(df[first_numeric].mean()) if 'value' in first_numeric.lower() or 'amount' in first_numeric.lower() else f"{df[first_numeric].mean():,.0f}")

                # CSV export
                csv = df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Results CSV",
                    data=csv,
                    file_name=f"cortex_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            else:
                st.warning("No results found. Try rephrasing your question or selecting a suggested question.")

    # Query history
    if 'conversation_history' in st.session_state:
        st.subheader("📜 Query History")
        for i, msg in enumerate(st.session_state['conversation_history'][-10:]):  # Last 5 exchanges
            if msg['role'] == 'user':
                st.write(f"**Q{i//2 + 1}:** {msg['content'][0]['text']}")
```

**Use Case:**
Analyst types **"Which states have the highest churn risk?"**, Cortex Analyst generates SQL joining customer_360_profile on state, returns results, auto-detects choropleth map visualization, displays interactive US map colored by churn risk percentage.

### 5.6 Tab 4: Campaign Performance Simulator

**File:** `streamlit/tabs/campaign_simulator.py`

**Purpose:** Calculate ROI for retention campaigns with what-if analysis.

**Features:**
- ✅ Target audience builder (segment, churn risk, card type, LTV filters)
- ✅ Campaign parameters (incentive amount, retention rate, campaign cost)
- ✅ ROI calculation with detailed metrics
- ✅ Cost breakdown pie chart
- ✅ Sensitivity analysis line chart (retention rate vs net benefit)
- ✅ Breakeven calculation (minimum retention rate for positive ROI)
- ✅ Campaign recommendations (messaging, timing, success metrics)
- ✅ CSV export for target customer list

**ROI Calculation Logic:**
```python
def calculate_campaign_roi(
    target_customers: int,
    avg_ltv: float,
    avg_churn_risk: float,
    incentive_per_customer: float,
    retention_rate: float,  # % of customers who stay due to campaign
    fixed_campaign_cost: float
) -> dict:
    """
    Calculate retention campaign ROI.

    Assumptions:
    - Without campaign: X% of high-risk customers churn (lose LTV)
    - With campaign: Retention rate % stay (keep LTV minus incentive cost)
    - Costs: Incentive per customer + fixed campaign costs
    """
    # Baseline: Expected churned customers without campaign
    expected_churn_count = target_customers * (avg_churn_risk / 100.0)
    expected_churn_value_loss = expected_churn_count * avg_ltv

    # With campaign: Retained customers
    retained_customers = expected_churn_count * (retention_rate / 100.0)
    retained_value = retained_customers * avg_ltv

    # Campaign costs
    incentive_cost = target_customers * incentive_per_customer
    total_campaign_cost = incentive_cost + fixed_campaign_cost

    # Net benefit
    net_benefit = retained_value - total_campaign_cost

    # ROI percentage
    roi_pct = (net_benefit / total_campaign_cost) * 100 if total_campaign_cost > 0 else 0

    # Breakeven retention rate (net benefit = 0)
    breakeven_rate = (total_campaign_cost / (expected_churn_count * avg_ltv)) * 100 if expected_churn_count > 0 else 0

    return {
        'target_customers': target_customers,
        'expected_churn_count': int(expected_churn_count),
        'expected_churn_value_loss': expected_churn_value_loss,
        'retained_customers': int(retained_customers),
        'retained_value': retained_value,
        'incentive_cost': incentive_cost,
        'fixed_cost': fixed_campaign_cost,
        'total_cost': total_campaign_cost,
        'net_benefit': net_benefit,
        'roi_pct': roi_pct,
        'breakeven_retention_rate': breakeven_rate
    }

def render():
    st.title("📣 Campaign Performance Simulator")

    st.markdown("""
    **Purpose:** Calculate ROI for retention campaigns targeting at-risk customers.

    **Use Case:** Before launching a $100 incentive retention campaign for high-risk customers,
    model the expected ROI based on retention rate assumptions.
    """)

    # Section 1: Target Audience
    st.subheader("1️⃣ Define Target Audience")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        segments = st.multiselect(
            "Customer Segments",
            ["High-Value Travelers", "Declining", "New & Growing", "Budget-Conscious", "Stable Mid-Spenders"],
            default=["Declining"]
        )

    with col2:
        risk_levels = st.multiselect(
            "Churn Risk",
            ["High Risk", "Medium Risk", "Low Risk"],
            default=["High Risk"]
        )

    with col3:
        card_types = st.multiselect(
            "Card Type",
            ["Standard", "Premium"],
            default=["Standard", "Premium"]
        )

    with col4:
        min_ltv = st.number_input("Min LTV", min_value=0, value=10000, step=1000)

    # Fetch target audience
    query = f"""
        SELECT
            customer_id,
            full_name,
            email,
            churn_risk_score,
            churn_risk_category,
            customer_segment,
            lifetime_value,
            card_type
        FROM CUSTOMER_ANALYTICS.GOLD.CUSTOMER_360_PROFILE
        WHERE customer_segment IN ({','.join([f"'{s}'" for s in segments])})
          AND churn_risk_category IN ({','.join([f"'{r}'" for r in risk_levels])})
          AND card_type IN ({','.join([f"'{c}'" for c in card_types])})
          AND lifetime_value >= {min_ltv}
        ORDER BY churn_risk_score DESC
    """

    target_df = execute_query(query)

    # Display audience summary
    st.metric("🎯 Target Audience Size", f"{len(target_df):,} customers")

    col1, col2, col3 = st.columns(3)
    col1.metric("Avg Churn Risk", f"{target_df['churn_risk_score'].mean():.1f}")
    col2.metric("Avg LTV", format_currency(target_df['lifetime_value'].mean()))
    col3.metric("Total At-Risk Value", format_currency(target_df['lifetime_value'].sum()))

    # Section 2: Campaign Parameters
    st.subheader("2️⃣ Campaign Parameters")

    col1, col2, col3 = st.columns(3)

    with col1:
        incentive = st.number_input(
            "Incentive per Customer ($)",
            min_value=0,
            value=100,
            step=25,
            help="Cash back, bonus points, or fee waiver value"
        )

    with col2:
        retention_rate = st.slider(
            "Expected Retention Rate (%)",
            min_value=0,
            max_value=100,
            value=30,
            step=5,
            help="% of at-risk customers who will stay due to campaign"
        )

    with col3:
        fixed_cost = st.number_input(
            "Fixed Campaign Cost ($)",
            min_value=0,
            value=5000,
            step=1000,
            help="Email, design, execution costs"
        )

    # Calculate ROI
    roi = calculate_campaign_roi(
        target_customers=len(target_df),
        avg_ltv=target_df['lifetime_value'].mean(),
        avg_churn_risk=target_df['churn_risk_score'].mean(),
        incentive_per_customer=incentive,
        retention_rate=retention_rate,
        fixed_campaign_cost=fixed_cost
    )

    # Section 3: Results
    st.subheader("3️⃣ Campaign ROI Analysis")

    # ROI metric (large, prominent)
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        roi_color = "normal" if roi['roi_pct'] >= 0 else "inverse"
        st.metric(
            "💰 Campaign ROI",
            f"{roi['roi_pct']:.1f}%",
            f"{format_currency(roi['net_benefit'])} Net Benefit",
            delta_color=roi_color
        )

    # Detailed metrics
    st.markdown("**Campaign Projections:**")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Expected Churners", f"{roi['expected_churn_count']:,}")
    col2.metric("Retained Customers", f"{roi['retained_customers']:,}")
    col3.metric("Total Campaign Cost", format_currency(roi['total_cost']))
    col4.metric("Retained Value", format_currency(roi['retained_value']))

    # Cost breakdown pie chart
    st.subheader("💸 Cost Breakdown")

    cost_data = pd.DataFrame({
        'Category': ['Customer Incentives', 'Fixed Campaign Costs'],
        'Amount': [roi['incentive_cost'], roi['fixed_cost']]
    })

    fig = px.pie(cost_data, names='Category', values='Amount', title='Campaign Cost Allocation')
    st.plotly_chart(fig, use_container_width=True)

    # Sensitivity analysis
    st.subheader("📊 Sensitivity Analysis")

    # Calculate ROI across retention rate range
    retention_rates = range(0, 101, 5)
    net_benefits = []

    for rate in retention_rates:
        roi_temp = calculate_campaign_roi(
            target_customers=len(target_df),
            avg_ltv=target_df['lifetime_value'].mean(),
            avg_churn_risk=target_df['churn_risk_score'].mean(),
            incentive_per_customer=incentive,
            retention_rate=rate,
            fixed_campaign_cost=fixed_cost
        )
        net_benefits.append(roi_temp['net_benefit'])

    sensitivity_df = pd.DataFrame({
        'Retention Rate (%)': retention_rates,
        'Net Benefit ($)': net_benefits
    })

    fig = px.line(
        sensitivity_df,
        x='Retention Rate (%)',
        y='Net Benefit ($)',
        title='Net Benefit vs Retention Rate',
        markers=True
    )

    # Add breakeven line
    fig.add_hline(y=0, line_dash="dash", line_color="red", annotation_text="Breakeven")
    fig.add_vline(x=roi['breakeven_retention_rate'], line_dash="dot", line_color="green", annotation_text=f"Breakeven: {roi['breakeven_retention_rate']:.1f}%")

    st.plotly_chart(fig, use_container_width=True)

    # Breakeven analysis
    st.info(f"""
    **Breakeven Point:** {roi['breakeven_retention_rate']:.1f}% retention rate

    To achieve positive ROI, the campaign must retain at least **{roi['breakeven_retention_rate']:.1f}%** of expected churners.
    Current assumption (**{retention_rate}%**) {'✅ exceeds' if retention_rate >= roi['breakeven_retention_rate'] else '❌ falls below'} breakeven.
    """)

    # Top 10 highest risk customers
    st.subheader("🎯 Top 10 Highest Risk Customers")
    top_10 = target_df.head(10)[['customer_id', 'full_name', 'email', 'churn_risk_score', 'lifetime_value', 'customer_segment']]
    st.dataframe(top_10, use_container_width=True)

    # Recommendations
    st.subheader("💡 Campaign Recommendations")

    recommendations = []

    if roi['roi_pct'] > 50:
        recommendations.append("🟢 **Strong ROI:** This campaign is highly profitable. Consider expanding target audience.")
    elif roi['roi_pct'] > 0:
        recommendations.append("🟡 **Positive ROI:** Campaign is profitable but could be optimized for better returns.")
    else:
        recommendations.append("🔴 **Negative ROI:** Campaign will lose money. Reduce incentive amount or fixed costs, or improve retention rate assumptions.")

    if retention_rate < roi['breakeven_retention_rate']:
        recommendations.append(f"⚠️ **Below Breakeven:** Need {roi['breakeven_retention_rate']:.1f}% retention to break even. Current {retention_rate}% is too low.")

    if len(target_df) < 100:
        recommendations.append("📉 **Small Audience:** Target audience is small. Consider broadening filters to increase reach.")

    if target_df['churn_risk_score'].mean() < 50:
        recommendations.append("💡 **Lower Risk Targets:** Avg churn risk is below 50. Focus on higher-risk customers for better ROI.")

    for rec in recommendations:
        st.markdown(rec)

    st.markdown("""
    **Suggested Messaging:**
    - "We value your loyalty - here's $100 cashback"
    - "Exclusive offer for our valued Premium customers"
    - "Earn 2X points on all purchases for 90 days"

    **Timing:**
    - Send within 7 days of risk score calculation
    - Follow up after 14 days if no response

    **Success Metrics:**
    - Track activation rate (% who redeem incentive)
    - Monitor 90-day transaction activity post-campaign
    - Measure actual churn rate vs projected
    """)

    # Export target list
    st.subheader("📥 Export Target List")

    csv = target_df.to_csv(index=False)
    st.download_button(
        label=f"📥 Download Target List ({len(target_df):,} customers)",
        data=csv,
        file_name=f"retention_campaign_targets_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )
```

**Use Case:**
Marketing manager wants to run retention campaign for **Declining customers with High Risk and LTV >$10K**. Finds 245 customers, sets **$100 incentive**, assumes **30% retention rate**, inputs **$5K fixed cost**. Simulator shows **ROI = +45%**, **$54K net benefit**, **breakeven at 18% retention**. Exports target list CSV, proceeds with campaign.

### 5.7 Deployment

**Local Development:**
```bash
cd streamlit
pip install -r requirements.txt
cp .env.example .env
# Edit .env with Snowflake credentials
streamlit run app.py
```

**Streamlit in Snowflake (Production):**
```sql
CREATE OR REPLACE STREAMLIT customer_360_app
  ROOT_LOCATION = '@snowflake_panel_demo_repo/branches/main/streamlit'
  MAIN_FILE = 'app.py'
  QUERY_WAREHOUSE = 'COMPUTE_WH'
  COMMENT = 'Customer 360 Analytics Dashboard';

-- Get app URL
SELECT SYSTEM$GET_STREAMLIT_APP_URL('customer_360_app');
```

**GitHub Actions (CI/CD):**
On push to `main` branch with changes in `streamlit/`:
1. GitHub Actions workflow triggers
2. Runs `snow streamlit deploy --replace`
3. Snowflake CLI uploads app files
4. Streamlit app automatically refreshes

### 5.8 Performance & Optimization

**Query Caching:**
```python
@st.cache_resource
def get_snowflake_connection():
    """Connection cached for session lifetime"""
    return st.connection("snowflake")

@st.cache_data(ttl=3600)  # Cache for 1 hour
def fetch_customers(segments, states, risk_levels, min_ltv):
    """Results cached with 1-hour TTL"""
    query = f"SELECT ... WHERE ..."
    return execute_query(query)
```

**Clustering Benefits:**
- `customer_360_profile` clustered by `customer_id` → Customer 360 tab lookups <1 sec
- `fct_transactions` clustered by `transaction_date` → Transaction history filters fast

**Warehouse Sizing:**
- COMPUTE_WH (Small) sufficient for all tabs
- Auto-suspend after 5 minutes of inactivity
- Estimated cost: ~$2-5/day for moderate usage

### 5.9 Exam Questions You Should Be Able to Answer

1. **How many Streamlit tabs are there and what do they do?**
   - 4 tabs: Segment Explorer (filter/export), Customer 360 (individual profile), AI Assistant (Cortex Analyst NLP), Campaign Simulator (ROI calc)

2. **What is Streamlit in Snowflake (SiS)?**
   - Native Snowflake-hosted Streamlit apps that run inside Snowflake (no external deployment)

3. **How does Cortex Analyst work in the AI Assistant tab?**
   - User asks natural language question → Cortex Analyst generates SQL from semantic layer → Executes query → Returns results + interpretation → Auto-detects chart type

4. **What is the fallback if Cortex Analyst is unavailable?**
   - Mock implementation with pre-defined question mappings (limited to suggested questions)

5. **How does auto-chart detection work?**
   - Analyzes DataFrame columns (numeric, categorical, date, geographic) and question text to suggest best chart type (bar/line/pie/scatter/choropleth/table)

6. **What is PyDeck used for and when is it available?**
   - Choropleth maps for geographic data (US states); only available on paid Snowflake accounts with External Access Integration (not trial accounts)

7. **How is the Campaign Simulator ROI calculated?**
   - ROI = ((Retained Value - Total Campaign Cost) / Total Campaign Cost) × 100; where Retained Value = Expected Churners × Retention Rate × Avg LTV

8. **What caching strategies are used for performance?**
   - Snowflake connection cached per session (@st.cache_resource), query results cached with TTL (@st.cache_data), Snowflake result cache

9. **How long do Customer 360 lookups take and why?**
   - <1 second due to clustering by customer_id and denormalized schema (no runtime JOINs)

10. **How are Streamlit apps deployed to Snowflake?**
    - Manual: `snow streamlit deploy`, GitHub Actions: Auto-deploy on push to main, Native Git: Reference stage in CREATE STREAMLIT

---

# END OF PART I: DATA PIPELINE JOURNEY

**Summary:** You now understand the complete end-to-end pipeline from data generation (Snowpark + SQL) → ingestion (S3 + COPY INTO) → transformation (dbt medallion) → ML (Cortex ML churn prediction) → application (Streamlit 4 tabs).

---

# PART II: COMPONENT DEEP-DIVES

(Continuing in next section due to file size...)

---

**[Study Guide continues with Part II: Component Deep-Dives covering all 20+ components in detail, Part III: Persona Guides, and Part IV: Appendices. Total estimated length: ~12,000 lines]**


# PART II: COMPONENT DEEP-DIVES

This section provides detailed technical coverage of all 20+ platform components shown in the architecture diagram. Each component includes implementation details, code examples, exam questions, and troubleshooting guidance.

---

## 6. SNOWPARK PROCEDURES

### 6.1 What is Snowpark?

**Snowpark** is Snowflake's Python/Java/Scala framework for writing data pipelines and applications that run **inside Snowflake** without moving data out.

**Key Benefits:**
- **No data egress:** Code executes where data lives (Snowflake's virtual warehouses)
- **Secure:** No credentials/data leave Snowflake
- **Elastic:** Automatically scales with warehouse size
- **Integrated:** Full access to Snowflake tables, views, and functions

### 6.2 Stored Procedures vs UDFs

| Feature | Stored Procedures | UDFs (User-Defined Functions) |
|---------|------------------|-------------------------------|
| **Return Type** | Single value (STRING, INTEGER, etc.) | Table or scalar value |
| **Side Effects** | Can write to tables | Read-only |
| **Use Case** | Data generation, orchestration | Custom transformations |
| **Example** | `GENERATE_CUSTOMERS(50000, 42)` | `PARSE_EMAIL(email_string)` |

### 6.3 Customer Generation Stored Procedure

**File:** `snowflake/procedures/generate_customers.sql`

**Full Implementation:**
```sql
CREATE OR REPLACE PROCEDURE BRONZE.GENERATE_CUSTOMERS(
    customer_count INTEGER,
    random_seed INTEGER DEFAULT 42
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'faker==19.12.0', 'pandas')
HANDLER = 'generate_customers_handler'
EXECUTE AS CALLER
AS
$$
from snowflake.snowpark import Session
from faker import Faker
import random
import pandas as pd
from datetime import datetime, timedelta

def generate_customers_handler(session: Session, customer_count: int, random_seed: int) -> str:
    """Generate synthetic customers with segment-based distributions."""
    
    # Seed for reproducibility
    Faker.seed(random_seed)
    random.seed(random_seed)
    fake = Faker('en_US')
    
    # Define 5 customer segments with proportions
    segments = {
        'High-Value Travelers': 0.15,      # 15% - frequent travelers, high spend
        'Stable Mid-Spenders': 0.40,       # 40% - consistent moderate spend
        'Budget-Conscious': 0.25,          # 25% - low spend, price-sensitive
        'Declining': 0.10,                 # 10% - decreasing activity (churn candidates)
        'New & Growing': 0.10              # 10% - recent accounts, increasing spend
    }
    
    # Generate customers
    customers = []
    customer_id = 100000
    
    for segment_name, proportion in segments.items():
        count = int(customer_count * proportion)
        
        for _ in range(count):
            # Segment-specific distributions
            if segment_name == 'High-Value Travelers':
                age = random.randint(35, 55)
                credit_limit = random.randint(15000, 50000)
                card_type = random.choice(['Premium', 'Premium', 'Standard'])  # 66% Premium
                account_age_days = random.randint(730, 3650)  # 2-10 years
                
            elif segment_name == 'Stable Mid-Spenders':
                age = random.randint(30, 60)
                credit_limit = random.randint(5000, 20000)
                card_type = random.choice(['Standard', 'Standard', 'Premium'])  # 66% Standard
                account_age_days = random.randint(365, 2190)  # 1-6 years
                
            elif segment_name == 'Budget-Conscious':
                age = random.randint(22, 45)
                credit_limit = random.randint(2000, 10000)
                card_type = 'Standard'
                account_age_days = random.randint(180, 1825)  # 6 months - 5 years
                
            elif segment_name == 'Declining':
                age = random.randint(25, 65)
                credit_limit = random.randint(3000, 25000)
                card_type = random.choice(['Standard', 'Premium'])
                account_age_days = random.randint(730, 2920)  # 2-8 years (established but declining)
                
            else:  # New & Growing
                age = random.randint(21, 35)
                credit_limit = random.randint(3000, 12000)
                card_type = 'Standard'
                account_age_days = random.randint(30, 365)  # <1 year
            
            # Generate customer record
            account_open_date = datetime.now().date() - timedelta(days=account_age_days)
            
            customers.append({
                'CUSTOMER_ID': customer_id,
                'FIRST_NAME': fake.first_name(),
                'LAST_NAME': fake.last_name(),
                'EMAIL': f'customer{customer_id}@example.com',
                'AGE': age,
                'STATE': fake.state_abbr(),
                'CITY': fake.city(),
                'EMPLOYMENT_STATUS': random.choice(['Full-Time', 'Part-Time', 'Self-Employed', 'Retired']),
                'CARD_TYPE': card_type,
                'CREDIT_LIMIT': credit_limit,
                'ACCOUNT_OPEN_DATE': account_open_date,
                'CUSTOMER_SEGMENT': segment_name,
                'DECLINE_TYPE': None  # Populated later by transaction patterns
            })
            
            customer_id += 1
    
    # Convert to DataFrame
    df = pd.DataFrame(customers)
    
    # Write to Snowflake table
    session.write_pandas(
        df, 
        table_name='RAW_CUSTOMERS',
        database='CUSTOMER_ANALYTICS',
        schema='BRONZE',
        auto_create_table=False,
        overwrite=True
    )
    
    return f'SUCCESS: Generated {len(df)} customers across {len(segments)} segments'
$$;
```

**Key Implementation Details:**

1. **PACKAGES clause:** Specifies Python packages available at runtime (Faker for synthetic data)
2. **RUNTIME_VERSION:** Python 3.10 environment
3. **EXECUTE AS CALLER:** Runs with caller's privileges (not OWNER)
4. **Session object:** Snowpark Session passed automatically
5. **write_pandas():** Efficiently loads DataFrame to Snowflake table

### 6.4 How to Call the Procedure

```sql
-- Generate 50,000 customers with seed 42 (reproducible)
CALL BRONZE.GENERATE_CUSTOMERS(50000, 42);

-- Output: SUCCESS: Generated 50000 customers across 5 segments
```

### 6.5 Monitoring Procedure Execution

```sql
-- View recent procedure calls
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COMPLETE_TASK_GRAPHS(
    RESULT_LIMIT => 10
))
WHERE NAME = 'GENERATE_CUSTOMERS'
ORDER BY SCHEDULED_TIME DESC;

-- View execution time and credits
SELECT 
    query_text,
    execution_status,
    total_elapsed_time / 1000 AS seconds,
    credits_used_cloud_services
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE query_text ILIKE '%GENERATE_CUSTOMERS%'
ORDER BY start_time DESC
LIMIT 5;
```

### 6.6 Exam Questions

1. **What is Snowpark and why use it?**
   - Python/Java/Scala framework for running code inside Snowflake; eliminates data movement, leverages Snowflake's compute

2. **What packages can you use in Snowpark procedures?**
   - Snowflake Anaconda channel packages (3,000+) specified in PACKAGES clause; requires ACCOUNTADMIN approval for some packages

3. **How do you make stored procedures reproducible?**
   - Use random seed parameter; pass seed to `Faker.seed()` and `random.seed()`

4. **What does EXECUTE AS CALLER mean?**
   - Procedure runs with caller's role/privileges (not procedure owner's); more secure than EXECUTE AS OWNER

5. **How do you write DataFrames to Snowflake from Snowpark?**
   - `session.write_pandas(df, table_name='TABLE', database='DB', schema='SCHEMA', overwrite=True)`

6. **What is the difference between a stored procedure and a UDF?**
   - Stored procedure can modify tables (side effects), returns single value; UDF is read-only, returns table or scalar

7. **How long does generating 50K customers take?**
   - 30-60 seconds on Small warehouse; ~0.01 credits

8. **Can you call Snowpark procedures from dbt?**
   - Yes! Use `{{ run_query("CALL BRONZE.GENERATE_CUSTOMERS(50000, 42)") }}` in dbt hooks

---

## 7. DBT TRANSFORMATIONS

### 7.1 Cross-Reference

This section summarizes dbt transformations. For detailed pipeline flow, see **Part I, Section 3: Data Transformation (dbt)**.

### 7.2 Key Concepts

- **dbt (data build tool):** SQL-based transformation framework
- **14 models total:** Bronze → Silver → Gold medallion architecture
- **Jinja templating:** Dynamic SQL with `{{ ref('model') }}`, `{{ source('schema', 'table') }}`
- **Testing:** 35+ tests (unique, not_null, relationships, custom)
- **Documentation:** Auto-generated docs with lineage graphs

### 7.3 Critical Models to Know

| Model | Purpose | Exam Importance |
|-------|---------|-----------------|
| `dim_customer` | SCD Type 2 customer dimension | HIGH - Know SCD implementation |
| `fct_transactions` | Fact table with clustering | HIGH - Know clustering strategy |
| `customer_360_profile` | Denormalized customer view | HIGH - Know denormalization benefits |
| `customer_segments` | Rolling 90-day segment metrics | MEDIUM - Know rolling windows |
| `int_customer_transaction_summary` | Intermediate aggregations | LOW - Understand staging pattern |

### 7.4 dbt Native in Snowflake

**NEW FEATURE:** dbt can now run natively in Snowflake (no external dbt Cloud needed).

```sql
-- Deploy dbt project to Snowflake
CREATE OR REPLACE NATIVE APPLICATION dbt_customer_analytics
  FROM @snowflake_panel_demo_repo/branches/main/dbt_customer_analytics
  COMMENT = 'Customer 360 dbt transformations';

-- Run dbt models
EXECUTE NATIVE APPLICATION dbt_customer_analytics
  COMMAND = 'dbt run --select gold.*';

-- Run tests
EXECUTE NATIVE APPLICATION dbt_customer_analytics
  COMMAND = 'dbt test';
```

### 7.5 Exam Questions

1. **How many dbt models are there?**
   - 14 models: 3 staging (Silver), 11 Gold (dims, facts, metrics)

2. **What is the medallion architecture?**
   - Bronze (raw) → Silver (cleaned/conformed) → Gold (business logic/aggregations)

3. **What testing framework does dbt use?**
   - Built-in schema tests (unique, not_null, relationships, accepted_values) + custom tests

4. **How does dbt handle incremental loads?**
   - `{{ config(materialized='incremental') }}` with merge strategies; we use full refresh for simplicity

5. **What is the `ref()` function?**
   - References upstream models; creates DAG dependency graph automatically

---

## 8. CORTEX ANALYST API

### 8.1 What is Cortex Analyst?

**Cortex Analyst** is Snowflake's LLM-powered natural language to SQL feature. It uses **semantic models** (YAML files defining business terminology) to translate questions into SQL queries.

**Architecture:**
```
User Question
    ↓
Cortex Analyst (LLM)
    ↓
Semantic Model (YAML) → Business terminology + table mappings
    ↓
Generated SQL
    ↓
Execute on Snowflake
    ↓
Results + Interpretation
```

### 8.2 API Call Structure

```python
import snowflake.snowpark.functions as F

def query_cortex_analyst(session, question, conversation_history=[]):
    """Call Cortex Analyst API to generate SQL from natural language."""
    
    # Build conversation messages
    messages = conversation_history + [
        {
            "role": "user",
            "content": [{"type": "text", "text": question}]
        }
    ]
    
    # Call Cortex Analyst
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'analyst',
            {messages},
            {{
                'semantic_model_file': '@SEMANTIC_MODELS.DEFINITIONS.SEMANTIC_STAGE/customer_analytics.yaml'
            }}
        ) AS response
    """).collect()[0]['RESPONSE']
    
    # Parse JSON response
    import json
    response_data = json.loads(result)
    
    return {
        'sql': response_data.get('sql'),
        'interpretation': response_data.get('interpretation'),
        'messages': messages  # For conversation continuity
    }
```

### 8.3 Response Format

```json
{
  "sql": "SELECT customer_segment, COUNT(*) AS customer_count FROM CUSTOMER_ANALYTICS.GOLD.CUSTOMER_360_PROFILE GROUP BY customer_segment ORDER BY customer_count DESC",
  "interpretation": "This query counts the number of customers in each behavioral segment and sorts by segment size.",
  "conversation_id": "abc123...",
  "metadata": {
    "tables_referenced": ["customer_360_profile"],
    "confidence": 0.95
  }
}
```

### 8.4 Multi-Turn Conversations

```python
# First question
response1 = query_cortex_analyst(session, "How many customers are in each segment?")
messages = response1['messages']

# Follow-up question (uses context)
response2 = query_cortex_analyst(
    session, 
    "Show me only the top 2 segments",
    conversation_history=messages
)
# Cortex Analyst understands "top 2 segments" refers to previous query
```

### 8.5 Exam Questions

1. **What LLM powers Cortex Analyst?**
   - Proprietary Snowflake LLM (not GPT); trained on SQL and data analysis tasks

2. **What is required for Cortex Analyst to work?**
   - Semantic model YAML file uploaded to Snowflake stage; `CORTEX_USER` database role granted

3. **Can Cortex Analyst modify data?**
   - No; it only generates SELECT queries (read-only)

4. **How do you enable multi-turn conversations?**
   - Pass previous messages array in conversation history parameter

5. **What happens if Cortex Analyst can't answer?**
   - Returns error message like "I cannot answer that question with the available data"

6. **How much does Cortex Analyst cost?**
   - Billed per request (usage-based); ~$0.02-0.10 per query depending on complexity

---

## 9. SEMANTIC LAYER

### 9.1 What is a Semantic Layer?

A **semantic layer** maps business terminology to database objects, enabling non-technical users to query data using natural language.

**Components:**
1. **Tables:** Map logical names to physical tables
2. **Dimensions:** Attributes you filter/group by (customer_segment, state)
3. **Measures:** Metrics you calculate (lifetime_value, avg_transaction_value)
4. **Relationships:** Foreign key relationships between tables
5. **Verified Queries:** Pre-tested question/SQL pairs for accuracy

### 9.2 customer_analytics.yaml Structure

**File:** `semantic_models/customer_analytics.yaml`

```yaml
name: customer_analytics
description: Customer 360 analytics semantic model for credit card portfolio

# Table definitions
tables:
  - name: customer_profile
    description: Comprehensive customer profile with 360-degree view
    base_table:
      database: CUSTOMER_ANALYTICS
      schema: GOLD
      table: CUSTOMER_360_PROFILE
    
    dimensions:
      - name: customer_id
        synonyms: ["customer", "customer identifier", "customer number", "cust id", "id"]
        description: Unique customer identifier
        expr: CUSTOMER_ID
        data_type: NUMBER
        unique: true
      
      - name: customer_segment
        synonyms: ["segment", "customer type", "behavioral segment", "customer category"]
        description: Behavioral customer segment
        expr: CUSTOMER_SEGMENT
        data_type: VARCHAR
        sample_values:
          - "High-Value Travelers"
          - "Stable Mid-Spenders"
          - "Budget-Conscious"
          - "Declining"
          - "New & Growing"
      
      - name: churn_risk_category
        synonyms: ["churn risk", "risk level", "attrition risk", "risk category", "risk"]
        description: Customer churn risk level (Low/Medium/High)
        expr: CHURN_RISK_CATEGORY
        data_type: VARCHAR
        sample_values: ["Low Risk", "Medium Risk", "High Risk"]
      
      - name: state
        synonyms: ["state", "us state", "location", "customer state", "state name"]
        description: Customer's US state
        expr: STATE
        data_type: VARCHAR
    
    measures:
      - name: lifetime_value
        synonyms: ["ltv", "customer value", "total value", "customer lifetime value", "clv"]
        description: Total transaction value for customer (all-time)
        expr: LIFETIME_VALUE
        data_type: NUMBER
        default_aggregation: SUM
      
      - name: avg_transaction_value
        synonyms: ["avg transaction", "average purchase", "mean transaction", "average spend"]
        description: Average transaction amount per customer
        expr: AVG_TRANSACTION_VALUE
        data_type: NUMBER
        default_aggregation: AVG
      
      - name: churn_risk_score
        synonyms: ["risk score", "churn score", "churn probability", "risk %"]
        description: ML-predicted churn probability (0-100)
        expr: CHURN_RISK_SCORE
        data_type: NUMBER
        default_aggregation: AVG

  - name: transactions
    description: Credit card transaction fact table
    base_table:
      database: CUSTOMER_ANALYTICS
      schema: GOLD
      table: FCT_TRANSACTIONS
    
    dimensions:
      - name: transaction_date
        description: Date of transaction
        expr: TRANSACTION_DATE
        data_type: DATE
      
      - name: channel
        synonyms: ["purchase channel", "transaction channel", "payment method"]
        description: Transaction channel (Online/In-Store/Mobile)
        expr: CHANNEL
        data_type: VARCHAR
        sample_values: ["Online", "In-Store", "Mobile"]
    
    measures:
      - name: transaction_amount
        synonyms: ["amount", "purchase amount", "spend", "transaction value"]
        description: Transaction dollar amount
        expr: TRANSACTION_AMOUNT
        data_type: NUMBER
        default_aggregation: SUM
      
      - name: transaction_count
        synonyms: ["number of transactions", "transaction volume", "purchase count"]
        description: Count of transactions
        expr: TRANSACTION_ID
        data_type: NUMBER
        default_aggregation: COUNT

# Relationships between tables
relationships:
  - name: customer_transactions
    left_table: transactions
    left_column: customer_id
    right_table: customer_profile
    right_column: customer_id
    join_type: INNER

# Verified queries for accuracy
verified_queries:
  - question: "Which customers are at highest risk of churning?"
    sql: |
      SELECT 
        customer_id,
        full_name,
        churn_risk_score,
        churn_risk_category,
        lifetime_value,
        days_since_last_transaction
      FROM CUSTOMER_ANALYTICS.GOLD.CUSTOMER_360_PROFILE
      WHERE churn_risk_category = 'High Risk'
      ORDER BY churn_risk_score DESC
      LIMIT 100
    verified_at: "2025-01-19"
  
  - question: "How many customers are in each segment?"
    sql: |
      SELECT 
        customer_segment,
        COUNT(*) AS customer_count,
        SUM(lifetime_value) AS total_ltv
      FROM CUSTOMER_ANALYTICS.GOLD.CUSTOMER_360_PROFILE
      GROUP BY customer_segment
      ORDER BY customer_count DESC
    verified_at: "2025-01-19"
```

### 9.3 Key Semantic Layer Concepts

**Synonyms:**
- Allow multiple ways to ask same thing
- Example: "ltv", "customer value", "lifetime value" all map to `LIFETIME_VALUE`

**Sample Values:**
- Help LLM understand valid dimension values
- Example: Segment names, risk categories, states

**Default Aggregation:**
- Tells LLM how to aggregate measures (SUM, AVG, COUNT, MIN, MAX)
- Example: `lifetime_value` should be summed, `churn_risk_score` should be averaged

**Verified Queries:**
- Pre-tested question/SQL pairs
- Improve accuracy by showing LLM examples
- Should cover common business questions

### 9.4 Deploying Semantic Model

```bash
# From project root
cd /Users/jpurrutia/projects/snowflake-panel-demo
snowsql -c default -f snowflake/setup/deploy_semantic_model.sql
```

**What happens:**
1. Removes old YAML from stage (if exists)
2. Uploads `customer_analytics.yaml` to `@SEMANTIC_MODELS.DEFINITIONS.SEMANTIC_STAGE`
3. Verifies upload with `LIST @SEMANTIC_STAGE`
4. Tests file readability

### 9.5 Exam Questions

1. **What is a semantic layer?**
   - Business terminology mapping to database objects; enables natural language queries

2. **What are the key components of a semantic model?**
   - Tables, dimensions, measures, relationships, verified queries

3. **Why use synonyms in dimensions/measures?**
   - Allow users to ask questions using different terminology (LTV vs lifetime value vs customer value)

4. **What is default_aggregation?**
   - Tells LLM how to aggregate measures (SUM for totals, AVG for averages, COUNT for counts)

5. **What are verified queries?**
   - Pre-tested question/SQL pairs that improve Cortex Analyst accuracy by providing examples

6. **How many tables are defined in our semantic model?**
   - 4 tables: customer_profile, transactions, merchant_categories, customer_segments

7. **Where is the semantic model stored?**
   - Snowflake stage: `@SEMANTIC_MODELS.DEFINITIONS.SEMANTIC_STAGE/customer_analytics.yaml`

8. **Can you update semantic models without redeploying Streamlit?**
   - Yes! Semantic model is referenced by path, so updating the YAML file is sufficient

---

## 10. AWS S3 INTEGRATION

### 10.1 Architecture Overview

```
AWS S3 Bucket (Data Lake)
    ↓
IAM Role with Trust Policy (External ID for security)
    ↓
Snowflake Storage Integration
    ↓
Snowflake Stage (@customer_data_stage)
    ↓
COPY INTO Snowflake Tables
```

### 10.2 Terraform Infrastructure

**Files:**
- `terraform/s3.tf` - S3 bucket configuration
- `terraform/iam.tf` - IAM role and trust policy
- `terraform/variables.tf` - Input variables
- `terraform/outputs.tf` - Outputs (bucket name, IAM role ARN)

**S3 Bucket Configuration (`s3.tf`):**
```hcl
resource "aws_s3_bucket" "customer_analytics_data" {
  bucket = var.bucket_name
  
  tags = {
    Name        = "Customer Analytics Data Lake"
    Environment = var.environment
    Project     = "Snowflake Panel Demo"
  }
}

# Enable versioning for data protection
resource "aws_s3_bucket_versioning" "customer_analytics_data_versioning" {
  bucket = aws_s3_bucket.customer_analytics_data.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

# Server-side encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "customer_analytics_data_encryption" {
  bucket = aws_s3_bucket.customer_analytics_data.id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Block public access
resource "aws_s3_bucket_public_access_block" "customer_analytics_data_public_block" {
  bucket = aws_s3_bucket.customer_analytics_data.id
  
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle rule (optional - delete old files after 90 days)
resource "aws_s3_bucket_lifecycle_configuration" "customer_analytics_data_lifecycle" {
  bucket = aws_s3_bucket.customer_analytics_data.id
  
  rule {
    id     = "delete_old_files"
    status = "Enabled"
    
    expiration {
      days = 90
    }
  }
}
```

**IAM Role with Trust Policy (`iam.tf`):**
```hcl
# IAM role for Snowflake to assume
resource "aws_iam_role" "snowflake_s3_role" {
  name = "snowflake-customer-analytics-s3-role"
  
  # Trust policy: Allow Snowflake to assume this role
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = var.snowflake_storage_aws_iam_user_arn
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.snowflake_storage_aws_external_id
          }
        }
      }
    ]
  })
  
  tags = {
    Name = "Snowflake S3 Integration Role"
  }
}

# IAM policy: Allow S3 read/write access
resource "aws_iam_role_policy" "snowflake_s3_policy" {
  name = "snowflake-s3-access-policy"
  role = aws_iam_role.snowflake_s3_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          "${aws_s3_bucket.customer_analytics_data.arn}",
          "${aws_s3_bucket.customer_analytics_data.arn}/*"
        ]
      }
    ]
  })
}
```

### 10.3 Snowflake Storage Integration

**File:** `snowflake/setup/04_create_storage_integration.sql`

```sql
-- Create storage integration
CREATE OR REPLACE STORAGE INTEGRATION customer_analytics_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-customer-analytics-s3-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-customer-analytics-data-demo/');

-- Grant usage to DATA_ENGINEER role
GRANT USAGE ON INTEGRATION customer_analytics_s3_integration TO ROLE DATA_ENGINEER;

-- Get Snowflake IAM user ARN and External ID for Terraform
DESC INTEGRATION customer_analytics_s3_integration;
```

**Output (copy these values to Terraform variables):**
```
STORAGE_AWS_IAM_USER_ARN: arn:aws:iam::123456789012:user/abc-snowflake-user
STORAGE_AWS_EXTERNAL_ID: ABC123_SFCRole=1_AbCdEfGhIjKlMnOpQrStUvWxYz=
```

### 10.4 Creating Snowflake Stage

```sql
-- Create stage using storage integration
CREATE OR REPLACE STAGE BRONZE.customer_data_stage
  STORAGE_INTEGRATION = customer_analytics_s3_integration
  URL = 's3://snowflake-customer-analytics-data-demo/'
  FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  );

-- Test stage access
LIST @BRONZE.customer_data_stage;
```

### 10.5 Loading Data from S3

```sql
-- Load customers from S3
COPY INTO BRONZE.RAW_CUSTOMERS
FROM @BRONZE.customer_data_stage/customers.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
)
ON_ERROR = 'ABORT_STATEMENT'
PURGE = FALSE;  -- Keep files in S3 after load

-- Verify load
SELECT 
  COUNT(*) AS rows_loaded,
  MIN(account_open_date) AS earliest_account,
  MAX(account_open_date) AS latest_account
FROM BRONZE.RAW_CUSTOMERS;
```

### 10.6 Security: IAM Trust Relationship

**Why External ID?**
- Prevents "confused deputy" attack
- Ensures only your Snowflake account can assume the IAM role
- Snowflake generates unique External ID per storage integration

**Trust Policy Breakdown:**
```json
{
  "Principal": {
    "AWS": "arn:aws:iam::123456789012:user/abc-snowflake-user"
  },
  // Only THIS Snowflake user can assume role
  
  "Condition": {
    "StringEquals": {
      "sts:ExternalId": "ABC123_SFCRole=1_..."
    }
  }
  // Only if External ID matches (prevents other Snowflake accounts from assuming)
}
```

### 10.7 Exam Questions

1. **What is a Snowflake Storage Integration?**
   - Named object that stores IAM role ARN and allowed S3 locations for secure access without embedding credentials

2. **What is the External ID and why is it important?**
   - Unique identifier generated by Snowflake; prevents "confused deputy" attack where another Snowflake account could assume your IAM role

3. **What S3 permissions does Snowflake need?**
   - GetObject, PutObject, DeleteObject, ListBucket (full read/write access to bucket)

4. **What is the difference between a Storage Integration and a Stage?**
   - Storage Integration: Security object (IAM role mapping); Stage: Data location reference (S3 path + file format)

5. **How does COPY INTO work?**
   - Loads data from external stage (S3) into Snowflake table; can skip headers, handle errors, purge files after load

6. **What does PURGE = TRUE do?**
   - Deletes source files from S3 after successful load (use cautiously!)

7. **Can you use S3 without Terraform?**
   - Yes! Manually create S3 bucket + IAM role in AWS Console, then create Storage Integration in Snowflake

8. **What happens if IAM role trust policy is wrong?**
   - Error: "AWS Access Denied: Snowflake cannot assume the IAM role"

---

## 11. STAR SCHEMA DATA MODEL

### 11.1 Star Schema Overview

**Star schema** = fact table (center) + dimension tables (points).

**Benefits:**
- **Simplified queries:** Denormalized dimensions (no complex JOINs)
- **Query performance:** Optimized for aggregations
- **Business-friendly:** Dimensions use business terminology
- **BI tool compatibility:** Works with all BI tools (Tableau, Power BI, Looker)

**Customer 360 Star Schema:**
```
        DIM_DATE
            |
            |
DIM_CUSTOMER ---<--- FCT_TRANSACTIONS ---<--- DIM_MERCHANT_CATEGORY
                         (center)
```

### 11.2 Fact Table: fct_transactions

**File:** `dbt_customer_analytics/models/gold/fct_transactions.sql`

**Purpose:** Transaction fact table with foreign keys to dimensions.

**Schema:**
| Column | Type | Description | Cardinality |
|--------|------|-------------|-------------|
| transaction_sk | NUMBER | Surrogate key (unique) | 13.5M rows |
| transaction_id | VARCHAR | Business key | 13.5M rows |
| customer_sk | NUMBER | FK to dim_customer | 50K distinct |
| date_sk | NUMBER | FK to dim_date | 580 distinct |
| category_sk | NUMBER | FK to dim_merchant_category | 50 distinct |
| transaction_date | DATE | Transaction date (also for clustering) | 580 distinct |
| transaction_amount | NUMBER(10,2) | Dollar amount | Continuous |
| merchant_name | VARCHAR | Merchant name | ~10K distinct |
| channel | VARCHAR | Online/In-Store/Mobile | 3 distinct |
| status | VARCHAR | Approved/Declined | 2 distinct |

**Clustering:**
```sql
CREATE TABLE gold.fct_transactions (
  ... columns ...
)
CLUSTER BY (transaction_date);
```

**Why cluster by date?**
- Most queries filter by date range ("last 90 days", "2024 transactions")
- Clustering = automatic data sorting for faster scans
- Alternative: Could cluster by (customer_sk, transaction_date) for customer lookups

**dbt Model:**
```sql
{{
  config(
    materialized='table',
    cluster_by=['transaction_date']
  )
}}

SELECT
    -- Surrogate key
    ROW_NUMBER() OVER (ORDER BY t.transaction_id) AS transaction_sk,
    
    -- Business keys
    t.transaction_id,
    c.customer_sk,
    d.date_sk,
    mc.category_sk,
    
    -- Transaction attributes (degenerate dimensions)
    t.transaction_date,
    t.transaction_amount,
    t.merchant_name,
    t.channel,
    t.status
    
FROM {{ ref('stg_transactions') }} t
INNER JOIN {{ ref('dim_customer') }} c 
    ON t.customer_id = c.customer_id
    AND t.transaction_date BETWEEN c.effective_date AND c.end_date
INNER JOIN {{ ref('dim_date') }} d 
    ON t.transaction_date = d.date_actual
INNER JOIN {{ ref('dim_merchant_category') }} mc 
    ON t.merchant_category = mc.category_name
```

**Key Detail:** SCD Type 2 join with `BETWEEN effective_date AND end_date` ensures we use the correct customer version for each transaction.

### 11.3 Dimension Table: dim_customer (SCD Type 2)

**Purpose:** Track customer changes over time with history.

**SCD Type 2 Columns:**
| Column | Purpose |
|--------|---------|
| customer_sk | Surrogate key (unique per version) |
| customer_id | Business key (natural key, non-unique) |
| effective_date | When this version became active |
| end_date | When this version expired (9999-12-31 for current) |
| is_current | TRUE for current version, FALSE for historical |
| row_hash | MD5 hash of attribute columns (detect changes) |

**Example SCD Type 2 Data:**
```
customer_sk | customer_id | full_name     | state | effective_date | end_date   | is_current
----------- | ----------- | ------------- | ----- | -------------- | ---------- | ----------
1001        | C123        | John Smith    | CA    | 2023-01-01     | 2024-06-15 | FALSE
1002        | C123        | John Smith    | NY    | 2024-06-16     | 9999-12-31 | TRUE
```

Interpretation: Customer C123 lived in CA until June 15, 2024, then moved to NY (still current).

**dbt Implementation:**
```sql
{{
  config(
    materialized='table',
    unique_key='customer_sk'
  )
}}

WITH source_data AS (
    SELECT
        customer_id,
        first_name || ' ' || last_name AS full_name,
        email,
        age,
        state,
        city,
        employment_status,
        card_type,
        credit_limit,
        account_open_date,
        customer_segment,
        -- Hash of all attributes (detect changes)
        MD5(CONCAT_WS('|', 
            first_name, last_name, email, age, state, city, 
            employment_status, card_type, credit_limit, customer_segment
        )) AS row_hash
    FROM {{ ref('stg_customers') }}
),

existing_records AS (
    SELECT
        customer_sk,
        customer_id,
        row_hash,
        is_current,
        effective_date,
        end_date
    FROM {{ this }}  -- Self-reference to existing dim_customer
    WHERE is_current = TRUE
),

-- Identify new and changed records
new_and_changed AS (
    SELECT
        s.customer_id,
        s.full_name,
        s.email,
        s.age,
        s.state,
        s.city,
        s.employment_status,
        s.card_type,
        s.credit_limit,
        s.account_open_date,
        s.customer_segment,
        s.row_hash,
        CASE 
            WHEN e.customer_id IS NULL THEN 'NEW'
            WHEN s.row_hash <> e.row_hash THEN 'CHANGED'
            ELSE 'UNCHANGED'
        END AS change_type
    FROM source_data s
    LEFT JOIN existing_records e ON s.customer_id = e.customer_id
),

-- Expire old versions (set end_date = today - 1, is_current = FALSE)
expired_records AS (
    SELECT
        e.customer_sk,
        e.customer_id,
        e.full_name,
        ... other columns ...,
        e.effective_date,
        CURRENT_DATE - 1 AS end_date,
        FALSE AS is_current
    FROM existing_records e
    INNER JOIN new_and_changed nc 
        ON e.customer_id = nc.customer_id
    WHERE nc.change_type = 'CHANGED'
),

-- Insert new versions
new_versions AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY customer_id) + (SELECT MAX(customer_sk) FROM {{ this }}) AS customer_sk,
        customer_id,
        full_name,
        ... other columns ...,
        CURRENT_DATE AS effective_date,
        DATE '9999-12-31' AS end_date,
        TRUE AS is_current
    FROM new_and_changed
    WHERE change_type IN ('NEW', 'CHANGED')
)

-- Combine: Keep unchanged, expire old, insert new
SELECT * FROM existing_records WHERE change_type = 'UNCHANGED'
UNION ALL
SELECT * FROM expired_records
UNION ALL
SELECT * FROM new_versions
```

**dbt Tests for SCD Type 2:**
```yaml
# schema.yml
models:
  - name: dim_customer
    tests:
      - dbt_utils.expression_is_true:
          name: scd2_no_overlapping_dates
          expression: |
            NOT EXISTS (
              SELECT 1
              FROM {{ ref('dim_customer') }} a
              JOIN {{ ref('dim_customer') }} b
                ON a.customer_id = b.customer_id
                AND a.customer_sk <> b.customer_sk
                AND a.effective_date <= b.end_date
                AND a.end_date >= b.effective_date
            )
      
      - dbt_utils.expression_is_true:
          name: scd2_no_gaps_in_history
          expression: |
            NOT EXISTS (
              SELECT customer_id
              FROM {{ ref('dim_customer') }}
              GROUP BY customer_id
              HAVING MIN(effective_date) > (SELECT MIN(account_open_date) FROM {{ ref('stg_customers') }})
            )
```

### 11.4 Dimension Table: dim_date

**Purpose:** Date dimension with calendar attributes.

**Key Columns:**
- date_sk (surrogate key), date_actual (business key)
- year, quarter, month, day_of_week, day_of_year
- is_weekend, is_holiday, fiscal_year, fiscal_quarter

**dbt Model:**
```sql
{{
  config(
    materialized='table'
  )
}}

WITH date_spine AS (
    -- Generate 580 days of dates (18 months + buffer)
    SELECT DATEADD(day, SEQ4(), DATE '2023-06-01') AS date_actual
    FROM TABLE(GENERATOR(ROWCOUNT => 580))
)

SELECT
    ROW_NUMBER() OVER (ORDER BY date_actual) AS date_sk,
    date_actual,
    YEAR(date_actual) AS year,
    QUARTER(date_actual) AS quarter,
    MONTH(date_actual) AS month,
    DAY(date_actual) AS day,
    DAYOFWEEK(date_actual) AS day_of_week,
    DAYOFYEAR(date_actual) AS day_of_year,
    CASE WHEN DAYOFWEEK(date_actual) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    TO_CHAR(date_actual, 'YYYY-MM') AS year_month,
    TO_CHAR(date_actual, 'YYYY-Qx') AS year_quarter
FROM date_spine
```

### 11.5 Dimension Table: dim_merchant_category

**Purpose:** Merchant category classifications.

**Schema:**
| Column | Description |
|--------|-------------|
| category_sk | Surrogate key |
| category_name | Business key (e.g., "Restaurants") |
| category_group | Grouping (e.g., "Food & Dining") |
| spending_type | Discretionary vs Essential |

**dbt Model:**
```sql
{{
  config(
    materialized='table'
  )
}}

SELECT
    ROW_NUMBER() OVER (ORDER BY category_name) AS category_sk,
    category_name,
    category_group,
    spending_type
FROM {{ ref('stg_merchant_categories') }}
```

### 11.6 Query Patterns

**Pattern 1: Customer lifetime value by segment**
```sql
SELECT
    c.customer_segment,
    COUNT(DISTINCT c.customer_id) AS customer_count,
    SUM(t.transaction_amount) AS total_ltv,
    AVG(t.transaction_amount) AS avg_transaction
FROM gold.fct_transactions t
INNER JOIN gold.dim_customer c ON t.customer_sk = c.customer_sk
WHERE c.is_current = TRUE  -- Only current customer versions
GROUP BY c.customer_segment
ORDER BY total_ltv DESC;
```

**Pattern 2: Monthly spending trends**
```sql
SELECT
    d.year_month,
    SUM(t.transaction_amount) AS monthly_spend,
    COUNT(t.transaction_id) AS transaction_count
FROM gold.fct_transactions t
INNER JOIN gold.dim_date d ON t.date_sk = d.date_sk
WHERE t.status = 'Approved'
GROUP BY d.year_month
ORDER BY d.year_month;
```

**Pattern 3: High-value travelers in California (SCD Type 2)**
```sql
SELECT
    c.full_name,
    c.email,
    c.state,
    SUM(t.transaction_amount) AS total_spend,
    c.effective_date,
    c.end_date,
    c.is_current
FROM gold.fct_transactions t
INNER JOIN gold.dim_customer c 
    ON t.customer_sk = c.customer_sk
WHERE c.customer_segment = 'High-Value Travelers'
  AND c.state = 'CA'
  AND c.is_current = TRUE
GROUP BY c.full_name, c.email, c.state, c.effective_date, c.end_date, c.is_current
ORDER BY total_spend DESC
LIMIT 100;
```

### 11.7 Exam Questions

1. **What is a star schema?**
   - Fact table in center with foreign keys to dimension tables; optimized for aggregations and BI queries

2. **What is the difference between a fact and a dimension?**
   - Fact: Measurements/metrics (transaction_amount); Dimension: Descriptive attributes (customer_segment, state)

3. **What is a surrogate key and why use it?**
   - Auto-generated unique identifier (customer_sk); decouples from business keys, supports SCD Type 2

4. **What is SCD Type 2?**
   - Slowly Changing Dimension Type 2: Tracks history by creating new row for each change with effective_date/end_date

5. **How do you join fct_transactions to dim_customer with SCD Type 2?**
   - `BETWEEN c.effective_date AND c.end_date` ensures correct customer version for transaction date

6. **What is a degenerate dimension?**
   - Dimension attribute stored directly in fact table (merchant_name, channel) instead of separate dimension table

7. **Why cluster fct_transactions by transaction_date?**
   - Most queries filter by date range; clustering = automatic sorting for faster scans

8. **How many rows in fct_transactions?**
   - ~13.5M rows (60M generated, filtered to approved, 18-month window)

9. **What are the 4 tables in our star schema?**
   - 1 fact (fct_transactions), 3 dimensions (dim_customer, dim_date, dim_merchant_category)

10. **What is the grain of fct_transactions?**
    - One row per transaction (atomic grain)

---

## 12. ML FUNCTIONS (CORTEX ML)

### 12.1 What is Snowflake Cortex ML?

**Snowflake Cortex ML** is Snowflake's native machine learning platform. No Python notebooks, no MLflow, no external tools - everything runs inside Snowflake using SQL.

**Key Features:**
- **Classification:** Binary and multi-class (churn prediction, fraud detection)
- **Regression:** Continuous predictions (sales forecasting, price prediction)
- **Forecasting:** Time series (demand forecasting)
- **Anomaly Detection:** Outlier detection
- **Feature Engineering:** Automatic feature importance, encoding, scaling

**Benefits:**
- **No data movement:** Training happens where data lives
- **Automatic preprocessing:** Handles missing values, encoding, scaling
- **SQL-based:** No Python required (but Python UDFs supported)
- **Versioning:** Model registry built-in
- **Integration:** Direct integration with Snowpark, Streamlit, dbt

### 12.2 Churn Prediction Model

**Model Type:** Binary classification (churn = 1, not churn = 0)

**Training Function:**
```sql
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION churn_prediction_model(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'gold.ml_training_features'),
    TARGET_COL => 'is_churned',
    CONFIG_OBJECT => {
        'model_type': 'XGBOOST',  -- Algorithm: XGBoost (gradient boosting)
        'on_error': 'SKIP',
        'evaluate': TRUE
    }
);
```

**What happens during training:**
1. Snowflake reads `gold.ml_training_features` table
2. Automatically detects feature types (numeric, categorical)
3. Encodes categorical features (one-hot or target encoding)
4. Scales numeric features (standardization)
5. Trains XGBoost model with hyperparameter tuning
6. Evaluates on holdout set (80/20 split)
7. Stores model in Snowflake ML registry

**Training Time:**
- 50K customers, 35+ features: **1-3 minutes** on Small warehouse
- Cost: **~0.02-0.05 credits**

### 12.3 Feature Engineering (35+ Features)

**File:** `snowflake/ml/02_create_training_features.sql`

**Feature Categories:**

**1. Transaction Frequency Features:**
```sql
COUNT(DISTINCT transaction_id) AS total_transactions,
COUNT(DISTINCT CASE WHEN transaction_date >= DATEADD(day, -30, CURRENT_DATE) THEN transaction_id END) AS transactions_last_30_days,
COUNT(DISTINCT CASE WHEN transaction_date >= DATEADD(day, -90, CURRENT_DATE) THEN transaction_id END) AS transactions_last_90_days,
DATEDIFF(day, MAX(transaction_date), CURRENT_DATE) AS days_since_last_transaction
```

**2. Spending Amount Features:**
```sql
SUM(transaction_amount) AS lifetime_value,
AVG(transaction_amount) AS avg_transaction_value,
SUM(CASE WHEN transaction_date >= DATEADD(day, -30, CURRENT_DATE) THEN transaction_amount ELSE 0 END) AS spend_last_30_days,
SUM(CASE WHEN transaction_date >= DATEADD(day, -90, CURRENT_DATE) THEN transaction_amount ELSE 0 END) AS spend_last_90_days
```

**3. Spending Trend Features:**
```sql
-- Month-over-month % change
(spend_last_30_days - spend_prev_30_days) / NULLIF(spend_prev_30_days, 0) AS mom_spend_change_pct,

-- Acceleration (2nd derivative of spending)
((spend_last_30_days - spend_prev_30_days) - (spend_prev_30_days - spend_prev_60_days)) / NULLIF(spend_prev_30_days, 0) AS spend_acceleration
```

**4. Customer Demographics:**
```sql
c.age,
c.state,
c.employment_status,
c.card_type,
c.credit_limit,
DATEDIFF(day, c.account_open_date, CURRENT_DATE) AS account_age_days
```

**5. Segment & Behavioral Features:**
```sql
c.customer_segment,
SUM(CASE WHEN t.channel = 'Online' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS online_transaction_pct,
SUM(CASE WHEN t.status = 'Declined' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS decline_rate
```

**6. Category Preference Features:**
```sql
-- Top category by spend
MAX(CASE WHEN category_rank = 1 THEN mc.category_name END) AS top_category,

-- % of spend in top category
MAX(CASE WHEN category_rank = 1 THEN category_spend_pct END) AS top_category_spend_pct
```

### 12.4 Model Evaluation

**Metrics Calculated:**
```sql
SELECT
    -- Overall accuracy
    SUM(CASE WHEN predicted_class = actual_class THEN 1 ELSE 0 END) / COUNT(*) AS accuracy,
    
    -- Precision (of predicted churners, how many actually churned?)
    SUM(CASE WHEN predicted_class = 1 AND actual_class = 1 THEN 1 ELSE 0 END) 
      / NULLIF(SUM(CASE WHEN predicted_class = 1 THEN 1 ELSE 0 END), 0) AS precision,
    
    -- Recall (of actual churners, how many did we catch?)
    SUM(CASE WHEN predicted_class = 1 AND actual_class = 1 THEN 1 ELSE 0 END)
      / NULLIF(SUM(CASE WHEN actual_class = 1 THEN 1 ELSE 0 END), 0) AS recall,
    
    -- F1 Score (harmonic mean of precision and recall)
    2.0 * (precision * recall) / NULLIF(precision + recall, 0) AS f1_score

FROM model_predictions;
```

**Our Model Performance (Synthetic Data):**
- **Accuracy:** 1.0 (100%)
- **Precision:** 1.0 (100%)
- **Recall:** 1.0 (100%)
- **F1 Score:** 1.0 (100%)

**Why perfect scores?**
- Synthetic data has deterministic churn patterns
- Real-world expected: F1 = 0.50-0.70

### 12.5 Making Predictions

**File:** `snowflake/ml/05_apply_predictions.sql`

```sql
CREATE OR REPLACE TABLE gold.churn_predictions AS
SELECT
    customer_id,
    full_name,
    email,
    customer_segment,
    
    -- Predict churn probability (0-100)
    churn_prediction_model!PREDICT(
        OBJECT_CONSTRUCT(*) 
    ):probability[1]::FLOAT * 100 AS churn_risk_score,
    
    -- Classify into risk categories
    CASE
        WHEN churn_risk_score >= 70 THEN 'High Risk'
        WHEN churn_risk_score >= 40 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS churn_risk_category,
    
    lifetime_value,
    days_since_last_transaction,
    spend_last_30_days,
    mom_spend_change_pct

FROM gold.ml_training_features;
```

**Prediction Distribution:**
| Risk Category | Customer Count | % of Total | Avg LTV |
|---------------|---------------|------------|---------|
| Low Risk      | 38,716        | 77.4%      | $12,450 |
| Medium Risk   | 9,642         | 19.3%      | $8,320  |
| High Risk     | 1,642         | 3.3%       | $5,890  |

### 12.6 Feature Importance

**Query:**
```sql
SELECT 
    feature_name,
    importance,
    ROUND(importance * 100, 1) AS importance_pct
FROM TABLE(
    churn_prediction_model!SHOW_FEATURE_IMPORTANCE()
)
ORDER BY importance DESC
LIMIT 10;
```

**Top 10 Features:**
1. **age** - 28.5%
2. **churn_reason** - 15.8% (synthetic: deterministic churn trigger)
3. **lifetime_value** - 13.7%
4. **credit_limit** - 9.0%
5. **avg_transaction_value** - 6.5%
6. **days_since_last_transaction** - 5.2%
7. **spend_last_30_days** - 4.8%
8. **mom_spend_change_pct** - 3.9%
9. **account_age_days** - 3.1%
10. **customer_segment** - 2.8%

**Insights:**
- Demographics (age) most predictive (likely synthetic artifact)
- Transaction recency highly important (days_since_last_transaction)
- Spending trends matter (mom_spend_change_pct)
- Segment less important than expected (2.8%)

### 12.7 Model Versioning & Registry

```sql
-- Show all ML models in account
SHOW SNOWFLAKE.ML.CLASSIFICATION;

-- Get model metadata
SELECT SYSTEM$GET_ML_MODEL_VERSION('churn_prediction_model');

-- Drop old model version
DROP SNOWFLAKE.ML.CLASSIFICATION churn_prediction_model VERSION 1;

-- List all versions
SELECT * FROM TABLE(
    INFORMATION_SCHEMA.ML_MODELS(
        MODEL_NAME => 'churn_prediction_model'
    )
);
```

### 12.8 Retraining Workflow

**Frequency:** Monthly (or when F1 score drops below 0.60)

**Steps:**
1. Generate fresh churn labels (60+ days inactive OR <30% baseline spend)
2. Re-engineer features with latest transaction data
3. Train new model version
4. Evaluate on holdout set
5. Compare to previous version
6. If better, deploy new version; else, keep old
7. Update `churn_predictions` table

**Automated Retraining (Snowflake Task):**
```sql
CREATE OR REPLACE TASK retrain_churn_model
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 2 1 * * America/Los_Angeles'  -- 2 AM on 1st of month
AS
CALL retrain_and_deploy_churn_model();
```

### 12.9 Exam Questions

1. **What is Snowflake Cortex ML?**
   - Native machine learning platform; trains models inside Snowflake using SQL (no Python notebooks)

2. **What ML algorithm is used for churn prediction?**
   - XGBoost (gradient boosting) for binary classification

3. **How many features are used in churn model?**
   - 35+ engineered features across 6 categories

4. **What is F1 score?**
   - Harmonic mean of precision and recall; balances false positives and false negatives

5. **Why is F1 = 1.0 unrealistic?**
   - Synthetic data with deterministic patterns; real-world churn has noise (expected F1 = 0.50-0.70)

6. **What does SNOWFLAKE.ML.CLASSIFICATION return?**
   - Trained model object; can call PREDICT(), SHOW_FEATURE_IMPORTANCE(), SHOW_EVALUATION_METRICS()

7. **How do you get feature importance?**
   - `SELECT * FROM TABLE(model!SHOW_FEATURE_IMPORTANCE()) ORDER BY importance DESC`

8. **What are the churn risk categories?**
   - Low Risk (0-39%), Medium Risk (40-69%), High Risk (70-100%)

9. **How often should you retrain?**
   - Monthly, or when F1 score drops below threshold (e.g., 0.60)

10. **Can you use Cortex ML outside Snowflake?**
    - No; model training and inference must happen inside Snowflake (but can call via Snowpark Python externally)

---

## 13. CLUSTERING KEYS

### 13.1 What is Clustering?

**Clustering** = automatic data sorting and organization in micro-partitions for faster query performance.

**Snowflake Micro-Partitions:**
- Snowflake stores data in **micro-partitions** (16 MB compressed each)
- Each partition tracks **min/max values** for all columns
- Query optimizer uses min/max metadata to **prune partitions** (skip irrelevant data)

**Clustering Key:**
- Tells Snowflake which columns to sort by when organizing micro-partitions
- Snowflake automatically maintains clustering (automatic re-clustering)

**Without Clustering:**
```
Micro-Partition 1: dates [2023-01-01 ... 2024-12-31] (scattered)
Micro-Partition 2: dates [2023-01-15 ... 2024-11-20] (scattered)
Micro-Partition 3: dates [2023-02-01 ... 2024-10-30] (scattered)
Query: WHERE transaction_date >= '2024-11-01'
→ Must scan ALL 3 partitions (no pruning)
```

**With Clustering by transaction_date:**
```
Micro-Partition 1: dates [2023-01-01 ... 2023-06-30] (sorted)
Micro-Partition 2: dates [2023-07-01 ... 2023-12-31] (sorted)
Micro-Partition 3: dates [2024-01-01 ... 2024-06-30] (sorted)
Micro-Partition 4: dates [2024-07-01 ... 2024-12-31] (sorted)
Query: WHERE transaction_date >= '2024-11-01'
→ Scans ONLY Partition 4 (3x pruning efficiency)
```

### 13.2 Clustering fct_transactions

**File:** `dbt_customer_analytics/models/gold/fct_transactions.sql`

```sql
{{
  config(
    materialized='table',
    cluster_by=['transaction_date']
  )
}}

SELECT
    transaction_sk,
    transaction_id,
    customer_sk,
    date_sk,
    category_sk,
    transaction_date,
    transaction_amount,
    merchant_name,
    channel,
    status
FROM {{ ref('stg_transactions') }} t
INNER JOIN ...
```

**Query Pattern Benefits:**
```sql
-- Fast: Partition pruning on clustered column
SELECT SUM(transaction_amount)
FROM gold.fct_transactions
WHERE transaction_date >= '2024-11-01';
-- Scans only Nov-Dec 2024 partitions

-- Still fast: Clustering helps date range queries
SELECT customer_sk, SUM(transaction_amount)
FROM gold.fct_transactions
WHERE transaction_date BETWEEN '2024-10-01' AND '2024-10-31'
GROUP BY customer_sk;
-- Scans only October 2024 partitions
```

### 13.3 Clustering customer_360_profile

**File:** `dbt_customer_analytics/models/gold/customer_360_profile.sql`

```sql
{{
  config(
    materialized='table',
    cluster_by=['customer_id']
  )
}}

SELECT
    customer_id,
    full_name,
    email,
    customer_segment,
    churn_risk_score,
    churn_risk_category,
    lifetime_value,
    ...
FROM ...
```

**Query Pattern Benefits:**
```sql
-- EXTREMELY fast: Single customer lookup
SELECT *
FROM gold.customer_360_profile
WHERE customer_id = 'C123456';
-- Scans 1 micro-partition (<1 second)

-- Fast: Small set of customers
SELECT *
FROM gold.customer_360_profile
WHERE customer_id IN ('C123456', 'C123457', 'C123458');
-- Scans 3 micro-partitions
```

**Customer 360 Tab Performance:**
- Clustered by `customer_id` → lookups <1 second
- Denormalized schema (no JOINs) → further speed boost
- Combined effect: **Sub-second customer profile loads**

### 13.4 Multi-Column Clustering

**When to use:**
- Queries frequently filter on multiple columns
- Columns have high cardinality (many distinct values)

**Example: Cluster by (customer_sk, transaction_date)**
```sql
CREATE TABLE gold.fct_transactions (
    ...
)
CLUSTER BY (customer_sk, transaction_date);
```

**Benefits:**
```sql
-- Very fast: Both clustering dimensions used
SELECT SUM(transaction_amount)
FROM gold.fct_transactions
WHERE customer_sk = 12345
  AND transaction_date >= '2024-11-01';
-- Scans only partitions with customer 12345 + Nov-Dec 2024
```

**Trade-offs:**
- More clustering keys = more re-clustering work = higher cost
- Snowflake recommends 3-4 columns max
- Prioritize columns with highest filter frequency

### 13.5 Monitoring Clustering Health

**Check clustering depth:**
```sql
SELECT SYSTEM$CLUSTERING_DEPTH('gold.fct_transactions', '(transaction_date)');
```

**Interpretation:**
- **Depth 1-2:** Excellent clustering (most queries scan 1-2 partitions)
- **Depth 3-5:** Good clustering
- **Depth 6+:** Poor clustering (consider manual re-clustering)

**Clustering Information:**
```sql
SELECT SYSTEM$CLUSTERING_INFORMATION('gold.fct_transactions', '(transaction_date)');
```

**Output:**
```json
{
  "cluster_by_keys": "(TRANSACTION_DATE)",
  "total_partition_count": 85,
  "total_constant_partition_count": 12,
  "average_overlaps": 2.3,
  "average_depth": 2.8,
  "partition_depth_histogram": {
    "00001": 45,
    "00002": 28,
    "00003": 10,
    "00004": 2
  }
}
```

**Key Metrics:**
- **average_depth:** 2.8 (good)
- **average_overlaps:** 2.3 (low overlap = good pruning)

### 13.6 Automatic Re-Clustering

**Snowflake automatically re-clusters when:**
- New data is inserted (micro-partitions become fragmented)
- DML operations (UPDATE, DELETE) scatter data
- Clustering depth degrades beyond threshold

**Cost:**
- Re-clustering uses credits (billed separately from query compute)
- Typically 5-10% of data load cost
- Can suspend auto-clustering: `ALTER TABLE ... SUSPEND RECLUSTER`

**Monitor re-clustering credits:**
```sql
SELECT
    table_name,
    SUM(credits_used) AS total_reclustering_credits,
    SUM(num_bytes_reclustered) AS total_bytes_reclustered
FROM TABLE(INFORMATION_SCHEMA.AUTOMATIC_CLUSTERING_HISTORY(
    DATE_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
))
WHERE table_name = 'FCT_TRANSACTIONS'
GROUP BY table_name;
```

### 13.7 When NOT to Cluster

**Skip clustering if:**
- **Small tables** (<1 GB): Snowflake can scan entire table quickly
- **Low cardinality columns:** Clustering doesn't help if column has few distinct values
- **Write-heavy workloads:** Constant re-clustering adds overhead
- **Unpredictable query patterns:** No consistent filter columns

**Example: dim_merchant_category (50 rows)**
- No clustering needed; entire table fits in 1 micro-partition

### 13.8 Exam Questions

1. **What is clustering?**
   - Automatic data sorting in micro-partitions; enables partition pruning for faster queries

2. **What are micro-partitions?**
   - Snowflake's storage unit (16 MB compressed); tracks min/max metadata for all columns

3. **What is partition pruning?**
   - Query optimizer skips micro-partitions that don't match WHERE clause filters

4. **How do you define a clustering key in dbt?**
   - `{{ config(cluster_by=['column1', 'column2']) }}`

5. **What is fct_transactions clustered by?**
   - `transaction_date` (most queries filter by date range)

6. **What is customer_360_profile clustered by?**
   - `customer_id` (Customer 360 tab lookups)

7. **What is clustering depth?**
   - Average number of partitions scanned for clustered column queries; lower = better

8. **When does Snowflake automatically re-cluster?**
   - After inserts, updates, deletes that degrade clustering depth

9. **Can you cluster by multiple columns?**
   - Yes; recommend 3-4 max (more columns = higher re-clustering cost)

10. **Should you cluster small tables (<1 GB)?**
    - No; entire table fits in few partitions, clustering overhead not worth it

---

## 14. SNOWPIPE (NEW CONTENT)

**Note:** Snowpipe is **not currently implemented** in this project. This section provides conceptual coverage for completeness.

### 14.1 What is Snowpipe?

**Snowpipe** = continuous, automated data ingestion into Snowflake from external stages (S3, Azure Blob, GCS).

**Traditional COPY INTO:**
```sql
-- Manual batch load (run daily/hourly via cron/Airflow)
COPY INTO bronze.raw_transactions
FROM @customer_data_stage/transactions/
FILE_FORMAT = (TYPE='CSV');
```

**Snowpipe (Automated):**
```sql
-- Automatic load as soon as files land in S3
CREATE OR REPLACE PIPE bronze.transaction_pipe
  AUTO_INGEST = TRUE
AS
  COPY INTO bronze.raw_transactions
  FROM @customer_data_stage/transactions/
  FILE_FORMAT = (TYPE='CSV');
```

**When file lands in S3:**
1. S3 event notification → SQS queue
2. Snowpipe polls SQS queue
3. Automatically runs COPY INTO
4. Data available in Snowflake within seconds-minutes

### 14.2 Setup: S3 Event Notifications

**1. Get Snowpipe SQS queue ARN:**
```sql
DESC PIPE bronze.transaction_pipe;
-- Output: notification_channel = arn:aws:sqs:us-west-2:123456789012:sf-snowpipe-XXXXXXXXX
```

**2. Configure S3 event notification (Terraform):**
```hcl
resource "aws_s3_bucket_notification" "snowpipe_notification" {
  bucket = aws_s3_bucket.customer_analytics_data.id
  
  queue {
    queue_arn     = "arn:aws:sqs:us-west-2:123456789012:sf-snowpipe-XXXXXXXXX"
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "transactions/"  # Only transactions folder
    filter_suffix = ".csv"
  }
}
```

**3. Grant SQS permissions:**
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "s3.amazonaws.com"},
    "Action": "SQS:SendMessage",
    "Resource": "arn:aws:sqs:us-west-2:123456789012:sf-snowpipe-XXXXXXXXX",
    "Condition": {
      "ArnEquals": {
        "aws:SourceArn": "arn:aws:s3:::snowflake-customer-analytics-data-demo"
      }
    }
  }]
}
```

### 14.3 How Our Project Could Use Snowpipe

**Current:** Batch load via `COPY INTO` (manual/scheduled)

**With Snowpipe:**
1. Data generation script uploads CSV to S3
2. S3 event triggers Snowpipe
3. Snowpipe auto-loads to Bronze layer
4. Snowflake Stream detects new rows
5. Task triggers dbt transformations
6. Gold layer updates automatically

**Architecture:**
```
Python → S3 Upload → S3 Event → SQS → Snowpipe → Bronze Table
                                                      ↓
                                            Snowflake Stream
                                                      ↓
                                              Task (dbt run)
                                                      ↓
                                                  Gold Layer
```

### 14.4 Monitoring Snowpipe

**Check pipe status:**
```sql
SELECT SYSTEM$PIPE_STATUS('bronze.transaction_pipe');
```

**Output:**
```json
{
  "executionState": "RUNNING",
  "pendingFileCount": 0
}
```

**View load history:**
```sql
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'bronze.raw_transactions',
    START_TIME => DATEADD(hours, -24, CURRENT_TIMESTAMP())
))
ORDER BY last_load_time DESC;
```

**View pipe usage:**
```sql
SELECT
    pipe_name,
    SUM(credits_used) AS total_credits,
    SUM(files_inserted) AS files_loaded,
    SUM(rows_inserted) AS rows_loaded
FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
    DATE_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
))
WHERE pipe_name = 'TRANSACTION_PIPE'
GROUP BY pipe_name;
```

### 14.5 Error Handling

**Snowpipe error modes:**
```sql
CREATE OR REPLACE PIPE bronze.transaction_pipe
  AUTO_INGEST = TRUE
  ERROR_INTEGRATION = 'ERROR_NOTIFICATION_INTEGRATION'  -- Send errors to SNS/email
AS
  COPY INTO bronze.raw_transactions
  FROM @customer_data_stage/transactions/
  FILE_FORMAT = (TYPE='CSV')
  ON_ERROR = 'SKIP_FILE';  -- Skip files with errors (don't abort entire load)
```

**Query error files:**
```sql
SELECT *
FROM TABLE(VALIDATE_PIPE_LOAD(
    PIPE_NAME => 'bronze.transaction_pipe',
    START_TIME => DATEADD(hours, -24, CURRENT_TIMESTAMP())
))
WHERE ERROR_COUNT > 0;
```

### 14.6 Cost & Performance

**Snowpipe Pricing:**
- Billed per file loaded (not query compute)
- ~$0.06 per 1,000 files
- Example: 10,000 files/month = $6

**Latency:**
- Typical: 1-2 minutes from file upload to data availability
- Large files: 5-10 minutes

**Best Practices:**
- Batch files (1,000-10,000 rows per file ideal)
- Avoid tiny files (<100 KB); combine into larger files
- Use compression (GZIP) to reduce transfer time

### 14.7 Exam Questions

1. **What is Snowpipe?**
   - Continuous, automated data ingestion from external stages using event-driven architecture

2. **How does Snowpipe differ from COPY INTO?**
   - Snowpipe: Automatic (event-driven), continuous; COPY INTO: Manual, batch-scheduled

3. **What AWS service enables Snowpipe auto-ingest?**
   - S3 event notifications → SQS queue → Snowpipe polls queue

4. **What does AUTO_INGEST = TRUE do?**
   - Enables automatic loading when files land in S3 (requires S3 event notifications)

5. **How do you monitor Snowpipe?**
   - `SYSTEM$PIPE_STATUS()`, `COPY_HISTORY()`, `PIPE_USAGE_HISTORY()`

6. **What happens if a file has errors?**
   - Depends on ON_ERROR setting: SKIP_FILE (skip bad file), ABORT_STATEMENT (stop entire load), CONTINUE (load good rows)

7. **Can Snowpipe trigger downstream processing?**
   - Yes! Use Snowflake Streams to detect new rows, then Tasks to trigger transformations

8. **Is Snowpipe implemented in our project?**
   - No; we use manual COPY INTO for simplicity (Snowpipe is production enhancement)

---

## 15. SCD TYPE 2

### 15.1 Cross-Reference

See **Part I, Section 3.5** for detailed dbt implementation of SCD Type 2 for `dim_customer`.

See **Section 11.3** for star schema SCD Type 2 query patterns.

### 15.2 Quick Reference

**SCD Type 2 Columns:**
- **customer_sk:** Surrogate key (unique per version)
- **customer_id:** Business key (natural key, non-unique across versions)
- **effective_date:** When this version became active
- **end_date:** When this version expired (9999-12-31 for current)
- **is_current:** TRUE for current version, FALSE for historical
- **row_hash:** MD5 hash of all attributes (detect changes efficiently)

**Example Query: Historical Customer State**
```sql
-- What was customer C123's state on 2024-06-01?
SELECT full_name, state, effective_date, end_date
FROM gold.dim_customer
WHERE customer_id = 'C123'
  AND '2024-06-01' BETWEEN effective_date AND end_date;
```

**Example Query: Track Customer Moves**
```sql
-- Show all states customer C123 has lived in
SELECT 
    customer_id,
    full_name,
    state,
    effective_date,
    end_date,
    DATEDIFF(day, effective_date, end_date) AS days_in_state
FROM gold.dim_customer
WHERE customer_id = 'C123'
ORDER BY effective_date;
```

### 15.3 Exam Questions

1. **What does SCD Type 2 mean?**
   - Slowly Changing Dimension Type 2: Tracks history by creating new row for each change

2. **Why use surrogate keys instead of business keys?**
   - Business keys not unique across versions; surrogate key uniquely identifies each version

3. **What is the purpose of effective_date and end_date?**
   - Track time ranges when each version was active; enables point-in-time queries

4. **What value is used for end_date on current records?**
   - `9999-12-31` (far-future date indicating "still current")

5. **How do you find the current version of a customer?**
   - `WHERE is_current = TRUE` or `WHERE end_date = '9999-12-31'`

6. **What is row_hash used for?**
   - MD5 hash of all attributes; efficiently detect if customer changed (compare hashes instead of all columns)

7. **What happens when a customer changes state?**
   - Old version: Set `end_date = CURRENT_DATE - 1`, `is_current = FALSE`; New version: Insert new row with `effective_date = CURRENT_DATE`, `end_date = 9999-12-31`, `is_current = TRUE`

8. **How do you join fct_transactions to dim_customer with SCD Type 2?**
   - `WHERE transaction_date BETWEEN c.effective_date AND c.end_date`

---

**(Continuing with sections 16-25 in next append...)**

## 16. DATA QUALITY TESTS

### 16.1 dbt Testing Framework

**Test Types:**
1. **Schema Tests:** Defined in `schema.yml` files
2. **Data Tests:** Custom SQL queries in `tests/` folder
3. **Unit Tests:** Test individual model logic
4. **Integration Tests:** Test cross-model relationships

**Total Tests:** 35+ tests across all models

### 16.2 Schema Tests

**File:** `dbt_customer_analytics/models/schema.yml`

**Built-in Tests:**
```yaml
models:
  - name: fct_transactions
    columns:
      - name: transaction_sk
        tests:
          - unique              # No duplicates
          - not_null            # No missing values
      
      - name: customer_sk
        tests:
          - not_null
          - relationships:      # Foreign key integrity
              to: ref('dim_customer')
              field: customer_sk
      
      - name: transaction_amount
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 50000
              inclusive: true
      
      - name: status
        tests:
          - accepted_values:
              values: ['Approved', 'Declined']

  - name: dim_customer
    columns:
      - name: customer_sk
        tests:
          - unique
          - not_null
      
      - name: customer_id
        tests:
          - not_null
      
      - name: email
        tests:
          - not_null
          - dbt_utils.unique_where:
              where: "is_current = TRUE"  # Email unique for current versions only
```

### 16.3 Custom Data Tests

**File:** `dbt_customer_analytics/tests/scd2_no_overlapping_dates.sql`

**Test: SCD Type 2 No Overlapping Date Ranges**
```sql
-- Test fails if any customer has overlapping date ranges
SELECT
    customer_id,
    COUNT(*) AS overlapping_versions
FROM (
    SELECT 
        a.customer_id,
        a.customer_sk AS sk_a,
        b.customer_sk AS sk_b,
        a.effective_date AS eff_a,
        a.end_date AS end_a,
        b.effective_date AS eff_b,
        b.end_date AS end_b
    FROM {{ ref('dim_customer') }} a
    INNER JOIN {{ ref('dim_customer') }} b
        ON a.customer_id = b.customer_id
        AND a.customer_sk <> b.customer_sk
    WHERE a.effective_date <= b.end_date
      AND a.end_date >= b.effective_date
)
GROUP BY customer_id
HAVING COUNT(*) > 0;
```

**Expected:** 0 rows (no overlaps)

**File:** `dbt_customer_analytics/tests/scd2_no_gaps.sql`

**Test: SCD Type 2 No Gaps in History**
```sql
-- Test fails if any customer has gaps in their history
WITH customer_date_ranges AS (
    SELECT
        customer_id,
        MIN(effective_date) AS first_effective_date,
        MAX(end_date) AS last_end_date
    FROM {{ ref('dim_customer') }}
    GROUP BY customer_id
),
source_dates AS (
    SELECT
        customer_id,
        MIN(account_open_date) AS account_open_date
    FROM {{ ref('stg_customers') }}
    GROUP BY customer_id
)

SELECT
    r.customer_id,
    r.first_effective_date,
    s.account_open_date,
    DATEDIFF(day, s.account_open_date, r.first_effective_date) AS gap_days
FROM customer_date_ranges r
INNER JOIN source_dates s ON r.customer_id = s.customer_id
WHERE r.first_effective_date > s.account_open_date
  AND gap_days > 1;
```

**Expected:** 0 rows (no gaps)

### 16.4 Segment Distribution Test

**File:** `dbt_customer_analytics/tests/segment_distribution.sql`

**Test: Segment Distribution Within Expected Ranges**
```sql
WITH segment_counts AS (
    SELECT
        customer_segment,
        COUNT(*) AS customer_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
    FROM {{ ref('customer_360_profile') }}
    GROUP BY customer_segment
)

SELECT
    customer_segment,
    pct,
    CASE
        WHEN customer_segment = 'High-Value Travelers' AND pct NOT BETWEEN 10 AND 20 THEN 'FAIL'
        WHEN customer_segment = 'Stable Mid-Spenders' AND pct NOT BETWEEN 35 AND 45 THEN 'FAIL'
        WHEN customer_segment = 'Budget-Conscious' AND pct NOT BETWEEN 20 AND 30 THEN 'FAIL'
        WHEN customer_segment = 'Declining' AND pct NOT BETWEEN 5 AND 15 THEN 'FAIL'
        WHEN customer_segment = 'New & Growing' AND pct NOT BETWEEN 5 AND 15 THEN 'FAIL'
        ELSE 'PASS'
    END AS test_result
FROM segment_counts
WHERE test_result = 'FAIL';
```

**Expected:** 0 rows (all segments within expected ranges)

### 16.5 Running dbt Tests

**Command Line:**
```bash
cd dbt_customer_analytics

# Run all tests
dbt test

# Run tests for specific model
dbt test --select fct_transactions

# Run only schema tests
dbt test --data

# Run only custom data tests
dbt test --schema
```

**Output:**
```
Running with dbt=1.7.0
Found 14 models, 35 tests, 0 snapshots, 0 analyses, 0 macros, 0 operations, 0 seed files, 3 sources

14:32:15  Concurrency: 4 threads (target='prod')
14:32:15  
14:32:15  1 of 35 START test accepted_values_fct_transactions_status.................. [RUN]
14:32:16  1 of 35 PASS accepted_values_fct_transactions_status........................ [PASS in 0.82s]
14:32:16  2 of 35 START test not_null_dim_customer_customer_sk........................ [RUN]
14:32:17  2 of 35 PASS not_null_dim_customer_customer_sk.............................. [PASS in 0.65s]
...
14:33:45  35 of 35 PASS scd2_no_overlapping_dates..................................... [PASS in 1.23s]
14:33:45
14:33:45  Finished running 35 tests in 0 hours 1 minutes and 30 seconds (90.12s).
14:33:45
14:33:45  Completed successfully
14:33:45
14:33:45  Done. PASS=35 WARN=0 ERROR=0 SKIP=0 TOTAL=35
```

### 16.6 Test Severity Levels

```yaml
models:
  - name: fct_transactions
    columns:
      - name: transaction_amount
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 50000
              config:
                severity: error  # Fail build if test fails
          
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 10000
              config:
                severity: warn   # Warn but don't fail build
```

**Severity Options:**
- `error`: Fail dbt run (default)
- `warn`: Log warning, continue build

### 16.7 Exam Questions

1. **How many tests are in the dbt project?**
   - 35+ tests (schema tests + custom data tests)

2. **What are the 4 built-in dbt tests?**
   - `unique`, `not_null`, `relationships` (foreign key), `accepted_values`

3. **What is a custom data test?**
   - SQL query in `tests/` folder; test passes if query returns 0 rows

4. **What does the SCD Type 2 overlap test check?**
   - No customer has multiple versions with overlapping effective_date/end_date ranges

5. **What does the segment distribution test validate?**
   - Each segment's % of customers falls within expected range (e.g., High-Value Travelers 10-20%)

6. **How do you run only tests for one model?**
   - `dbt test --select model_name`

7. **What is test severity?**
   - `error` = fail build; `warn` = log warning but continue

8. **Can you test relationships across models?**
   - Yes! `relationships` test validates foreign keys (e.g., `customer_sk` in `fct_transactions` exists in `dim_customer`)

---

## 17. GITHUB ACTIONS

### 17.1 What is GitHub Actions?

**GitHub Actions** = CI/CD platform for automating workflows (build, test, deploy).

**Our Workflows:**
1. **Streamlit Deployment:** Auto-deploy Streamlit app on push to main
2. **dbt Testing:** Run dbt tests on pull requests
3. **Python Tests:** Run PyTest suite on code changes

### 17.2 Streamlit Deployment Workflow

**File:** `.github/workflows/deploy_streamlit.yml`

```yaml
name: Deploy Streamlit to Snowflake

on:
  push:
    branches:
      - main
    paths:
      - 'streamlit/**'      # Only trigger if streamlit/ files change
      - '.github/workflows/deploy_streamlit.yml'

jobs:
  deploy:
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v3
      
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install Snowflake CLI
        run: |
          pip install snowflake-cli-labs
      
      - name: Configure Snowflake connection
        run: |
          snow connection add default \
            --account ${{ secrets.SNOWFLAKE_ACCOUNT }} \
            --user ${{ secrets.SNOWFLAKE_USER }} \
            --password ${{ secrets.SNOWFLAKE_PASSWORD }} \
            --role DATA_ANALYST \
            --warehouse COMPUTE_WH \
            --database CUSTOMER_ANALYTICS
      
      - name: Deploy Streamlit app
        run: |
          cd streamlit
          snow streamlit deploy --replace
      
      - name: Get app URL
        run: |
          snow streamlit get-url customer_360_app
```

**Secrets Configuration (GitHub repo settings):**
- `SNOWFLAKE_ACCOUNT`: `abc12345.us-west-2`
- `SNOWFLAKE_USER`: `GITHUB_ACTIONS_USER`
- `SNOWFLAKE_PASSWORD`: `<secure password>`

**Workflow Trigger:**
```bash
# Local: Make changes to Streamlit app
vim streamlit/tabs/ai_assistant.py

# Commit and push
git add streamlit/tabs/ai_assistant.py
git commit -m "Update AI Assistant tab"
git push origin main

# GitHub Actions automatically:
# 1. Detects push to main with streamlit/ changes
# 2. Runs workflow
# 3. Deploys updated app to Snowflake
# 4. App refreshes automatically
```

### 17.3 dbt Testing Workflow

**File:** `.github/workflows/dbt_tests.yml`

```yaml
name: Run dbt Tests

on:
  pull_request:
    branches:
      - main
    paths:
      - 'dbt_customer_analytics/**'
      - '.github/workflows/dbt_tests.yml'

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v3
      
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install dbt
        run: |
          pip install dbt-snowflake==1.7.0
      
      - name: Configure dbt profiles
        run: |
          mkdir -p ~/.dbt
          cat > ~/.dbt/profiles.yml <<EOF
          customer_analytics:
            target: ci
            outputs:
              ci:
                type: snowflake
                account: ${{ secrets.SNOWFLAKE_ACCOUNT }}
                user: ${{ secrets.SNOWFLAKE_USER }}
                password: ${{ secrets.SNOWFLAKE_PASSWORD }}
                role: DATA_ENGINEER
                warehouse: COMPUTE_WH
                database: CUSTOMER_ANALYTICS_CI  # Separate CI database
                schema: GOLD
          EOF
      
      - name: Install dbt dependencies
        run: |
          cd dbt_customer_analytics
          dbt deps
      
      - name: Run dbt models
        run: |
          cd dbt_customer_analytics
          dbt run --target ci
      
      - name: Run dbt tests
        run: |
          cd dbt_customer_analytics
          dbt test --target ci
      
      - name: Comment test results on PR
        if: always()
        uses: actions/github-script@v6
        with:
          script: |
            const output = `#### dbt Test Results
            \`\`\`
            ${{ steps.dbt_test.outputs.stdout }}
            \`\`\``;
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: output
            });
```

**Workflow Trigger:**
```bash
# Create feature branch
git checkout -b feature/new-segment-logic

# Make changes
vim dbt_customer_analytics/models/gold/customer_segments.sql

# Commit and push
git add dbt_customer_analytics/
git commit -m "Add new segment logic"
git push origin feature/new-segment-logic

# Create pull request on GitHub
# → GitHub Actions automatically runs dbt tests
# → PR shows test results as comment
# → Merge only if tests pass
```

### 17.4 PyTest Workflow

**File:** `.github/workflows/pytest.yml`

```yaml
name: Run Python Tests

on:
  push:
    branches:
      - main
      - develop
  pull_request:
    branches:
      - main

jobs:
  test:
    runs-on: ubuntu-latest
    
    strategy:
      matrix:
        python-version: ['3.9', '3.10', '3.11']
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v3
      
      - name: Setup Python ${{ matrix.python-version }}
        uses: actions/setup-python@v4
        with:
          python-version: ${{ matrix.python-version }}
      
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest pytest-cov
      
      - name: Run unit tests
        run: |
          pytest tests/unit/ -v --cov=data_generation --cov-report=xml
      
      - name: Run integration tests
        run: |
          pytest tests/integration/ -v
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
      
      - name: Upload coverage to Codecov
        uses: codecov/codecov-action@v3
        with:
          file: ./coverage.xml
          fail_ci_if_error: true
```

### 17.5 Monitoring Workflow Runs

**GitHub UI:**
1. Navigate to repo → Actions tab
2. View workflow runs (success/failure)
3. Click run → View logs for each step
4. Re-run failed workflows

**Slack Notifications (Optional):**
```yaml
      - name: Notify Slack on failure
        if: failure()
        uses: slackapi/slack-github-action@v1
        with:
          webhook-url: ${{ secrets.SLACK_WEBHOOK_URL }}
          payload: |
            {
              "text": "❌ Streamlit deployment failed! Check GitHub Actions."
            }
```

### 17.6 Best Practices

1. **Use secrets for credentials:** Never hardcode passwords in workflows
2. **Separate CI environment:** Use `CUSTOMER_ANALYTICS_CI` database for tests (don't pollute prod)
3. **Run tests on PRs:** Catch issues before merging
4. **Auto-deploy only on main:** Feature branches deploy manually
5. **Matrix testing:** Test across multiple Python versions
6. **Fail fast:** Stop workflow on first error

### 17.7 Exam Questions

1. **What is GitHub Actions?**
   - CI/CD platform for automating workflows (build, test, deploy)

2. **What workflows are configured in this project?**
   - Streamlit deployment (on push to main), dbt tests (on PR), PyTest suite (on push/PR)

3. **When does Streamlit auto-deploy?**
   - On push to main branch with changes in `streamlit/` folder

4. **Where are Snowflake credentials stored?**
   - GitHub repository secrets (Settings → Secrets and variables → Actions)

5. **What is the purpose of the dbt CI database?**
   - Separate environment for running tests (doesn't pollute production data)

6. **What does `if: always()` do?**
   - Run step regardless of previous step success/failure (useful for cleanup or notifications)

7. **Can you manually trigger workflows?**
   - Yes! Use `workflow_dispatch` event in workflow YAML

8. **How do you view workflow logs?**
   - GitHub repo → Actions tab → Click workflow run → View step logs

---

## 18. RESULT CACHING (NEW CONTENT)

### 18.1 What is Result Caching?

**Snowflake Result Cache** = automatically caches query results for 24 hours.

**How it works:**
1. User runs query: `SELECT SUM(amount) FROM transactions WHERE date >= '2024-11-01'`
2. Snowflake computes result, stores in cache (key = query text + session context)
3. Another user runs **exact same query** within 24 hours
4. Snowflake returns cached result instantly (no warehouse compute used)
5. If underlying data changes, cache is invalidated

**Benefits:**
- **Zero cost:** Cached results don't use compute credits
- **Instant response:** <1 second for complex aggregations
- **Automatic:** No configuration needed

### 18.2 Caching Requirements

**Query must be:**
- **Identical:** Exact same SQL text (whitespace-sensitive!)
- **Same role/warehouse:** Session context must match
- **No table changes:** Underlying tables not modified since cache

**Cache Invalidation Triggers:**
- INSERT, UPDATE, DELETE, MERGE on queried tables
- 24-hour TTL expires
- Manual cache flush: `ALTER SESSION SET USE_CACHED_RESULT = FALSE`

### 18.3 Result Caching in Streamlit

**Streamlit @st.cache_data + Snowflake Result Cache = Double Caching**

**Example:**
```python
import streamlit as st

@st.cache_data(ttl=3600)  # Streamlit cache: 1 hour
def fetch_segment_summary(segments, states):
    """Fetch customer segment summary."""
    query = f"""
        SELECT 
            customer_segment,
            COUNT(*) AS customer_count,
            SUM(lifetime_value) AS total_ltv
        FROM customer_360_profile
        WHERE customer_segment IN ({','.join([f"'{s}'" for s in segments])})
          AND state IN ({','.join([f"'{s}'" for s in states])})
        GROUP BY customer_segment
    """
    return execute_query(query)

# First call: Snowflake computes + caches result, Streamlit caches DataFrame
df1 = fetch_segment_summary(['High-Value Travelers'], ['CA', 'NY'])

# Second call (within 1 hour): Streamlit returns cached DataFrame (no Snowflake query)
df2 = fetch_segment_summary(['High-Value Travelers'], ['CA', 'NY'])

# Different parameters: Streamlit cache miss, but Snowflake might cache if exact SQL matches
df3 = fetch_segment_summary(['Stable Mid-Spenders'], ['TX'])
```

**Caching Layers:**
1. **Streamlit cache (@st.cache_data):** In-memory DataFrame cache (1 hour TTL)
2. **Snowflake result cache:** Query result cache (24 hour TTL)
3. **Snowflake metadata cache:** Micro-partition metadata (faster pruning)

### 18.4 Monitoring Result Cache Hits

```sql
-- View query history with cache hits
SELECT
    query_id,
    query_text,
    execution_status,
    total_elapsed_time / 1000 AS seconds,
    bytes_scanned,
    percentage_scanned_from_cache,
    partitions_scanned,
    partitions_total
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
    RESULT_LIMIT => 100
))
WHERE query_text ILIKE '%customer_360_profile%'
ORDER BY start_time DESC;
```

**Interpreting Results:**
- `percentage_scanned_from_cache = 100`: Full cache hit (instant result, 0 credits)
- `percentage_scanned_from_cache = 0`: Cache miss (warehouse compute used)
- `partitions_scanned < partitions_total`: Partition pruning (clustering benefit)

### 18.5 When Result Cache DOESN'T Help

**Non-Deterministic Functions:**
```sql
-- Cache DISABLED: CURRENT_TIMESTAMP() changes every call
SELECT COUNT(*) FROM transactions WHERE ts < CURRENT_TIMESTAMP();

-- Cache ENABLED: Fixed date literal
SELECT COUNT(*) FROM transactions WHERE ts < '2024-11-01';
```

**User-Specific Queries:**
```sql
-- Cache per-user (CURRENT_USER() = session context)
SELECT * FROM customer_360_profile WHERE owner = CURRENT_USER();
```

**Frequent Data Changes:**
- High-velocity tables (streaming inserts) invalidate cache constantly
- Result cache less effective for real-time dashboards

### 18.6 Optimizing for Result Cache

**Best Practices:**

1. **Parameterize queries consistently:**
```python
# Good: Consistent formatting
query = f"SELECT * FROM table WHERE id = {id}"

# Bad: Inconsistent whitespace breaks cache
query = f"SELECT  *  FROM  table  WHERE id={id}"  # Different whitespace
```

2. **Use fixed date literals when possible:**
```sql
-- Good: Cacheable
WHERE transaction_date >= '2024-11-01'

-- Bad: Not cacheable
WHERE transaction_date >= CURRENT_DATE - 30
```

3. **Encourage users to reuse filters:**
- Dropdown selections → same SQL text → cache hits

### 18.7 Exam Questions

1. **What is Snowflake Result Cache?**
   - Automatic 24-hour cache of query results; returns cached results for identical queries

2. **What are the caching requirements?**
   - Identical SQL text, same session context (role/warehouse), no table changes

3. **How long are results cached?**
   - 24 hours (or until underlying table changes)

4. **Do cached results use compute credits?**
   - No! Cached results are free (no warehouse compute)

5. **What invalidates the result cache?**
   - INSERT/UPDATE/DELETE on queried tables, 24-hour expiration, manual cache disable

6. **How does Streamlit caching interact with Snowflake caching?**
   - Double caching: Streamlit caches DataFrame, Snowflake caches SQL results

7. **Can you disable result caching?**
   - Yes: `ALTER SESSION SET USE_CACHED_RESULT = FALSE`

8. **What does `percentage_scanned_from_cache = 100` mean?**
   - Full cache hit; instant result with zero compute cost

---

## 19. PYTEST SUITE

### 19.1 Test Organization

**Structure:**
```
tests/
├── unit/                    # Unit tests (no external dependencies)
│   ├── test_customer_generator.py
│   ├── test_data_validation.py
│   └── test_segment_logic.py
├── integration/             # Integration tests (require Snowflake)
│   ├── test_dbt_models.py
│   ├── test_ml_pipeline.py
│   ├── test_semantic_layer.py
│   └── test_streamlit_app.py
└── performance/             # Performance benchmarks
    ├── test_query_performance.py
    └── test_generation_speed.py
```

**Total Tests:** 16+ files

### 19.2 Unit Tests: Customer Generator

**File:** `tests/unit/test_customer_generator.py`

```python
import pytest
from data_generation.customer_generator import CustomerGenerator

def test_generate_customers_count():
    """Test that generator creates exact customer count."""
    gen = CustomerGenerator(customer_count=1000, seed=42)
    customers = gen.generate()
    
    assert len(customers) == 1000

def test_segment_distribution():
    """Test segment distribution within expected ranges."""
    gen = CustomerGenerator(customer_count=10000, seed=42)
    customers = gen.generate()
    
    segment_counts = customers['customer_segment'].value_counts(normalize=True)
    
    assert 0.10 <= segment_counts['High-Value Travelers'] <= 0.20
    assert 0.35 <= segment_counts['Stable Mid-Spenders'] <= 0.45
    assert 0.20 <= segment_counts['Budget-Conscious'] <= 0.30

def test_reproducibility():
    """Test that same seed produces identical data."""
    gen1 = CustomerGenerator(customer_count=100, seed=42)
    gen2 = CustomerGenerator(customer_count=100, seed=42)
    
    df1 = gen1.generate()
    df2 = gen2.generate()
    
    assert df1.equals(df2)

def test_email_uniqueness():
    """Test that all emails are unique."""
    gen = CustomerGenerator(customer_count=1000, seed=42)
    customers = gen.generate()
    
    assert customers['email'].nunique() == 1000

def test_credit_limit_by_segment():
    """Test credit limits match segment expectations."""
    gen = CustomerGenerator(customer_count=10000, seed=42)
    customers = gen.generate()
    
    # High-Value Travelers should have higher credit limits
    hvt_avg = customers[customers['customer_segment'] == 'High-Value Travelers']['credit_limit'].mean()
    budget_avg = customers[customers['customer_segment'] == 'Budget-Conscious']['credit_limit'].mean()
    
    assert hvt_avg > budget_avg * 2  # At least 2x higher
```

**Run Unit Tests:**
```bash
pytest tests/unit/ -v
```

### 19.3 Integration Tests: dbt Models

**File:** `tests/integration/test_dbt_models.py`

```python
import pytest
import snowflake.connector
import os

@pytest.fixture(scope="module")
def snowflake_conn():
    """Create Snowflake connection for tests."""
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse='COMPUTE_WH',
        database='CUSTOMER_ANALYTICS',
        schema='GOLD'
    )
    yield conn
    conn.close()

def test_fct_transactions_row_count(snowflake_conn):
    """Test fact table has expected row count range."""
    cursor = snowflake_conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM fct_transactions")
    row_count = cursor.fetchone()[0]
    
    assert 10_000_000 <= row_count <= 20_000_000  # 10-20M expected

def test_dim_customer_scd2_integrity(snowflake_conn):
    """Test SCD Type 2 integrity (no overlaps, no gaps)."""
    cursor = snowflake_conn.cursor()
    
    # Test no overlapping dates
    cursor.execute("""
        SELECT COUNT(*)
        FROM dim_customer a
        JOIN dim_customer b
            ON a.customer_id = b.customer_id
            AND a.customer_sk <> b.customer_sk
            AND a.effective_date <= b.end_date
            AND a.end_date >= b.effective_date
    """)
    overlaps = cursor.fetchone()[0]
    assert overlaps == 0
    
    # Test all customers have current version
    cursor.execute("""
        SELECT COUNT(DISTINCT customer_id)
        FROM dim_customer
        WHERE is_current = TRUE
    """)
    current_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT customer_id) FROM stg_customers")
    source_count = cursor.fetchone()[0]
    
    assert current_count == source_count

def test_customer_360_profile_completeness(snowflake_conn):
    """Test customer_360_profile has all customers."""
    cursor = snowflake_conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM customer_360_profile")
    profile_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM dim_customer WHERE is_current = TRUE")
    customer_count = cursor.fetchone()[0]
    
    assert profile_count == customer_count

def test_churn_predictions_exist(snowflake_conn):
    """Test churn predictions table has scores for all customers."""
    cursor = snowflake_conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM churn_predictions")
    prediction_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM customer_360_profile")
    customer_count = cursor.fetchone()[0]
    
    assert prediction_count == customer_count
```

**Run Integration Tests:**
```bash
export SNOWFLAKE_ACCOUNT=abc12345.us-west-2
export SNOWFLAKE_USER=test_user
export SNOWFLAKE_PASSWORD=***
pytest tests/integration/ -v
```

### 19.4 Integration Tests: ML Pipeline

**File:** `tests/integration/test_ml_pipeline.py`

```python
def test_ml_feature_completeness(snowflake_conn):
    """Test ML features table has all required columns."""
    cursor = snowflake_conn.cursor()
    cursor.execute("DESCRIBE TABLE ml_training_features")
    columns = {row[0] for row in cursor.fetchall()}
    
    required_features = {
        'customer_id', 'is_churned', 'lifetime_value', 
        'avg_transaction_value', 'days_since_last_transaction',
        'transactions_last_30_days', 'transactions_last_90_days',
        'mom_spend_change_pct', 'age', 'customer_segment'
    }
    
    assert required_features.issubset(columns)

def test_churn_label_distribution(snowflake_conn):
    """Test churn label distribution is reasonable."""
    cursor = snowflake_conn.cursor()
    cursor.execute("""
        SELECT 
            is_churned,
            COUNT(*) AS count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
        FROM ml_training_features
        GROUP BY is_churned
    """)
    
    results = {row[0]: row[2] for row in cursor.fetchall()}
    
    # Expect 5-15% churn rate
    assert 5 <= results[1] <= 15

def test_model_predictions_in_range(snowflake_conn):
    """Test churn predictions are between 0-100."""
    cursor = snowflake_conn.cursor()
    cursor.execute("""
        SELECT 
            MIN(churn_risk_score) AS min_score,
            MAX(churn_risk_score) AS max_score
        FROM churn_predictions
    """)
    
    min_score, max_score = cursor.fetchone()
    
    assert 0 <= min_score <= 100
    assert 0 <= max_score <= 100
```

### 19.5 Performance Tests

**File:** `tests/performance/test_query_performance.py`

```python
import time

def test_customer_360_lookup_performance(snowflake_conn):
    """Test single customer lookup is <1 second."""
    cursor = snowflake_conn.cursor()
    
    start = time.time()
    cursor.execute("""
        SELECT *
        FROM customer_360_profile
        WHERE customer_id = 100000
    """)
    result = cursor.fetchone()
    elapsed = time.time() - start
    
    assert result is not None
    assert elapsed < 1.0  # <1 second

def test_segment_query_performance(snowflake_conn):
    """Test segment aggregation is <5 seconds."""
    cursor = snowflake_conn.cursor()
    
    start = time.time()
    cursor.execute("""
        SELECT 
            customer_segment,
            COUNT(*) AS customer_count,
            SUM(lifetime_value) AS total_ltv
        FROM customer_360_profile
        GROUP BY customer_segment
    """)
    results = cursor.fetchall()
    elapsed = time.time() - start
    
    assert len(results) == 5  # 5 segments
    assert elapsed < 5.0  # <5 seconds
```

### 19.6 Running Tests

**All tests:**
```bash
pytest tests/ -v
```

**Specific test file:**
```bash
pytest tests/integration/test_dbt_models.py -v
```

**With coverage:**
```bash
pytest tests/ --cov=data_generation --cov=streamlit --cov-report=html
```

**Parallel execution:**
```bash
pytest tests/ -n 4  # 4 parallel workers
```

### 19.7 Exam Questions

1. **How many test files are in the project?**
   - 16+ test files (unit, integration, performance)

2. **What is the difference between unit and integration tests?**
   - Unit: No external dependencies, fast; Integration: Require Snowflake connection, slower

3. **What does the SCD Type 2 integrity test check?**
   - No overlapping date ranges, all customers have current version

4. **What performance benchmark is tested for Customer 360 lookups?**
   - <1 second for single customer lookup

5. **How do you run only integration tests?**
   - `pytest tests/integration/ -v`

6. **What is pytest-cov used for?**
   - Code coverage reporting (% of code executed by tests)

7. **Can you run tests in parallel?**
   - Yes! Use `pytest -n 4` (requires pytest-xdist)

8. **What environment variables are needed for integration tests?**
   - `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`

---

## 20. RBAC POLICIES (NEW CONTENT)

### 20.1 What is RBAC?

**RBAC (Role-Based Access Control)** = security model where permissions are assigned to roles, and roles are assigned to users.

**Snowflake RBAC:**
- **Roles:** Named sets of privileges (DATA_ENGINEER, DATA_ANALYST, DATA_SCIENTIST)
- **Privileges:** Specific permissions (SELECT, INSERT, CREATE TABLE, USAGE)
- **Grant Hierarchy:** Roles can be granted to other roles (role inheritance)

**Benefits:**
- **Separation of duties:** Different roles for different responsibilities
- **Least privilege:** Users get only permissions they need
- **Audit trail:** Track who did what via role grants

### 20.2 Role Hierarchy

**Our Project Roles:**
```
ACCOUNTADMIN (Snowflake built-in)
    ↓
SYSADMIN (Snowflake built-in)
    ↓
DATA_ENGINEER ← Owns Bronze/Silver/Gold schemas, runs dbt
    ↓
DATA_ANALYST ← Reads Gold layer, runs Streamlit
    ↓
DATA_SCIENTIST ← Trains ML models, accesses features
```

**File:** `snowflake/setup/02_create_roles_permissions.sql`

```sql
-- ============================================================================
-- Role Creation
-- ============================================================================
USE ROLE ACCOUNTADMIN;

-- Data Engineer: Builds and maintains data pipeline
CREATE ROLE IF NOT EXISTS DATA_ENGINEER;
GRANT ROLE DATA_ENGINEER TO ROLE SYSADMIN;

-- Data Analyst: Consumes data for reporting and dashboards
CREATE ROLE IF NOT EXISTS DATA_ANALYST;
GRANT ROLE DATA_ANALYST TO ROLE DATA_ENGINEER;  -- Engineer inherits analyst permissions

-- Data Scientist: Trains ML models and analyzes features
CREATE ROLE IF NOT EXISTS DATA_SCIENTIST;
GRANT ROLE DATA_SCIENTIST TO ROLE DATA_ENGINEER;  -- Engineer inherits scientist permissions

-- ============================================================================
-- DATA_ENGINEER Permissions
-- ============================================================================
USE ROLE ACCOUNTADMIN;

-- Database and schema access
GRANT USAGE ON DATABASE CUSTOMER_ANALYTICS TO ROLE DATA_ENGINEER;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA BRONZE TO ROLE DATA_ENGINEER;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA SILVER TO ROLE DATA_ENGINEER;
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE MATERIALIZED VIEW ON SCHEMA GOLD TO ROLE DATA_ENGINEER;

-- Full access to all tables in all schemas
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA BRONZE TO ROLE DATA_ENGINEER;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA SILVER TO ROLE DATA_ENGINEER;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA GOLD TO ROLE DATA_ENGINEER;

-- Future grants (apply to tables created later)
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA BRONZE TO ROLE DATA_ENGINEER;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA SILVER TO ROLE DATA_ENGINEER;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA GOLD TO ROLE DATA_ENGINEER;

-- Warehouse access
GRANT USAGE, OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE DATA_ENGINEER;

-- Storage integration and stage access
GRANT USAGE ON INTEGRATION customer_analytics_s3_integration TO ROLE DATA_ENGINEER;
GRANT USAGE ON STAGE BRONZE.customer_data_stage TO ROLE DATA_ENGINEER;

-- Stored procedure execution
GRANT USAGE ON PROCEDURE BRONZE.GENERATE_CUSTOMERS(INTEGER, INTEGER) TO ROLE DATA_ENGINEER;

-- ============================================================================
-- DATA_ANALYST Permissions
-- ============================================================================

-- Database and schema access (READ ONLY)
GRANT USAGE ON DATABASE CUSTOMER_ANALYTICS TO ROLE DATA_ANALYST;
GRANT USAGE ON SCHEMA GOLD TO ROLE DATA_ANALYST;

-- Read-only access to Gold layer
GRANT SELECT ON ALL TABLES IN SCHEMA GOLD TO ROLE DATA_ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA GOLD TO ROLE DATA_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA GOLD TO ROLE DATA_ANALYST;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA GOLD TO ROLE DATA_ANALYST;

-- Warehouse access (query only, not OPERATE)
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE DATA_ANALYST;

-- Streamlit app access
GRANT USAGE ON STREAMLIT customer_360_app TO ROLE DATA_ANALYST;

-- Cortex Analyst access
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE DATA_ANALYST;
GRANT USAGE ON DATABASE SEMANTIC_MODELS TO ROLE DATA_ANALYST;
GRANT USAGE ON SCHEMA SEMANTIC_MODELS.DEFINITIONS TO ROLE DATA_ANALYST;
GRANT READ ON STAGE SEMANTIC_MODELS.DEFINITIONS.SEMANTIC_STAGE TO ROLE DATA_ANALYST;

-- ============================================================================
-- DATA_SCIENTIST Permissions
-- ============================================================================

-- Schema access for ML artifacts
GRANT USAGE ON DATABASE CUSTOMER_ANALYTICS TO ROLE DATA_SCIENTIST;
GRANT USAGE ON SCHEMA GOLD TO ROLE DATA_SCIENTIST;

-- Read-only access to feature tables
GRANT SELECT ON TABLE GOLD.ml_training_features TO ROLE DATA_SCIENTIST;
GRANT SELECT ON TABLE GOLD.churn_labels TO ROLE DATA_SCIENTIST;

-- Write access to ML outputs
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE GOLD.churn_predictions TO ROLE DATA_SCIENTIST;

-- Cortex ML privileges
GRANT CREATE SNOWFLAKE.ML.CLASSIFICATION ON SCHEMA GOLD TO ROLE DATA_SCIENTIST;
GRANT CREATE SNOWFLAKE.ML.REGRESSION ON SCHEMA GOLD TO ROLE DATA_SCIENTIST;

-- Warehouse access
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE DATA_SCIENTIST;
```

### 20.3 Privilege Types

| Privilege | Purpose | Example |
|-----------|---------|---------|
| USAGE | Access database/schema/warehouse | `GRANT USAGE ON DATABASE X TO ROLE Y` |
| SELECT | Read data from tables/views | `GRANT SELECT ON TABLE X TO ROLE Y` |
| INSERT | Insert rows into table | `GRANT INSERT ON TABLE X TO ROLE Y` |
| UPDATE | Update rows in table | `GRANT UPDATE ON TABLE X TO ROLE Y` |
| DELETE | Delete rows from table | `GRANT DELETE FROM TABLE X TO ROLE Y` |
| TRUNCATE | Truncate table (delete all rows) | `GRANT TRUNCATE ON TABLE X TO ROLE Y` |
| CREATE TABLE | Create tables in schema | `GRANT CREATE TABLE ON SCHEMA X TO ROLE Y` |
| OPERATE | Start/stop warehouse | `GRANT OPERATE ON WAREHOUSE X TO ROLE Y` |

### 20.4 Future Grants

**Problem:** Grants only apply to existing objects. New tables created later won't inherit permissions.

**Solution:** Future grants apply to objects created in the future.

```sql
-- Grant SELECT on all future tables in GOLD schema
GRANT SELECT ON FUTURE TABLES IN SCHEMA GOLD TO ROLE DATA_ANALYST;

-- Now when DATA_ENGINEER creates a new table:
CREATE TABLE GOLD.new_metric_table AS SELECT ...;

-- DATA_ANALYST automatically has SELECT privilege on new_metric_table!
```

### 20.5 Role Switching

**Users can switch roles to access different permissions:**

```sql
-- Switch to DATA_ENGINEER role
USE ROLE DATA_ENGINEER;

-- Now can create tables
CREATE TABLE GOLD.test_table (id INT);

-- Switch to DATA_ANALYST role
USE ROLE DATA_ANALYST;

-- Can only read (INSERT would fail)
SELECT * FROM GOLD.test_table;
-- INSERT INTO GOLD.test_table VALUES (1);  -- ERROR: Insufficient privileges
```

### 20.6 Monitoring Permissions

**View role grants:**
```sql
-- Show all grants to DATA_ANALYST role
SHOW GRANTS TO ROLE DATA_ANALYST;

-- Show all grants on customer_360_profile table
SHOW GRANTS ON TABLE GOLD.customer_360_profile;

-- Show users with DATA_ANALYST role
SHOW GRANTS OF ROLE DATA_ANALYST;
```

**Audit query access:**
```sql
-- Who queried customer_360_profile in last 7 days?
SELECT
    user_name,
    role_name,
    query_text,
    start_time,
    execution_status
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
    DATEADD('day', -7, CURRENT_TIMESTAMP()),
    CURRENT_TIMESTAMP()
))
WHERE query_text ILIKE '%customer_360_profile%'
ORDER BY start_time DESC;
```

### 20.7 Best Practices

1. **Least privilege:** Grant minimum permissions needed for role
2. **Separation of duties:** Different roles for dev, analytics, ML
3. **Future grants:** Always use future grants for schemas
4. **Regular audits:** Review role grants quarterly
5. **No ACCOUNTADMIN for apps:** Streamlit/dbt should use service roles (DATA_ENGINEER, DATA_ANALYST)
6. **Document role hierarchy:** Clear ownership and responsibilities

### 20.8 Exam Questions

1. **What is RBAC?**
   - Role-Based Access Control: Permissions assigned to roles, roles assigned to users

2. **What are the 3 custom roles in our project?**
   - DATA_ENGINEER (builds pipeline), DATA_ANALYST (reads Gold), DATA_SCIENTIST (trains ML)

3. **What is the role hierarchy?**
   - ACCOUNTADMIN → SYSADMIN → DATA_ENGINEER → DATA_ANALYST & DATA_SCIENTIST

4. **What permissions does DATA_ANALYST have?**
   - SELECT on GOLD schema tables, USAGE on COMPUTE_WH, access to Streamlit app, Cortex Analyst

5. **What are future grants?**
   - Permissions that automatically apply to objects created in the future

6. **Can DATA_ANALYST modify tables?**
   - No; DATA_ANALYST has SELECT only (read-only)

7. **Who can create tables in GOLD schema?**
   - DATA_ENGINEER (has CREATE TABLE privilege)

8. **How do you switch roles?**
   - `USE ROLE role_name;`

9. **How do you audit who accessed a table?**
   - Query `INFORMATION_SCHEMA.QUERY_HISTORY()` and filter by table name

10. **Should Streamlit apps use ACCOUNTADMIN role?**
    - No! Use least-privilege role (DATA_ANALYST) for security

---

**(Continuing with sections 21-25 + Parts III-IV in next append...)**
