# dbt Customer Analytics Project

## Overview

This dbt project transforms raw customer and transaction data from the Bronze layer into clean, analytical-ready datasets in the Silver and Gold layers following the medallion architecture.

**Database**: Snowflake
**dbt Version**: 1.x
**Package Manager**: UV (Python)

---

## NEW FEATURE 

This is a brand new feature - dbt natively in Snowflake!

  What This Feature Does:

  Snowflake now has a DBT PROJECT object that lets you:
  1. Deploy your dbt project directly into Snowflake (from Git or workspace)
  2. Execute dbt models natively in Snowflake (no local dbt installation needed)
  3. Schedule runs using Snowflake Tasks
  4. Version control your dbt project files in Snowflake

  How It Works:

  Step 1: Create a DBT PROJECT Object from Your Git Repo

  USE ROLE SYSADMIN;
  USE DATABASE CUSTOMER_ANALYTICS;
  USE SCHEMA GOLD;

  -- Create dbt project from Git repository
  CREATE OR REPLACE DBT PROJECT dbt_customer_analytics_project
    FROM '@snowflake-customer360-application-demo/branches/main/dbt_customer_analytics/';

  Step 2: Execute dbt Models in Snowflake

  -- Run all models
  EXECUTE DBT PROJECT dbt_customer_analytics_project;

  -- Or run specific models
  EXECUTE DBT PROJECT dbt_customer_analytics_project
    WITH COMMAND = 'run --select staging.*';

  -- Run tests
  EXECUTE DBT PROJECT dbt_customer_analytics_project
    WITH COMMAND = 'test';

  Step 3: (Optional) Schedule Automatic Runs

  -- Create a task to run dbt daily at 6am
  CREATE OR REPLACE TASK run_dbt_daily
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 6 * * * UTC'
  AS
    EXECUTE DBT PROJECT dbt_customer_analytics_project;

  -- Start the task
  ALTER TASK run_dbt_daily RESUME;

  Benefits vs Local dbt:

  | Feature            | Local dbt                      | dbt in Snowflake (Native)      |
  |--------------------|--------------------------------|--------------------------------|
  | Installation       | Need Python + dbt-core locally | ✅ No installation needed       |
  | Execution          | Run from your machine          | ✅ Runs in Snowflake            |
  | Scheduling         | Manual/cron/Airflow            | ✅ Snowflake Tasks (built-in)   |
  | Version control    | Git only                       | ✅ Git + Snowflake versioning   |
  | CI/CD              | GitHub Actions needed          | ✅ Native Snowflake integration |
  | Team collaboration | Share Git repo                 | ✅ Share DBT PROJECT object     |

  Should You Use This?

  Pros:
  - ✅ No need to run dbt run locally anymore
  - ✅ Easier scheduling (native Snowflake Tasks)
  - ✅ Team can execute from Snowflake UI
  - ✅ Integrated monitoring

  Cons:
  - ⚠️ Newer feature (may have quirks)
  - ⚠️ Less familiar if you're used to local dbt workflow

  ---
  Would you like to try setting up native dbt in Snowflake? I can help you:
  1. Create the DBT PROJECT object from your Git repository
  2. Test running it natively in Snowflake
  3. (Optional) Set up scheduled runs




## Project Structure

```
dbt_customer_analytics/
├── dbt_project.yml          # Project configuration
├── profiles.yml             # Snowflake connection profiles
├── packages.yml             # External package dependencies
├── README.md                # This file
│
├── models/
│   ├── staging/             # Silver layer: Cleaned and normalized data
│   │   ├── _staging_sources.yml
│   │   ├── stg_customers.sql
│   │   ├── stg_customers.yml
│   │   ├── stg_transactions.sql
│   │   └── stg_transactions.yml
│   │
│   ├── intermediate/        # Silver layer: Business logic transformations
│   │   └── (future iterations)
│   │
│   └── marts/               # Gold layer: Dimensional models and aggregates
│       └── customer/        # Customer-focused mart
│           └── (future iterations)
│
├── macros/                  # Reusable SQL functions
├── tests/                   # Custom data tests
├── seeds/                   # CSV reference data
├── snapshots/               # SCD Type 2 snapshots
└── analyses/                # Ad-hoc analyses

```

---

## Quick Start

### 1. Prerequisites

- **Python 3.10+** with UV package manager
- **Snowflake account** with appropriate permissions
- **Bronze layer loaded** (50K customers, 13.5M transactions)

### 2. Installation

```bash
# Navigate to dbt project directory
cd dbt_customer_analytics

# Install dbt dependencies
dbt deps

# Verify installation
dbt --version
```

### 3. Configure Snowflake Connection

Set environment variables for Snowflake authentication:

```bash
export SNOWFLAKE_ACCOUNT="your_account_identifier"
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_PASSWORD="your_password"
```

Or create a `.env` file in the project root:

```env
SNOWFLAKE_ACCOUNT=abc12345.us-east-1
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
```

### 4. Test Connection

```bash
# Test that dbt can connect to Snowflake
dbt debug
```

Expected output: `All checks passed!`

### 5. Run Models

```bash
# Run all models
dbt run

# Run only staging models
dbt run --select staging

# Run specific model
dbt run --select stg_customers
```

### 6. Test Models

```bash
# Run all tests
dbt test

# Test only staging models
dbt test --select staging

# Test specific model
dbt test --select stg_customers
```

### 7. Generate Documentation

```bash
# Generate documentation site
dbt docs generate

# Serve documentation locally
dbt docs serve
```

Open http://localhost:8080 to view the documentation.

---

## Models

### Staging Layer (Silver)

#### stg_customers

**Purpose**: Cleaned and normalized customer data
**Source**: BRONZE.BRONZE_CUSTOMERS
**Materialization**: View
**Row Count**: 50,000

**Transformations**:
- Normalize email to lowercase
- Normalize state to uppercase
- Trim whitespace from all text fields

**Tests**:
- Unique customer_id
- No NULL critical fields
- Credit limits within $5K-$50K range
- Valid email addresses

**Usage**:
```sql
SELECT * FROM {{ ref('stg_customers') }}
WHERE customer_segment = 'High-Value Travelers'
```

#### stg_transactions

**Purpose**: Cleaned, deduplicated transaction data
**Source**: BRONZE.BRONZE_TRANSACTIONS
**Materialization**: Incremental table
**Row Count**: ~13.5M

**Transformations**:
- Deduplicate transactions by transaction_id
- Default NULL merchant_category to 'Uncategorized'
- Trim whitespace from merchant_name

**Incremental Strategy**:
- **unique_key**: transaction_id
- **Incremental logic**: Process only new records by ingestion_timestamp
- **Performance**: Avoids full table scan on millions of rows

**Tests**:
- Unique transaction_id
- Referential integrity with stg_customers
- Positive transaction amounts
- No NULL merchant_category

**Usage**:
```sql
-- Full refresh (reprocess all data)
dbt run --select stg_transactions --full-refresh

-- Incremental (only new data)
dbt run --select stg_transactions
```

---

## Common Commands

### Development Workflow

```bash
# 1. Install dependencies
dbt deps

# 2. Run models
dbt run --select staging

# 3. Test models
dbt test --select staging

# 4. Generate docs
dbt docs generate
dbt docs serve
```

### Model Selection

```bash
# Run all models
dbt run

# Run specific model
dbt run --select stg_customers

# Run model and all downstream models
dbt run --select stg_customers+

# Run model and all upstream models
dbt run --select +stg_transactions

# Run models with specific tag
dbt run --select tag:staging
```

### Testing

```bash
# Run all tests
dbt test

# Test specific model
dbt test --select stg_customers

# Test only sources
dbt test --select source:*

# Store test failures for analysis
dbt test --store-failures
```

### Incremental Models

```bash
# Run incremental (default - only new data)
dbt run --select stg_transactions

# Full refresh (reprocess all data)
dbt run --select stg_transactions --full-refresh

# Full refresh all incremental models
dbt run --full-refresh
```

---

## Testing Strategy

### Source Tests

Defined in `_staging_sources.yml`:
- Column presence and types
- Not NULL constraints
- Accepted values
- Uniqueness checks

### Model Tests

Defined in model YAML files:
- **Generic tests**: unique, not_null, relationships, accepted_values
- **dbt_utils tests**: accepted_range, expression_is_true, not_empty_string
- **Custom tests**: Business logic validations

### Test Execution

Tests run automatically with `dbt test` or can be run selectively:

```bash
# Test all models
dbt test

# Test specific model
dbt test --select stg_customers

# Test only relationships
dbt test --select test_type:relationships
```

---

## Observability

### Pipeline Run Metadata

dbt automatically logs run metadata to `OBSERVABILITY.PIPELINE_RUN_METADATA`:

```sql
SELECT
    run_id,
    run_timestamp,
    status,
    models_run,
    models_passed,
    models_failed
FROM CUSTOMER_ANALYTICS.OBSERVABILITY.PIPELINE_RUN_METADATA
ORDER BY run_timestamp DESC
LIMIT 10;
```

### Hooks

**on-run-start**: Logs dbt run initiation
**on-run-end**: Updates run status and model counts

---

## Troubleshooting

### Issue 1: "Database Error - Connection Failed"

**Solution**: Check environment variables and Snowflake credentials

```bash
dbt debug
# Look for connection errors
```

### Issue 2: "Compilation Error"

**Solution**: Check model SQL syntax

```bash
dbt compile --select problematic_model
# Review compiled SQL in target/ directory
```

### Issue 3: "Test Failures"

**Solution**: Review test results and investigate data

```bash
# Run tests with verbose output
dbt test --select stg_customers --store-failures

# Query failed test data
SELECT * FROM CUSTOMER_ANALYTICS.TEST_FAILURES.unique_stg_customers_customer_id;
```

### Issue 4: "Incremental Model Not Processing New Data"

**Solution**: Check incremental logic or force full refresh

```bash
# Full refresh to reprocess all data
dbt run --select stg_transactions --full-refresh
```

---

## Best Practices

### 1. Always Test After Changes

```bash
dbt run --select modified_model
dbt test --select modified_model
```

### 2. Use Tags for Organization

```sql
{{
    config(
        tags=['staging', 'customers', 'critical']
    )
}}
```

### 3. Document Models Thoroughly

Add descriptions in YAML files for all models and columns.

### 4. Use Incremental Wisely

Only use incremental materialization for large tables (>1M rows).

### 5. Monitor Test Failures

Set up alerting for test failures in production:

```bash
dbt test --store-failures
# Check TEST_FAILURES schema regularly
```

---

## Resources

- **dbt Documentation**: https://docs.getdbt.com/
- **dbt Utils Package**: https://hub.getdbt.com/dbt-labs/dbt_utils/latest/
- **dbt Slack Community**: https://www.getdbt.com/community/
- **Snowflake dbt Guide**: https://docs.snowflake.com/en/user-guide/dbt

---

## Project Roadmap

### Phase 1: Silver Layer ✅ (Current)
- [x] dbt project setup
- [x] Source definitions
- [x] Staging models (stg_customers, stg_transactions)
- [x] Data quality tests

### Phase 2: Intermediate Models (Next)
- [ ] Customer transaction aggregates
- [ ] Rolling window calculations
- [ ] Behavioral segmentation refinement

### Phase 3: Gold Layer (Future)
- [ ] dim_customer (SCD Type 2)
- [ ] fct_transactions
- [ ] customer_360_profile mart
- [ ] Churn prediction features

### Phase 4: Advanced Analytics (Future)
- [ ] Cortex ML integration
- [ ] Semantic layer
- [ ] Streamlit dashboard

---

## Support

For issues or questions:
1. Review dbt documentation: https://docs.getdbt.com/
2. Check project issues in GitHub
3. Contact data engineering team

---

## License

[Add license information]
