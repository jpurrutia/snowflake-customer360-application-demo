# Data Flow Architecture

## Complete End-to-End Pipeline

This document describes the complete data flow for the Customer 360 Analytics Platform, including both automated (Snowflake-native) and manual (local fallback) execution paths.

---

## Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                      PRIMARY PATH: Snowflake Native                 │
│                    (Automated Task Orchestration)                   │
└─────────────────────────────────────────────────────────────────────┘

    Stored Procedure (Python) → SQL Generation → DBT PROJECT → ML Model → Streamlit
            ↓                         ↓              ↓            ↓
    Orchestrated by Snowflake Tasks (automated pipeline + Streams for incremental)


┌─────────────────────────────────────────────────────────────────────┐
│                    FALLBACK PATH: Local Execution                    │
│                        (One Command: make run-all)                   │
└─────────────────────────────────────────────────────────────────────┘

    Python CLI → S3 Upload → Snowflake Load → SQL Generation → dbt CLI → Validation
```

---

## Detailed Data Flow

### Phase 1: Data Generation

#### 1.1 Customer Generation

**Snowflake Native** (Primary):
```sql
-- Stored procedure generates customers directly in Snowflake
CALL BRONZE.GENERATE_CUSTOMERS(50000, 42);
```

**Local Fallback**:
```bash
# Generate CSV locally
make generate-customers

# Upload to S3
make upload-customers BUCKET=your-bucket

# Load into Snowflake
make load-customers
```

**Output**: `BRONZE.BRONZE_CUSTOMERS` (50,000 rows)

**Schema**:
- customer_id, first_name, last_name, email
- age, state, city, employment_status
- card_type, credit_limit, account_open_date
- customer_segment, decline_type

**Segments**:
- High-Value Travelers (15%)
- Stable Mid-Spenders (40%)
- Budget-Conscious (25%)
- Declining (10%)
- New & Growing (10%)

---

#### 1.2 Transaction Generation

**Snowflake Native** (Primary):
```sql
-- SQL script generates transactions in Snowflake
EXECUTE IMMEDIATE FROM @snowflake_panel_demo_repo/branches/main/snowflake/data_generation/generate_transactions.sql;
```

**Local Fallback**:
```bash
make generate-transactions
```

**Output**: `BRONZE.BRONZE_TRANSACTIONS` (10M-17M rows, ~13.5M average)

**Schema**:
- transaction_id, customer_id, transaction_date
- transaction_amount, merchant_name, merchant_category
- channel (Online/In-Store/Mobile), status (approved/declined)

**Time Range**: 18 months of historical data

---

### Phase 2: Data Transformation (dbt)

#### 2.1 dbt Native Execution

**Snowflake Native** (Primary):
```sql
-- Execute dbt project natively in Snowflake
EXECUTE DBT PROJECT customer_analytics_dbt
  COMMAND = 'run'
  WAREHOUSE = COMPUTE_WH;
```

**Local Fallback**:
```bash
make run-dbt
```

**Medallion Architecture**:

**Bronze Layer** (Raw):
- `BRONZE_CUSTOMERS` - Raw customer data
- `BRONZE_TRANSACTIONS` - Raw transaction data

**Silver Layer** (Cleaned/Conformed):
- Built by dbt models (intermediate transformations)

**Gold Layer** (Business Logic):
- `DIM_CUSTOMER` - Customer dimension (SCD Type 2)
- `DIM_DATE` - Date dimension (580 days)
- `DIM_MERCHANT_CATEGORY` - Merchant categories
- `FCT_TRANSACTIONS` - Fact table (clustered by date)
- `CUSTOMER_SEGMENTS` - Rolling 90-day metrics by segment
- `CUSTOMER_360_PROFILE` - Denormalized customer view
- `METRIC_CUSTOMER_LTV` - Customer lifetime value
- `METRIC_MOM_SPEND_CHANGE` - Month-over-month trends
- `METRIC_AVG_TRANSACTION_VALUE` - Average transaction metrics

---

### Phase 3: ML Model Training

#### 3.1 Churn Prediction Model

**Snowflake Native** (Primary):
```sql
-- Train churn model using Snowflake ML
EXECUTE IMMEDIATE FROM @snowflake_panel_demo_repo/branches/main/snowflake/ml/03_train_churn_model.sql;
```

**Local Fallback**:
```bash
snowsql -c default -f snowflake/ml/03_train_churn_model.sql
```

**Output**: Churn predictions in `GOLD.CHURN_PREDICTIONS`

**Model Features**:
- Transaction frequency and recency
- Average transaction amount
- Spending trend (30/60/90 days)
- Customer segment
- Card type and credit utilization

---

### Phase 4: Application Layer

#### 4.1 Streamlit Dashboard

**Deployment**: GitHub Actions automatically deploys on push to main

**Features**:
- **Overview Tab**: High-level KPIs and trends
- **Segments Tab**: Customer segment analysis
- **Transactions Tab**: Transaction patterns and categories
- **Churn Tab**: At-risk customer identification

**Access**: Via Snowflake UI (Streamlit in Snowflake)

---

### Phase 5: Collaboration & Sharing

#### 5.1 Secure Data Sharing

**Purpose**: Share analytics with partners/teams without exposing raw data

```sql
-- Create secure share
CREATE SHARE customer_360_insights;
GRANT USAGE ON DATABASE CUSTOMER_ANALYTICS TO SHARE customer_360_insights;
GRANT USAGE ON SCHEMA GOLD TO SHARE customer_360_insights;
GRANT SELECT ON VIEW GOLD.customer_360_profile TO SHARE customer_360_insights;
ALTER SHARE customer_360_insights ADD ACCOUNTS = <consumer_account>;
```

---

## Orchestration

### Automated Task DAG (Snowflake Native)

```
generate_customer_data (Task 1)
  Schedule: Weekly Sunday 2AM UTC
  Action: CALL BRONZE.GENERATE_CUSTOMERS(50000, 42)
        ↓
generate_transaction_data (Task 2)
  Dependency: AFTER generate_customer_data
  Action: Execute generate_transactions.sql
        ↓
run_dbt_transformations (Task 3)
  Dependency: AFTER generate_transaction_data
  Action: EXECUTE DBT PROJECT customer_analytics_dbt COMMAND = 'run'
        ↓
train_churn_model (Task 4)
  Dependency: AFTER run_dbt_transformations
  Action: Execute ML training script
        ↓
refresh_analytics_views (Task 5)
  Dependency: AFTER train_churn_model
  Action: Refresh materialized views and update metadata
```

### Incremental Processing (Streams)

```
bronze_transactions_stream (Stream)
  Source: BRONZE.BRONZE_TRANSACTIONS
  Purpose: Track new transactions
        ↓
process_incremental_transactions (Task)
  Schedule: Every 5 minutes
  Condition: WHEN SYSTEM$STREAM_HAS_DATA()
  Action: MERGE into GOLD.fct_transactions
```

---

## Execution Methods

### Method 1: Snowflake Native (Automated)

**Deploy once, runs automatically**:

```sql
-- 1. Deploy stored procedure
\i snowflake/procedures/generate_customers.sql

-- 2. Deploy dbt project
\i snowflake/dbt/deploy_dbt_project.sql

-- 3. Create and start tasks
\i snowflake/orchestration/pipeline_tasks.sql
```

**Manual trigger** (for testing):
```sql
EXECUTE TASK BRONZE.generate_customer_data;
```

**Monitor execution**:
```sql
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP())
))
WHERE database_name = 'CUSTOMER_ANALYTICS'
ORDER BY scheduled_time DESC;
```

---

### Method 2: Local Fallback (One Command)

**Complete pipeline**:
```bash
make run-all BUCKET=snowflake-customer-analytics-data-demo
```

**Individual steps**:
```bash
# Step-by-step execution
make generate-customers CUSTOMER_COUNT=50000 SEED=42
make upload-customers BUCKET=your-bucket
make load-customers
make generate-transactions
make run-dbt
make validate-data
```

**EDA workflow**:
```bash
# Run complete EDA workflow
make run-full-eda

# Or individual EDA steps
make run-baseline-eda          # Before generation
make generate-transactions     # Generate data
make validate-data             # Validate quality
make run-delta-analysis        # Compare before/after
```

**Test with small dataset**:
```bash
make test-pipeline  # Uses 1,000 customers for quick testing
```

---

## Data Quality & Monitoring

### Baseline Metrics
```sql
-- Capture state before generation
\i snowflake/eda/01_baseline_metrics.sql
\i snowflake/eda/02_pre_generation_eda.sql
```

### Post-Generation Validation
```sql
-- Comprehensive validation (12 checks)
\i snowflake/eda/03_post_generation_validation.sql
```

**Checks include**:
- Row count (10M-17M expected)
- Unique transaction IDs
- NULL value validation (8 fields)
- Customer representation (all 50K)
- Referential integrity
- Date range (17-19 months)
- Transaction amounts (positive, <$10K)
- Status distribution (~97% approved)
- Channel distribution
- Merchant category distribution
- Segment-specific patterns
- Monthly trends

### Delta Analysis
```sql
-- Compare before/after metrics
\i snowflake/eda/04_delta_analysis.sql
```

### Telemetry Tracking
```sql
-- Set up ongoing monitoring
\i snowflake/eda/05_telemetry_tracking.sql
```

**Creates**:
- `generation_telemetry` - Execution metrics
- `data_quality_telemetry` - Quality check results
- `segment_telemetry` - Segment performance snapshots
- `v_data_quality_dashboard` - Quality summary view
- `v_segment_performance` - Performance trends view

---

## Panel Demo Strategy

### Primary Demo Path (Snowflake Native)

**Show automated orchestration**:
1. Display task DAG: `SHOW TASKS;`
2. Trigger pipeline: `EXECUTE TASK BRONZE.generate_customer_data;`
3. Monitor execution: Query `TASK_HISTORY()`
4. Show dbt native: `SHOW DBT PROJECTS;`
5. View results in Streamlit app
6. Demonstrate secure share

**Key Talking Points**:
- "Fully automated with Snowflake Tasks"
- "dbt runs natively - no external orchestration needed"
- "Streams enable real-time incremental processing"
- "Stored procedure generates data with Python in Snowflake"

---

### Fallback Demo Path (If Issues)

**Show resilience with local fallback**:
```bash
# Open terminal, show single command
make run-all BUCKET=snowflake-customer-analytics-data-demo
```

**Key Talking Points**:
- "Production systems need resilience"
- "Local fallback for CI/CD or debugging"
- "Same business logic, different execution context"
- "Demonstrates portability between cloud and local"

---

## Performance Characteristics

### Data Volumes
- **Customers**: 50,000 rows (~5 MB)
- **Transactions**: 10M-17M rows (~13.5M average, ~2 GB)
- **Gold Layer Tables**: 6 primary tables + metrics

### Execution Times (COMPUTE_WH - Small)
- **Customer Generation**: 30-60 seconds
- **Transaction Generation**: 5-15 minutes
- **dbt Transformations**: 2-5 minutes
- **ML Model Training**: 1-3 minutes
- **Total Pipeline**: 10-25 minutes

### Cost Estimates
- **Customer Generation**: ~0.01 credits
- **Transaction Generation**: ~0.2-0.4 credits
- **dbt Transformations**: ~0.05-0.1 credits
- **ML Training**: ~0.02-0.05 credits
- **Total per Run**: ~0.3-0.6 credits ($0.90-$1.80 on Standard tier)

---

## Troubleshooting

### Common Issues

**Issue**: Task execution fails
```sql
-- Check error details
SELECT error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE state = 'FAILED'
ORDER BY scheduled_time DESC
LIMIT 5;
```

**Issue**: dbt compilation error
```sql
-- View dbt execution logs
SELECT query_text, error_message
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE query_text ILIKE '%customer_analytics_dbt%'
AND execution_status = 'FAIL'
ORDER BY start_time DESC;
```

**Issue**: Stream not processing
```sql
-- Check if stream has data
SELECT SYSTEM$STREAM_HAS_DATA('bronze_transactions_stream');

-- View stream contents
SELECT * FROM bronze_transactions_stream LIMIT 10;
```

**Fallback**: Use local execution
```bash
make run-all BUCKET=your-bucket
```

---

## Next Steps

### After Successful Pipeline Run

1. **Verify Data Quality**:
   ```bash
   make validate-data
   make run-delta-analysis
   ```

2. **View Results**:
   - Open Streamlit app in Snowflake UI
   - Query Gold layer tables
   - Check telemetry dashboards

3. **Share Results**:
   ```sql
   -- Create secure share
   \i snowflake/collaboration/create_share.sql
   ```

4. **Monitor Performance**:
   ```sql
   -- Query warehouse usage
   SELECT * FROM v_data_quality_dashboard;
   SELECT * FROM v_segment_performance;
   ```

---

## References

- **Stored Procedures**: `snowflake/procedures/generate_customers.sql`
- **dbt Deployment**: `snowflake/dbt/deploy_dbt_project.sql`
- **Task Orchestration**: `snowflake/orchestration/pipeline_tasks.sql`
- **Makefile**: `Makefile` (run-all target)
- **EDA Scripts**: `snowflake/eda/*.sql`
- **Panel Criteria**: `EVALUATION_CRITERIA.md`

---

**Last Updated**: 2025-01-13
