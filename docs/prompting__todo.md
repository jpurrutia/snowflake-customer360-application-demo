# Snowflake Customer 360 Analytics Platform - Implementation Checklist

## Project Overview
Building a complete Customer 360 Analytics Platform with:
- 50,000 synthetic customers
- 13.5 million transactions
- Medallion architecture (Bronze/Silver/Gold)
- ML-powered churn prediction
- Interactive Streamlit dashboard
- Natural language query interface

---

## Phase 1: Foundation & Infrastructure Setup ✅

### Prompt 1.1: Project Structure & Configuration ✅
- [x] Create project root directory structure
- [x] Set up virtual environment (Python 3.12 with UV)
- [x] Create `.gitignore` with comprehensive entries
- [x] Create `requirements.txt` with all dependencies
  - [x] snowflake-connector-python
  - [x] faker
  - [x] boto3
  - [x] pytest
  - [x] pyyaml
  - [x] dbt-snowflake
  - [x] streamlit-in-snowflake
- [x] Create `Makefile` with common commands
  - [x] `make install`
  - [x] `make test`
  - [x] `make format`
  - [x] `make lint`
- [x] Create directory structure:
  - [x] `data_generation/`
  - [x] `terraform/`
  - [x] `snowflake/` (sql/)
  - [x] `dbt/`
  - [x] `streamlit/`
  - [x] `tests/`
  - [x] `docs/`
- [x] Create `README.md` with setup instructions
- [x] Write `tests/test_project_structure.py`
- [x] Run tests to validate structure (11/11 passed)
- [ ] Initialize git repository
- [ ] Create initial commit

### Prompt 1.2: AWS Infrastructure with Terraform ✅
- [x] Create `terraform/main.tf` with AWS provider config
- [x] Create `terraform/variables.tf` with all input variables
  - [x] AWS region
  - [x] S3 bucket name
  - [x] Snowflake account ID
  - [x] Snowflake external ID
  - [x] Environment tags
- [x] Create `terraform/s3.tf` for data lake bucket
  - [x] Versioning enabled
  - [x] Server-side encryption
  - [x] Lifecycle policies
  - [x] Folder structure (customers/, transactions/)
- [x] Create `terraform/iam.tf` for Snowflake access
  - [x] IAM role for Snowflake
  - [x] Trust policy for external ID
  - [x] S3 read/list permissions
- [x] Create `terraform/outputs.tf` to export:
  - [x] S3 bucket ARN
  - [x] IAM role ARN
  - [x] S3 bucket name
- [x] Run `terraform init`
- [x] Run `terraform validate`
- [x] Run `terraform plan`
- [x] Run `terraform apply` (infrastructure deployed)
- [x] Write `tests/unit/test_terraform_variables.py` (15/15 tests passed)
- [x] Write `tests/integration/test_terraform_config.sh` (8/8 tests passed)
- [x] Run tests to validate Terraform configs
- [x] Document Terraform module in `terraform/README.md`

### Prompt 1.3: Snowflake Foundation Setup ✅
- [x] Create `snowflake/setup/00_environment_check.sql`
  - [x] Check current environment (account, user, role, warehouse)
  - [x] Verify ACCOUNTADMIN privileges
- [x] Create `snowflake/setup/01_create_database_schemas.sql`
  - [x] Create CUSTOMER_ANALYTICS database
  - [x] Create BRONZE schema
  - [x] Create SILVER schema
  - [x] Create GOLD schema
  - [x] Create OBSERVABILITY schema
- [x] Create `snowflake/setup/02_create_roles_grants.sql`
  - [x] Create DATA_ENGINEER role (full access)
  - [x] Create MARKETING_MANAGER role (GOLD read-only)
  - [x] Create DATA_ANALYST role (all schemas read-only)
  - [x] Grant appropriate privileges to each role
  - [x] Grant warehouse USAGE to all roles
- [x] Create `snowflake/setup/03_create_observability_tables.sql`
  - [x] Create PIPELINE_RUN_METADATA table
  - [x] Create DATA_QUALITY_METRICS table
  - [x] Create LAYER_RECORD_COUNTS table
  - [x] Create MODEL_EXECUTION_LOG table
  - [x] Create V_LATEST_PIPELINE_RUNS view
  - [x] Create V_RECENT_DQ_FAILURES view
  - [x] Create V_RECORD_COUNT_TRENDS view
- [x] Create deployment script `snowflake/run_setup.sh`
- [x] Execute all foundation SQL scripts (00-03) in Snowflake
- [x] Write `tests/unit/test_sql_syntax.py` (32/35 tests passed - 3 regex issues only)
- [x] Write `tests/integration/test_snowflake_setup.py` (20 tests)
- [x] Run foundation tests
- [x] Document foundation in `snowflake/README.md`

---

## Phase 2: Data Generation & Ingestion

### Prompt 2.1: Customer Data Generator (Python)
- [ ] Create `data_generation/customer_generator.py`
- [ ] Implement `generate_customers(n, seed)` function
  - [ ] Generate customer_id (C000001-C050000)
  - [ ] Generate realistic names with Faker
  - [ ] Generate email addresses
  - [ ] Generate phone numbers (US format)
  - [ ] Generate dates of birth (18-80 years old)
  - [ ] Generate account_open_date (2020-2024)
  - [ ] Generate credit_limit ($500-$50,000)
  - [ ] Generate card_type (Platinum/Gold/Silver/Basic) based on credit_limit
  - [ ] Generate segment labels (5 segments with realistic distribution)
  - [ ] Generate city, state, zip_code
- [ ] Implement distribution logic:
  - [ ] High-Value Travelers: 15%
  - [ ] Stable Mid-Spenders: 35%
  - [ ] Budget-Conscious: 25%
  - [ ] Declining: 15%
  - [ ] New & Growing: 10%
- [ ] Implement `save_to_csv(customers, filepath)` function
- [ ] Create `tests/test_customer_generator.py`
  - [ ] Test customer count (50,000)
  - [ ] Test unique customer_ids
  - [ ] Test email format validation
  - [ ] Test credit_limit ranges
  - [ ] Test segment distribution
  - [ ] Test date ranges
  - [ ] Test deterministic output with seed
- [ ] Run customer generator tests
- [ ] Generate actual customer file: `data/customers.csv`
- [ ] Validate CSV file size and format
- [ ] Document generator in `docs/data_generation.md`

### Prompt 2.2: S3 Integration & Upload
- [ ] Apply Terraform infrastructure: `terraform apply`
- [ ] Verify S3 bucket creation in AWS console
- [ ] Verify IAM role creation
- [ ] Create `sql/integration/01_storage_integration.sql`
  - [ ] Create storage integration with IAM role ARN
  - [ ] Grant USAGE to DATA_ENGINEER role
- [ ] Execute storage integration SQL
- [ ] Retrieve storage integration details: `DESC STORAGE INTEGRATION`
- [ ] Update IAM trust policy with Snowflake user/external ID
- [ ] Create `scripts/upload_to_s3.py`
  - [ ] Implement S3 upload with boto3
  - [ ] Add retry logic (3 retries with exponential backoff)
  - [ ] Add progress tracking
  - [ ] Add error handling
  - [ ] Add validation after upload
- [ ] Upload `customers.csv` to `s3://bucket/customers/`
- [ ] Write `tests/test_s3_integration.py`
  - [ ] Test S3 connectivity
  - [ ] Test file upload
  - [ ] Test file existence validation
  - [ ] Test Snowflake storage integration
- [ ] Run S3 integration tests
- [ ] Verify file in S3 console
- [ ] Document integration in `docs/s3_integration.md`

### Prompt 2.3: Bronze Layer - Customer Bulk Load
- [ ] Create `sql/bronze/01_bronze_customers.sql`
  - [ ] Create BRONZE.BRONZE_CUSTOMERS table
  - [ ] Include all customer columns
  - [ ] Add metadata columns (_loaded_at, _source_file)
- [ ] Execute table creation SQL
- [ ] Create `sql/bronze/02_load_customers.sql`
  - [ ] Implement COPY INTO from S3
  - [ ] Use CSV_FORMAT file format
  - [ ] Add ON_ERROR = CONTINUE
  - [ ] Add validation queries
- [ ] Execute customer load SQL
- [ ] Validate load results:
  - [ ] Count rows (should be 50,000)
  - [ ] Check for NULL values in required fields
  - [ ] Validate data types
  - [ ] Check duplicate customer_ids
- [ ] Create `sql/bronze/03_validate_customers.sql` with validation queries
- [ ] Write `tests/test_bronze_customers.py`
  - [ ] Test row count
  - [ ] Test data completeness
  - [ ] Test data quality
  - [ ] Test load metadata
- [ ] Run Bronze customer tests
- [ ] Log load metrics to OBSERVABILITY.PIPELINE_RUNS
- [ ] Document Bronze layer in `docs/bronze_layer.md`

### Prompt 2.4: Transaction Data Generator (Snowflake SQL)
- [ ] Create `sql/bronze/04_generate_transactions.sql`
- [ ] Implement transaction generation using GENERATOR()
  - [ ] Target 13.5 million transactions
  - [ ] Date range: 2020-01-01 to 2024-12-31
  - [ ] Transaction_id: T00000000001 - T00013500000
- [ ] Implement segment-specific spending patterns:
  - [ ] High-Value Travelers: $75-$2,500, frequent
  - [ ] Stable Mid-Spenders: $15-$300, consistent
  - [ ] Budget-Conscious: $5-$100, frequent small purchases
  - [ ] Declining: Start high, decrease 40-60% over time
  - [ ] New & Growing: Start low, increase over time
- [ ] Implement merchant category logic (20 categories)
- [ ] Implement decline trajectory patterns:
  - [ ] Gradual decline (70% of declining segment)
  - [ ] Sudden drop (30% of declining segment)
- [ ] Add transaction status (approved/declined, 3% decline rate)
- [ ] Add realistic transaction amounts with segment correlation
- [ ] Create validation queries:
  - [ ] Total transaction count
  - [ ] Transactions per customer distribution
  - [ ] Amount distribution by segment
  - [ ] Date range coverage
  - [ ] Merchant category distribution
- [ ] Document transaction generation logic in `docs/transaction_generation.md`

### Prompt 2.5: Bronze Layer - Transaction Bulk Load
- [ ] Create `sql/bronze/05_bronze_transactions.sql`
  - [ ] Create BRONZE.BRONZE_TRANSACTIONS table
  - [ ] Include transaction_id, customer_id, transaction_date, amount, merchant_category, status
  - [ ] Add metadata columns (_loaded_at, _batch_id)
  - [ ] Add clustering on (transaction_date, customer_id)
- [ ] Execute transaction table creation
- [ ] Execute transaction generation SQL from Prompt 2.4
- [ ] Insert generated transactions into BRONZE_TRANSACTIONS
- [ ] Validate transaction load:
  - [ ] Count rows (should be ~13.5M)
  - [ ] Check date ranges
  - [ ] Validate amount ranges by segment
  - [ ] Check foreign key integrity (customer_id exists)
  - [ ] Validate merchant category values
- [ ] Create `sql/bronze/06_validate_transactions.sql`
- [ ] Write `tests/test_bronze_transactions.py`
  - [ ] Test row count (13M - 14M range acceptable)
  - [ ] Test data completeness
  - [ ] Test amount distributions
  - [ ] Test date coverage
  - [ ] Test clustering effectiveness
- [ ] Run Bronze transaction tests
- [ ] Log load metrics to OBSERVABILITY.PIPELINE_RUNS
- [ ] Measure query performance on clustered table
- [ ] Document transaction load in `docs/bronze_layer.md`

---

## Phase 3: dbt Transformations (Silver/Gold Layers)

### Prompt 3.1: dbt Setup & Silver Layer Staging
- [ ] Initialize dbt project: `dbt init customer360`
- [ ] Create `dbt/dbt_project.yml`
  - [ ] Configure project name and version
  - [ ] Set target database and schemas
  - [ ] Configure materialization defaults
  - [ ] Add on-run-start/end hooks for observability
- [ ] Create `dbt/profiles.yml` for Snowflake connection
- [ ] Create `dbt/models/staging/schema.yml`
- [ ] Create `dbt/models/staging/stg_customers.sql`
  - [ ] Source from BRONZE.BRONZE_CUSTOMERS
  - [ ] Implement deduplication logic (latest _loaded_at)
  - [ ] Standardize column names (snake_case)
  - [ ] Add data type casting
  - [ ] Add basic data quality checks
- [ ] Create `dbt/models/staging/stg_transactions.sql`
  - [ ] Source from BRONZE.BRONZE_TRANSACTIONS
  - [ ] Implement deduplication logic
  - [ ] Filter out declined transactions (status = 'approved')
  - [ ] Add date/time parsing
  - [ ] Add amount validation (> 0)
- [ ] Create `dbt/models/staging/sources.yml`
  - [ ] Define Bronze schema sources
  - [ ] Add source freshness checks
- [ ] Add dbt tests to schema.yml:
  - [ ] unique tests on primary keys
  - [ ] not_null tests on required fields
  - [ ] relationships tests (transactions -> customers)
  - [ ] accepted_values tests for categorical fields
- [ ] Run `dbt deps` to install packages
- [ ] Run `dbt debug` to validate connection
- [ ] Run `dbt run --models staging.*`
- [ ] Run `dbt test --models staging.*`
- [ ] Verify Silver staging tables created
- [ ] Document dbt setup in `docs/dbt_setup.md`

### Prompt 3.2: Gold Layer - Customer Dimension (SCD Type 2)
- [ ] Create `dbt/models/gold/dim_customer.sql`
- [ ] Implement SCD Type 2 logic for:
  - [ ] card_type changes
  - [ ] credit_limit changes (>10% change triggers new record)
- [ ] Add SCD columns:
  - [ ] effective_date (record start date)
  - [ ] end_date (record end date, NULL for current)
  - [ ] is_current (boolean flag)
  - [ ] row_hash (for change detection)
- [ ] Include all customer attributes:
  - [ ] customer_id (business key)
  - [ ] customer_sk (surrogate key)
  - [ ] name, email, phone
  - [ ] date_of_birth, age_at_effective_date
  - [ ] account_open_date
  - [ ] card_type, credit_limit (SCD tracked)
  - [ ] segment
  - [ ] city, state, zip_code
- [ ] Implement incremental materialization strategy
- [ ] Create `dbt/models/gold/schema.yml` for dim_customer
- [ ] Add custom tests:
  - [ ] Test SCD integrity (no overlapping date ranges per customer)
  - [ ] Test exactly one is_current = TRUE per customer_id
  - [ ] Test row_hash changes trigger new records
  - [ ] Test no gaps in effective/end dates
- [ ] Create `dbt/macros/test_scd_integrity.sql` custom test macro
- [ ] Run `dbt run --models dim_customer`
- [ ] Run `dbt test --models dim_customer`
- [ ] Validate SCD Type 2 logic:
  - [ ] Query customers with multiple records
  - [ ] Verify date ranges
  - [ ] Verify is_current flags
- [ ] Document SCD implementation in `docs/scd_type2.md`

### Prompt 3.3: Gold Layer - Fact Table & Dimensions
- [ ] Create `dbt/models/gold/dim_date.sql`
  - [ ] Generate date dimension (2020-2025)
  - [ ] Include date_key, full_date, year, quarter, month, week, day
  - [ ] Add fiscal period attributes
  - [ ] Add holiday flags
  - [ ] Add weekend/weekday flags
- [ ] Create `dbt/models/gold/dim_merchant_category.sql`
  - [ ] merchant_category_key (surrogate key)
  - [ ] merchant_category_name
  - [ ] category_group (consolidate into broader groups)
  - [ ] is_discretionary flag
- [ ] Create `dbt/models/gold/fact_transaction.sql`
  - [ ] transaction_key (surrogate key)
  - [ ] transaction_id (business key)
  - [ ] customer_sk (foreign key to dim_customer)
  - [ ] date_key (foreign key to dim_date)
  - [ ] merchant_category_key (foreign key to dim_merchant_category)
  - [ ] transaction_date, transaction_timestamp
  - [ ] transaction_amount
  - [ ] Add clustering on (date_key, customer_sk)
- [ ] Implement SCD lookup for customer dimension
  - [ ] Join based on customer_id and transaction_date
  - [ ] Match to correct historical customer record
- [ ] Update `dbt/models/gold/schema.yml` for all dimensions and fact
- [ ] Add referential integrity tests:
  - [ ] fact_transaction.customer_sk exists in dim_customer
  - [ ] fact_transaction.date_key exists in dim_date
  - [ ] fact_transaction.merchant_category_key exists in dim_merchant_category
- [ ] Add data quality tests:
  - [ ] transaction_amount > 0
  - [ ] No future-dated transactions
  - [ ] No orphaned foreign keys
- [ ] Run `dbt run --models gold.dim_date gold.dim_merchant_category gold.fact_transaction`
- [ ] Run `dbt test --models gold.*`
- [ ] Validate star schema:
  - [ ] Query fact table joined to all dimensions
  - [ ] Check query performance (<2 seconds for typical queries)
  - [ ] Verify row counts (fact should have ~13M rows)
- [ ] Document star schema in `docs/star_schema.md`

### Prompt 3.4: Customer Segmentation Model
- [ ] Create `dbt/models/gold/customer_segments.sql`
- [ ] Implement rolling 90-day window logic:
  - [ ] Total spend (last 90 days)
  - [ ] Transaction frequency (last 90 days)
  - [ ] Average transaction value (last 90 days)
  - [ ] Month-over-month spend change
  - [ ] Days since last transaction
  - [ ] Travel-related transaction percentage
- [ ] Implement segmentation rules:
  - [ ] **High-Value Travelers**: Total spend > $5,000 AND travel % > 30% AND frequency > 20
  - [ ] **Stable Mid-Spenders**: Total spend $1,000-$5,000 AND frequency 10-30 AND MoM change -10% to +10%
  - [ ] **Budget-Conscious**: Average transaction < $50 AND frequency > 15
  - [ ] **Declining**: MoM change < -20% AND days_since_last > 45
  - [ ] **New & Growing**: Account age < 12 months AND MoM change > 20%
- [ ] Add segment assignment logic with priority order
- [ ] Add segment assignment date and reason
- [ ] Implement as incremental model
- [ ] Create `dbt/macros/recalculate_segments.sql`
  - [ ] Macro to force full refresh of segments
  - [ ] Used for monthly segment updates
- [ ] Update `dbt/models/gold/schema.yml` for customer_segments
- [ ] Add tests:
  - [ ] Every customer has exactly one segment
  - [ ] Segment counts match expected distributions (~15%/35%/25%/15%/10%)
  - [ ] No NULL segment values
  - [ ] Rolling metrics calculated correctly
- [ ] Run `dbt run --models customer_segments`
- [ ] Run `dbt test --models customer_segments`
- [ ] Validate segment distribution:
  - [ ] Query segment counts
  - [ ] Sample customers from each segment
  - [ ] Verify business rules applied correctly
- [ ] Document segmentation logic in `docs/customer_segmentation.md`

### Prompt 3.5: Aggregate Marts & Customer 360 Profile
- [ ] Create `dbt/models/marts/metric_customer_ltv.sql`
  - [ ] customer_id
  - [ ] total_lifetime_value (all-time spend)
  - [ ] ltv_last_90_days
  - [ ] ltv_last_365_days
  - [ ] projected_12m_ltv (based on trends)
  - [ ] customer_tenure_days
  - [ ] average_monthly_spend
- [ ] Create `dbt/models/marts/metric_mom_spend_change.sql`
  - [ ] customer_id
  - [ ] current_month_spend
  - [ ] previous_month_spend
  - [ ] mom_absolute_change
  - [ ] mom_percent_change
  - [ ] three_month_trend (increasing/stable/decreasing)
- [ ] Create `dbt/models/marts/metric_avg_transaction_value.sql`
  - [ ] customer_id
  - [ ] avg_transaction_value_all_time
  - [ ] avg_transaction_value_90d
  - [ ] avg_transaction_value_30d
  - [ ] atv_trend (up/down/stable)
  - [ ] max_transaction_amount
  - [ ] min_transaction_amount
- [ ] Create `dbt/models/marts/customer_360_profile.sql`
  - [ ] Denormalized wide table combining:
    - [ ] Current customer dimension (SCD is_current = TRUE)
    - [ ] Current segment assignment
    - [ ] All three hero metrics (LTV, MoM, ATV)
    - [ ] Churn prediction (placeholder, will be populated later)
    - [ ] Transaction summary statistics
    - [ ] Merchant category preferences (top 3)
    - [ ] Last transaction date and amount
- [ ] Configure customer_360_profile as incremental model
- [ ] Update `dbt/models/marts/schema.yml`
- [ ] Add tests:
  - [ ] Row count matches distinct customers (~50K)
  - [ ] No NULL values in key metrics
  - [ ] LTV values are positive
  - [ ] MoM percent changes are reasonable (-100% to +1000%)
  - [ ] Query performance test (SELECT * should return in <1 second)
- [ ] Add exposure in schema.yml:
  - [ ] Type: dashboard
  - [ ] Depends on: customer_360_profile
  - [ ] Owner: Marketing team
- [ ] Run `dbt run --models marts.*`
- [ ] Run `dbt test --models marts.*`
- [ ] Create sample queries in `dbt/analysis/customer_360_queries.sql`
- [ ] Validate mart performance:
  - [ ] Run sample analytical queries
  - [ ] Measure query execution time
  - [ ] Verify results match expected business logic
- [ ] Generate dbt documentation: `dbt docs generate`
- [ ] Serve dbt docs: `dbt docs serve`
- [ ] Review data lineage graph
- [ ] Document mart layer in `docs/mart_layer.md`

---

## Phase 4: Machine Learning & Semantic Layer

### Prompt 4.1: Churn Training Data Preparation
- [ ] Create `sql/ml/01_churn_labels.sql`
- [ ] Define churn logic:
  - [ ] Customer is "churned" if no transactions in last 90 days
  - [ ] AND customer was previously active (had transactions in prior 90 days)
  - [ ] Label as of 2024-09-30 (3 months before current date)
- [ ] Create GOLD.CHURN_LABELS table:
  - [ ] customer_id
  - [ ] label_date (2024-09-30)
  - [ ] is_churned (BOOLEAN: 0 or 1)
  - [ ] days_since_last_transaction
  - [ ] was_previously_active
- [ ] Execute churn label creation
- [ ] Validate class distribution:
  - [ ] Target 8-15% positive class (churned customers)
  - [ ] Should have 4,000-7,500 churned customers
- [ ] Create `sql/ml/02_ml_training_data.sql`
- [ ] Create GOLD.ML_TRAINING_DATA table with features:
  - [ ] customer_id
  - [ ] is_churned (target variable)
  - [ ] **Demographic features**: age, account_tenure_days, card_type, credit_limit
  - [ ] **Behavioral features (90 days prior to label date)**:
    - [ ] total_spend_90d
    - [ ] transaction_count_90d
    - [ ] avg_transaction_value_90d
    - [ ] distinct_merchant_categories_90d
    - [ ] travel_transaction_pct
    - [ ] days_between_transactions_avg
  - [ ] **Trend features**:
    - [ ] spend_trend_60d_vs_90d (comparing periods)
    - [ ] transaction_frequency_trend
  - [ ] **Engagement features**:
    - [ ] weekend_transaction_pct
    - [ ] evening_transaction_pct (if timestamp available)
  - [ ] **Risk indicators**:
    - [ ] declined_transaction_count
    - [ ] max_days_inactive
- [ ] Execute training data creation
- [ ] Validate training data:
  - [ ] Row count matches CHURN_LABELS
  - [ ] No NULL values in features (impute if necessary)
  - [ ] Feature distributions are reasonable
  - [ ] Positive class percentage in acceptable range
- [ ] Create `sql/ml/03_validate_training_data.sql` with validation queries
- [ ] Write `tests/test_ml_training_data.py`
  - [ ] Test row count
  - [ ] Test feature completeness
  - [ ] Test target variable distribution
  - [ ] Test feature value ranges
- [ ] Run ML training data tests
- [ ] Create exploratory analysis queries
- [ ] Document training data in `docs/ml_training_data.md`

### Prompt 4.2: Cortex ML Model Training & Predictions
- [ ] Create `sql/ml/04_train_churn_model.sql`
- [ ] Implement Snowflake Cortex ML model:
  ```sql
  CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION GOLD.CHURN_MODEL(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'GOLD.ML_TRAINING_DATA'),
    TARGET_COLNAME => 'IS_CHURNED',
    CONFIG_OBJECT => {'on_error': 'skip', 'evaluate': TRUE}
  );
  ```
- [ ] Execute model training on ML_WH warehouse
- [ ] Monitor training job progress
- [ ] Wait for training completion (may take 10-30 minutes)
- [ ] Create `sql/ml/05_evaluate_model.sql`
- [ ] Retrieve model evaluation metrics:
  - [ ] F1 score (target >= 0.50)
  - [ ] Precision
  - [ ] Recall
  - [ ] ROC AUC
  - [ ] Feature importance
- [ ] Validate model performance:
  - [ ] F1 score meets threshold
  - [ ] ROC AUC > 0.65
  - [ ] Check for class imbalance handling
- [ ] Create `sql/ml/06_generate_predictions.sql`
- [ ] Create GOLD.CHURN_PREDICTIONS table:
  - [ ] customer_id
  - [ ] churn_probability (0.0 - 1.0)
  - [ ] churn_prediction (TRUE/FALSE at 0.5 threshold)
  - [ ] prediction_date
  - [ ] model_version
- [ ] Generate predictions for all 50,000 customers
- [ ] Validate predictions:
  - [ ] All customers have predictions
  - [ ] Probabilities between 0 and 1
  - [ ] Distribution of predictions is reasonable
- [ ] Create `sql/ml/07_retrain_model_procedure.sql`
- [ ] Implement stored procedure:
  ```sql
  CREATE OR REPLACE PROCEDURE GOLD.RETRAIN_CHURN_MODEL()
  RETURNS STRING
  LANGUAGE SQL
  AS
  $$
  BEGIN
    -- Recreate training data with latest date
    -- Drop and recreate model
    -- Generate new predictions
    -- Log metrics to OBSERVABILITY
  END;
  $$;
  ```
- [ ] Test stored procedure execution
- [ ] Create `sql/ml/08_update_customer_360.sql`
- [ ] Update customer_360_profile with churn predictions:
  ```sql
  UPDATE GOLD.CUSTOMER_360_PROFILE c
  SET
    churn_probability = p.churn_probability,
    churn_risk_category = CASE
      WHEN p.churn_probability > 0.7 THEN 'High'
      WHEN p.churn_probability > 0.4 THEN 'Medium'
      ELSE 'Low'
    END
  FROM GOLD.CHURN_PREDICTIONS p
  WHERE c.customer_id = p.customer_id;
  ```
- [ ] Execute customer_360 update
- [ ] Validate integration:
  - [ ] Query customer_360_profile with churn data
  - [ ] Verify high-risk customers identified
  - [ ] Check correlation between segment and churn risk
- [ ] Log model metrics to OBSERVABILITY.MODEL_METRICS
- [ ] Write `tests/test_ml_model.py`
  - [ ] Test model exists
  - [ ] Test prediction table completeness
  - [ ] Test prediction value ranges
  - [ ] Test customer_360 integration
- [ ] Run ML model tests
- [ ] Document ML pipeline in `docs/ml_churn_model.md`

### Prompt 4.3: Semantic Layer for Cortex Analyst
- [ ] Create `cortex_analyst/semantic_model.yaml`
- [ ] Define base tables:
  - [ ] **customer_360_profile** (primary table)
  - [ ] **fact_transaction**
  - [ ] **dim_merchant_category**
  - [ ] **customer_segments**
- [ ] Define dimensions (20+ dimensions):
  - [ ] customer_id, name, segment
  - [ ] card_type, credit_limit
  - [ ] age, account_tenure_days
  - [ ] city, state
  - [ ] churn_risk_category
  - [ ] merchant_category_name, category_group
  - [ ] transaction_date, year, month, quarter
- [ ] Define metrics (30+ metrics):
  - [ ] **Customer metrics**: total_customers, active_customers, churned_customers
  - [ ] **Financial metrics**: total_revenue, avg_customer_ltv, total_spend_90d
  - [ ] **Transaction metrics**: transaction_count, avg_transaction_value, median_transaction_value
  - [ ] **Trend metrics**: mom_spend_change_avg, customer_growth_rate
  - [ ] **Churn metrics**: avg_churn_probability, high_risk_customer_count
  - [ ] **Segment metrics**: customers_by_segment, revenue_by_segment
- [ ] Define relationships:
  - [ ] customer_360_profile -> fact_transaction (one-to-many on customer_id)
  - [ ] fact_transaction -> dim_merchant_category (many-to-one on merchant_category_key)
  - [ ] customer_360_profile -> customer_segments (one-to-one on customer_id)
- [ ] Add time dimensions and date hierarchies
- [ ] Add verified queries (sample natural language questions):
  - [ ] "What is the total revenue from High-Value Travelers in the last 90 days?"
  - [ ] "Show me the top 10 customers by lifetime value"
  - [ ] "What percentage of customers are at high risk of churn?"
  - [ ] "Compare spending trends between Declining and New & Growing segments"
  - [ ] "What are the most popular merchant categories for Platinum cardholders?"
- [ ] Add filters and logical expressions
- [ ] Create `scripts/deploy_semantic_layer.py`
- [ ] Implement deployment script:
  - [ ] Upload semantic_model.yaml to Snowflake stage
  - [ ] Register with Cortex Analyst service
  - [ ] Grant access to appropriate roles
- [ ] Execute semantic layer deployment
- [ ] Create `tests/test_semantic_layer.py`
  - [ ] Test YAML syntax validation
  - [ ] Test all table references exist
  - [ ] Test all column references exist
  - [ ] Test metric calculations
  - [ ] Test verified queries return results
- [ ] Test semantic layer manually:
  - [ ] Execute verified queries via Cortex Analyst
  - [ ] Test ad-hoc natural language queries
  - [ ] Verify result accuracy
  - [ ] Test ambiguity handling
- [ ] Run semantic layer tests
- [ ] Create sample query library in `docs/cortex_analyst_queries.md`
- [ ] Document semantic layer in `docs/semantic_layer.md`

---

## Phase 5: Streamlit Application Development

### Prompt 5.1: Streamlit Foundation + Segment Explorer Tab
- [ ] Create `streamlit/app.py` (main application file)
- [ ] Implement Snowflake connection management:
  - [ ] Use Snowflake Connector for Python
  - [ ] Implement connection pooling
  - [ ] Add error handling and retry logic
  - [ ] Create `get_snowflake_connection()` helper function
- [ ] Implement query execution wrapper:
  - [ ] Create `execute_query(sql, params)` function
  - [ ] Add query caching with @st.cache_data
  - [ ] Add error handling
  - [ ] Add query logging
- [ ] Create page layout structure:
  - [ ] Header with logo and title
  - [ ] Navigation tabs (4 tabs)
  - [ ] Footer with metadata
- [ ] Initialize session state variables
- [ ] Create `streamlit/tabs/segment_explorer.py`
- [ ] Implement Segment Explorer Tab UI:
  - [ ] **Filters** (sidebar):
    - [ ] Segment multi-select (all 5 segments)
    - [ ] Card type multi-select
    - [ ] Churn risk multi-select (High/Medium/Low)
    - [ ] Credit limit range slider
  - [ ] **KPI Cards** (top row):
    - [ ] Total customers in selection
    - [ ] Average LTV
    - [ ] Average churn probability
    - [ ] Total 90-day spend
  - [ ] **Visualizations**:
    - [ ] Segment distribution bar chart (with filtered counts)
    - [ ] LTV distribution box plot by segment
    - [ ] Churn probability distribution histogram
    - [ ] MoM spend change trend line chart
  - [ ] **Data Table**:
    - [ ] Paginated table with filtered customers
    - [ ] Columns: customer_id, name, segment, card_type, ltv, churn_probability, mom_spend_change
    - [ ] Sortable columns
    - [ ] Download as CSV button
- [ ] Implement filter logic:
  - [ ] Build dynamic WHERE clause based on selections
  - [ ] Apply filters to all queries
  - [ ] Update all visualizations when filters change
- [ ] Create helper functions:
  - [ ] `load_segment_data(filters)` - main data loading function
  - [ ] `calculate_kpis(data)` - compute KPI values
  - [ ] `create_segment_chart(data)` - segment bar chart
  - [ ] `create_ltv_boxplot(data)` - LTV distribution
  - [ ] `create_churn_histogram(data)` - churn probability
  - [ ] `create_trend_chart(data)` - MoM trends
- [ ] Add error handling:
  - [ ] Graceful handling of connection failures
  - [ ] User-friendly error messages
  - [ ] Fallback to cached data if available
- [ ] Add performance optimization:
  - [ ] Cache query results
  - [ ] Limit data table to 1000 rows with pagination
  - [ ] Use Snowflake query result caching
- [ ] Create `streamlit/config.toml` with app configuration
- [ ] Create `tests/test_streamlit_segment_explorer.py`
  - [ ] Test Snowflake connection
  - [ ] Test query execution
  - [ ] Test data loading
  - [ ] Test filter logic
  - [ ] Test KPI calculations
- [ ] Run Streamlit tests
- [ ] Launch app locally: `streamlit run app.py`
- [ ] Test all UI interactions
- [ ] Validate data accuracy
- [ ] Test error scenarios (disconnect, bad query)
- [ ] Document Segment Explorer in `docs/streamlit_segment_explorer.md`

### Prompt 5.2: Customer 360 Deep Dive Tab
- [ ] Create `streamlit/tabs/customer_360.py`
- [ ] Implement Customer 360 Tab UI:
  - [ ] **Customer Search** (top):
    - [ ] Search bar (by customer_id or name)
    - [ ] Autocomplete suggestions
    - [ ] Recent searches history
  - [ ] **Customer Profile Card** (left column):
    - [ ] Customer name, ID, email
    - [ ] Demographics (age, location, account tenure)
    - [ ] Card type and credit limit
    - [ ] Current segment with badge
    - [ ] Churn risk indicator (color-coded)
  - [ ] **Key Metrics Card** (right column):
    - [ ] Lifetime value (with trend indicator)
    - [ ] Last 90-day spend
    - [ ] Average transaction value
    - [ ] MoM spend change (with arrow)
    - [ ] Transaction frequency
    - [ ] Days since last transaction
  - [ ] **Spending Trends Chart** (full width):
    - [ ] Monthly spend over time (line chart)
    - [ ] Show 12-month trend
    - [ ] Highlight current month
    - [ ] Add moving average line
  - [ ] **Merchant Category Breakdown** (left column):
    - [ ] Pie chart of spend by category
    - [ ] Top 5 categories
    - [ ] Percentage of total spend
  - [ ] **Transaction History Table** (full width):
    - [ ] Recent 50 transactions
    - [ ] Columns: date, merchant_category, amount, status
    - [ ] Sortable and filterable
    - [ ] Download as CSV
  - [ ] **Segment History Timeline** (full width):
    - [ ] Visual timeline of segment changes
    - [ ] Show date, old segment, new segment
    - [ ] Reason for change
  - [ ] **Churn Risk Analysis** (right column):
    - [ ] Churn probability gauge chart
    - [ ] Risk factors contributing to churn
    - [ ] Retention recommendations
- [ ] Implement data loading functions:
  - [ ] `load_customer_profile(customer_id)` - core profile data
  - [ ] `load_customer_metrics(customer_id)` - calculated metrics
  - [ ] `load_spending_trend(customer_id)` - time series data
  - [ ] `load_merchant_breakdown(customer_id)` - category aggregates
  - [ ] `load_transaction_history(customer_id, limit)` - transaction list
  - [ ] `load_segment_history(customer_id)` - segment changes
  - [ ] `load_churn_analysis(customer_id)` - churn risk details
- [ ] Add interactive features:
  - [ ] Customer comparison mode (select 2 customers, show side-by-side)
  - [ ] Export customer report as PDF
  - [ ] Add customer to campaign list (if in retention mode)
- [ ] Add error handling:
  - [ ] Handle customer not found
  - [ ] Handle missing data gracefully
  - [ ] Show informative messages
- [ ] Create `tests/test_streamlit_customer_360.py`
  - [ ] Test customer search
  - [ ] Test profile data loading
  - [ ] Test metric calculations
  - [ ] Test chart rendering
  - [ ] Test transaction history pagination
- [ ] Run Customer 360 tests
- [ ] Test UI with various customer profiles
- [ ] Validate all metrics against source data
- [ ] Document Customer 360 tab in `docs/streamlit_customer_360.md`

### Prompt 5.3: AI Assistant (Cortex Analyst) Tab
- [ ] Create `streamlit/tabs/ai_assistant.py`
- [ ] Implement AI Assistant Tab UI:
  - [ ] **Chat Interface** (main area):
    - [ ] Chat message history display
    - [ ] User input text box
    - [ ] Submit button
    - [ ] Clear conversation button
  - [ ] **Suggested Questions** (sidebar):
    - [ ] List of 10+ example questions
    - [ ] Clickable to populate input box
    - [ ] Categories: Customer Insights, Revenue Analysis, Churn Prediction, Segment Analysis
  - [ ] **Query History** (sidebar):
    - [ ] Last 10 queries with timestamps
    - [ ] Clickable to re-run
    - [ ] Export history as JSON
  - [ ] **Response Display**:
    - [ ] Natural language answer
    - [ ] Generated SQL query (expandable)
    - [ ] Results table (if applicable)
    - [ ] Chart visualization (if applicable)
    - [ ] Export results as CSV
- [ ] Implement Cortex Analyst integration:
  - [ ] Create `call_cortex_analyst(question)` function
  - [ ] Use Snowflake Cortex Analyst SQL function:
    ```sql
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
      'cortex-analyst',
      OBJECT_CONSTRUCT(
        'question', :question,
        'semantic_model', 'customer360_semantic_model'
      )
    )
    ```
  - [ ] Parse Cortex Analyst response (JSON)
  - [ ] Extract SQL query, natural language answer, results
- [ ] Implement response processing:
  - [ ] Execute generated SQL query
  - [ ] Format results for display
  - [ ] Generate appropriate visualizations based on data type
  - [ ] Handle errors and ambiguous questions
- [ ] Add conversation management:
  - [ ] Store conversation history in session state
  - [ ] Allow follow-up questions with context
  - [ ] Implement "show me more details" functionality
- [ ] Add suggested questions:
  - [ ] "What are the top 10 customers by lifetime value?"
  - [ ] "Show me revenue trends by segment over the last 6 months"
  - [ ] "How many customers are at high risk of churn in each segment?"
  - [ ] "What is the average transaction value for Platinum cardholders?"
  - [ ] "Compare spending patterns between Declining and Stable Mid-Spenders"
  - [ ] "Which merchant categories generate the most revenue?"
  - [ ] "Show me customers with month-over-month spend increase greater than 50%"
  - [ ] "What percentage of New & Growing segment customers have churn probability above 0.5?"
  - [ ] "Show me year-over-year customer growth by state"
  - [ ] "What is the correlation between credit limit and lifetime value?"
- [ ] Implement visualization auto-generation:
  - [ ] Detect numeric trends -> line chart
  - [ ] Detect categorical aggregates -> bar chart
  - [ ] Detect distributions -> histogram
  - [ ] Detect comparisons -> grouped bar chart
  - [ ] Detect geographic data -> map (if state/city available)
- [ ] Add feedback mechanism:
  - [ ] Thumbs up/down on responses
  - [ ] Log feedback to OBSERVABILITY schema
  - [ ] Report incorrect results
- [ ] Create `tests/test_streamlit_ai_assistant.py`
  - [ ] Test Cortex Analyst connection
  - [ ] Test question submission
  - [ ] Test response parsing
  - [ ] Test SQL execution
  - [ ] Test visualization generation
  - [ ] Test error handling
- [ ] Run AI Assistant tests
- [ ] Test with all suggested questions
- [ ] Test with ambiguous questions
- [ ] Validate result accuracy
- [ ] Document AI Assistant in `docs/streamlit_ai_assistant.md`

### Prompt 5.4: Campaign Performance Tab
- [ ] Create `streamlit/tabs/campaign_performance.py`
- [ ] Implement Campaign Performance Tab UI:
  - [ ] **Campaign Simulation Setup** (sidebar):
    - [ ] Target segment multi-select
    - [ ] Churn risk filter (target high-risk only)
    - [ ] Offer type dropdown (Cashback, Points Bonus, Fee Waiver, Credit Limit Increase)
    - [ ] Estimated response rate slider (5% - 40%)
    - [ ] Estimated lift in spend slider (10% - 100%)
    - [ ] Campaign duration (months)
  - [ ] **Target Audience Analysis** (top section):
    - [ ] KPI cards:
      - [ ] Total eligible customers
      - [ ] Total current 90-day spend
      - [ ] Average churn probability
      - [ ] Estimated reachable customers (eligible * response rate)
    - [ ] Audience composition chart (by segment, card type)
  - [ ] **ROI Projection** (middle section):
    - [ ] Input campaign cost per customer
    - [ ] Calculate total campaign cost
    - [ ] Estimate incremental revenue (lift * current spend * response rate * duration)
    - [ ] Calculate ROI = (incremental revenue - cost) / cost
    - [ ] Display ROI gauge chart (color-coded)
    - [ ] Break-even analysis
  - [ ] **Scenario Comparison Table** (bottom section):
    - [ ] Create 3-5 pre-built scenarios
    - [ ] Compare ROI, cost, revenue, customer reach
    - [ ] Highlight best scenario
  - [ ] **Customer List Export** (bottom):
    - [ ] Generate target customer list based on filters
    - [ ] Show prioritized list (highest churn risk first)
    - [ ] Include: customer_id, name, email, segment, churn_probability, ltv
    - [ ] Download as CSV for campaign execution
- [ ] Implement data loading functions:
  - [ ] `load_campaign_audience(filters)` - get eligible customers
  - [ ] `calculate_current_spend(customer_list)` - sum current spend
  - [ ] `estimate_campaign_roi(audience, offer_params)` - ROI calculation
  - [ ] `generate_customer_list(filters, sort_by)` - export list
- [ ] Implement ROI calculation logic:
  ```python
  eligible_customers = count(filtered customers)
  reachable_customers = eligible_customers * response_rate
  current_spend = sum(90_day_spend for eligible customers)
  incremental_spend = current_spend * lift_percentage * response_rate
  incremental_revenue = incremental_spend * campaign_duration
  campaign_cost = reachable_customers * cost_per_customer
  roi = (incremental_revenue - campaign_cost) / campaign_cost
  ```
- [ ] Add pre-built scenarios:
  - [ ] **Scenario 1: Aggressive Retention**
    - Target: High churn risk (>0.7), all segments
    - Offer: 5% cashback on all purchases
    - Cost: $50/customer, Response: 30%, Lift: 40%
  - [ ] **Scenario 2: High-Value Focus**
    - Target: High-Value Travelers + Stable Mid-Spenders with churn >0.5
    - Offer: 10,000 bonus points
    - Cost: $75/customer, Response: 25%, Lift: 50%
  - [ ] **Scenario 3: Declining Recovery**
    - Target: Declining segment only, churn >0.6
    - Offer: Annual fee waiver
    - Cost: $95/customer, Response: 35%, Lift: 60%
  - [ ] **Scenario 4: New Customer Nurture**
    - Target: New & Growing segment with low spend
    - Offer: Credit limit increase
    - Cost: $20/customer, Response: 40%, Lift: 80%
  - [ ] **Scenario 5: Budget Engagement**
    - Target: Budget-Conscious segment, churn >0.4
    - Offer: 2% cashback on groceries
    - Cost: $30/customer, Response: 35%, Lift: 30%
- [ ] Implement interactive scenario builder:
  - [ ] Allow user to create custom scenarios
  - [ ] Save scenarios to session state
  - [ ] Compare custom scenario to pre-built ones
- [ ] Add sensitivity analysis:
  - [ ] Show ROI at different response rates
  - [ ] Show ROI at different lift percentages
  - [ ] Tornado chart for sensitivity
- [ ] Create visualizations:
  - [ ] ROI gauge chart (with color zones: red <0%, yellow 0-50%, green >50%)
  - [ ] Incremental revenue waterfall chart
  - [ ] Customer reach funnel (eligible -> reachable -> responders)
  - [ ] Segment composition stacked bar
- [ ] Create `tests/test_streamlit_campaign_performance.py`
  - [ ] Test audience filtering
  - [ ] Test ROI calculations
  - [ ] Test scenario generation
  - [ ] Test customer list export
  - [ ] Test visualization rendering
- [ ] Run Campaign Performance tests
- [ ] Test all pre-built scenarios
- [ ] Validate ROI calculations manually
- [ ] Test custom scenario builder
- [ ] Document Campaign Performance in `docs/streamlit_campaign_performance.md`

---

## Phase 6: Deployment & Documentation

### Final Deployment
- [ ] Create `deploy/deploy_to_snowflake.sh` master deployment script
- [ ] Execute full deployment from scratch:
  - [ ] Apply Terraform infrastructure
  - [ ] Run all SQL foundation scripts
  - [ ] Generate and upload customer data
  - [ ] Generate and load transaction data
  - [ ] Run dbt models (all phases)
  - [ ] Train ML model
  - [ ] Generate predictions
  - [ ] Deploy semantic layer
  - [ ] Deploy Streamlit app to Snowflake
- [ ] Create `deploy/rollback.sh` for rollback procedures
- [ ] Create Snowflake Native App (optional):
  - [ ] Package application as Snowflake Native App
  - [ ] Create manifest.yml
  - [ ] Create setup script
  - [ ] Test installation in separate account
- [ ] Set up CI/CD pipeline (GitHub Actions):
  - [ ] Create `.github/workflows/ci.yml`
  - [ ] Run tests on PR
  - [ ] Run dbt tests on merge to main
  - [ ] Deploy to dev/staging/prod environments
- [ ] Create monitoring dashboards:
  - [ ] Query OBSERVABILITY.PIPELINE_RUNS for ETL monitoring
  - [ ] Query OBSERVABILITY.DATA_QUALITY_CHECKS for data quality
  - [ ] Query OBSERVABILITY.MODEL_METRICS for ML performance
  - [ ] Set up email alerts for failures

### Documentation
- [ ] Create comprehensive `README.md` in root:
  - [ ] Project overview
  - [ ] Architecture diagram
  - [ ] Prerequisites
  - [ ] Installation instructions
  - [ ] Usage guide
  - [ ] Contributing guidelines
- [ ] Create `docs/ARCHITECTURE.md`
  - [ ] Medallion architecture overview
  - [ ] Star schema design
  - [ ] Data flow diagram
  - [ ] Technology stack details
- [ ] Create `docs/USER_GUIDE.md`
  - [ ] How to use each Streamlit tab
  - [ ] Sample analyses and workflows
  - [ ] FAQ
  - [ ] Troubleshooting
- [ ] Create `docs/ADMIN_GUIDE.md`
  - [ ] Deployment procedures
  - [ ] Monitoring and observability
  - [ ] Data refresh procedures
  - [ ] Model retraining schedule
  - [ ] Backup and recovery
- [ ] Create `docs/API_REFERENCE.md`
  - [ ] Python function documentation
  - [ ] SQL stored procedure documentation
  - [ ] dbt macro documentation
- [ ] Generate dbt documentation:
  - [ ] `dbt docs generate`
  - [ ] Host on Snowflake stage or S3
- [ ] Create video walkthrough (optional):
  - [ ] Screen recording of Streamlit app
  - [ ] Narrated demo of key features
  - [ ] Upload to YouTube/Loom
- [ ] Create presentation deck:
  - [ ] Business context
  - [ ] Solution overview
  - [ ] Demo screenshots
  - [ ] Technical architecture
  - [ ] Results and insights

### Quality Assurance
- [ ] Run full test suite:
  - [ ] `pytest tests/ -v`
  - [ ] `dbt test`
  - [ ] Integration tests
  - [ ] Performance tests
- [ ] Validate data quality:
  - [ ] Run all validation queries
  - [ ] Check data completeness
  - [ ] Verify metric calculations
- [ ] Perform UAT (User Acceptance Testing):
  - [ ] Marketing team tests Segment Explorer
  - [ ] Analysts test Customer 360
  - [ ] Business users test AI Assistant
  - [ ] Marketing tests Campaign Performance
- [ ] Security review:
  - [ ] Verify RBAC implementation
  - [ ] Check data masking policies (if PII)
  - [ ] Review Snowflake access controls
  - [ ] Audit IAM permissions
- [ ] Performance testing:
  - [ ] Load test Streamlit app (concurrent users)
  - [ ] Query performance benchmarks
  - [ ] Warehouse sizing validation
  - [ ] Cost optimization review
- [ ] Create final validation report

### Handoff & Training
- [ ] Create training materials:
  - [ ] User training slides
  - [ ] Admin training slides
  - [ ] Quick reference guides
- [ ] Conduct training sessions:
  - [ ] End-user training (Streamlit app)
  - [ ] Admin training (operations, monitoring)
  - [ ] Developer training (code structure, maintenance)
- [ ] Knowledge transfer:
  - [ ] Code walkthrough
  - [ ] Architecture review
  - [ ] Q&A session
- [ ] Create support plan:
  - [ ] Issue tracking process (GitHub Issues)
  - [ ] SLA for bug fixes
  - [ ] Enhancement request process
- [ ] Final deliverables checklist:
  - [ ] Complete codebase in GitHub repo
  - [ ] All documentation
  - [ ] Test results and validation reports
  - [ ] Training materials
  - [ ] Deployed application
  - [ ] Access credentials (securely shared)

---

## Acceptance Criteria Validation

### User Story 1: Customer Segmentation Dashboard
- [ ] Segment Explorer tab shows 5 segments
- [ ] Filters work correctly (segment, card type, churn risk, credit limit)
- [ ] KPIs update dynamically with filters
- [ ] Charts display correctly
- [ ] Data table is sortable and downloadable
- [ ] Performance: page loads in <3 seconds

### User Story 2: Customer 360 View
- [ ] Customer search works (by ID and name)
- [ ] Profile card displays all required information
- [ ] Key metrics match source data
- [ ] Spending trends chart shows 12-month history
- [ ] Merchant category breakdown is accurate
- [ ] Transaction history table shows recent 50 transactions
- [ ] Segment history timeline shows changes
- [ ] Churn risk analysis displays correctly

### User Story 3: Churn Prediction Model
- [ ] Model achieves F1 score >= 0.50
- [ ] All 50K customers have predictions
- [ ] Churn probabilities are between 0 and 1
- [ ] customer_360_profile includes churn data
- [ ] High-risk customers (>0.7) are correctly identified
- [ ] Retrain procedure works without errors
- [ ] Model metrics logged to OBSERVABILITY

### User Story 4: Natural Language Analytics (AI Assistant)
- [ ] Cortex Analyst responds to natural language questions
- [ ] At least 10 suggested questions work correctly
- [ ] Generated SQL is valid and executes
- [ ] Results are accurate
- [ ] Charts auto-generate appropriately
- [ ] Conversation history is maintained
- [ ] Error handling for ambiguous questions works

### User Story 5: Campaign Performance Simulation
- [ ] Target audience filters work correctly
- [ ] ROI calculations are mathematically correct
- [ ] All 5 pre-built scenarios display
- [ ] Custom scenario builder works
- [ ] Customer list export generates correct CSV
- [ ] Charts display correctly
- [ ] Sensitivity analysis shows impact of parameter changes

### User Story 6: Data Engineering Pipeline
- [ ] Medallion architecture implemented (Bronze/Silver/Gold)
- [ ] SCD Type 2 works correctly on dim_customer
- [ ] Star schema with referential integrity
- [ ] All dbt models run without errors
- [ ] All dbt tests pass
- [ ] Observability logging captures pipeline runs
- [ ] Data quality checks automated

---

## Post-Implementation Tasks

### Optimization
- [ ] Review query performance, add indexes if needed
- [ ] Optimize Snowflake warehouse sizing
- [ ] Implement query result caching
- [ ] Review clustering keys on large tables
- [ ] Optimize dbt incremental strategies

### Enhancements (Future)
- [ ] Add more ML models (fraud detection, product recommendation)
- [ ] Implement real-time streaming ingestion
- [ ] Add geospatial analysis (customer location mapping)
- [ ] Build additional Streamlit tabs (Executive Dashboard, Data Quality)
- [ ] Integrate with external systems (CRM, marketing platforms)
- [ ] Implement A/B testing framework for campaigns
- [ ] Add advanced alerting (anomaly detection)

### Maintenance
- [ ] Schedule monthly dbt runs
- [ ] Schedule monthly segment recalculation
- [ ] Schedule quarterly model retraining
- [ ] Review and update documentation quarterly
- [ ] Monitor Snowflake costs and optimize
- [ ] Keep dependencies up to date (Python packages, dbt)

---

## Notes
- This checklist covers all 17 implementation prompts
- Total estimated effort: 8-10 weeks
- Each phase builds on the previous one
- Test frequently and iterate
- Document as you go
- Prioritize data quality and testing

**Ready to build a world-class Customer 360 platform!** 🚀
