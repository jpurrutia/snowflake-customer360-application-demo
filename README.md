# Snowflake Customer 360 Analytics Platform - Onboarding Guide

**Version**: 1.0
**Last Updated**: 2025-11-12
**Target Audience**: New developers, data engineers, analysts exploring the project

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture Overview](#architecture-overview)
3. [Prerequisites](#prerequisites)
4. [Quick Start (30 Minutes)](#quick-start-30-minutes)
5. [Deep Dive by Phase](#deep-dive-by-phase)
6. [Local Development Setup](#local-development-setup)
7. [Snowflake Deployment](#snowflake-deployment)
8. [GitHub Integration](#github-integration)
9. [Testing the Platform](#testing-the-platform)
10. [Troubleshooting](#troubleshooting)
11. [Next Steps](#next-steps)

---

## Project Overview

### What is this project?

The **Snowflake Customer 360 Analytics Platform** is a complete, production-ready data platform demonstrating Snowflake's four pillars:

1. **Data Engineering**: Medallion architecture (Bronze → Silver → Gold)
2. **Data Warehousing**: Star schema with 50K customers and 13.5M transactions
3. **Data Science/ML**: Churn prediction using Snowflake Cortex ML
4. **Data Applications**: Interactive Streamlit dashboard with 4 tabs

### Business Context

**Scenario**: Your company acquired a regional credit card portfolio. You need to:
- Integrate customer data from legacy systems
- Identify at-risk customers (churn prediction)
- Segment customers for targeted campaigns
- Enable business users to explore data without SQL

### Key Features

- **Customer Segmentation**: 5 behavioral segments (High-Value Travelers, Declining, etc.)
- **Churn Prediction**: ML model (F1 ≥ 0.50) predicting customer churn risk
- **Hero Metrics**: Lifetime Value, MoM Spend Change, Average Transaction Value
- **Natural Language Queries**: Ask questions in plain English (Cortex Analyst)
- **Campaign ROI Calculator**: Model retention campaign performance

---

## Architecture Overview

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                             │
│  - Synthetic Customer Data (Python/Faker)                    │
│  - Transaction Generator (Snowflake GENERATOR function)      │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────┐
│                  AWS S3 DATA LAKE                            │
│  - customers/ (50K records, CSV)                             │
│  - transactions/ (future: streaming data)                    │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────┐
│                SNOWFLAKE - BRONZE LAYER                      │
│  - raw_customers (50K rows, loaded via COPY INTO)            │
│  - raw_transactions (13.5M rows, GENERATOR function)         │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  ▼ (dbt transformations)
┌─────────────────────────────────────────────────────────────┐
│                SNOWFLAKE - SILVER LAYER                      │
│  - stg_customers (cleaned, standardized)                     │
│  - stg_transactions (cleaned, standardized)                  │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  ▼ (dbt transformations)
┌─────────────────────────────────────────────────────────────┐
│                SNOWFLAKE - GOLD LAYER                        │
│  - dim_customer (SCD Type 2, 50K+ rows)                      │
│  - dim_date (580 days)                                       │
│  - dim_merchant_category (50 categories)                     │
│  - fct_transactions (13.5M rows, clustered by date)          │
│  - customer_segments (5 segments with rolling metrics)       │
│  - customer_360_profile (denormalized for dashboards)        │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────┐
│              SNOWFLAKE CORTEX ML                             │
│  - churn_model (binary classification)                       │
│  - churn_predictions (all 50K customers scored)              │
│  - Automated retraining (monthly stored procedure)           │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────┐
│           SNOWFLAKE CORTEX ANALYST                           │
│  - semantic_model.yaml (30+ metrics, 40+ dimensions)         │
│  - Natural language → SQL generation                         │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────┐
│              STREAMLIT APPLICATION                           │
│  Tab 1: Segment Explorer (customer filtering)               │
│  Tab 2: Customer 360 (individual customer profiles)          │
│  Tab 3: AI Assistant (natural language queries)              │
│  Tab 4: Campaign Simulator (marketing ROI calculator)        │
└─────────────────────────────────────────────────────────────┘
```

### Technology Stack

| Layer | Technology |
|-------|------------|
| Infrastructure | AWS S3, IAM, Terraform |
| Data Warehouse | Snowflake (Enterprise/Business Critical) |
| Transformations | dbt (data build tool) |
| Machine Learning | Snowflake Cortex ML |
| Semantic Layer | Snowflake Cortex Analyst |
| Application | Streamlit in Snowflake |
| Testing | pytest |
| Data Generation | Python (Faker), Snowflake GENERATOR |

---

## Prerequisites

### Required Accounts

1. **Snowflake Account**
   - Sign up: [https://signup.snowflake.com/](https://signup.snowflake.com/)
   - Recommended: Trial account with Enterprise edition
   - Must have Cortex ML and Cortex Analyst enabled
   - Minimum credit requirements: ~$50 for full setup

2. **AWS Account** (optional for full setup)
   - Only needed if you want to set up S3 data lake
   - Free tier sufficient for this project
   - Alternative: Use Snowflake internal stages

3. **GitHub Account**
   - Clone this repository: `git clone <repo-url>`

### Required Software

Install the following on your local machine:

```bash
# 1. Python 3.10+
python --version  # Should be 3.10 or higher

# 2. UV package manager (recommended) or pip
pip install uv
# OR use pip for all installations

# 3. Git
git --version

# 4. Terraform (optional - only for AWS setup)
terraform --version

# 5. AWS CLI (optional - only for AWS setup)
aws --version

# 6. SnowSQL (recommended for testing)
# Download from: https://docs.snowflake.com/en/user-guide/snowsql-install-config
```

### Recommended Skills

- **Beginner**: Basic SQL, command line usage
- **Intermediate**: Python, dbt, data warehousing concepts
- **Advanced**: Terraform, AWS, ML concepts

**Don't worry if you're missing some skills!** This guide will walk you through everything step-by-step.

---

## Quick Start (30 Minutes)

**Goal**: Get a working demo in Snowflake without AWS setup

This quick start skips AWS and uses Snowflake internal stages for data.

### Step 1: Clone Repository (2 minutes)

```bash
# Clone the repository
git clone <repository-url>
cd snowflake-panel-demo

# Install Python dependencies
uv sync
# OR
pip install -r requirements.txt
```

### Step 2: Configure Snowflake Credentials (3 minutes)

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your Snowflake credentials
nano .env  # or use your favorite editor
```

Update `.env`:
```
SNOWFLAKE_ACCOUNT=abc12345.us-east-1
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=CUSTOMER_ANALYTICS
SNOWFLAKE_SCHEMA=GOLD
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

### Step 3: Set Up Snowflake Database (5 minutes)

```bash
# Run Snowflake setup scripts
snowsql -a <your-account> -u <your-user> -f snowflake/setup/01_create_database.sql
snowsql -a <your-account> -u <your-user> -f snowflake/setup/02_create_schemas.sql
snowsql -a <your-account> -u <your-user> -f snowflake/setup/03_create_roles.sql
```

**What this does**:
- Creates `CUSTOMER_ANALYTICS` database
- Creates `BRONZE`, `SILVER`, `GOLD` schemas (medallion architecture)
- Creates `DATA_ANALYST` role with appropriate permissions

### Step 4: Generate & Load Customer Data (5 minutes)

```bash
# Generate 50,000 synthetic customers
uv run python -m data_generation generate-customers \
    --count 50000 \
    --output data/customers.csv

# Load into Snowflake (using internal stage)
snowsql -a <your-account> -u <your-user> -f snowflake/bronze/01_load_customers_from_stage.sql
```

**What this does**:
- Generates 50K customers with realistic demographics (Faker library)
- Loads CSV into Snowflake `BRONZE.raw_customers` table

### Step 5: Run dbt Transformations (10 minutes)

```bash
cd dbt_customer_analytics

# Configure dbt profile
cp profiles.yml.example ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with your Snowflake credentials

# Run dbt transformations
dbt deps  # Install dependencies
dbt run   # Run all models (Bronze → Silver → Gold)
dbt test  # Run data quality tests

cd ..
```

**What this does**:
- Transforms raw data through Bronze → Silver → Gold layers
- Creates star schema (fact and dimension tables)
- Calculates customer segmentation
- Generates customer_360_profile table

### Step 6: Train Churn Model (3 minutes)

```bash
# Train Cortex ML churn model
snowsql -a <your-account> -u <your-user> -f snowflake/ml/01_train_churn_model.sql

# Generate predictions
snowsql -a <your-account> -u <your-user> -f snowflake/ml/02_predict_churn.sql
```

**What this does**:
- Trains binary classification model on customer features
- Scores all 50K customers with churn risk (0-100)

### Step 7: Launch Streamlit App (2 minutes)

```bash
cd streamlit

# Run locally
streamlit run app.py
```

**Open browser**: http://localhost:8501

**Explore the 4 tabs**:
1. **Segment Explorer**: Filter and export customer segments
2. **Customer 360**: Look up individual customer profiles
3. **AI Assistant**: Ask questions in natural language
4. **Campaign Simulator**: Model retention campaign ROI

🎉 **You now have a working demo!**

---

## Deep Dive by Phase

Now that you have a working demo, let's understand each phase in detail.

### Phase 1: Foundation & Infrastructure

**Goal**: Set up AWS and Snowflake infrastructure

**Key Files**:
- `terraform/` - AWS infrastructure as code
- `snowflake/setup/` - Snowflake database setup scripts

**What to explore**:

1. **Terraform Configuration** (`terraform/main.tf`)
   - Review S3 bucket configuration
   - Understand IAM role for Snowflake access
   - See how storage integration works

2. **Snowflake Setup** (`snowflake/setup/`)
   - `01_create_database.sql` - Database and warehouse creation
   - `02_create_schemas.sql` - Bronze, Silver, Gold schemas
   - `03_create_roles.sql` - Role-based access control (RBAC)

**Try it**:
```bash
# Review Terraform plan
cd terraform
terraform init
terraform plan

# Review Snowflake setup
cat snowflake/setup/01_create_database.sql
```

**Documentation**: [terraform/README.md](../terraform/README.md)

---

### Phase 2: Data Generation & Ingestion

**Goal**: Generate synthetic data and load into Snowflake

**Key Files**:
- `data_generation/customer_generator.py` - 50K customer generator
- `snowflake/bronze/01_load_customers_from_stage.sql` - Load customers
- `snowflake/bronze/02_generate_transactions.sql` - 13.5M transactions

**What to explore**:

1. **Customer Generator** (`data_generation/customer_generator.py`)
   ```python
   # Key functions
   generate_customer(customer_id) -> dict
   generate_customers_batch(count) -> List[dict]
   ```
   - Uses Faker library for realistic names, addresses, emails
   - Generates card types, credit limits, account open dates
   - Weighted distributions (e.g., 70% Standard, 30% Premium)

2. **Transaction Generator** (`snowflake/bronze/02_generate_transactions.sql`)
   ```sql
   -- Generates 13.5M transactions using GENERATOR function
   SELECT
       customer_id,
       transaction_date,
       merchant_name,
       amount,
       status
   FROM TABLE(GENERATOR(rowcount => 13500000))
   ```
   - Average 270 transactions per customer over 18 months
   - Realistic merchant names, categories, amounts
   - 95% approval rate, 5% declined

**Try it**:
```bash
# Generate small sample locally
uv run python -m data_generation generate-customers --count 100 --output test.csv
head -20 test.csv

# Review transaction generator logic
cat snowflake/bronze/02_generate_transactions.sql
```

**Documentation**:
- [docs/prompt_2.1_completion_summary.md](prompt_2.1_completion_summary.md)
- [docs/prompt_2.2_completion_summary.md](prompt_2.2_completion_summary.md)

---

### Phase 3: dbt Transformations

**Goal**: Transform raw data into analytics-ready tables

**Key Directories**:
- `dbt_customer_analytics/models/bronze/` - Raw data staging
- `dbt_customer_analytics/models/silver/` - Cleaned data
- `dbt_customer_analytics/models/gold/` - Star schema
- `dbt_customer_analytics/models/marts/` - Business aggregates

**What to explore**:

1. **Bronze Layer** (staging)
   - `stg_customers.sql` - Clean and standardize customer data
   - `stg_transactions.sql` - Clean and standardize transactions

2. **Silver Layer** (cleaned)
   - Data type conversions (strings → dates, numbers)
   - Null handling
   - Deduplication

3. **Gold Layer** (dimensional model)
   - **dim_customer.sql** - SCD Type 2 customer dimension
     - Tracks changes to card_type and credit_limit over time
     - `valid_from` and `valid_to` columns
   - **dim_date.sql** - Date dimension (580 days)
   - **dim_merchant_category.sql** - 50 merchant categories
   - **fct_transactions.sql** - 13.5M transaction fact table
     - Clustered by transaction_date for query performance

4. **Marts Layer** (business aggregates)
   - **customer_segments.sql** - 5 behavioral segments
     - High-Value Travelers, Declining, New & Growing, etc.
     - Rolling 90-day window calculations
   - **hero_metrics.sql** - LTV, MoM Spend Change, ATV
   - **customer_360_profile.sql** - Denormalized view for dashboards

**Try it**:
```bash
cd dbt_customer_analytics

# Generate dbt documentation
dbt docs generate
dbt docs serve  # Opens browser with lineage graph

# Run specific model
dbt run --select customer_360_profile

# Test specific model
dbt test --select customer_360_profile
```

**Key Concepts**:

- **SCD Type 2**: Tracks historical changes with `valid_from` and `valid_to`
  ```sql
  -- Example: Customer upgrades from Standard to Premium
  customer_id | card_type | valid_from  | valid_to    | is_current
  ------------|-----------|-------------|-------------|------------
  1000        | Standard  | 2023-01-01  | 2024-06-15  | False
  1000        | Premium   | 2024-06-15  | 9999-12-31  | True
  ```

- **Rolling Window Metrics**: Dynamic 90-day calculations
  ```sql
  spend_last_90_days = SUM(amount) WHERE txn_date >= CURRENT_DATE - 90
  ```

**Documentation**:
- [docs/star_schema_design.md](star_schema_design.md)
- [docs/customer_segmentation_guide.md](customer_segmentation_guide.md)
- [docs/aggregate_marts_guide.md](aggregate_marts_guide.md)

---

### Phase 4: Machine Learning & Semantic Layer

**Goal**: Add ML predictions and natural language query capability

**Key Files**:
- `snowflake/ml/01_train_churn_model.sql` - Train Cortex ML model
- `snowflake/ml/02_predict_churn.sql` - Generate predictions
- `semantic_layer/semantic_model.yaml` - Cortex Analyst config

#### 4A: Churn Prediction Model

**What to explore**:

1. **Training Data Preparation** (`snowflake/ml/00_prepare_training_data.sql`)
   ```sql
   -- 35+ features for churn prediction
   SELECT
       customer_id,
       -- Demographics
       age_years, state, city,
       -- Account features
       days_since_account_open, card_type, credit_limit,
       -- Spending behavior
       lifetime_value, avg_monthly_spend, spend_last_90_days,
       -- Activity patterns
       days_since_last_transaction, transaction_count,
       -- Trend indicators
       mom_spend_change_pct, spend_volatility,
       -- Target variable
       has_churned  -- Binary: 0 or 1
   FROM GOLD.CUSTOMER_SEGMENTS
   ```

2. **Model Training** (`snowflake/ml/01_train_churn_model.sql`)
   ```sql
   CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION churn_model(
       INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'GOLD.CHURN_TRAINING_DATA'),
       TARGET_COLNAME => 'HAS_CHURNED',
       CONFIG_OBJECT => {'evaluate': true}
   );
   ```

3. **Predictions** (`snowflake/ml/02_predict_churn.sql`)
   ```sql
   -- Score all customers
   SELECT
       customer_id,
       churn_model!PREDICT(object_construct(*)) as prediction
   FROM GOLD.CUSTOMER_360_PROFILE;
   ```

**Model Performance**:
- F1 Score: ≥ 0.50
- Precision: ≥ 0.60
- Recall: ≥ 0.40

**Try it**:
```bash
# Check model performance
snowsql -a <account> -u <user> -q "
SELECT * FROM GOLD.CHURN_MODEL_METRICS;
"

# View predictions
snowsql -a <account> -u <user> -q "
SELECT customer_id, churn_risk_score, churn_risk_category
FROM GOLD.CUSTOMER_360_PROFILE
WHERE churn_risk_category = 'High Risk'
LIMIT 10;
"
```

**Documentation**: [docs/ml_model_card.md](ml_model_card.md)

#### 4B: Semantic Layer (Cortex Analyst)

**What to explore**:

1. **Semantic Model** (`semantic_layer/semantic_model.yaml`)
   ```yaml
   name: customer_360_analytics
   tables:
     - name: customer_360_profile
       description: Complete customer profile with demographics, segmentation, and churn risk
       base_table:
         database: CUSTOMER_ANALYTICS
         schema: GOLD
         table: CUSTOMER_360_PROFILE
       dimensions:
         - name: customer_segment
           synonyms: ["segment", "customer type"]
           data_type: TEXT
         - name: churn_risk_category
           synonyms: ["churn risk", "risk level"]
           data_type: TEXT
       measures:
         - name: lifetime_value
           synonyms: ["LTV", "customer value"]
           data_type: NUMBER
           aggregation: SUM
   ```

2. **Example Questions**:
   - "Which customers are at highest risk of churning?"
   - "What is the average lifetime value by segment?"
   - "Show me Premium cardholders in California"

**Try it**:
```bash
# Deploy semantic model
cd semantic_layer
./deploy_semantic_model.sh

# Test with SQL
snowsql -a <account> -u <user> -f test_semantic_model.sql
```

**Documentation**: [semantic_layer/README.md](../semantic_layer/README.md)

---

### Phase 5: Streamlit Application

**Goal**: Build interactive dashboard for business users

**Key Files**:
- `streamlit/app.py` - Main application
- `streamlit/tabs/segment_explorer.py` - Tab 1
- `streamlit/tabs/customer_360.py` - Tab 2
- `streamlit/tabs/ai_assistant.py` - Tab 3
- `streamlit/tabs/campaign_simulator.py` - Tab 4

**What to explore**:

#### Tab 1: Segment Explorer

**Purpose**: Filter and export customer segments

**Key Features**:
- Multi-select filters (segment, state, churn risk, LTV, card type)
- 4 summary metrics (count, total LTV, avg LTV, avg churn risk)
- 3 visualizations (pie charts, bar charts)
- CSV export

**Try it**:
1. Run Streamlit app: `cd streamlit && streamlit run app.py`
2. Navigate to "Segment Explorer"
3. Filter to: "Declining" segment + "High Risk" churn + California
4. Export customer list as CSV

**Code walkthrough**:
```python
# streamlit/tabs/segment_explorer.py

def render(execute_query, conn):
    # 1. Multi-select filters
    segment_options = st.multiselect("Customer Segments", [...])

    # 2. Build dynamic query
    where_clause = build_where_clause(segment_options, churn_risk_options)

    # 3. Execute query
    df = execute_query(query)

    # 4. Display metrics
    st.metric("Total Customers", len(df))

    # 5. Visualizations
    fig = px.pie(df, values='count', names='segment')
    st.plotly_chart(fig)
```

**Documentation**: [docs/prompt_5.1_completion_summary.md](prompt_5.1_completion_summary.md)

#### Tab 2: Customer 360 Deep Dive

**Purpose**: Look up individual customer profiles and transaction history

**Key Features**:
- 3 search methods (ID, name, email)
- 6 key metrics (LTV, avg transaction, 90d spend, etc.)
- Transaction history (last 1,000)
- 2 visualizations (spending trend, category breakdown)

**Try it**:
1. Navigate to "Customer 360"
2. Search by name: "Smith"
3. Select a customer
4. Review transaction history
5. Filter to last 30 days + "Travel" category

**Code walkthrough**:
```python
# streamlit/tabs/customer_360.py

def render(execute_query, conn):
    # 1. Customer search
    if search_method == "Name":
        query = f"SELECT * FROM ... WHERE LOWER(full_name) LIKE LOWER('%{name}%')"

    # 2. Profile display
    st.metric("Lifetime Value", f"${customer['LIFETIME_VALUE']:,.0f}")

    # 3. Transaction history with JOIN
    txn_query = """
        SELECT t.*, c.category_name
        FROM FCT_TRANSACTIONS t
        JOIN DIM_MERCHANT_CATEGORY c ON t.merchant_category_key = c.category_key
        WHERE t.customer_id = ?
        LIMIT 1000
    """

    # 4. Visualizations
    fig = px.line(daily_spend, x='date', y='amount')
```

**Documentation**: [docs/prompt_5.2_completion_summary.md](prompt_5.2_completion_summary.md)

#### Tab 3: AI Assistant

**Purpose**: Ask questions in natural language

**Key Features**:
- 5 question categories
- 20+ suggested questions
- Mock Cortex Analyst (keyword matching)
- Generated SQL display
- Query history (last 5)

**Try it**:
1. Navigate to "AI Assistant"
2. Click "Which customers are at highest risk of churning?"
3. Review generated SQL and results
4. Export results as CSV

**Code walkthrough**:
```python
# streamlit/tabs/ai_assistant.py

# Suggested questions library
SUGGESTED_QUESTIONS = {
    "Churn Analysis": [
        "Which customers are at highest risk of churning?",
        "What is the average churn risk score by segment?",
    ],
    ...
}

# Mock Cortex Analyst
def call_cortex_analyst_mock(conn, question):
    if 'highest risk' in question.lower():
        sql = "SELECT ... WHERE churn_risk_category = 'High Risk'"
    # ... more patterns

    return {'sql': sql, 'results': df, 'error': None}
```

**Documentation**: [docs/prompt_5.3_completion_summary.md](prompt_5.3_completion_summary.md)

#### Tab 4: Campaign Performance Simulator

**Purpose**: Model retention campaign ROI

**Key Features**:
- Target audience selection (segment, churn risk, LTV)
- Campaign parameters (incentive, retention rate, cost)
- ROI calculation with sensitivity analysis
- Breakeven calculation
- Campaign recommendations

**Try it**:
1. Navigate to "Campaign Performance"
2. Select "Declining" + "High Risk"
3. Set incentive to $50, retention rate to 30%
4. Review ROI calculation
5. Check sensitivity analysis (ROI vs retention rate)
6. Export target customer list

**Code walkthrough**:
```python
# streamlit/tabs/campaign_simulator.py

def calculate_campaign_roi(target_customers, incentive, retention_rate, cost):
    # Costs
    total_cost = len(target_customers) * (incentive + cost)

    # Expected retention
    retained = len(target_customers) * (retention_rate / 100)

    # Expected value (20% of LTV as annual value)
    value = retained * avg_ltv * 0.20

    # ROI
    net_benefit = value - total_cost
    roi_pct = (net_benefit / total_cost) * 100

    return {'roi_pct': roi_pct, 'net_benefit': net_benefit, ...}
```

**Documentation**: [docs/prompt_5.4_completion_summary.md](prompt_5.4_completion_summary.md)

---

## Local Development Setup

### 1. Environment Setup

```bash
# Clone repository
git clone <repo-url>
cd snowflake-panel-demo

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# OR use UV (faster)
pip install uv
uv sync
```

### 2. Configure Credentials

```bash
# Copy environment template
cp .env.example .env

# Edit .env
nano .env
```

Update `.env`:
```
SNOWFLAKE_ACCOUNT=abc12345.us-east-1
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=CUSTOMER_ANALYTICS
SNOWFLAKE_SCHEMA=GOLD
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

### 3. Configure dbt

```bash
# Copy dbt profile template
cp dbt_customer_analytics/profiles.yml.example ~/.dbt/profiles.yml

# Edit dbt profile
nano ~/.dbt/profiles.yml
```

Update `~/.dbt/profiles.yml`:
```yaml
customer_analytics:
  outputs:
    dev:
      type: snowflake
      account: abc12345.us-east-1
      user: your_username
      password: your_password
      role: ACCOUNTADMIN
      warehouse: COMPUTE_WH
      database: CUSTOMER_ANALYTICS
      schema: GOLD
      threads: 4
  target: dev
```

### 4. Test Connections

```bash
# Test Snowflake connection
snowsql -a <account> -u <user> -q "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();"

# Test dbt connection
cd dbt_customer_analytics
dbt debug

# Test Python Snowflake connector
cd streamlit
python -c "from dotenv import load_dotenv; import snowflake.connector; load_dotenv(); print('Connection test successful')"
```

---

## Snowflake Deployment

### Option 1: Manual Deployment (Recommended for Learning)

**Step 1: Set Up Database** (5 minutes)
```bash
# Run setup scripts
snowsql -a <account> -u <user> -f snowflake/setup/01_create_database.sql
snowsql -a <account> -u <user> -f snowflake/setup/02_create_schemas.sql
snowsql -a <account> -u <user> -f snowflake/setup/03_create_roles.sql
```

**Step 2: Load Data** (10 minutes)
```bash
# Generate customers
uv run python -m data_generation generate-customers --count 50000 --output data/customers.csv

# Load into Snowflake (upload to stage first)
snowsql -a <account> -u <user> << EOF
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA BRONZE;

-- Create internal stage
CREATE STAGE IF NOT EXISTS customer_data_stage;

-- Upload CSV (from SnowSQL)
PUT file://data/customers.csv @customer_data_stage AUTO_COMPRESS=FALSE;

-- Load into table
COPY INTO raw_customers
FROM @customer_data_stage/customers.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
EOF

# Generate transactions
snowsql -a <account> -u <user> -f snowflake/bronze/02_generate_transactions.sql
```

**Step 3: Run dbt** (15 minutes)
```bash
cd dbt_customer_analytics
dbt run --full-refresh  # First run with full refresh
dbt test  # Validate data quality
cd ..
```

**Step 4: Train ML Model** (5 minutes)
```bash
snowsql -a <account> -u <user> -f snowflake/ml/00_prepare_training_data.sql
snowsql -a <account> -u <user> -f snowflake/ml/01_train_churn_model.sql
snowsql -a <account> -u <user> -f snowflake/ml/02_predict_churn.sql
```

**Step 5: Deploy Semantic Model** (5 minutes)
```bash
cd semantic_layer
./deploy_semantic_model.sh
cd ..
```

**Step 6: Launch Streamlit** (2 minutes)
```bash
cd streamlit
streamlit run app.py
```

### Option 2: Automated Deployment (Faster)

```bash
# Run end-to-end setup script
./scripts/setup_end_to_end.sh

# Follow prompts for:
# - Snowflake credentials
# - Customer count (default 50000)
# - AWS setup (optional)
```

**Script does**:
1. Validates prerequisites
2. Sets up Snowflake database
3. Generates and loads data
4. Runs dbt transformations
5. Trains ML model
6. Deploys semantic model
7. Launches Streamlit app

---

## GitHub Integration

### Overview

Once you've completed local development and testing, you can deploy your code from GitHub to Snowflake using Snowflake's native Git integration. This enables version-controlled deployments and automated synchronization.

### Why Use GitHub Integration?

✅ **Version Control**: All code changes tracked in Git
✅ **Automated Deployment**: Push to GitHub → Sync to Snowflake
✅ **Streamlit from Git**: Deploy Streamlit apps directly from repository
✅ **Team Collaboration**: Multiple developers working on same codebase
✅ **Rollback Capability**: Easily revert to previous versions

### Quick Setup (3 Steps)

#### Step 1: Push to GitHub (5 minutes)

```bash
cd /Users/jpurrutia/projects/snowflake-panel-demo

# Initialize git (if not already done)
git add .
git commit -m "Initial commit: Customer 360 Analytics Platform"

# Create repository on GitHub (via web UI)
# Then add remote and push:
git remote add origin https://github.com/<your-username>/snowflake-panel-demo.git
git push -u origin main
```

#### Step 2: Create Git Integration in Snowflake (10 minutes)

Run the provided SQL script:

```bash
# Edit the script with your GitHub details
vim snowflake/setup/06_create_git_integration.sql

# Update these lines:
# - API_ALLOWED_PREFIXES: Replace <your-github-org> with your username
# - ORIGIN: Replace with your repository URL

# Run the script
snowsql -a <account> -u <user> -f snowflake/setup/06_create_git_integration.sql
```

**What this creates**:
- API integration for GitHub access
- Git repository object in Snowflake
- Streamlit app deployed from GitHub

#### Step 3: Sync and Deploy (2 minutes)

```sql
-- Fetch latest code from GitHub
ALTER GIT REPOSITORY snowflake_panel_demo_repo FETCH;

-- Refresh Streamlit app
ALTER STREAMLIT customer_360_app REFRESH;

-- Get app URL
SELECT SYSTEM$GET_STREAMLIT_APP_URL('customer_360_app');
```

### Ongoing Workflow

After initial setup, your workflow becomes:

```bash
# 1. Make changes locally
vim streamlit/app.py

# 2. Test locally
cd streamlit && streamlit run app.py

# 3. Commit and push to GitHub
git add .
git commit -m "Update feature"
git push origin main

# 4. Sync Snowflake with GitHub
snowsql -a <account> -u <user> -q "
ALTER GIT REPOSITORY CUSTOMER_ANALYTICS.GOLD.snowflake_panel_demo_repo FETCH;
ALTER STREAMLIT CUSTOMER_ANALYTICS.GOLD.customer_360_app REFRESH;
"
```

### Optional: GitHub Actions for Automated Deployment

For fully automated deployment, use the provided GitHub Actions workflow:

1. **Configure GitHub Secrets**:
   - Go to repository → Settings → Secrets and variables → Actions
   - Add secrets:
     - `SNOWFLAKE_ACCOUNT`
     - `SNOWFLAKE_USER`
     - `SNOWFLAKE_PASSWORD`
     - `SNOWFLAKE_ROLE`
     - `SNOWFLAKE_WAREHOUSE`
     - `SNOWFLAKE_DATABASE`
     - `SNOWFLAKE_SCHEMA`

2. **Workflow automatically deploys on push**:
   - Push to main → GitHub Actions runs
   - Streamlit app automatically updated in Snowflake
   - No manual FETCH/REFRESH needed

3. **Monitor deployments**:
   - GitHub repository → Actions tab
   - View deployment logs and status

### Complete Documentation

For detailed setup instructions, troubleshooting, and best practices, see:
- **[docs/GITHUB_DEPLOYMENT_GUIDE.md](GITHUB_DEPLOYMENT_GUIDE.md)** - Complete GitHub integration guide

### What About dbt?

**You'll continue running dbt locally** - no changes needed:

```bash
# Run dbt locally as usual
cd dbt_customer_analytics
dbt run
dbt test
```

**Optional**: If you want to explore dbt Cloud (hosted dbt with scheduling), see:
- **[docs/DBT_CLOUD_SETUP_GUIDE.md](DBT_CLOUD_SETUP_GUIDE.md)** - Optional reference guide

---

## Testing the Platform

### 1. Data Quality Tests

```bash
cd dbt_customer_analytics

# Run all dbt tests
dbt test

# Run specific test
dbt test --select customer_360_profile

# Tests include:
# - Uniqueness (customer_id, transaction_id)
# - Not null (key columns)
# - Relationships (foreign keys)
# - Accepted values (segment names, churn categories)
# - Custom tests (LTV >= 0, churn_risk_score 0-100)
```

### 2. Integration Tests

```bash
# Run all integration tests
pytest tests/integration/ -v

# Run specific tab tests
pytest tests/integration/test_streamlit_segment_explorer.py -v
pytest tests/integration/test_customer_360_tab.py -v
pytest tests/integration/test_ai_assistant_tab.py -v
pytest tests/integration/test_campaign_simulator.py -v

# Total: 35 integration tests
```

### 3. Manual Testing Scenarios

#### Scenario 1: Segment Explorer
1. Open Streamlit app → Segment Explorer
2. Filter to "Declining" segment
3. Add "High Risk" churn filter
4. Add "California" state filter
5. Verify count matches expected (use SQL to validate)
6. Export CSV and verify contents

**Validation SQL**:
```sql
SELECT COUNT(*)
FROM GOLD.CUSTOMER_360_PROFILE
WHERE customer_segment = 'Declining'
  AND churn_risk_category = 'High Risk'
  AND state = 'CA';
```

#### Scenario 2: Customer 360
1. Search for customer ID 1000
2. Verify profile metrics match database
3. Check transaction history loads (should show ≤1000 transactions)
4. Filter to last 30 days
5. Verify chart updates

**Validation SQL**:
```sql
SELECT
    lifetime_value,
    avg_transaction_amount,
    spend_last_90_days
FROM GOLD.CUSTOMER_360_PROFILE
WHERE customer_id = 1000;
```

#### Scenario 3: AI Assistant
1. Click "Which customers are at highest risk of churning?"
2. Verify SQL generated correctly
3. Verify results show only "High Risk" customers
4. Export CSV
5. Check query appears in history

#### Scenario 4: Campaign Simulator
1. Set target audience: "Declining" + "High Risk"
2. Set incentive: $50, retention: 30%, cost: $5
3. Verify ROI calculation:
   - Total cost = (num_customers × $50) + (num_customers × $5)
   - Retained = num_customers × 0.30
   - Value = retained × avg_ltv × 0.20
   - ROI = (value - cost) / cost × 100
4. Check sensitivity analysis shows increasing trend
5. Verify breakeven point is reasonable

---

## Troubleshooting

### Common Issues

#### Issue 1: Snowflake Connection Failed

**Error**: `snowflake.connector.errors.DatabaseError: 250001: Could not connect to Snowflake backend`

**Solutions**:
1. Check account identifier format:
   ```
   ❌ Wrong: https://abc12345.snowflakecomputing.com
   ✅ Right: abc12345.us-east-1
   ```

2. Verify credentials:
   ```bash
   snowsql -a <account> -u <user>
   # Should prompt for password
   ```

3. Check firewall/VPN:
   - Snowflake requires outbound HTTPS (443)
   - Corporate firewalls may block

#### Issue 2: dbt Models Failed

**Error**: `Compilation Error in model ...`

**Solutions**:
1. Check dbt profile:
   ```bash
   dbt debug  # Should show all green checks
   ```

2. Verify database exists:
   ```bash
   snowsql -a <account> -u <user> -q "SHOW DATABASES;"
   ```

3. Run with verbose logging:
   ```bash
   dbt run --select model_name --debug
   ```

#### Issue 3: Streamlit App Crashes

**Error**: `KeyError: 'CUSTOMER_360_PROFILE'`

**Solutions**:
1. Verify table exists:
   ```bash
   snowsql -a <account> -u <user> -q "SELECT COUNT(*) FROM GOLD.CUSTOMER_360_PROFILE;"
   ```

2. Check .env file:
   ```bash
   cat .env | grep SNOWFLAKE_
   ```

3. Clear Streamlit cache:
   ```bash
   streamlit cache clear
   ```

#### Issue 4: ML Model Training Failed

**Error**: `Cortex ML not available in this account`

**Solutions**:
1. Verify Cortex ML is enabled:
   ```sql
   SHOW FUNCTIONS IN ACCOUNT;
   -- Should see SNOWFLAKE.ML.CLASSIFICATION
   ```

2. Check account edition:
   - Cortex ML requires Enterprise or Business Critical
   - Upgrade if on Standard edition

3. Contact Snowflake support to enable Cortex features

#### Issue 5: No Data After dbt Run

**Error**: Tables exist but have 0 rows

**Solutions**:
1. Check Bronze layer:
   ```sql
   SELECT COUNT(*) FROM BRONZE.raw_customers;
   SELECT COUNT(*) FROM BRONZE.raw_transactions;
   ```

2. If Bronze is empty, reload data:
   ```bash
   snowsql -a <account> -u <user> -f snowflake/bronze/01_load_customers_from_stage.sql
   ```

3. Re-run dbt with full refresh:
   ```bash
   dbt run --full-refresh
   ```

### Getting Help

1. **Check Documentation**:
   - Main README: [README.md](../README.md)
   - Iteration summaries: `docs/prompt_5.*_completion_summary.md`
   - Component guides: `docs/*.md`

2. **Review Test Output**:
   ```bash
   pytest tests/integration/ -v --tb=short
   dbt test --debug
   ```

3. **Enable Debug Logging**:
   ```python
   # In streamlit/app.py
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

4. **Snowflake Query History**:
   - Log into Snowsight
   - Navigate to Activity → Query History
   - Review failed queries

---

## Next Steps

### Immediate (Day 1-2)

✅ **Complete Quick Start** (this guide)
- [x] Set up Snowflake database
- [x] Load sample data
- [x] Run dbt transformations
- [x] Launch Streamlit app

✅ **Explore Each Tab**
- [x] Segment Explorer: Filter and export customers
- [x] Customer 360: Look up individual profiles
- [x] AI Assistant: Ask natural language questions
- [x] Campaign Simulator: Model campaign ROI

✅ **Review Documentation**
- [x] [README.md](../README.md) - Project overview
- [x] [docs/star_schema_design.md](star_schema_design.md) - Data model
- [x] [docs/customer_segmentation_guide.md](customer_segmentation_guide.md) - Segmentation logic

### Short-term (Week 1)

📚 **Deep Dive into Components**
- [ ] Study dbt models (lineage, transformations)
- [ ] Understand ML model features and performance
- [ ] Review semantic model for Cortex Analyst
- [ ] Analyze Streamlit code patterns

🧪 **Run All Tests**
- [ ] dbt tests: `dbt test`
- [ ] Integration tests: `pytest tests/integration/ -v`
- [ ] Manual testing scenarios (see Testing section)

📊 **Experiment with Data**
- [ ] Modify segmentation logic in `dbt_customer_analytics/models/marts/customer_segments.sql`
- [ ] Adjust churn definition in `snowflake/ml/00_prepare_training_data.sql`
- [ ] Add new questions to AI Assistant
- [ ] Create custom filters in Segment Explorer

### Medium-term (Month 1)

🚀 **Deploy to Production**
- [ ] Set up production Snowflake account
- [ ] Configure AWS S3 for data lake (if using AWS)
- [ ] Deploy Streamlit in Snowflake (SiS)
- [ ] Enable production Cortex Analyst
- [ ] Set up scheduled dbt runs (daily/weekly)
- [ ] Configure automated ML retraining (monthly)

🎨 **Customize for Your Use Case**
- [ ] Replace synthetic data with real data sources
- [ ] Adjust customer segmentation criteria
- [ ] Add new metrics to hero_metrics model
- [ ] Create additional Streamlit tabs
- [ ] Customize campaign recommendations

📈 **Monitor and Optimize**
- [ ] Set up Snowflake resource monitors
- [ ] Review query performance (clustering, materialization)
- [ ] Monitor ML model performance (F1, precision, recall)
- [ ] Track campaign ROI (predicted vs actual)

### Long-term (Quarter 1)

🔧 **Advanced Features**
- [ ] Real-time transaction ingestion (Snowpipe)
- [ ] A/B testing for campaigns
- [ ] Advanced ML features (feature engineering)
- [ ] Row-level security for multi-tenant access
- [ ] Mobile-responsive Streamlit UI
- [ ] Automated alerting (high churn risk, declining spend)

📚 **Knowledge Sharing**
- [ ] Create video walkthrough
- [ ] Present to team
- [ ] Document customizations
- [ ] Share best practices

---

## Appendix: Project Structure

```
snowflake-panel-demo/
├── README.md                           # Main project overview
├── .env.example                        # Environment variable template
├── requirements.txt                    # Python dependencies
├── pyproject.toml                      # UV package config
│
├── docs/                               # Documentation
│   ├── ONBOARDING_GUIDE.md             # This file
│   ├── star_schema_design.md           # Data model guide
│   ├── customer_segmentation_guide.md  # Segmentation logic
│   ├── aggregate_marts_guide.md        # Hero metrics guide
│   ├── ml_model_card.md                # ML model documentation
│   ├── prompt_5.1_completion_summary.md # Iteration 5.1 summary
│   ├── prompt_5.2_completion_summary.md # Iteration 5.2 summary
│   ├── prompt_5.3_completion_summary.md # Iteration 5.3 summary
│   └── prompt_5.4_completion_summary.md # Iteration 5.4 summary
│
├── terraform/                          # AWS infrastructure
│   ├── main.tf                         # Provider config
│   ├── s3.tf                           # S3 bucket
│   ├── iam.tf                          # IAM roles
│   └── README.md                       # Terraform guide
│
├── snowflake/                          # Snowflake SQL scripts
│   ├── setup/                          # Database setup
│   │   ├── 01_create_database.sql
│   │   ├── 02_create_schemas.sql
│   │   └── 03_create_roles.sql
│   ├── bronze/                         # Data loading
│   │   ├── 01_load_customers_from_stage.sql
│   │   └── 02_generate_transactions.sql
│   └── ml/                             # Machine learning
│       ├── 00_prepare_training_data.sql
│       ├── 01_train_churn_model.sql
│       └── 02_predict_churn.sql
│
├── dbt_customer_analytics/             # dbt transformations
│   ├── dbt_project.yml                 # dbt config
│   ├── profiles.yml.example            # Connection template
│   ├── models/
│   │   ├── bronze/                     # Staging models
│   │   ├── silver/                     # Cleaned models
│   │   ├── gold/                       # Star schema
│   │   │   ├── dim_customer.sql        # Customer dimension (SCD2)
│   │   │   ├── dim_date.sql            # Date dimension
│   │   │   ├── dim_merchant_category.sql
│   │   │   └── fct_transactions.sql    # Fact table
│   │   └── marts/                      # Business aggregates
│   │       ├── customer_segments.sql   # 5 segments
│   │       ├── hero_metrics.sql        # LTV, MoM, ATV
│   │       └── customer_360_profile.sql # Denormalized view
│   └── tests/                          # dbt tests
│
├── data_generation/                    # Synthetic data generators
│   ├── __init__.py
│   └── customer_generator.py           # 50K customers
│
├── semantic_layer/                     # Cortex Analyst
│   ├── semantic_model.yaml             # Semantic model definition
│   ├── deploy_semantic_model.sh        # Deployment script
│   └── README.md                       # Semantic layer guide
│
├── streamlit/                          # Streamlit app
│   ├── app.py                          # Main entry point
│   ├── tabs/
│   │   ├── __init__.py
│   │   ├── segment_explorer.py         # Tab 1
│   │   ├── customer_360.py             # Tab 2
│   │   ├── ai_assistant.py             # Tab 3
│   │   └── campaign_simulator.py       # Tab 4
│   ├── requirements.txt                # Streamlit dependencies
│   ├── .env.example                    # Credentials template
│   └── README.md                       # Streamlit guide
│
└── tests/                              # Test suite
    └── integration/                    # Integration tests
        ├── test_streamlit_segment_explorer.py    # 9 tests
        ├── test_customer_360_tab.py              # 10 tests
        ├── test_ai_assistant_tab.py              # 9 tests
        └── test_campaign_simulator.py            # 7 tests
```

---
