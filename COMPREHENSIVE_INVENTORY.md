# SNOWFLAKE-PANEL-DEMO: COMPREHENSIVE FEATURE INVENTORY

**Project Name:** Customer 360 Analytics Platform  
**Status:** CORE PIPELINE COMPLETE - Ready for Production  
**Last Updated:** 2025-11-19  
**Architecture:** Bronze → Silver → Gold → ML → Streamlit UI  

---

## TABLE OF CONTENTS

1. [STREAMLIT APPLICATION](#streamlit-application)
2. [DBT TRANSFORMATION PIPELINE](#dbt-transformation-pipeline)
3. [MACHINE LEARNING (CHURN PREDICTION)](#machine-learning-churn-prediction)
4. [SEMANTIC LAYER (CORTEX ANALYST)](#semantic-layer-cortex-analyst)
5. [DATA GENERATION & INGESTION](#data-generation--ingestion)
6. [SNOWFLAKE INFRASTRUCTURE](#snowflake-infrastructure)
7. [TERRAFORM PROVISIONING](#terraform-provisioning)
8. [DATA QUALITY & TESTING](#data-quality--testing)
9. [DOCUMENTATION](#documentation)

---

## STREAMLIT APPLICATION

### Overview
Interactive dashboard for customer analytics with 4 main tabs. Deployed locally and via Streamlit in Snowflake.

**Location:** `/Users/jpurrutia/projects/snowflake-panel-demo/streamlit/`

---

### 1. AI Assistant Tab
**File:** `streamlit/tabs/ai_assistant.py`  
**Status:** ✅ ACTIVE/WORKING  
**Purpose:** Natural language query interface powered by Snowflake Cortex Analyst

**Features:**
- Natural language question input
- 5 question categories with 20+ suggested questions
- Cortex Analyst integration with mock fallback
- Auto-detection of appropriate chart types
- Chart rendering (bar, line, pie, scatter, histogram, area, sunburst, choropleth maps)
- Generated SQL display
- Query history with last 5 queries
- Results export to CSV
- Conversation context for multi-turn interactions

**How to Use:**
1. Select a question category (Churn Analysis, Segmentation, Spending Trends, Geographic, Campaign)
2. Click suggested questions OR type custom question
3. View results as table or interactive chart
4. Export results as CSV
5. Access query history for previous queries

**Demo Talking Points:**
- Democratizes data access: business users ask questions in English
- No SQL required - AI generates SQL automatically
- Intelligent chart type selection based on data
- Falls back to pre-written SQL if Cortex Analyst unavailable
- Real-time conversation history tracking
- Multiple visualization options (maps, charts, tables)

---

### 2. Customer 360 Deep Dive Tab
**File:** `streamlit/tabs/customer_360.py`  
**Status:** ✅ ACTIVE/WORKING  
**Purpose:** Individual customer profile and transaction analysis

**Features:**
- Customer search (by ID, Name, Email with partial matching)
- Profile summary with key demographics
- Churn risk alerts (High/Medium/Low with color coding)
- Key metrics display (LTV, Avg Transaction, 90-day spend, days since last txn)
- 90-day spend change percentage with trend
- Average monthly spending calculation
- Transaction history filtering (date range, category, status)
- Visualizations:
  - Daily spending trend line chart
  - Category breakdown pie chart
- Transaction table (1,000 most recent with formatting)
- Transaction summary metrics
- CSV export of transactions

**How to Use:**
1. Search by Customer ID, Name, or Email
2. Select customer if multiple results
3. View profile with churn risk indicator
4. Review key metrics and spending trends
5. Filter transaction history by date/category/status
6. Export transaction history

**Demo Talking Points:**
- Single unified view of customer activity
- Churn risk prominently highlighted
- Spending trends show recent behavior changes
- Transaction-level detail available for deep analysis
- Visual trends help identify patterns quickly
- Export for external use (Excel, presentations)

---

### 3. Segment Explorer Tab
**File:** `streamlit/tabs/segment_explorer.py`  
**Status:** ✅ ACTIVE/WORKING  
**Purpose:** Customer segmentation with export capabilities for marketing

**Features:**
- Multi-select filters:
  - Customer segments (5 types)
  - US states (all 50)
  - Churn risk levels (3 levels)
  - Card type (Standard/Premium)
  - Min lifetime value threshold
- Summary metrics (customer count, total LTV, avg LTV, avg churn risk)
- Visualizations:
  - Segment distribution pie chart
  - Churn risk distribution bar chart
  - LTV by segment bar chart
- Customer list table (5,000 max)
- CSV export for marketing campaigns
- Hint for future integrations (Salesforce, HubSpot, Google Ads)

**How to Use:**
1. Select filter criteria (segments, states, risk levels, card types, LTV)
2. Click "Apply Filters"
3. View summary metrics and visualizations
4. Review customer list
5. Export CSV for marketing use

**Demo Talking Points:**
- Quick segmentation for targeted campaigns
- Multi-dimensional filtering (segment, geography, risk, card type)
- Export ready for external marketing platforms
- Summary metrics show total addressable market
- Visual breakdown helps with prioritization
- Foundation for multi-channel marketing

---

### 4. Campaign Performance Simulator
**File:** `streamlit/tabs/campaign_simulator.py`  
**Status:** ✅ ACTIVE/WORKING  
**Purpose:** Marketing ROI analysis for retention campaigns

**Features:**
- Target audience selection:
  - Customer segments (multi-select)
  - Churn risk levels (multi-select)
  - Card types (multi-select)
  - Min LTV threshold
  - Min churn risk score threshold
- Campaign parameters:
  - Incentive per customer ($0-$500)
  - Expected retention rate (0-100%)
  - Campaign cost per customer ($0-$100)
- ROI calculation and display:
  - Target customers count
  - Total campaign cost
  - Expected retained customers
  - ROI percentage
  - Net benefit ($)
- Visualizations:
  - Cost breakdown pie chart (Incentives vs Operations)
  - Sensitivity analysis line chart (ROI vs Retention Rate)
- Breakeven analysis (minimum retention rate for positive ROI)
- Top 10 highest risk customers display
- Campaign recommendations (messaging, timing, success metrics)
- Full target list CSV export

**How to Use:**
1. Define target audience with filters
2. Click "Find Target Audience"
3. Set campaign parameters (incentive, retention rate, costs)
4. Review ROI metrics and visualizations
5. Check sensitivity analysis for different retention rates
6. Export target customer list

**Demo Talking Points:**
- Quantifies ROI for retention campaigns
- Sensitivity analysis shows breakeven points
- Personalized recommendations based on audience
- Cost breakdown transparency
- Export for immediate campaign execution
- Data-driven decision making for marketing spend

---

### Streamlit App Configuration

**Main Entry Point:** `streamlit/app.py`

**Features:**
- Dark theme (Snowflake brand colors)
- Cached Snowflake connection management
- Error handling for query timeouts
- Safe execution in Streamlit in Snowflake environment
- Responsive layout (wide view)
- Custom CSS styling

**Requirements:**
```
streamlit==1.30.0
snowflake-connector-python[pandas]==3.5.0
pandas==2.1.4
plotly==5.18.0
pydeck==0.8.0  # For choropleth maps (requires External Access Integration)
python-dotenv==1.0.0
```

**Deployment:**
- Local: `streamlit run app.py`
- Snowflake: Via `snow streamlit deploy --replace`
- Configuration: `snowflake.yml`

---

## DBT TRANSFORMATION PIPELINE

### Overview
dbt project implementing medallion architecture (Bronze → Silver → Gold) with star schema dimensional modeling.

**Location:** `/Users/jpurrutia/projects/snowflake-panel-demo/dbt_customer_analytics/`

**Current Version:** 1.10.13  
**Adapter:** snowflake 1.10.3

---

### Data Layers

#### Bronze Layer (Raw Data)
**Status:** ✅ COMPLETE  
**Location:** `CUSTOMER_ANALYTICS.BRONZE`

**Tables:**
1. **BRONZE_CUSTOMERS** (50,000 rows)
   - Raw customer data from S3
   - Loaded via COPY INTO
   
2. **BRONZE_TRANSACTIONS** (60,000,000 rows)
   - Raw transaction data
   - Generated via GENERATOR() function
   
3. **BRONZE_MERCHANT_CATEGORIES** (50 rows)
   - Merchant category reference data

**Data Sources:**
- Customers: Python generator → CSV → S3 → Snowflake
- Transactions: Snowflake GENERATOR() function
- Categories: Hardcoded in SQL

---

#### Silver Layer (Staging/Cleaning)
**Status:** ✅ COMPLETE  
**Location:** `CUSTOMER_ANALYTICS.SILVER`  
**Models:** `dbt_customer_analytics/models/staging/`

**Tables:**

1. **STG_CUSTOMERS**
   - File: `stg_customers.sql`
   - Purpose: Clean and standardize customer data
   - Transformations:
     - Type casting (integer, decimal, timestamp)
     - Email normalization
     - Name standardization
     - Materialized as VIEW

2. **STG_TRANSACTIONS**
   - File: `stg_transactions.sql`
   - Purpose: Clean transaction data
   - Transformations:
     - Type casting
     - Null handling
     - Amount validation
     - Materialized as VIEW

3. **STG_MERCHANT_CATEGORIES**
   - File: `stg_merchant_categories.sql`
   - Purpose: Clean category data
   - Materialized as VIEW

**Key Characteristics:**
- All materialized as VIEWs (lightweight)
- Handle data type conversions
- Remove/fix malformed records
- Document expected data quality

---

#### Gold Layer (Business Logic & Analytics)
**Status:** ✅ COMPLETE  
**Location:** `CUSTOMER_ANALYTICS.GOLD`  
**Models:** `dbt_customer_analytics/models/marts/`

##### Dimensions

**1. DIM_CUSTOMER** (SCD Type 2)
- File: `models/marts/core/dim_customer.sql`
- Grain: One row per customer version
- Keys: 
  - `customer_key` (surrogate key, unique per version)
  - `customer_id` (natural key, same across versions)
- Slowly Changing Attributes (Type 2 - tracks history):
  - `card_type` (Standard/Premium changes)
  - `credit_limit` (limit increases/decreases)
- Type 1 Attributes (overwrite):
  - Demographics (name, age, location, employment)
  - Segment, decline_type
- SCD Fields:
  - `valid_from` (effective start date)
  - `valid_to` (effective end date)
  - `is_current` (boolean flag for current version)
- Clustering: By customer_id for performance
- Materialized as TABLE

**Usage Examples:**
```sql
-- Get current customer records only
SELECT * FROM dim_customer WHERE is_current = TRUE;

-- Get customer history
SELECT * FROM dim_customer 
WHERE customer_id = 'CUST00000001'
ORDER BY valid_from;

-- Track when card was upgraded
SELECT * FROM dim_customer
WHERE card_type = 'Premium' AND valid_from > '2025-01-01';
```

**2. DIM_MERCHANT_CATEGORY**
- File: `models/marts/core/dim_merchant_category.sql`
- Grain: One row per category
- Keys:
  - `category_key` (surrogate key)
  - `category_id` (natural key)
- Attributes:
  - `category_name` (Grocery, Travel, Dining, etc.)
  - `category_group` (Leisure, Necessities, Other)
  - `is_discretionary` (boolean flag)
- Row Count: ~20 categories
- Materialized as TABLE

**3. DIM_DATE**
- File: `models/marts/core/dim_date.sql`
- Grain: One row per calendar day
- Date Range: 580 days of history
- Attributes:
  - Date components (year, month, day, quarter, week)
  - Day-of-week, day-of-month
  - Holiday indicators
  - Fiscal period info
- Materialized as TABLE

##### Fact Tables

**1. FCT_TRANSACTIONS**
- File: `models/marts/core/fct_transactions.sql`
- Grain: One row per transaction
- Row Count: ~13.5M transactions
- Keys:
  - Foreign keys: `customer_key`, `merchant_category_key`, `date_key`
  - `transaction_key` (surrogate key)
  - `transaction_id` (natural key)
- Metrics:
  - `transaction_amount` (approved amounts only)
  - Transaction count
- Attributes:
  - `merchant_name`
  - `channel` (Online, In-Store, Mobile)
  - `status` (approved, declined)
- Clustering: By transaction_date (partition-like performance)
- Materialized as TABLE

**Usage Examples:**
```sql
-- Daily spending trends
SELECT DATE(transaction_date) as txn_date, SUM(transaction_amount) as daily_spend
FROM fct_transactions
WHERE customer_key IN (...)
GROUP BY DATE(transaction_date)
ORDER BY txn_date;

-- Category spending
SELECT c.category_name, SUM(t.transaction_amount) as category_spend
FROM fct_transactions t
JOIN dim_merchant_category c ON t.merchant_category_key = c.category_key
WHERE t.customer_key = ?
GROUP BY c.category_name;
```

##### Analytics Marts

**1. CUSTOMER_360_PROFILE** (Hero Metrics View)
- File: `models/marts/customer_analytics/customer_360_profile.sql`
- Grain: One row per customer (current state)
- Row Count: ~50,000 customers
- Purpose: Denormalized view optimized for dashboard consumption
- Contains:
  - All demographics from `dim_customer`
  - Customer segment classification
  - Lifetime metrics (LTV, ATV, transaction count)
  - Recent activity (90-day spend, prior 90d, spend change %)
  - Category preferences (travel %, necessities %)
  - Credit utilization percentage
  - Churn risk score & category (from ML)
  - Recency status (Active/Recent/At Risk/Inactive)
  - Days since last transaction
- Materialized as TABLE
- Optimized for single customer lookups (<1 second)

**Query Examples:**
```sql
-- High-value customers
SELECT customer_id, full_name, lifetime_value
FROM customer_360_profile
WHERE customer_segment = 'High-Value Travelers'
ORDER BY lifetime_value DESC LIMIT 100;

-- Churn risk dashboard
SELECT customer_segment, churn_risk_category, COUNT(*) as customer_count,
       AVG(churn_risk_score) as avg_risk
FROM customer_360_profile
GROUP BY customer_segment, churn_risk_category;
```

**2. CUSTOMER_SEGMENTS**
- File: `models/marts/customer_analytics/customer_segments.sql`
- Grain: One row per customer segment classification
- Segments (RFM-based with custom logic):
  - **High-Value Travelers** (10-15%): $5K+/month, 25%+ travel spend
  - **Stable Mid-Spenders** (40-50%): Consistent moderate behavior
  - **Budget-Conscious** (20-25%): <$1.5K/month, 60%+ necessities
  - **New & Growing** (10-15%): <6 months old, +50% growth
  - **Declining** (5-10%): -30%+ spend decrease (churn risk)
- Attributes:
  - Rolling 90-day window metrics
  - Spend trends
  - Category preferences
  - Tenure in months
- Updated daily
- Materialized as TABLE

**3. Monthly Metrics Tables**
- `metric_customer_ltv.sql`: Lifetime value by customer
- `metric_avg_transaction_value.sql`: ATV by customer
- `monthly_customer_spending.sql`: Monthly aggregations

---

### dbt Configuration

**Profile:** `customer_analytics` (Snowflake-specific)  
**Target:** `dev` schema

**Schema Routing:**
- Raw models → BRONZE schema
- Staging models → SILVER schema
- Gold/Marts → GOLD schema

**Custom Macros:**
- `get_custom_schema.sql`: Routes models to correct schema
- `recalculate_segments.sql`: Daily segment refresh logic
- `test_scd_type_2_no_gaps.sql`: Validates SCD Type 2 integrity

**Tests:**
- `assert_scd_type_2_integrity.sql`: Ensures no overlapping versions
- `assert_scd_type_2_no_gaps.sql`: Validates continuous date coverage
- `assert_segment_distribution.sql`: Validates segment percentages

---

## MACHINE LEARNING (CHURN PREDICTION)

### Overview
Snowflake Cortex ML-powered churn prediction model identifying at-risk customers.

**Location:** `/Users/jpurrutia/projects/snowflake-panel-demo/snowflake/ml/`  
**Status:** ✅ DEPLOYED TO PRODUCTION

---

### Business Context

**Churn Definition:**
Customer churned if **EITHER**:
- No transactions for 60+ days (inactivity)
- Recent spending < 30% of baseline (significant decline)

**Risk Categories:**
- **Low Risk (0-39):** 70-80% of customers → Normal marketing
- **Medium Risk (40-69):** 15-25% of customers → Engagement campaigns
- **High Risk (70-100):** 5-10% of customers → Retention offers

---

### Model Architecture

**Framework:** Snowflake ML Classification (`SNOWFLAKE.ML.CLASSIFICATION`)  
**Model Type:** Binary classification  
**Target Variable:** `churned` (BOOLEAN)  
**Training Data:** 40K-45K labeled customers  
**Feature Count:** 35+ engineered features

---

### Training Data Features

**Demographics:**
- `age` (28.5% importance - TOP)
- `state`
- `employment_status`
- `card_type`

**Account Metrics:**
- `credit_limit`
- `account_tenure_months`
- `credit_utilization_pct`

**Spending Behavior:**
- `lifetime_value` (13.7% importance)
- `avg_transaction_value` (6.5% importance)
- `spend_change_pct`
- `spend_momentum`
- `travel_spend_pct`
- `necessities_spend_pct`

**Recency/Frequency:**
- `days_since_last_transaction`
- `transaction_count`
- `transactions_last_90_days`
- `recency_status`

**Segmentation:**
- Customer segment (one-hot encoded: 5 categories)
- Churn reason (15.8% importance - TOP)

**Derived Metrics:**
- Monthly spend trends
- Seasonal adjustments
- Volatility measures

---

### Pipeline Steps

**File:** `snowflake/ml/01_create_churn_labels.sql`  
**Purpose:** Label customers as churned/active
- Defines churn criteria (60+ day inactivity OR <30% baseline spend)
- Creates CHURN_LABELS table with 50K labeled customers
- Overall churn rate: 3.28% (realistic for credit cards)

**File:** `snowflake/ml/02_create_training_features.sql`  
**Purpose:** Engineer 35+ features for model training
- Aggregates transaction data
- Calculates rolling averages
- Creates derived metrics
- Produces ML_TRAINING_DATA table

**File:** `snowflake/ml/03_train_churn_model.sql`  
**Purpose:** Train Cortex ML classification model
- Model name: `CHURN_MODEL`
- Trains on ML_TRAINING_DATA
- Uses target variable: `churned`
- Creates reusable model for predictions

**File:** `snowflake/ml/04_validate_model_performance.sql`  
**Purpose:** Evaluate model performance
- Calculates metrics: F1, Precision, Recall, AUC
- Current performance: **F1=1.0** (synthetic data has clear separation)
- Real-world expectation: F1 = 0.50-0.70

**File:** `snowflake/ml/05_apply_predictions.sql`  
**Purpose:** Score all customers with churn predictions
- Creates CHURN_PREDICTIONS table
- Scores 50K customers (those with ≥5 transactions)
- Generates risk scores 0-100
- Maps to risk categories (Low/Medium/High)

---

### Model Performance

**Metrics (on synthetic data):**
- **F1 Score:** 1.0 (perfect precision + recall balance)
- **Precision:** 1.0 (100% of predicted churners actually churn)
- **Recall:** 1.0 (catches 100% of actual churners)
- **AUC:** 1.0 (perfect discrimination)

**Feature Importance (Top 5):**
1. Age - 28.5%
2. Churn Reason - 15.8%
3. Lifetime Value - 13.7%
4. Credit Limit - 9.0%
5. Avg Transaction Value - 6.5%

---

### Operational Usage

**Viewing High-Risk Customers:**
```sql
SELECT customer_id, full_name, churn_risk_score, lifetime_value
FROM GOLD.CUSTOMER_360_PROFILE
WHERE churn_risk_category = 'High Risk'
ORDER BY churn_risk_score DESC
LIMIT 100;
```

**Retrain Model (Manual):**
```sql
CALL RETRAIN_CHURN_MODEL();
```

**Refresh Predictions (Daily):**
```sql
CALL REFRESH_CHURN_PREDICTIONS();
```

**Segment Analysis:**
```sql
SELECT customer_segment, churn_risk_category, COUNT(*) as customer_count
FROM GOLD.CUSTOMER_360_PROFILE
GROUP BY customer_segment, churn_risk_category;
```

---

### Retraining Strategy

**Current:** Monthly retraining via `RETRAIN_CHURN_MODEL()` stored procedure  
**Trigger:** New transaction data, seasonal patterns, model drift detection  
**Validation:** Compare new model F1/Precision/Recall vs current production model

---

### Integration Points

- **Customer 360 Profile:** Churn risk score & category included
- **Segment Explorer:** Filter by risk category
- **Campaign Simulator:** Target by risk level
- **AI Assistant:** Natural language queries on churn risk
- **Semantic Layer:** Churn metrics exposed to Cortex Analyst

---

## SEMANTIC LAYER (CORTEX ANALYST)

### Overview
Semantic model enabling natural language queries over customer analytics data.

**Location:** `/Users/jpurrutia/projects/snowflake-panel-demo/semantic_layer/`  
**Status:** ✅ READY FOR DEPLOYMENT  
**Framework:** Snowflake Cortex Analyst

---

### Configuration

**File:** `semantic_model.yaml`  
**Version:** 1.0  
**Name:** `customer_analytics_semantic_model`

**Deployment:**
```bash
cd semantic_layer
./deploy_semantic_model.sh
```

**Testing:**
```bash
snowsql -f test_semantic_model.sql
pytest tests/integration/test_semantic_layer.py -v
```

---

### Base Tables (4)

#### 1. customer_360_profile (Primary)
**Purpose:** Complete customer profile  
**Row Count:** ~50,000 customers

**Dimensions (40+):**
- **Identifiers:** customer_id, full_name
- **Demographics:** age, state, city, employment_status
- **Account:** card_type, credit_limit, account_open_date
- **Segmentation:** customer_segment
- **Risk:** churn_risk_category, recency_status
- **Profile:** spending_profile, eligibility flags

**Metrics (30+):**
- **Lifetime:** lifetime_value, total_transactions, avg_transaction_value
- **Activity:** spend_last_90_days, spend_prior_90_days, days_since_last_transaction
- **Trends:** spend_change_pct, avg_monthly_spend
- **Profile:** travel_spend_pct, necessities_spend_pct, credit_utilization_pct
- **Risk:** churn_risk_score

#### 2. fct_transactions
**Purpose:** Detailed transaction data  
**Row Count:** ~13.5M transactions (18 months)

**Dimensions:**
- transaction_key, transaction_id, transaction_date
- merchant_name, channel, status

**Metrics:**
- transaction_amount, transaction_count

#### 3. dim_merchant_category
**Purpose:** Merchant category classification  
**Row Count:** ~20 categories

**Dimensions:**
- category_key, category_name, category_group, is_discretionary

#### 4. customer_segments
**Purpose:** Segment classification  
**Row Count:** ~50,000 customers

**Dimensions:**
- customer_segment, segment_assigned_date

**Metrics:**
- tenure_months

---

### Sample Questions (50+)

**Churn Risk (6):**
- Which customers are at highest risk of churning?
- What is the average churn risk score by segment?
- Show me High-Value Travelers with high churn risk
- Which states have the highest churn risk?
- Show me customers who haven't transacted in over 60 days
- Which Premium cardholders are at risk?

**Segmentation (5):**
- How many customers in each segment?
- Compare lifetime value across segments
- Show Budget-Conscious customers who increased spending
- What is average monthly spend by segment?
- Which segments have the highest churn risk?

**Spending (5):**
- Show spending trends in travel over last 6 months
- Which customers increased spending the most?
- What is average transaction value by card type?
- Which customers have declining spend trends?
- What was total spending in last 90 days?

**Geographic (4):**
- What is average lifetime value by state?
- Which states have most Premium cardholders?
- Show customer distribution across states
- Compare spending between California and Texas

**Campaign (3):**
- Show customers eligible for retention campaigns
- Find customers with declining spend in last 90 days
- Show high-value customers with low recent activity

**Advanced (3):**
- What is correlation between age and lifetime value?
- Distribution of churn risk scores
- Which spending categories predict churn?

---

### Relationships

**fct_transactions → customer_360_profile**
- Join: customer_id (many-to-one)
- Purpose: Trace transactions to customer

**fct_transactions → dim_merchant_category**
- Join: merchant_category_key (many-to-one)
- Purpose: Categorize transactions

**customer_360_profile → customer_segments**
- Join: customer_id (one-to-one)
- Purpose: Add segment classification

---

### Query Optimization

**Recommended Filters by Table:**
- customer_360_profile: customer_segment, state, churn_risk_category, card_type
- fct_transactions: transaction_date, status
- dim_merchant_category: category_group
- customer_segments: customer_segment

**Clustering Keys:**
- customer_360_profile: customer_id
- fct_transactions: transaction_date
- customer_segments: customer_id

---

### Integration with AI Assistant

**Flow:**
1. User asks question in natural English
2. Cortex Analyst reads semantic_model.yaml
3. AI generates SQL from semantic definitions
4. SQL executes on Snowflake
5. Results returned with natural language interpretation

**Mock Fallback:**
- If Cortex Analyst unavailable, maps common questions to pre-written SQL
- 5+ pre-mapped questions ensure functionality
- Seamless fallback transparent to users

---

## DATA GENERATION & INGESTION

### Overview
Synthetic data pipeline generating realistic credit card customer data.

**Location:** `/Users/jpurrutia/projects/snowflake-panel-demo/data_generation/`

---

### Customer Data Generation

**File:** `customer_generator.py`  
**Status:** ✅ COMPLETE  
**Generated:** 50,000 synthetic customers

**Data Points Generated:**
- customer_id (CUST00000001 format)
- first_name, last_name
- email
- age (22-75)
- state (all 50 US states)
- city
- employment_status (Full-Time, Part-Time, Self-Employed, Retired, Unemployed)
- card_type (Standard or Premium)
- credit_limit ($5K-$50K, in $1K increments)
- account_open_date (0.5 - 3.5 years ago)
- customer_segment (5 behavioral segments)
- decline_type (for Declining segment only)

**Segment Distributions:**
- High-Value Travelers: 10-15%
- Stable Mid-Spenders: 40-50%
- Budget-Conscious: 20-25%
- New & Growing: 10-15%
- Declining: 5-10%

**Segment Characteristics:**

**High-Value Travelers:**
- Monthly spend: $5,000+
- Card type: 70% Premium
- Age: 35-60
- Travel spend: 25%+
- Employment: Mostly full-time, some self-employed

**Stable Mid-Spenders:**
- Monthly spend: $1,500-$3,000
- Card type: 40% Premium
- Age: 25-55
- Balanced spending
- Employment: Mix of all types

**Budget-Conscious:**
- Monthly spend: <$1,500
- Card type: 10% Premium
- Age: 20-45
- Necessities spend: 60%+
- Employment: All types, younger avg age

**New & Growing:**
- Account age: <6 months
- Card type: 30% Premium
- Growth rate: +50%+/month
- Spend ramping up
- Employment: Mostly younger, employed

**Declining:**
- Monthly spend: -30%+ trend
- Churn reason: Job loss, relocation, lifestyle
- Account age: 1-3 years
- Historical spend higher
- Employment: Various status changes

---

### Transaction Data Generation

**File:** `snowflake/data_generation/generate_transactions.sql`  
**Status:** ✅ COMPLETE  
**Generated:** 60,000,000 synthetic transactions

**Generation Method:**
- Uses Snowflake's GENERATOR() table function
- Realistic transaction patterns per segment
- Clustered by transaction_date

**Attributes:**
- transaction_id (TXN########## format)
- customer_id (linked to customers)
- merchant_name (Faker-generated)
- transaction_date (spread over 18 months)
- transaction_amount ($5-$500)
- channel (Online 40%, In-Store 40%, Mobile 20%)
- status (approved 98%, declined 2%)
- merchant_category_id (linked to categories)

**Realism Features:**
- Seasonal patterns
- High-value customer higher transaction amounts
- Travel customers: more airline/hotel transactions
- Budget-conscious: more grocery/gas
- Declined transactions: higher for declining segment

---

### S3 Integration

**Purpose:** Staging location for data loads  
**Terraform:** AWS S3 bucket + IAM role provisioned

**Upload Flow:**
```bash
uv run python -m data_generation upload-customers \
    --file data/customers.csv \
    --bucket <your-s3-bucket>
```

**Snowflake Integration:**
- Storage integration created (Terraform)
- Stage defined in Snowflake
- COPY INTO commands load from S3

---

### Data Loading Scripts

**Files:**
- `snowflake/load/load_customers_bulk.sql`: COPY INTO for customers
- `snowflake/load/load_transactions_bulk.sql`: COPY INTO for transactions
- `snowflake/load/verify_customer_load.sql`: Validation checks
- `snowflake/load/verify_transaction_load.sql`: Validation checks

**Process:**
1. Generate synthetic data (Python/SQL)
2. Upload to S3 (if needed)
3. Execute COPY INTO from S3 to BRONZE tables
4. Validate row counts and data quality
5. Run dbt transformations (Bronze → Silver → Gold)

---

## SNOWFLAKE INFRASTRUCTURE

### Overview
Complete Snowflake database setup with enterprise features.

**Location:** `/Users/jpurrutia/projects/snowflake-panel-demo/snowflake/setup/`

---

### Setup Scripts

**00_environment_check.sql**
- Validates Snowflake account version
- Checks required features available
- Lists current databases/schemas

**01_create_database_schemas.sql**
- Creates CUSTOMER_ANALYTICS database
- Creates schemas: BRONZE, SILVER, GOLD
- Sets schema ownership

**02_create_roles_grants.sql**
- Creates custom roles:
  - DATA_ENGINEER (dbt, transformations)
  - DATA_ANALYST (reporting, queries)
  - DATA_SCIENTIST (ML operations)
- Assigns permissions (CREATE, SELECT, EXECUTE)

**03_create_observability_tables.sql**
- Lineage tracking
- Data quality metrics
- Pipeline monitoring tables

**04_create_storage_integration.sql**
- Snowflake ↔ AWS S3 integration
- IAM role authentication
- Stage definition

**05_create_stages.sql**
- External stages for S3 data
- Internal stages for Streamlit artifacts

**06_create_bronze_tables.sql**
- BRONZE_CUSTOMERS
- BRONZE_TRANSACTIONS
- BRONZE_MERCHANT_CATEGORIES

**07_create_bronze_transaction_table.sql**
- Additional transaction table configuration

**08_create_git_integration.sql**
- GitHub integration (optional)
- For storing dbt artifacts

**09_create_internal_stages.sql**
- Internal staging for Streamlit
- For storing semantic models

**cortex_analyst_setup.sql**
- Semantic model stage
- Cortex Analyst configuration

**deploy_semantic_model.sql**
- Creates semantic model stage
- Uploads semantic_model.yaml

**create_monthly_spending_view.sql**
- Pre-aggregated monthly metrics
- Performance optimization

---

### Database Structure

```
CUSTOMER_ANALYTICS (Database)
├── BRONZE (Schema) - Raw data
│   ├── BRONZE_CUSTOMERS (50K rows)
│   ├── BRONZE_TRANSACTIONS (60M rows)
│   └── BRONZE_MERCHANT_CATEGORIES (50 rows)
│
├── SILVER (Schema) - Staging/Cleaned
│   ├── STG_CUSTOMERS (View)
│   ├── STG_TRANSACTIONS (View)
│   ├── STG_MERCHANT_CATEGORIES (View)
│   └── INT_CUSTOMER_TRANSACTION_SUMMARY (Intermediate table)
│
└── GOLD (Schema) - Analytics/Business Logic
    ├── Dimensions:
    │   ├── DIM_CUSTOMER (SCD Type 2, current + history)
    │   ├── DIM_MERCHANT_CATEGORY
    │   └── DIM_DATE
    ├── Facts:
    │   └── FCT_TRANSACTIONS (13.5M rows, clustered)
    ├── Analytics Marts:
    │   ├── CUSTOMER_360_PROFILE (denormalized, dashboard optimized)
    │   ├── CUSTOMER_SEGMENTS (5 segments)
    │   └── MONTHLY_CUSTOMER_SPENDING (aggregated metrics)
    └── ML Artifacts:
        ├── CHURN_LABELS (training labels)
        ├── ML_TRAINING_DATA (engineered features)
        ├── CHURN_MODEL (trained model)
        ├── CHURN_PREDICTIONS (scored results)
        └── CHURN_INSIGHTS (interpretation)
```

---

### Key Database Features

**Clustering:**
- fct_transactions: Clustered by transaction_date
- dim_customer: Clustered by customer_id
- customer_360_profile: Optimized for customer_id lookups

**Performance Optimizations:**
- Denormalized customer_360_profile for sub-second customer lookups
- Materialized tables for gold layer (not views)
- Indexes on foreign keys

**Data Retention:**
- 18 months of transaction history
- Complete customer dimension history (SCD Type 2)
- Daily segment refresh

---

## TERRAFORM PROVISIONING

### Overview
Infrastructure-as-Code for AWS resources (S3, IAM).

**Location:** `/Users/jpurrutia/projects/snowflake-panel-demo/terraform/`

---

### AWS Resources Provisioned

**S3 Bucket:**
- Data lake storage for customer data
- Versioning enabled
- Encryption enabled
- Lifecycle policies
- Folder structure: customers/, transactions/

**IAM Role:**
- Service role for Snowflake
- S3 bucket read/write permissions
- Trust relationship with Snowflake AWS account
- External ID for security

**SNS/SQS (Optional):**
- Event notifications for S3 uploads
- Queue for async processing

---

### Terraform Structure

**Files:**
- `main.tf`: Provider and backend config
- `variables.tf`: Input variables (account ID, external ID, region)
- `s3.tf`: S3 bucket configuration
- `iam.tf`: IAM role and policies
- `outputs.tf`: Output values (bucket name, role ARN)

**Variables:**
- `snowflake_account_id`: Your Snowflake account identifier
- `snowflake_external_id`: External ID for trust relationship
- `aws_region`: AWS region (default: us-east-1)
- `environment`: Environment name (dev/staging/prod)

**Deployment:**
```bash
cd terraform
terraform init
terraform plan
terraform apply
# Capture outputs for Snowflake setup
terraform output
```

---

## DATA QUALITY & TESTING

### Overview
Comprehensive test suite for data pipeline validation.

**Location:** `/Users/jpurrutia/projects/snowflake-panel-demo/tests/`

---

### Unit Tests

**Location:** `tests/unit/`

**Files:**
- `test_customer_generator.py`: Validates customer data generation
- `test_project_structure.py`: Ensures directory structure
- `test_sql_syntax.py`: SQL syntax validation
- `test_transaction_sql_syntax.py`: Transaction SQL validation
- `test_terraform_variables.py`: Terraform config validation

---

### Integration Tests

**Location:** `tests/integration/`

**Coverage:**

1. **Data Ingestion:**
   - `test_customer_generation_e2e.py`: End-to-end customer generation
   - `test_transaction_bulk_load.py`: Transaction bulk loading
   - `test_transaction_generation.py`: Transaction generation validation

2. **dbt Transformations:**
   - `test_dbt_setup.py`: dbt project initialization
   - `test_dim_customer.sql`: Dimension table validation
   - `test_fact_transaction.py`: Fact table validation
   - `test_aggregate_marts.py`: Aggregate mart validation

3. **Data Models:**
   - `test_customer_segmentation.py`: Segment classification accuracy
   - `test_customer_360_tab.py`: Profile table completeness

4. **Machine Learning:**
   - `test_churn_model.py`: Model training and validation
   - `test_churn_training_data.py`: Feature engineering validation

5. **Semantic Layer:**
   - `test_semantic_layer.py`: Semantic model SQL generation

6. **Streamlit:**
   - `test_streamlit_segment_explorer.py`: UI component validation
   - `test_ai_assistant_tab.py`: AI assistant functionality
   - `test_campaign_simulator.py`: Campaign ROI calculations

7. **Infrastructure:**
   - `test_snowflake_setup.py`: Database/schema/role creation
   - `test_terraform_config.sh`: Terraform validation

---

### Performance Tests

**Location:** `tests/performance/`

- `test_transaction_load_performance.py`: Bulk load speed
- `test_transaction_generation_performance.py`: Data generation speed

---

### Data Quality Tests (dbt)

**Location:** `dbt_customer_analytics/tests/`

1. **SCD Type 2 Integrity:**
   - No overlapping valid_from/valid_to dates
   - No gaps in customer history
   - Exactly one is_current version per customer

2. **Segment Distribution:**
   - Validates segment percentages within expected ranges
   - Flags statistical outliers

3. **Null Handling:**
   - Required fields not null
   - Expected nulls present

---

## DOCUMENTATION

### Overview
Extensive documentation covering architecture, setup, and usage.

**Location:** `/Users/jpurrutia/projects/snowflake-panel-demo/docs/`

---

### Key Documentation Files

**Architecture & Design:**
- `ARCHITECTURE.md`: System architecture overview
- `architecture_diagram.md`: Visual architecture
- `star_schema_design.md`: Star schema documentation
- `DATA_FLOW.md`: End-to-end data flow

**Setup & Deployment:**
- `SETUP_GUIDE.md`: Step-by-step setup instructions
- `ONBOARDING_GUIDE.md`: New user onboarding
- `GITHUB_DEPLOYMENT_GUIDE.md`: CI/CD deployment

**Feature Documentation:**
- `customer_segmentation_guide.md`: RFM segmentation logic
- `aggregate_marts_guide.md`: Hero metrics definition
- `ml_model_card.md`: ML model documentation

**Cortex Analyst:**
- `CORTEX_ANALYST_DEPLOYMENT.md`: Deployment instructions
- `semantic_layer/README.md`: Semantic layer guide

**Implementation Records:**
- `IMPLEMENTATION_PROMPTS.md`: Original implementation tasks
- `prompt_X_completion_summary.md`: Iteration completion records (Phases 1-5)
- `ML_PIPELINE_GUIDE.md`: ML pipeline walkthrough

**Project Status:**
- `PROJECT_STATUS.md`: Current project status and completion checklist
- `EVALUATION_CRITERIA.md`: Success criteria

**Demo:**
- `DEMO_QUESTIONS.md`: Suggested demo questions
- `spec.md`: High-level specification

---

### README Files

**Main README:**
- Project overview
- Prerequisites
- Quick start (automated and manual)
- Architecture summary
- Technology stack
- Key features

**Component READMEs:**
- `terraform/README.md`: Terraform setup guide
- `snowflake/README.md`: Snowflake setup guide
- `dbt_customer_analytics/README.md`: dbt project guide
- `data_generation/README.md`: Data generation guide
- `semantic_layer/README.md`: Semantic layer guide
- `streamlit/README.md`: Streamlit app guide

---

---

## SUMMARY TABLE

| Component | Status | Purpose | Key Files | Row Count |
|-----------|--------|---------|-----------|-----------|
| **STREAMLIT APP** | ✅ ACTIVE | Interactive dashboard | app.py, tabs/*.py | N/A |
| AI Assistant | ✅ ACTIVE | Natural language queries | ai_assistant.py | N/A |
| Customer 360 | ✅ ACTIVE | Individual profiles | customer_360.py | N/A |
| Segment Explorer | ✅ ACTIVE | Segment analysis | segment_explorer.py | N/A |
| Campaign Simulator | ✅ ACTIVE | ROI calculation | campaign_simulator.py | N/A |
| **dBT PIPELINE** | ✅ COMPLETE | Data transformations | models/** | 13.5M+ |
| Bronze Layer | ✅ COMPLETE | Raw data | BRONZE schema | 60M txns |
| Silver Layer | ✅ COMPLETE | Cleaned data | SILVER schema | N/A |
| Gold Layer | ✅ COMPLETE | Analytics | GOLD schema | 50K customers |
| Dimensions | ✅ COMPLETE | SCD Type 2, lookup tables | dim_*.sql | ~20 categories |
| Facts | ✅ COMPLETE | Transaction detail | fct_transactions.sql | 13.5M rows |
| Marts | ✅ COMPLETE | Denormalized views | customer_360_profile.sql | 50K customers |
| **ML MODEL** | ✅ DEPLOYED | Churn prediction | snowflake/ml/*.sql | 50K scores |
| Training | ✅ COMPLETE | Feature engineering | 02_create_training_features.sql | 40K-45K |
| Model | ✅ COMPLETE | Cortex ML classification | 03_train_churn_model.sql | F1=1.0 |
| Predictions | ✅ COMPLETE | Risk scoring | 05_apply_predictions.sql | 50K customers |
| **SEMANTIC LAYER** | ✅ DEPLOYED | Cortex Analyst | semantic_model.yaml | 4 tables |
| **DATA GENERATION** | ✅ COMPLETE | Synthetic data | customer_generator.py | 50K customers |
| **INFRASTRUCTURE** | ✅ COMPLETE | Database setup | snowflake/setup/*.sql | N/A |
| **TERRAFORM** | ✅ COMPLETE | AWS provisioning | terraform/*.tf | N/A |
| **TESTS** | ✅ COMPLETE | Test suite | tests/** | 16+ test files |

---

## QUICK REFERENCE

### How to Run End-to-End

1. **Generate Data:**
   ```bash
   uv run python -m data_generation generate-customers --count 50000 --output data/customers.csv
   ```

2. **Setup Infrastructure:**
   ```bash
   cd terraform && terraform apply
   ```

3. **Setup Snowflake:**
   ```bash
   snowsql -c default -f snowflake/setup/01_create_database_schemas.sql
   ```

4. **Load Data:**
   ```bash
   snowsql -c default -f snowflake/load/load_customers_bulk.sql
   ```

5. **Run dbt:**
   ```bash
   cd dbt_customer_analytics && dbt run
   ```

6. **Train ML Model:**
   ```bash
   snowsql -c default -f snowflake/ml/03_train_churn_model.sql
   ```

7. **Run Streamlit:**
   ```bash
   cd streamlit && streamlit run app.py
   ```

---

## CONTACT & SUPPORT

**Documentation:** See `/docs` directory  
**Status:** View `PROJECT_STATUS.md`  
**Issues/Improvements:** Check `todo.md`

