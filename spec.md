# Snowflake Customer 360 Analytics Platform
## Complete Technical Specification & Project Proposal

**Project Name:** Credit Card Customer Analytics & Churn Prevention Platform
**Version:** 1.0
**Date:** 2025-11-11
**Document Type:** Technical Specification & Consulting Proposal

---

## Executive Summary

This document provides a comprehensive specification for building a Customer 360 Analytics Platform on Snowflake that demonstrates the four pillars of Snowflake's Data Cloud: Data Engineering, Analytics, Applications, and AI/ML. The platform addresses a post-acquisition integration scenario for a fintech company that has acquired 50,000 credit card customers and needs to quickly identify valuable segments, prevent churn, and enable data-driven marketing campaigns.

**Business Value Delivered:**
- Reduce customer churn by 15-20% through early risk detection
- Identify $2-5M in upsell opportunities (premium card upgrades)
- Enable self-service analytics reducing analyst workload by 40%
- Reduce data integration time from months to weeks

**Technical Showcase:**
- Modern data engineering (medallion architecture, streaming ingestion, data quality)
- Advanced analytics (customer segmentation, ML-powered churn prediction)
- Production-ready applications (Streamlit dashboards, AI chat interface)
- Enterprise governance (RBAC, lineage tracking, metadata management)

---

## Table of Contents

1. [Business Context](#business-context)
2. [Solution Architecture](#solution-architecture)
3. [Technical Specifications](#technical-specifications)
4. [Data Models](#data-models)
5. [Component Details](#component-details)
6. [User Stories & Acceptance Criteria](#user-stories)
7. [Implementation Roadmap](#implementation-roadmap)
8. [Project Proposal & Pricing](#project-proposal)
9. [Deliverables](#deliverables)
10. [Risks & Mitigation](#risks)

---

## 1. Business Context

### Acquisition Integration Scenario

**Background:**
Your company has acquired a fintech startup with a large credit card customer portfolio. You need to quickly integrate their transaction dataset into your Snowflake platform to identify high-value segments, prevent churn during the transition period, and optimize marketing spend.

**The Challenge - What We Inherited:**
- 50,000 credit cardholders with diverse spending patterns
- 13.5M transactions spanning 18 months of historical data
- 20 merchant categories with varying engagement levels
- Unknown customer segments requiring analysis and classification
- Ongoing transaction activity (new transactions generated hourly)

**Key Business Questions:**
1. Who are our most valuable customers?
2. Which customers are at risk of churning post-acquisition?
3. What spending patterns drive customer retention?
4. How can we personalize marketing campaigns?
5. Which customers should we target for premium card upgrades?
6. What is the ROI of our retention campaigns?

### Target Personas

**1. Marketing Manager**
- Needs to identify and export customer segments for campaigns
- Wants churn alerts and risk scores for proactive engagement
- Measures campaign performance and ROI
- Limited SQL skills - needs self-service tools

**2. Data Analyst**
- Deep-dives into customer behavior and transaction patterns
- Investigates anomalies and validates hypotheses
- Creates ad-hoc reports and analysis
- Comfortable with SQL but appreciates visual tools

**3. Data Engineer**
- Responsible for data pipeline reliability and quality
- Monitors ingestion, transformation, and data freshness
- Ensures governance and compliance
- Manages infrastructure and observability

---

## 2. Solution Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
│  ┌──────────────────┐        ┌──────────────────┐              │
│  │ Historical Bulk  │        │ Streaming Hourly │              │
│  │ Load (18 months) │        │  Transactions    │              │
│  └────────┬─────────┘        └────────┬─────────┘              │
└───────────┼──────────────────────────┼────────────────────────┘
            │                           │
            ▼                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                       AWS S3 STORAGE                             │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  - customers.csv                                          │  │
│  │  - transactions_historical/                               │  │
│  │  - transactions_streaming/YYYY-MM-DD-HH/                  │  │
│  │  SNS/SQS Event Notifications → Snowpipe                   │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SNOWFLAKE DATA PLATFORM                       │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐    │
│  │               BRONZE LAYER (Raw)                        │    │
│  │  - bronze_customers                                     │    │
│  │  - bronze_transactions                                  │    │
│  │  - Duplicate detection & logging                        │    │
│  └──────────────────┬─────────────────────────────────────┘    │
│                     │ dbt transformations                        │
│                     ▼                                            │
│  ┌────────────────────────────────────────────────────────┐    │
│  │               SILVER LAYER (Cleaned)                    │    │
│  │  - silver_customers (deduplicated)                      │    │
│  │  - silver_transactions (validated, no dupes)            │    │
│  └──────────────────┬─────────────────────────────────────┘    │
│                     │ dbt business logic                         │
│                     ▼                                            │
│  ┌────────────────────────────────────────────────────────┐    │
│  │               GOLD LAYER (Analytics-Ready)              │    │
│  │                                                          │    │
│  │  Dimensions:                                            │    │
│  │  - dim_customer (SCD Type 2 on card_type, credit_limit)│    │
│  │  - dim_date                                             │    │
│  │  - dim_merchant_category                                │    │
│  │                                                          │    │
│  │  Facts:                                                 │    │
│  │  - fact_transaction                                     │    │
│  │                                                          │    │
│  │  Marts:                                                 │    │
│  │  customer_analytics/                                    │    │
│  │    - customer_segments                                  │    │
│  │    - customer_360_profile                               │    │
│  │    - churn_risk_features                                │    │
│  │  marketing/                                             │    │
│  │    - campaign_performance                               │    │
│  │    - segment_metrics                                    │    │
│  │    - metric_customer_ltv                                │    │
│  │    - metric_mom_spend_change                            │    │
│  │    - metric_avg_transaction_value                       │    │
│  └──────────────────┬─────────────────────────────────────┘    │
│                     │                                            │
│  ┌────────────────────────────────────────────────────────┐    │
│  │            ML MODELS (Cortex ML)                        │    │
│  │  - Churn prediction model (classification)              │    │
│  │  - Feature importance tracking                          │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐    │
│  │         SEMANTIC LAYER (Cortex Analyst)                 │    │
│  │  - semantic_model.yaml                                  │    │
│  │  - Metrics, dimensions, relationships defined           │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐    │
│  │         OBSERVABILITY & GOVERNANCE                      │    │
│  │  - pipeline_run_metadata                                │    │
│  │  - data_quality_metrics                                 │    │
│  │  - layer_record_counts                                  │    │
│  │  - Horizon Catalog (automatic lineage)                  │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────┐
│              APPLICATION LAYER (Streamlit in Snowflake)          │
│                                                                  │
│  ┌──────────────┬──────────────┬──────────────┬─────────────┐  │
│  │   Tab 1:     │   Tab 2:     │   Tab 3:     │   Tab 4:    │  │
│  │  Segment     │  Customer    │  AI          │  Campaign   │  │
│  │  Explorer    │  360 View    │  Assistant   │ Performance │  │
│  │              │              │              │             │  │
│  │ - Filters    │ - Profile    │ - Chat UI    │ - A/B Test  │  │
│  │ - Segments   │ - Charts     │ - Cortex     │ - ROI Calc  │  │
│  │ - Export CSV │ - Tx Table   │   Analyst    │ - Metrics   │  │
│  └──────────────┴──────────────┴──────────────┴─────────────┘  │
└─────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────┐
│                          END USERS                               │
│  Marketing Managers  │  Data Analysts  │  Business Users         │
└─────────────────────────────────────────────────────────────────┘
```

### Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Cloud Storage** | AWS S3 | Raw data storage, staging for bulk/streaming loads |
| **Infrastructure as Code** | Terraform | S3 bucket, IAM roles, SNS/SQS, Snowflake storage integration |
| **Data Warehouse** | Snowflake | Central data platform (compute, storage, governance) |
| **Data Ingestion** | Snowpipe (S3 event-driven) | Automated streaming ingestion with <5 min latency |
| **Data Transformation** | dbt Core | SQL-based transformations, testing, documentation |
| **Synthetic Data** | Python (Faker, Pandas) + Snowflake SQL | Customer generation (Python), Transaction generation (Snowflake) |
| **ML Platform** | Snowflake Cortex ML | Churn prediction classification model |
| **Semantic Layer** | Cortex Analyst | Natural language query interface |
| **Application Framework** | Streamlit in Snowflake | Interactive dashboards and AI chat |
| **Version Control** | Git | Code versioning and collaboration |
| **Development IDE** | VS Code + Snowflake Extension | Local development environment |
| **CLI Tools** | SnowSQL | Command-line Snowflake operations |

---

## 3. Technical Specifications

### 3.1 Data Ingestion Strategy

**Historical Bulk Load (One-Time):**
- **Data Source:** Acquired company's historical data (18 months)
- **Volume:** 50,000 customers, ~13.5M transactions
- **Method:** COPY INTO from S3 (external stage)
- **Format:** CSV or Parquet files
- **Execution:** Manual trigger via SnowSQL after data generation
- **Target:** Bronze layer tables

**Streaming Ingestion (Ongoing):**
- **Data Source:** Ongoing customer transaction activity
- **Cadence:** Hourly batches (simulates near-real-time)
- **Volume per batch:** ~750 transactions/hour average
- **Method:** Snowpipe with S3 event notifications (SNS/SQS)
- **Trigger:** Automatic upon file arrival in S3
- **Latency:** <5 minutes from S3 upload to Bronze table availability
- **Target:** Bronze transactions table (append-only)

### 3.2 Medallion Architecture

**Bronze Layer (Raw):**
- Purpose: Land data exactly as received, no transformations
- Retention: All historical data retained for audit
- Schema: Matches source system schema
- Tables:
  - `bronze_customers` - Raw customer dimension
  - `bronze_transactions` - Raw transaction facts
- Data Quality: None at this layer (log issues, don't fail)

**Silver Layer (Cleaned & Conformed):**
- Purpose: Deduplicated, validated, conformed data
- Transformations:
  - Remove duplicate transactions (primary DQ focus)
  - Standardize data types and formats
  - Handle nulls (e.g., missing category → "Uncategorized")
  - Add technical metadata (ingestion_timestamp, source_file)
- Tables:
  - `silver_customers` - Cleaned customer records
  - `silver_transactions` - Validated, deduplicated transactions
- Data Quality: dbt tests enforce uniqueness, not-null constraints

**Gold Layer (Analytics-Ready):**
- Purpose: Business-oriented dimensional models and aggregate marts
- Structure: Star schema + denormalized marts for performance
- Tables:
  - **Dimensions:** dim_customer (SCD Type 2), dim_date, dim_merchant_category
  - **Facts:** fact_transaction
  - **Marts:** customer_segments, customer_360_profile, metric tables, campaign_performance
- Optimizations: Clustering keys, materialized views where beneficial

### 3.3 Data Quality Framework

**Primary Focus: Duplicate Detection**

**Implementation:**
- **Detection Point:** Silver layer transformation
- **Logic:** Identify duplicate `transaction_id` within each batch
- **Action:**
  - Keep first occurrence
  - Log duplicates to `data_quality_metrics` table
  - Increment duplicate counter in observability dashboard
- **dbt Test:** Custom test on `transaction_id` uniqueness in silver layer

**Secondary Quality Checks:**
- Missing merchant categories (default to "Uncategorized")
- Null customer references (logged but not failed)

**Observability Schema:**
```sql
CREATE TABLE observability.data_quality_metrics (
  run_id STRING,
  run_timestamp TIMESTAMP,
  layer STRING,
  table_name STRING,
  check_type STRING,  -- 'duplicate', 'null', 'referential'
  records_checked INT,
  records_failed INT,
  failure_rate FLOAT
);
```

### 3.4 SCD Type 2 Implementation

**Tracked Attributes:**
- `card_type` (standard/premium) - Demonstrates upsell tracking
- `credit_limit` - Shows spending capacity changes

**SCD Type 2 Columns:**
```sql
customer_key BIGINT,           -- Surrogate key
customer_id STRING,            -- Natural key (multiple records per customer)
card_type STRING,              -- Slowly changing
credit_limit DECIMAL(10,2),    -- Slowly changing
valid_from DATE,
valid_to DATE,                 -- NULL for current record
is_current BOOLEAN,
effective_timestamp TIMESTAMP
```

**Change Detection Logic:**
- dbt snapshot or incremental model comparing current vs. previous state
- On change: Expire old record (`is_current = FALSE`, set `valid_to`), insert new record

**Static Attributes (No History Tracking):**
- name, email, state, city, age, employment_status

### 3.5 Customer Segmentation

**5 Segments Defined:**

| Segment | % of Customers | Monthly Spend Range | Characteristics |
|---------|---------------|---------------------|-----------------|
| High-Value Travelers | 15% | $5K - $12K | Heavy travel/dining spending, premium card candidates |
| Stable Mid-Spenders | 40% | $2K - $4K | Consistent behavior, low churn risk |
| Budget-Conscious | 25% | $500 - $1.5K | Grocery/gas focus, price-sensitive |
| Declining | 10% | Varies (dropping 40%+) | High churn risk, intervention needed |
| New & Growing | 10% | Growing 50%+ | Recent customers, high engagement |

**Segmentation Logic:**
- **Initial Assignment:** Calculated during data generation based on spending patterns
- **Recalculation Cadence:** Monthly (rolling 90-day window)
- **Implementation:** dbt model `customer_segments` with business logic rules
- **Storage:** Pre-calculated segment stored in `dim_customer` for performance

**Segment Criteria Examples:**
- **High-Value Travelers:** avg_monthly_spend > $5K AND travel_category_pct > 30%
- **Declining:** mom_spend_change < -30% over rolling 90 days

### 3.6 Hero Metrics

**3 Core Metrics (Dashboard Spotlight):**

**1. Customer Lifetime Value (CLV)**
- **Definition:** Total transaction amount over 18-month period
- **Formula:** `SUM(transaction_amount) WHERE customer_id = X`
- **Business Use:** Identifies high-value customers to protect/nurture
- **Grain:** Per customer
- **Storage:** `metric_customer_ltv` mart

**2. Month-over-Month Spend Change %**
- **Definition:** Percentage change in spending from previous month
- **Formula:** `((current_month_spend - prior_month_spend) / prior_month_spend) * 100`
- **Business Use:** Early warning indicator for churn (leading metric)
- **Grain:** Per customer, per month
- **Storage:** `metric_mom_spend_change` mart
- **Alert Threshold:** <-30% triggers churn risk flag

**3. Average Transaction Value (ATV)**
- **Definition:** Mean transaction amount per customer
- **Formula:** `AVG(transaction_amount) GROUP BY customer_id`
- **Business Use:** Identifies premium vs. budget spenders for campaign targeting
- **Grain:** Per customer
- **Storage:** `metric_avg_transaction_value` mart

**Derived Metric: Churn Risk Score**
- Not a standalone hero metric but a composite filter/segment
- Calculated from: MoM Spend Change + Transaction Frequency + Days Since Last Transaction
- Implemented via Cortex ML classification model

### 3.7 Churn Prediction ML Model

**Model Type:** Binary classification (Cortex ML)

**Target Variable Definition (Churned = TRUE if):**
- No transactions for 60+ consecutive days OR
- Last 3-month average spend < 30% of baseline average

**Features:**
- avg_monthly_spend (baseline)
- mom_spend_change_pct
- transaction_frequency (count per month)
- days_since_last_transaction
- category_concentration (% spend in top category)
- card_type
- credit_utilization (spend / credit_limit)
- tenure_months
- age
- employment_status

**Training Data:**
- Time window: Months 1-15 (training), Months 16-18 (validation)
- Positive class (churned): ~10-12% of customers (realistic imbalance)
- Labels generated synthetically based on "Declining" segment behavior

**Model Implementation:**
```sql
-- Train model
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION churn_model(
  INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'ml_training_data'),
  TARGET_COLNAME => 'churned',
  CONFIG_OBJECT => {'EVALUATION_METRIC': 'F1'}
);

-- Apply predictions
CREATE TABLE churn_predictions AS
SELECT
  customer_id,
  churn_model!PREDICT(input_features) AS churn_risk_score
FROM customer_features;
```

**Output:** Churn risk score (0-100) per customer, updated monthly

### 3.8 Campaign Analytics

**Scenario:** Simple A/B test on "Declining" segment

**Test Design:**
- **Treatment Group:** 50% of Declining segment receive retention offer (Month 16)
- **Control Group:** 50% receive no intervention
- **Offer Type:** "$50 cashback on next $500 spend"
- **Campaign Duration:** 2 months (Months 16-17)
- **Measurement Period:** Month 18 (did behavior recover?)

**Metrics Tracked:**
- Average spend change (pre vs. post campaign)
- Retention rate (% who resumed normal spending)
- ROI calculation: (Recovered revenue - Campaign cost) / Campaign cost

**Data Generation:**
- Synthetically boost spending for treatment group customers by 10-20% in Months 17-18
- Control group continues decline trajectory

**Dashboard Display:**
- Side-by-side comparison: Treatment vs. Control
- Visualizations: Spend trend lines, retention %, ROI calculation
- Export capability: Campaign results to CSV

---

## 4. Data Models

### 4.1 Customer Dimension (SCD Type 2)

```sql
CREATE TABLE gold.dim_customer (
  customer_key BIGINT AUTOINCREMENT,        -- Surrogate key
  customer_id STRING NOT NULL,              -- Natural key

  -- Demographic attributes (static)
  first_name STRING,
  last_name STRING,
  email STRING,
  age INT,
  state STRING,
  city STRING,
  employment_status STRING,                 -- 'Employed', 'Self-Employed', 'Unemployed', 'Retired'

  -- Account attributes (slowly changing)
  card_type STRING,                         -- 'Standard', 'Premium'
  credit_limit DECIMAL(10,2),
  account_open_date DATE,

  -- Segmentation (recalculated monthly)
  customer_segment STRING,                  -- 'High-Value Travelers', 'Stable Mid-Spenders', etc.
  segment_assigned_date DATE,

  -- SCD Type 2 tracking
  valid_from DATE NOT NULL,
  valid_to DATE,                            -- NULL = current record
  is_current BOOLEAN DEFAULT TRUE,

  -- Metadata
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  source_system STRING DEFAULT 'ACQUIRED_FINTECH',

  PRIMARY KEY (customer_key)
);
```

### 4.2 Transaction Fact Table

```sql
CREATE TABLE gold.fact_transaction (
  transaction_key BIGINT AUTOINCREMENT,     -- Surrogate key
  transaction_id STRING NOT NULL UNIQUE,    -- Natural key

  -- Foreign keys
  customer_key BIGINT NOT NULL,             -- Links to dim_customer
  date_key INT NOT NULL,                    -- Links to dim_date (YYYYMMDD)
  merchant_category_key INT NOT NULL,       -- Links to dim_merchant_category

  -- Transaction attributes
  transaction_date TIMESTAMP NOT NULL,
  transaction_amount DECIMAL(10,2) NOT NULL,
  merchant_name STRING,
  channel STRING,                           -- 'Online', 'In-Store', 'Mobile'

  -- Metadata
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  source_file STRING,

  PRIMARY KEY (transaction_key),
  FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
  FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
  FOREIGN KEY (merchant_category_key) REFERENCES dim_merchant_category(category_key)
);
```

### 4.3 Aggregate Marts

**Customer 360 Profile Mart:**
```sql
CREATE TABLE gold.customer_analytics.customer_360_profile AS
SELECT
  c.customer_id,
  c.first_name || ' ' || c.last_name AS full_name,
  c.email,
  c.age,
  c.state,
  c.city,
  c.card_type,
  c.credit_limit,
  c.customer_segment,
  c.employment_status,

  -- Aggregated metrics
  COUNT(t.transaction_key) AS total_transactions,
  SUM(t.transaction_amount) AS lifetime_value,
  AVG(t.transaction_amount) AS avg_transaction_value,
  MIN(t.transaction_date) AS first_transaction_date,
  MAX(t.transaction_date) AS last_transaction_date,
  DATEDIFF('day', MAX(t.transaction_date), CURRENT_DATE()) AS days_since_last_transaction,

  -- Spending by category (top 3)
  -- (Pivot logic or separate table)

  -- Churn risk
  cr.churn_risk_score,
  CASE WHEN cr.churn_risk_score > 70 THEN 'High Risk'
       WHEN cr.churn_risk_score > 40 THEN 'Medium Risk'
       ELSE 'Low Risk' END AS churn_risk_category

FROM gold.dim_customer c
LEFT JOIN gold.fact_transaction t ON c.customer_key = t.customer_key
LEFT JOIN gold.customer_analytics.churn_risk_features cr ON c.customer_id = cr.customer_id
WHERE c.is_current = TRUE
GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.age, c.state, c.city,
         c.card_type, c.credit_limit, c.customer_segment, c.employment_status,
         cr.churn_risk_score;
```

**Monthly Metrics Mart:**
```sql
CREATE TABLE gold.marketing.metric_mom_spend_change AS
SELECT
  customer_id,
  DATE_TRUNC('month', transaction_date) AS month,
  SUM(transaction_amount) AS monthly_spend,
  LAG(SUM(transaction_amount)) OVER (PARTITION BY customer_id ORDER BY DATE_TRUNC('month', transaction_date)) AS prior_month_spend,
  ((SUM(transaction_amount) - LAG(SUM(transaction_amount)) OVER (PARTITION BY customer_id ORDER BY DATE_TRUNC('month', transaction_date)))
    / NULLIF(LAG(SUM(transaction_amount)) OVER (PARTITION BY customer_id ORDER BY DATE_TRUNC('month', transaction_date)), 0)) * 100 AS mom_change_pct
FROM gold.fact_transaction t
JOIN gold.dim_customer c ON t.customer_key = c.customer_key
WHERE c.is_current = TRUE
GROUP BY customer_id, DATE_TRUNC('month', transaction_date);
```

### 4.4 Semantic Model for Cortex Analyst

**File:** `semantic_model.yaml`

**Structure:**
```yaml
name: customer_analytics_semantic_model
description: Credit card customer analytics semantic layer for natural language queries

tables:
  - name: customer_360_profile
    base_table: gold.customer_analytics.customer_360_profile
    description: Complete customer profile with aggregated metrics
    dimensions:
      - name: customer_id
        type: string
        description: Unique customer identifier
      - name: full_name
        type: string
        description: Customer full name
      - name: state
        type: string
        description: Customer state of residence
      - name: city
        type: string
        description: Customer city
      - name: customer_segment
        type: string
        description: Customer segment (High-Value Travelers, Stable Mid-Spenders, etc.)
      - name: card_type
        type: string
        description: Card product type (Standard or Premium)
      - name: employment_status
        type: string
        description: Employment status
      - name: age
        type: number
        description: Customer age

    metrics:
      - name: lifetime_value
        type: number
        aggregation: sum
        description: Total spending over all time
      - name: avg_transaction_value
        type: number
        aggregation: avg
        description: Average amount per transaction
      - name: total_transactions
        type: number
        aggregation: sum
        description: Total number of transactions
      - name: churn_risk_score
        type: number
        aggregation: avg
        description: ML-predicted churn risk score (0-100)

  - name: fact_transaction
    base_table: gold.fact_transaction
    description: Individual credit card transactions
    dimensions:
      - name: transaction_date
        type: date
        description: Date of transaction
      - name: merchant_name
        type: string
        description: Merchant where transaction occurred
      - name: channel
        type: string
        description: Transaction channel (Online, In-Store, Mobile)

    metrics:
      - name: transaction_amount
        type: number
        aggregation: sum
        description: Transaction dollar amount
      - name: transaction_count
        type: number
        aggregation: count
        description: Number of transactions

  - name: dim_merchant_category
    base_table: gold.dim_merchant_category
    description: Merchant category classification
    dimensions:
      - name: category_name
        type: string
        description: Merchant category (Travel, Dining, Grocery, etc.)

relationships:
  - from_table: fact_transaction
    to_table: customer_360_profile
    join_key: customer_id
  - from_table: fact_transaction
    to_table: dim_merchant_category
    join_key: merchant_category_key

sample_questions:
  - "What is the average spend of customers in California?"
  - "Show me spending trends in the travel category over the last 6 months"
  - "Which merchant categories are most popular among high-income customers?"
  - "Who are the top 10 customers by lifetime value?"
  - "Show me customers in the Northeast spending over $5K/month who don't have premium cards"
  - "Which acquired customers are at highest risk of churning this month?"
```

---

## 5. Component Details

### 5.1 AWS Infrastructure (Terraform)

**Resources to Provision:**

1. **S3 Bucket:**
   - Name: `snowflake-customer-analytics-data`
   - Purpose: Storage for customer files, transaction batches
   - Folder structure:
     ```
     /customers/
       customers.csv
     /transactions/historical/
       transactions_YYYY-MM-DD.csv
     /transactions/streaming/
       YYYY/MM/DD/HH/transactions_timestamp.csv
     ```
   - Versioning: Enabled (for audit)
   - Encryption: SSE-S3

2. **IAM Role for Snowflake:**
   - Trust relationship: Snowflake AWS account
   - Policy: Read access to S3 bucket

3. **SNS Topic + SQS Queue:**
   - Purpose: S3 event notifications for Snowpipe
   - Configuration: S3 → SNS → SQS → Snowflake

4. **Snowflake Storage Integration:**
   - Created in Snowflake, references IAM role
   - Allows Snowflake to access S3 without long-term credentials

**Terraform Modules:**
- `modules/s3/` - Bucket, policies
- `modules/iam/` - Role, assume role policy
- `modules/sns_sqs/` - Event notification infrastructure
- `main.tf` - Orchestrates all modules

### 5.2 Snowflake Objects (SQL Scripts)

**Setup Scripts (executed in order):**

1. **`01_create_database_schemas.sql`**
   ```sql
   CREATE DATABASE IF NOT EXISTS customer_analytics;

   CREATE SCHEMA IF NOT EXISTS customer_analytics.bronze;
   CREATE SCHEMA IF NOT EXISTS customer_analytics.silver;
   CREATE SCHEMA IF NOT EXISTS customer_analytics.gold;
   CREATE SCHEMA IF NOT EXISTS customer_analytics.observability;
   ```

2. **`02_create_roles_grants.sql`**
   ```sql
   -- Data Engineer Role
   CREATE ROLE IF NOT EXISTS data_engineer;
   GRANT ALL ON DATABASE customer_analytics TO ROLE data_engineer;

   -- Marketing Manager Role
   CREATE ROLE IF NOT EXISTS marketing_manager;
   GRANT USAGE ON DATABASE customer_analytics TO ROLE marketing_manager;
   GRANT USAGE ON SCHEMA customer_analytics.gold TO ROLE marketing_manager;
   GRANT SELECT ON ALL TABLES IN SCHEMA customer_analytics.gold TO ROLE marketing_manager;
   -- Cannot access bronze/silver layers

   -- Data Analyst Role
   CREATE ROLE IF NOT EXISTS data_analyst;
   GRANT USAGE ON DATABASE customer_analytics TO ROLE data_analyst;
   GRANT USAGE ON ALL SCHEMAS IN DATABASE customer_analytics TO ROLE data_analyst;
   GRANT SELECT ON ALL TABLES IN DATABASE customer_analytics TO ROLE data_analyst;
   GRANT SELECT ON FUTURE TABLES IN DATABASE customer_analytics TO ROLE data_analyst;
   ```

3. **`03_create_stages.sql`**
   ```sql
   -- External stage pointing to S3
   CREATE OR REPLACE STORAGE INTEGRATION s3_integration
     TYPE = EXTERNAL_STAGE
     STORAGE_PROVIDER = 'S3'
     ENABLED = TRUE
     STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::ACCOUNT_ID:role/snowflake-s3-role'
     STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-customer-analytics-data/');

   CREATE OR REPLACE STAGE customer_analytics.bronze.customer_stage
     STORAGE_INTEGRATION = s3_integration
     URL = 's3://snowflake-customer-analytics-data/customers/'
     FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

   CREATE OR REPLACE STAGE customer_analytics.bronze.transaction_stage
     STORAGE_INTEGRATION = s3_integration
     URL = 's3://snowflake-customer-analytics-data/transactions/'
     FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
   ```

4. **`04_create_bronze_tables.sql`**
   ```sql
   CREATE OR REPLACE TABLE bronze.bronze_customers (
     customer_id STRING,
     first_name STRING,
     last_name STRING,
     email STRING,
     age INT,
     state STRING,
     city STRING,
     employment_status STRING,
     card_type STRING,
     credit_limit DECIMAL(10,2),
     account_open_date DATE,
     customer_segment STRING,
     ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
     source_file STRING
   );

   CREATE OR REPLACE TABLE bronze.bronze_transactions (
     transaction_id STRING,
     customer_id STRING,
     transaction_date TIMESTAMP,
     transaction_amount DECIMAL(10,2),
     merchant_name STRING,
     merchant_category STRING,
     channel STRING,
     ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
     source_file STRING
   );
   ```

5. **`05_create_snowpipe.sql`**
   ```sql
   CREATE OR REPLACE PIPE bronze.transaction_pipe
     AUTO_INGEST = TRUE
     AWS_SNS_TOPIC = 'arn:aws:sns:us-east-1:ACCOUNT_ID:snowflake-s3-events'
   AS
   COPY INTO bronze.bronze_transactions
   FROM @bronze.transaction_stage/streaming/
   FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
   PATTERN = '.*\.csv'
   ON_ERROR = 'CONTINUE';
   ```

### 5.3 dbt Project Structure

```
dbt_customer_analytics/
├── dbt_project.yml
├── profiles.yml
├── packages.yml
├── models/
│   ├── staging/                      # Bronze → Silver
│   │   ├── _staging_sources.yml
│   │   ├── stg_customers.sql
│   │   ├── stg_transactions.sql
│   │   └── schema.yml
│   ├── intermediate/                 # Silver transformations
│   │   ├── int_transactions_deduplicated.sql
│   │   ├── int_customer_monthly_spend.sql
│   │   └── schema.yml
│   ├── marts/
│   │   ├── customer_analytics/       # Gold - Customer domain
│   │   │   ├── dim_customer.sql
│   │   │   ├── customer_segments.sql
│   │   │   ├── customer_360_profile.sql
│   │   │   ├── churn_risk_features.sql
│   │   │   └── schema.yml
│   │   ├── marketing/                # Gold - Marketing domain
│   │   │   ├── metric_customer_ltv.sql
│   │   │   ├── metric_mom_spend_change.sql
│   │   │   ├── metric_avg_transaction_value.sql
│   │   │   ├── campaign_performance.sql
│   │   │   └── schema.yml
│   │   └── core/                     # Gold - Core dimensions/facts
│   │       ├── dim_date.sql
│   │       ├── dim_merchant_category.sql
│   │       ├── fact_transaction.sql
│   │       └── schema.yml
├── tests/
│   ├── assert_no_duplicate_transactions.sql
│   ├── assert_transaction_amounts_positive.sql
│   └── assert_all_customers_have_segment.sql
├── macros/
│   ├── generate_surrogate_key.sql
│   └── calculate_churn_label.sql
└── snapshots/
    └── customer_scd_snapshot.sql
```

**Key dbt Models:**

**`stg_transactions.sql` (Staging - Deduplication):**
```sql
{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    on_schema_change='fail'
) }}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'bronze_transactions') }}
    {% if is_incremental() %}
    WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY ingestion_timestamp) AS row_num
    FROM source
),

duplicate_log AS (
    -- Log duplicates to observability schema
    INSERT INTO {{ ref('data_quality_metrics') }}
    SELECT
        '{{ run_started_at }}' AS run_id,
        CURRENT_TIMESTAMP AS run_timestamp,
        'silver' AS layer,
        'transactions' AS table_name,
        'duplicate' AS check_type,
        COUNT(*) AS records_checked,
        SUM(CASE WHEN row_num > 1 THEN 1 ELSE 0 END) AS records_failed,
        SUM(CASE WHEN row_num > 1 THEN 1 ELSE 0 END)::FLOAT / COUNT(*) AS failure_rate
    FROM deduplicated
)

SELECT
    transaction_id,
    customer_id,
    transaction_date,
    transaction_amount,
    merchant_name,
    COALESCE(merchant_category, 'Uncategorized') AS merchant_category,
    channel,
    ingestion_timestamp,
    source_file
FROM deduplicated
WHERE row_num = 1
```

**`dim_customer.sql` (SCD Type 2):**
```sql
{{ config(
    materialized='table',
    post_hook="UPDATE {{ this }} SET is_current = FALSE WHERE valid_to IS NOT NULL"
) }}

WITH current_customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

existing_dim AS (
    SELECT * FROM {{ this }}
    WHERE is_current = TRUE
),

changes AS (
    SELECT
        c.customer_id,
        c.card_type AS new_card_type,
        c.credit_limit AS new_credit_limit,
        e.card_type AS old_card_type,
        e.credit_limit AS old_credit_limit,
        CASE
            WHEN e.customer_id IS NULL THEN 'NEW'
            WHEN c.card_type != e.card_type OR c.credit_limit != e.credit_limit THEN 'CHANGED'
            ELSE 'NO_CHANGE'
        END AS change_type
    FROM current_customers c
    LEFT JOIN existing_dim e ON c.customer_id = e.customer_id
),

-- Expire changed records
expired_records AS (
    UPDATE {{ this }}
    SET
        valid_to = CURRENT_DATE - 1,
        is_current = FALSE
    WHERE customer_id IN (SELECT customer_id FROM changes WHERE change_type = 'CHANGED')
      AND is_current = TRUE
),

-- Insert new/changed records
new_records AS (
    SELECT
        {{ generate_surrogate_key(['customer_id', 'CURRENT_TIMESTAMP']) }} AS customer_key,
        c.*,
        CURRENT_DATE AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current
    FROM current_customers c
    JOIN changes ch ON c.customer_id = ch.customer_id
    WHERE ch.change_type IN ('NEW', 'CHANGED')
)

SELECT * FROM new_records
```

### 5.4 Synthetic Data Generation

**Python Script: `generate_customers.py`**

**Purpose:** Generate 50,000 customers with realistic demographics and segment assignments

**Libraries:**
- `faker` - Realistic names, emails, addresses
- `pandas` - DataFrame manipulation
- `boto3` - Upload to S3

**Logic:**
```python
from faker import Faker
import pandas as pd
import boto3
import random
from datetime import datetime, timedelta

fake = Faker('en_US')
Faker.seed(42)  # Reproducibility

# Segment distributions
SEGMENTS = {
    'High-Value Travelers': 0.15,
    'Stable Mid-Spenders': 0.40,
    'Budget-Conscious': 0.25,
    'Declining': 0.10,
    'New & Growing': 0.10
}

# Spend ranges by segment (monthly avg)
SEGMENT_SPEND_RANGES = {
    'High-Value Travelers': (5000, 12000),
    'Stable Mid-Spenders': (2000, 4000),
    'Budget-Conscious': (500, 1500),
    'Declining': (2000, 4000),  # Will decline over time
    'New & Growing': (1000, 3000)  # Will grow
}

def generate_customers(n=50000):
    customers = []

    for i in range(n):
        # Assign segment
        segment = random.choices(
            list(SEGMENTS.keys()),
            weights=list(SEGMENTS.values())
        )[0]

        # Generate customer
        customer = {
            'customer_id': f'CUST{str(i+1).zfill(8)}',
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'age': random.randint(22, 75),
            'state': fake.state_abbr(),
            'city': fake.city(),
            'employment_status': random.choice(['Employed', 'Self-Employed', 'Retired', 'Unemployed']),
            'card_type': 'Premium' if segment == 'High-Value Travelers' and random.random() > 0.3 else 'Standard',
            'credit_limit': random.randint(5000, 50000),
            'account_open_date': fake.date_between(start_date='-5y', end_date='-2y'),
            'customer_segment': segment,
            'decline_type': random.choice(['gradual', 'sudden']) if segment == 'Declining' else None
        }
        customers.append(customer)

    df = pd.DataFrame(customers)

    # Save to CSV
    df.to_csv('customers.csv', index=False)

    # Upload to S3
    s3 = boto3.client('s3')
    s3.upload_file('customers.csv', 'snowflake-customer-analytics-data', 'customers/customers.csv')

    print(f"Generated {n} customers and uploaded to S3")
    return df

if __name__ == '__main__':
    customers = generate_customers(50000)
```

**Snowflake SQL: `generate_transactions.sql`**

**Purpose:** Generate 13.5M historical transactions (18 months) based on customer segments

**Approach:** Use Snowflake's `GENERATOR()` function for scale

```sql
-- Step 1: Create date spine (18 months, daily)
CREATE OR REPLACE TEMP TABLE date_spine AS
SELECT
    DATEADD('day', SEQ4(), DATEADD('month', -18, CURRENT_DATE())) AS transaction_date
FROM TABLE(GENERATOR(ROWCOUNT => 540));  -- 18 months * 30 days

-- Step 2: Generate transactions per customer
CREATE OR REPLACE TEMP TABLE transaction_base AS
SELECT
    c.customer_id,
    c.customer_segment,
    c.decline_type,
    d.transaction_date,
    DATEDIFF('month', DATEADD('month', -18, CURRENT_DATE()), d.transaction_date) AS month_num,
    -- Monthly transaction count varies by segment
    CASE c.customer_segment
        WHEN 'High-Value Travelers' THEN UNIFORM(40, 80, RANDOM())
        WHEN 'Stable Mid-Spenders' THEN UNIFORM(20, 40, RANDOM())
        WHEN 'Budget-Conscious' THEN UNIFORM(15, 30, RANDOM())
        WHEN 'Declining' THEN UNIFORM(20, 40, RANDOM())
        WHEN 'New & Growing' THEN UNIFORM(25, 50, RANDOM())
    END AS monthly_transactions
FROM bronze.bronze_customers c
CROSS JOIN date_spine d
WHERE DAY(d.transaction_date) = 1;  -- One row per customer per month

-- Step 3: Expand to individual transactions
CREATE OR REPLACE TEMP TABLE transactions_expanded AS
SELECT
    tb.*,
    DATEADD('day', UNIFORM(0, 28, RANDOM()), tb.transaction_date) AS actual_transaction_date,
    SEQ4() AS transaction_seq
FROM transaction_base tb,
     TABLE(GENERATOR(ROWCOUNT => 100))  -- Max transactions per month
WHERE SEQ4() < tb.monthly_transactions;

-- Step 4: Generate transaction details with segment-specific patterns
CREATE OR REPLACE TABLE transactions_historical AS
SELECT
    'TXN' || LPAD(ROW_NUMBER() OVER (ORDER BY actual_transaction_date), 10, '0') AS transaction_id,
    customer_id,
    actual_transaction_date AS transaction_date,

    -- Amount varies by segment and month (Declining segment trends down)
    CASE
        WHEN customer_segment = 'High-Value Travelers' THEN
            ROUND(UNIFORM(50, 500, RANDOM()), 2)
        WHEN customer_segment = 'Stable Mid-Spenders' THEN
            ROUND(UNIFORM(30, 150, RANDOM()), 2)
        WHEN customer_segment = 'Budget-Conscious' THEN
            ROUND(UNIFORM(10, 80, RANDOM()), 2)
        WHEN customer_segment = 'Declining' THEN
            -- Implement decline pattern
            CASE decline_type
                WHEN 'gradual' THEN
                    ROUND(UNIFORM(30, 150, RANDOM()) * (1 - (month_num - 12) * 0.1), 2)  -- Linear decline after month 12
                WHEN 'sudden' THEN
                    ROUND(UNIFORM(30, 150, RANDOM()) * IFF(month_num < 16, 1, 0.4), 2)  -- Sudden 60% drop at month 16
            END
        WHEN customer_segment = 'New & Growing' THEN
            ROUND(UNIFORM(20, 100, RANDOM()) * (1 + month_num * 0.05), 2)  -- 5% growth per month
    END AS transaction_amount,

    -- Merchant name (simple)
    'Merchant_' || UNIFORM(1, 1000, RANDOM()) AS merchant_name,

    -- Category varies by segment
    CASE
        WHEN customer_segment = 'High-Value Travelers' THEN
            ARRAY_CONSTRUCT('Travel', 'Dining', 'Hotels', 'Airlines')[UNIFORM(0, 3, RANDOM())]
        WHEN customer_segment = 'Budget-Conscious' THEN
            ARRAY_CONSTRUCT('Grocery', 'Gas', 'Utilities')[UNIFORM(0, 2, RANDOM())]
        ELSE
            ARRAY_CONSTRUCT('Retail', 'Dining', 'Entertainment', 'Grocery', 'Gas', 'Travel', 'Healthcare')[UNIFORM(0, 6, RANDOM())]
    END AS merchant_category,

    -- Channel
    ARRAY_CONSTRUCT('Online', 'In-Store', 'Mobile')[UNIFORM(0, 2, RANDOM())] AS channel

FROM transactions_expanded
WHERE transaction_amount > 0;  -- Remove negative amounts from decline logic

-- Step 5: Unload to S3 for bulk load demo
COPY INTO @bronze.transaction_stage/historical/transactions_historical.csv
FROM transactions_historical
FILE_FORMAT = (TYPE = 'CSV' COMPRESSION = 'GZIP')
HEADER = TRUE
OVERWRITE = TRUE;
```

**Hourly Batch Script: `generate_hourly_transactions.py`**

**Purpose:** Generate ongoing transaction batches (750 transactions/hour) and upload to S3 to trigger Snowpipe

```python
import pandas as pd
import boto3
import random
from datetime import datetime

def generate_hourly_batch():
    # Query active customers from Snowflake
    # For simplicity, randomly select 750 customer IDs

    transactions = []
    for i in range(750):
        txn = {
            'transaction_id': f'TXN{datetime.now().strftime("%Y%m%d%H%M%S")}{str(i).zfill(4)}',
            'customer_id': f'CUST{random.randint(1, 50000):08d}',
            'transaction_date': datetime.now(),
            'transaction_amount': round(random.uniform(10, 500), 2),
            'merchant_name': f'Merchant_{random.randint(1, 1000)}',
            'merchant_category': random.choice(['Grocery', 'Gas', 'Dining', 'Retail', 'Travel']),
            'channel': random.choice(['Online', 'In-Store', 'Mobile'])
        }
        transactions.append(txn)

    df = pd.DataFrame(transactions)

    # Save with timestamp
    timestamp = datetime.now().strftime('%Y/%m/%d/%H')
    filename = f'transactions_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

    df.to_csv(filename, index=False)

    # Upload to S3 (triggers Snowpipe via SNS)
    s3 = boto3.client('s3')
    s3_key = f'transactions/streaming/{timestamp}/{filename}'
    s3.upload_file(filename, 'snowflake-customer-analytics-data', s3_key)

    print(f"Uploaded {len(transactions)} transactions to {s3_key}")

if __name__ == '__main__':
    generate_hourly_batch()
```

**Scheduling:** Run via cron job or manually for demos

### 5.5 Streamlit Application

**Deployment:** Streamlit in Snowflake (SiS)

**File:** `customer_analytics_app.py`

**Structure:**

```python
import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
from snowflake.cortex import Complete

# Page config
st.set_page_config(page_title="Customer 360 Analytics", layout="wide")

# Snowflake connection (uses SiS context)
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database="customer_analytics",
        schema="gold"
    )

conn = get_connection()

# Sidebar navigation
tab = st.sidebar.radio("Navigate", ["Segment Explorer", "Customer 360", "AI Assistant", "Campaign Performance"])

# ========================================
# TAB 1: SEGMENT EXPLORER
# ========================================
if tab == "Segment Explorer":
    st.title("Customer Segment Explorer")
    st.markdown("Identify and export customer segments for targeted campaigns")

    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        selected_segments = st.multiselect(
            "Customer Segment",
            ["High-Value Travelers", "Stable Mid-Spenders", "Budget-Conscious", "Declining", "New & Growing"],
            default=["High-Value Travelers", "Declining"]
        )
    with col2:
        selected_states = st.multiselect("State", ["CA", "NY", "TX", "FL", "All"])
    with col3:
        churn_risk_filter = st.selectbox("Churn Risk", ["All", "High Risk", "Medium Risk", "Low Risk"])

    # Build query
    query = f"""
    SELECT
        customer_id,
        full_name,
        email,
        state,
        customer_segment,
        lifetime_value,
        avg_transaction_value,
        churn_risk_category,
        card_type
    FROM gold.customer_analytics.customer_360_profile
    WHERE customer_segment IN ({','.join([f"'{s}'" for s in selected_segments])})
    """

    if selected_states != ["All"]:
        query += f" AND state IN ({','.join([f'{s}' for s in selected_states])})"
    if churn_risk_filter != "All":
        query += f" AND churn_risk_category = '{churn_risk_filter}'"

    # Execute
    df = pd.read_sql(query, conn)

    # Metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Customers", f"{len(df):,}")
    col2.metric("Total LTV", f"${df['lifetime_value'].sum():,.0f}")
    col3.metric("Avg LTV", f"${df['lifetime_value'].mean():,.0f}")

    # Segment distribution chart
    fig = px.pie(df, names='customer_segment', title='Segment Distribution')
    st.plotly_chart(fig)

    # Data table
    st.dataframe(df, use_container_width=True)

    # Export button
    csv = df.to_csv(index=False)
    st.download_button(
        label="Export Segment to CSV",
        data=csv,
        file_name=f"customer_segment_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )

    st.info("💡 Coming soon: Direct export to Salesforce, HubSpot, and Google Ads")

# ========================================
# TAB 2: CUSTOMER 360
# ========================================
elif tab == "Customer 360":
    st.title("Customer 360 View")
    st.markdown("Deep dive into individual customer profiles and behavior")

    # Search for customer
    customer_id = st.text_input("Enter Customer ID", value="CUST00012345")

    if customer_id:
        # Fetch customer profile
        profile_query = f"""
        SELECT * FROM gold.customer_analytics.customer_360_profile
        WHERE customer_id = '{customer_id}'
        """
        profile = pd.read_sql(profile_query, conn).iloc[0]

        # Display profile card
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Name", profile['full_name'])
        col2.metric("Segment", profile['customer_segment'])
        col3.metric("Lifetime Value", f"${profile['lifetime_value']:,.0f}")
        col4.metric("Churn Risk", profile['churn_risk_category'])

        # Spending trend chart
        trend_query = f"""
        SELECT
            month,
            monthly_spend
        FROM gold.marketing.metric_mom_spend_change
        WHERE customer_id = '{customer_id}'
        ORDER BY month
        """
        trend_df = pd.read_sql(trend_query, conn)

        fig = px.line(trend_df, x='month', y='monthly_spend', title='Monthly Spend Trend')
        st.plotly_chart(fig, use_container_width=True)

        # Category breakdown
        category_query = f"""
        SELECT
            mc.category_name,
            SUM(t.transaction_amount) AS total_spend
        FROM gold.fact_transaction t
        JOIN gold.dim_customer c ON t.customer_key = c.customer_key
        JOIN gold.dim_merchant_category mc ON t.merchant_category_key = mc.category_key
        WHERE c.customer_id = '{customer_id}' AND c.is_current = TRUE
        GROUP BY mc.category_name
        ORDER BY total_spend DESC
        LIMIT 10
        """
        category_df = pd.read_sql(category_query, conn)

        fig2 = px.bar(category_df, x='category_name', y='total_spend', title='Top Spending Categories')
        st.plotly_chart(fig2, use_container_width=True)

        # Transaction history table (paginated)
        st.subheader("Recent Transactions")
        tx_query = f"""
        SELECT
            t.transaction_date,
            t.transaction_amount,
            t.merchant_name,
            mc.category_name,
            t.channel
        FROM gold.fact_transaction t
        JOIN gold.dim_customer c ON t.customer_key = c.customer_key
        JOIN gold.dim_merchant_category mc ON t.merchant_category_key = mc.category_key
        WHERE c.customer_id = '{customer_id}' AND c.is_current = TRUE
        ORDER BY t.transaction_date DESC
        LIMIT 100
        """
        tx_df = pd.read_sql(tx_query, conn)
        st.dataframe(tx_df, use_container_width=True)

# ========================================
# TAB 3: AI ASSISTANT
# ========================================
elif tab == "AI Assistant":
    st.title("AI-Powered Analytics Assistant")
    st.markdown("Ask questions in plain English about your customer data")

    # Chat interface
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # User input
    if prompt := st.chat_input("Ask a question about your customers..."):
        # Add user message
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Call Cortex Analyst
        with st.chat_message("assistant"):
            with st.spinner("Analyzing..."):
                # Cortex Analyst API call (simplified)
                response = Complete(
                    model="cortex-analyst",
                    semantic_model="gold.customer_analytics.semantic_model",
                    question=prompt
                )

                st.markdown(response['answer'])

                # Display generated SQL
                with st.expander("View SQL Query"):
                    st.code(response['sql'], language='sql')

                # Display results table/chart
                if response['data']:
                    st.dataframe(pd.DataFrame(response['data']))

        st.session_state.messages.append({"role": "assistant", "content": response['answer']})

    # Sample questions
    st.sidebar.subheader("Sample Questions")
    sample_questions = [
        "What is the average spend of customers in California?",
        "Show me spending trends in the travel category over the last 6 months",
        "Which merchant categories are most popular among high-income customers?",
        "Who are the top 10 customers by lifetime value?",
        "Show me customers in the Northeast spending over $5K/month who don't have premium cards"
    ]
    for q in sample_questions:
        if st.sidebar.button(q):
            st.session_state.messages.append({"role": "user", "content": q})
            st.rerun()

# ========================================
# TAB 4: CAMPAIGN PERFORMANCE
# ========================================
elif tab == "Campaign Performance":
    st.title("Campaign Performance Analytics")
    st.markdown("Measure retention campaign impact and ROI")

    # Campaign selector
    campaign_name = st.selectbox("Select Campaign", ["Declining Segment Retention Offer (Month 16)"])

    # Fetch campaign results
    campaign_query = """
    SELECT
        treatment_group,
        COUNT(DISTINCT customer_id) AS customers,
        AVG(pre_campaign_avg_spend) AS avg_spend_before,
        AVG(post_campaign_avg_spend) AS avg_spend_after,
        AVG(post_campaign_avg_spend - pre_campaign_avg_spend) AS avg_lift,
        SUM(post_campaign_avg_spend - pre_campaign_avg_spend) AS total_revenue_recovered
    FROM gold.marketing.campaign_performance
    WHERE campaign_name = 'retention_offer_month16'
    GROUP BY treatment_group
    """
    campaign_df = pd.read_sql(campaign_query, conn)

    # Display results
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Treatment Group")
        treatment = campaign_df[campaign_df['treatment_group'] == 'treatment'].iloc[0]
        st.metric("Customers", f"{treatment['customers']:,.0f}")
        st.metric("Avg Spend Before", f"${treatment['avg_spend_before']:,.0f}")
        st.metric("Avg Spend After", f"${treatment['avg_spend_after']:,.0f}")
        st.metric("Avg Lift", f"${treatment['avg_lift']:,.0f}", delta=f"{(treatment['avg_lift']/treatment['avg_spend_before']*100):.1f}%")

    with col2:
        st.subheader("Control Group")
        control = campaign_df[campaign_df['treatment_group'] == 'control'].iloc[0]
        st.metric("Customers", f"{control['customers']:,.0f}")
        st.metric("Avg Spend Before", f"${control['avg_spend_before']:,.0f}")
        st.metric("Avg Spend After", f"${control['avg_spend_after']:,.0f}")
        st.metric("Avg Lift", f"${control['avg_lift']:,.0f}", delta=f"{(control['avg_lift']/control['avg_spend_before']*100):.1f}%")

    # ROI calculation
    st.subheader("Campaign ROI")
    campaign_cost = 50 * treatment['customers']  # $50 offer per customer
    incremental_revenue = treatment['total_revenue_recovered'] - control['total_revenue_recovered']
    roi = (incremental_revenue - campaign_cost) / campaign_cost * 100

    col1, col2, col3 = st.columns(3)
    col1.metric("Campaign Cost", f"${campaign_cost:,.0f}")
    col2.metric("Incremental Revenue", f"${incremental_revenue:,.0f}")
    col3.metric("ROI", f"{roi:.1f}%")

    # Comparison chart
    fig = px.bar(
        campaign_df,
        x='treatment_group',
        y=['avg_spend_before', 'avg_spend_after'],
        barmode='group',
        title='Pre vs. Post Campaign Spend Comparison'
    )
    st.plotly_chart(fig, use_container_width=True)

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("**Customer 360 Analytics Platform**")
st.sidebar.markdown("Built on Snowflake Data Cloud")
```

### 5.6 Observability Dashboard

**Simple Observability View (embedded in Streamlit or separate dashboard):**

**Metrics Tracked:**
1. **Pipeline Health:**
   - Last dbt run status (success/failure)
   - Run duration
   - Rows processed per layer (Bronze → Silver → Gold)

2. **Data Quality:**
   - Duplicate transactions detected and rejected (per batch)
   - Null rate for key fields
   - Data freshness (time since last load)

3. **Snowpipe Status:**
   - Files ingested in last hour
   - Average ingestion latency
   - Error count

**Implementation:**
- Query `observability.pipeline_run_metadata` and `observability.data_quality_metrics`
- Display as simple table or metrics cards in Streamlit sidebar

---

## 6. User Stories & Acceptance Criteria

### User Story 1: Data Engineer - Pipeline Ingestion

**AS A** Data Engineer on the integration team
**I WANT TO** ingest the acquired company's credit card transaction data into our Snowflake environment using automated pipelines
**SO THAT** the marketing team can quickly analyze customer behavior without manual data transfers

**ACCEPTANCE CRITERIA:**
- ✅ Transaction data from acquired company's S3 buckets streams into Snowflake via Snowpipe
- ✅ Data passes through Bronze → Silver → Gold medallion layers with quality checks
- ✅ Historical data (18 months) and ongoing transactions are both captured
- ✅ Data lineage and metadata are tracked in Horizon catalog
- ✅ Duplicate transactions are detected and logged (primary DQ check)
- ✅ Pipeline run metadata captured in observability schema

**DEMO FLOW:**
1. Show S3 bucket with customer and transaction files
2. Explain Snowpipe configuration with SNS/SQS
3. Manually upload an hourly batch file → Show Snowpipe ingestion in <5 mins
4. Run dbt models → Show data flowing through Bronze/Silver/Gold
5. Navigate to Snowsight → Show Horizon lineage graph
6. Query observability tables → Show duplicate detection logs

---

### User Story 2: Marketing Manager - Segment Identification

**AS A** Marketing Manager
**I WANT TO** identify high-value customer segments from the newly acquired customer base
**SO THAT** I can create targeted campaigns for our premium travel rewards card

**ACCEPTANCE CRITERIA:**
- ✅ Dashboard shows customer segments by monthly spend, transaction categories, and demographics
- ✅ Can filter to customers spending $3K+/month on travel and dining
- ✅ Export segment lists to CSV for campaign tools
- ✅ AI agent can answer "Show me customers in the Northeast spending over $5K/month who don't have premium cards"

**DEMO FLOW:**
1. Open Streamlit app → "Segment Explorer" tab
2. Select "High-Value Travelers" segment
3. Apply filters: State = "NY, MA, CT", Monthly Spend > $5K, Card Type = "Standard"
4. Show resulting customer list with LTV, spend metrics
5. Click "Export Segment to CSV" → Download file
6. Switch to "AI Assistant" tab → Ask: "Show me customers in the Northeast spending over $5K/month who don't have premium cards"
7. AI returns natural language answer + data table

---

### User Story 3: Marketing Manager - Churn Detection

**AS A** Marketing Manager
**I WANT TO** detect customers from the acquired portfolio who show declining spend patterns or churn risk
**SO THAT** I can proactively engage them with retention offers before they leave

**ACCEPTANCE CRITERIA:**
- ✅ Dashboard shows monthly spend trends for each customer
- ✅ Alerts for customers with 30%+ spend decline over 2-3 months
- ✅ Churn risk scores calculated using ML model (Cortex ML)
- ✅ AI agent can answer "Which acquired customers are at highest risk of churning this month?"

**DEMO FLOW:**
1. "Segment Explorer" tab → Filter to "Declining" segment + "High Churn Risk"
2. Show list of at-risk customers with churn risk scores
3. Click on a customer → Navigate to "Customer 360" tab
4. Show monthly spend trend chart with visible decline pattern
5. Highlight MoM spend change metric showing -35% drop
6. Explain ML model: "Cortex ML trained on historical patterns, predicts churn probability"
7. AI Assistant: "Which acquired customers are at highest risk of churning this month?" → Returns top 20

---

### User Story 4: Data Analyst - Customer 360 View

**AS A** Data Analyst
**I WANT TO** see a complete 360-degree view of any customer from the acquired portfolio
**SO THAT** I can understand their behavior, preferences, and recommend the best next action

**ACCEPTANCE CRITERIA:**
- ✅ Single dashboard showing customer demographics, transaction history, spend by category, card products, risk scores
- ✅ Transaction timeline visualization (monthly spend trend chart)
- ✅ ML-driven churn risk score displayed prominently
- ✅ AI agent can provide complete customer profile on demand

**DEMO FLOW:**
1. "Customer 360" tab → Enter customer ID
2. Display profile card: Name, segment, LTV, churn risk
3. Show monthly spend trend line chart (18 months)
4. Show category breakdown bar chart (top 10 spending categories)
5. Show transaction history table (last 100 transactions) with filters
6. AI Assistant: "Give me a complete profile for customer CUST00012345" → Returns summary

---

### User Story 5: Marketing Manager - Self-Service AI Queries

**AS A** Marketing Manager with limited SQL skills
**I WANT TO** ask questions about the acquired customer data in plain English
**SO THAT** I can get fast insights without waiting for data analysts to write queries

**ACCEPTANCE CRITERIA:**
- ✅ Streamlit app with chat interface powered by Cortex Analyst
- ✅ Can handle questions like:
  - "What's the average spend of customers in California?"
  - "Show me spending trends in the travel category over the last 6 months"
  - "Which merchant categories are most popular among high-income customers?"
- ✅ Responses include visualizations and data tables
- ✅ AI explains insights in business-friendly language

**DEMO FLOW:**
1. "AI Assistant" tab → Chat interface
2. Ask: "What's the average spend of customers in California?"
   - AI returns: "The average monthly spend for California customers is $2,847. This is 12% higher than the national average."
   - Shows data table with state-by-state breakdown
3. Ask: "Show me spending trends in the travel category over the last 6 months"
   - AI returns line chart showing travel spending trend
   - Insight: "Travel spending increased 23% from Month 12 to Month 18, driven primarily by airline purchases."
4. Click "View SQL Query" → Show generated SQL for transparency

---

### User Story 6: Marketing Manager - Campaign ROI

**AS A** Marketing Manager
**I WANT TO** measure the impact of our post-acquisition welcome campaigns
**SO THAT** I can optimize our marketing spend and improve ROI

**ACCEPTANCE CRITERIA:**
- ✅ Track pre/post campaign spend by customer segment
- ✅ A/B test results comparing different offer types (treatment vs. control)
- ✅ Campaign attribution metrics (customers reached, lift, incremental revenue)
- ✅ ROI calculations displayed prominently

**DEMO FLOW:**
1. "Campaign Performance" tab
2. Select "Declining Segment Retention Offer (Month 16)"
3. Show side-by-side comparison:
   - Treatment group: 2,500 customers, avg spend lifted from $1,800 to $2,400 (+33%)
   - Control group: 2,500 customers, avg spend declined from $1,800 to $1,400 (-22%)
4. ROI calculation:
   - Campaign cost: $125,000 ($50 offer × 2,500 customers)
   - Incremental revenue: $1.5M (treatment group recovered, control continued declining)
   - ROI: 1,100%
5. Chart: Pre vs. post campaign spend bars (treatment vs. control)

---

## 7. Implementation Roadmap

### Phase 1: Foundation & Data Pipeline (Weeks 1-3)

**Week 1: Infrastructure & Setup**
- Set up Snowflake trial account
- Configure AWS account and S3 bucket
- Write Terraform modules for S3, IAM, SNS/SQS
- Apply Terraform → Provision infrastructure
- Create Snowflake database, schemas, roles (RBAC)
- Create stages and storage integration
- Test S3 → Snowflake connectivity

**Deliverables:**
- Terraform codebase (functional)
- Snowflake environment configured
- Documentation: Setup guide

**Week 2: Data Generation**
- Build Python customer generator script
- Generate 50,000 customers → Upload to S3
- Build Snowflake transaction generation SQL
- Generate 13.5M historical transactions → Upload to S3
- Build hourly batch generator script
- Test data quality (distributions, null rates)

**Deliverables:**
- Synthetic data generator scripts
- 50K customer file in S3
- Historical transaction files in S3
- Documentation: Data generation guide

**Week 3: Ingestion Pipelines**
- Create Bronze layer tables
- Bulk load customers and historical transactions (COPY INTO)
- Configure Snowpipe for streaming ingestion
- Test Snowpipe with manual hourly batch upload
- Verify S3 event notifications → Snowpipe trigger
- Set up observability schema and tables
- Test duplicate detection and logging

**Deliverables:**
- Bronze layer populated with data
- Snowpipe operational
- Observability framework in place
- Documentation: Pipeline architecture

---

### Phase 2: Transformations & Analytics (Weeks 4-6)

**Week 4: dbt Setup & Silver Layer**
- Initialize dbt project structure
- Configure dbt profiles (Snowflake connection)
- Build staging models (bronze → silver)
- Implement duplicate detection logic
- Write dbt tests (uniqueness, not-null)
- Build SCD Type 2 snapshot for dim_customer
- Test incremental loads

**Deliverables:**
- dbt project (staging + tests)
- Silver layer tables populated
- SCD Type 2 working
- Documentation: dbt model documentation

**Week 5: Gold Layer - Dimensions & Facts**
- Build dim_customer (with SCD Type 2)
- Build dim_date (date dimension)
- Build dim_merchant_category
- Build fact_transaction
- Test star schema joins and performance
- Add clustering keys for optimization

**Deliverables:**
- Gold layer dimensional model
- Documentation: Data model ERD

**Week 6: Gold Layer - Marts & Metrics**
- Build customer segmentation model
- Build customer_360_profile mart
- Build metric marts (CLV, MoM Spend Change, ATV)
- Build campaign_performance mart (with synthetic A/B test data)
- Build churn risk features table
- Test all marts with sample queries

**Deliverables:**
- All gold marts populated
- Segmentation logic validated
- Documentation: Mart definitions

---

### Phase 3: ML & Semantic Layer (Weeks 7-8)

**Week 7: Churn Prediction Model**
- Create labeled training dataset (churned vs. retained)
- Define features for ML model
- Train Cortex ML classification model
- Evaluate model performance (F1, precision, recall)
- Apply model to score all customers
- Store predictions in churn_predictions table

**Deliverables:**
- Trained ML model
- Churn risk scores for all customers
- Documentation: ML model card

**Week 8: Semantic Layer for Cortex Analyst**
- Design semantic model YAML structure
- Define all tables, dimensions, metrics
- Define relationships (joins)
- Add sample questions
- Deploy semantic model to Snowflake
- Test Cortex Analyst queries

**Deliverables:**
- semantic_model.yaml file
- Cortex Analyst functional
- Documentation: Semantic model guide

---

### Phase 4: Applications (Weeks 9-11)

**Week 9: Streamlit App - Core Tabs**
- Set up Streamlit in Snowflake project
- Build "Segment Explorer" tab (filters, charts, export)
- Build "Customer 360" tab (profile, charts, transaction table)
- Test Snowflake connection and query performance
- Deploy to SiS

**Deliverables:**
- Streamlit app (2 tabs functional)
- Deployed to Snowflake
- Documentation: App user guide

**Week 10: Streamlit App - AI & Campaign Tabs**
- Build "AI Assistant" tab (chat interface, Cortex Analyst integration)
- Build "Campaign Performance" tab (A/B test results, ROI)
- Polish UI/UX (styling, responsive design)
- Add error handling and loading states
- Test end-to-end user flows

**Deliverables:**
- Complete 4-tab Streamlit app
- All user stories validated
- Documentation: Demo script

**Week 11: Observability & Final Integration**
- Build observability dashboard (pipeline health, DQ metrics)
- Integrate observability into Streamlit sidebar
- Test Horizon catalog lineage views
- Conduct end-to-end testing (bulk load → streaming → transformations → app)
- Performance tuning (query optimization, caching)
- Security review (RBAC enforcement)

**Deliverables:**
- Observability dashboard
- End-to-end system functional
- Documentation: Troubleshooting guide

---

### Phase 5: Documentation & Handoff (Week 12)

**Week 12: Comprehensive Documentation**
- Write architecture documentation
- Create setup guides (Terraform, Snowflake, dbt, Streamlit)
- Write demo script with talking points
- Create data dictionary
- Document RBAC policies
- Write troubleshooting guide
- Record demo video (optional)
- Package all code and docs for handoff

**Deliverables:**
- Complete documentation package
- Demo-ready platform
- Training materials
- Handoff meeting

---

## 8. Project Proposal & Pricing

### 8.1 Consulting Engagement Overview

**Engagement Type:** Fixed-Price, Phased Delivery
**Duration:** 12 weeks
**Team Structure:**
- 1 Senior Snowflake Solutions Architect (60% allocation)
- 1 Data Engineer (80% allocation)
- 1 Analytics Engineer (dbt specialist) (60% allocation)
- 1 ML Engineer (30% allocation - Weeks 7-8 focused)
- 1 Frontend Developer (Streamlit) (40% allocation - Weeks 9-11 focused)

**Methodology:**
- Agile sprints (2-week iterations)
- Weekly status meetings and demos
- Bi-weekly steering committee reviews
- Dedicated Slack channel for real-time collaboration
- GitHub repository for code versioning

---

### 8.2 Level of Effort (LOE) Breakdown

#### Phase 1: Foundation & Data Pipeline (Weeks 1-3)

| Task | Role | Hours | Rate | Cost |
|------|------|-------|------|------|
| Snowflake environment setup | Solutions Architect | 16 | $250 | $4,000 |
| AWS infrastructure (Terraform) | Data Engineer | 32 | $200 | $6,400 |
| RBAC design and implementation | Solutions Architect | 12 | $250 | $3,000 |
| Synthetic data generator (Python) | Data Engineer | 40 | $200 | $8,000 |
| Transaction generation (SQL) | Data Engineer | 32 | $200 | $6,400 |
| Snowpipe configuration | Data Engineer | 24 | $200 | $4,800 |
| Observability framework setup | Data Engineer | 16 | $200 | $3,200 |
| Testing and validation | Data Engineer | 20 | $200 | $4,000 |
| **Phase 1 Subtotal** | | **192 hrs** | | **$39,800** |

#### Phase 2: Transformations & Analytics (Weeks 4-6)

| Task | Role | Hours | Rate | Cost |
|------|------|-------|------|------|
| dbt project setup and structure | Analytics Engineer | 16 | $200 | $3,200 |
| Bronze → Silver models | Analytics Engineer | 40 | $200 | $8,000 |
| SCD Type 2 implementation | Analytics Engineer | 24 | $200 | $4,800 |
| Dimensional model (dims + facts) | Analytics Engineer | 48 | $200 | $9,600 |
| Segmentation logic | Solutions Architect | 20 | $250 | $5,000 |
| Aggregate marts (metrics) | Analytics Engineer | 40 | $200 | $8,000 |
| Campaign performance mart | Analytics Engineer | 16 | $200 | $3,200 |
| dbt tests and documentation | Analytics Engineer | 24 | $200 | $4,800 |
| Query optimization | Data Engineer | 16 | $200 | $3,200 |
| **Phase 2 Subtotal** | | **244 hrs** | | **$49,800** |

#### Phase 3: ML & Semantic Layer (Weeks 7-8)

| Task | Role | Hours | Rate | Cost |
|------|------|-------|------|------|
| Churn label generation | Data Engineer | 16 | $200 | $3,200 |
| Feature engineering | ML Engineer | 24 | $220 | $5,280 |
| Cortex ML model training | ML Engineer | 32 | $220 | $7,040 |
| Model evaluation and tuning | ML Engineer | 20 | $220 | $4,400 |
| Semantic model design | Solutions Architect | 16 | $250 | $4,000 |
| Semantic model YAML development | Analytics Engineer | 24 | $200 | $4,800 |
| Cortex Analyst testing | Solutions Architect | 16 | $250 | $4,000 |
| **Phase 3 Subtotal** | | **148 hrs** | | **$32,720** |

#### Phase 4: Applications (Weeks 9-11)

| Task | Role | Hours | Rate | Cost |
|------|------|-------|------|------|
| Streamlit project setup (SiS) | Frontend Developer | 12 | $180 | $2,160 |
| Segment Explorer tab | Frontend Developer | 32 | $180 | $5,760 |
| Customer 360 tab | Frontend Developer | 40 | $180 | $7,200 |
| AI Assistant tab (Cortex integration) | Frontend Developer | 32 | $180 | $5,760 |
| Campaign Performance tab | Frontend Developer | 24 | $180 | $4,320 |
| UI/UX polish and responsive design | Frontend Developer | 20 | $180 | $3,600 |
| Observability dashboard | Data Engineer | 24 | $200 | $4,800 |
| End-to-end testing | Data Engineer | 24 | $200 | $4,800 |
| Performance tuning | Solutions Architect | 16 | $250 | $4,000 |
| Security review | Solutions Architect | 12 | $250 | $3,000 |
| **Phase 4 Subtotal** | | **236 hrs** | | **$45,400** |

#### Phase 5: Documentation & Handoff (Week 12)

| Task | Role | Hours | Rate | Cost |
|------|------|-------|------|------|
| Architecture documentation | Solutions Architect | 16 | $250 | $4,000 |
| Setup guides (all components) | Data Engineer | 24 | $200 | $4,800 |
| Demo script and talking points | Solutions Architect | 12 | $250 | $3,000 |
| Data dictionary | Analytics Engineer | 12 | $200 | $2,400 |
| User training materials | Frontend Developer | 16 | $180 | $2,880 |
| Troubleshooting guide | Data Engineer | 12 | $200 | $2,400 |
| Demo video recording (optional) | Solutions Architect | 8 | $250 | $2,000 |
| Handoff meeting and training | Solutions Architect | 8 | $250 | $2,000 |
| **Phase 5 Subtotal** | | **108 hrs** | | **$23,480** |

---

### 8.3 Total Project Cost Summary

| Phase | Duration | Hours | Cost |
|-------|----------|-------|------|
| Phase 1: Foundation & Data Pipeline | Weeks 1-3 | 192 | $39,800 |
| Phase 2: Transformations & Analytics | Weeks 4-6 | 244 | $49,800 |
| Phase 3: ML & Semantic Layer | Weeks 7-8 | 148 | $32,720 |
| Phase 4: Applications | Weeks 9-11 | 236 | $45,400 |
| Phase 5: Documentation & Handoff | Week 12 | 108 | $23,480 |
| **SUBTOTAL** | **12 weeks** | **928 hrs** | **$191,200** |
| **Project Management (10%)** | | 93 hrs | $19,120 |
| **Contingency (10%)** | | | $21,032 |
| **TOTAL PROJECT COST** | | **1,021 hrs** | **$231,352** |

**Payment Terms:**
- 30% deposit upon contract signing ($69,406)
- 20% upon Phase 2 completion ($46,270)
- 20% upon Phase 4 completion ($46,270)
- 30% upon final delivery and acceptance ($69,406)

**Assumptions:**
- Client provides Snowflake trial account with sufficient credits
- Client provides AWS account for S3/IAM setup
- Client provides timely feedback during bi-weekly reviews
- Scope is fixed per this specification (change requests will be assessed separately)

---

### 8.4 Ongoing Support Options (Post-Delivery)

**Option 1: Maintenance & Support Package**
- 20 hours/month of on-call support
- Bug fixes and minor enhancements
- Monthly health checks
- **Cost:** $4,000/month

**Option 2: Managed Services**
- Full platform management (monitoring, optimization, updates)
- Dedicated Slack channel
- SLA: 4-hour response time
- **Cost:** $12,000/month

**Option 3: Ad-Hoc Consulting**
- Pay-as-you-go for enhancements or new features
- **Rate:** $250/hour (Solutions Architect) or $200/hour (Engineers)

---

### 8.5 Client Responsibilities

To ensure project success, the client must:
1. Provide Snowflake trial account access within 3 business days of kickoff
2. Provide AWS account with permissions to create S3, IAM, SNS/SQS resources
3. Assign a product owner for weekly meetings and decision-making
4. Provide feedback on demos within 2 business days
5. Conduct user acceptance testing (UAT) in Phase 5

---

### 8.6 Success Criteria

The project will be considered successful when:
1. All 6 user stories pass acceptance criteria testing
2. End-to-end demo (bulk load → streaming → transformations → ML → app) executes without errors
3. Performance benchmarks met:
   - Snowpipe ingestion latency <5 minutes
   - dbt run completes in <15 minutes
   - Streamlit app queries return in <3 seconds
4. Documentation package delivered and reviewed
5. Client team trained on platform operation
6. Code and artifacts transferred to client's Git repository

---

## 9. Deliverables

### Code Deliverables
1. **Terraform Modules**
   - S3 bucket, IAM roles, SNS/SQS configuration
   - `README.md` with deployment instructions
   - Variable definitions and outputs

2. **Snowflake SQL Scripts**
   - Database/schema creation
   - RBAC setup (roles, grants)
   - Stages and storage integration
   - Bronze layer tables
   - Snowpipe definitions
   - Observability schema

3. **dbt Project**
   - Complete dbt project with all models
   - Staging (Bronze → Silver)
   - Intermediate (transformations)
   - Marts (Gold layer)
   - Tests (data quality)
   - Documentation (model descriptions)
   - `profiles.yml.example` for setup

4. **Synthetic Data Generators**
   - `generate_customers.py` (Python script)
   - `generate_transactions.sql` (Snowflake SQL)
   - `generate_hourly_batch.py` (Python script for streaming)
   - Requirements file (`requirements.txt`)

5. **ML Models**
   - Churn prediction model SQL scripts
   - Training data preparation scripts
   - Model evaluation notebooks (optional)

6. **Semantic Layer**
   - `semantic_model.yaml` for Cortex Analyst
   - Sample queries documentation

7. **Streamlit Application**
   - `customer_analytics_app.py` (4-tab app)
   - `requirements.txt`
   - `secrets.toml.example` for configuration
   - Deployment instructions for SiS

8. **Configuration Files**
   - `.gitignore`
   - Environment variable templates
   - Snowflake connection profiles

### Documentation Deliverables

1. **Architecture Documentation**
   - High-level architecture diagram
   - Component interaction diagram
   - Technology stack overview
   - Medallion architecture explanation

2. **Setup Guides**
   - Terraform deployment guide
   - Snowflake environment setup
   - dbt installation and configuration
   - Streamlit deployment (local and SiS)
   - Data generation guide

3. **Data Documentation**
   - Entity-Relationship Diagram (ERD)
   - Data dictionary (all tables and columns)
   - Data lineage documentation
   - Semantic model guide

4. **User Guides**
   - Streamlit app user manual
   - AI Assistant usage tips
   - Export functionality guide
   - Sample questions for Cortex Analyst

5. **Operational Guides**
   - Monitoring and observability guide
   - Troubleshooting common issues
   - Snowpipe management
   - dbt run schedule recommendations

6. **Demo Materials**
   - Demo script with talking points
   - User story walkthroughs
   - Business value narrative
   - ROI calculations

7. **Project Documentation**
   - This specification document
   - Meeting notes and decisions log
   - Test results and UAT sign-off
   - Handoff checklist

---

## 10. Error Handling & Recovery Strategies

### 10.1 Data Ingestion Error Handling

#### Snowpipe Failures

**Error Scenarios:**
1. Malformed CSV files (missing columns, incorrect delimiters)
2. S3 access denied (IAM role issues)
3. SNS/SQS notification failures
4. Schema evolution (unexpected columns)

**Handling Strategy:**
```sql
-- Snowpipe with error handling
CREATE OR REPLACE PIPE bronze.transaction_pipe
  AUTO_INGEST = TRUE
  AWS_SNS_TOPIC = 'arn:aws:sns:us-east-1:ACCOUNT_ID:snowflake-s3-events'
  ERROR_INTEGRATION = error_notification_integration  -- Email/webhook on errors
AS
COPY INTO bronze.bronze_transactions
FROM @bronze.transaction_stage/streaming/
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
PATTERN = '.*\.csv'
ON_ERROR = 'CONTINUE'  -- Skip bad files, log errors
VALIDATION_MODE = 'RETURN_ERRORS';  -- Test mode to preview errors

-- Monitor Snowpipe errors
SELECT
    SYSTEM$PIPE_STATUS('bronze.transaction_pipe') AS pipe_status;

-- Query error table
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'bronze_transactions',
    START_TIME => DATEADD(hours, -24, CURRENT_TIMESTAMP())
))
WHERE STATUS = 'LOAD_FAILED'
ORDER BY LAST_LOAD_TIME DESC;
```

**Recovery Procedures:**
1. **Automatic:** `ON_ERROR = 'CONTINUE'` allows pipeline to proceed with good files
2. **Manual Review:** Query `COPY_HISTORY` daily for failed files
3. **Reprocessing:** Fix source files in S3, Snowpipe auto-retries on new S3 events
4. **Alerting:** Set up Snowflake notification integration to email data engineering team on pipe errors

**Python Data Generator Error Handling:**
```python
import logging
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
def upload_to_s3(file_path, bucket, key):
    """Upload file to S3 with retry logic"""
    try:
        s3 = boto3.client('s3')
        s3.upload_file(file_path, bucket, key)
        logger.info(f"Successfully uploaded {key} to {bucket}")
        return True
    except ClientError as e:
        logger.error(f"S3 upload failed: {e}")
        raise  # Retry via tenacity
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

def generate_hourly_batch():
    try:
        # Generate data
        df = create_transactions()

        # Validate data
        if df.empty:
            raise ValueError("Generated dataframe is empty")

        if df['transaction_amount'].isnull().sum() > 0:
            logger.warning(f"Found {df['transaction_amount'].isnull().sum()} null amounts, filtering...")
            df = df.dropna(subset=['transaction_amount'])

        # Save locally
        filename = f'transactions_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        df.to_csv(filename, index=False)

        # Upload with retry
        s3_key = f'transactions/streaming/{datetime.now().strftime("%Y/%m/%d/%H")}/{filename}'
        upload_to_s3(filename, 'snowflake-customer-analytics-data', s3_key)

        # Cleanup
        os.remove(filename)

    except Exception as e:
        logger.error(f"Batch generation failed: {e}")
        # Send alert to monitoring system
        send_slack_alert(f"Hourly batch failed: {e}")
        raise
```

#### Bulk Load Failures

**Error Scenarios:**
1. Timeout on large file loads
2. Network interruptions
3. Out of memory errors

**Handling Strategy:**
```sql
-- Use multiple smaller files instead of one giant file
-- Split historical load into monthly chunks

-- Transactional load with rollback
BEGIN TRANSACTION;

COPY INTO bronze.bronze_transactions
FROM @bronze.transaction_stage/historical/2024_01.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
ON_ERROR = 'ABORT_STATEMENT';  -- Fail fast on bulk loads

-- Validate row count matches expected
SET expected_rows = 750000;
SET actual_rows = (SELECT COUNT(*) FROM bronze.bronze_transactions WHERE source_file = '2024_01.csv');

-- Rollback if mismatch
IF ($actual_rows != $expected_rows) THEN
    ROLLBACK;
    RAISE EXCEPTION 'Row count mismatch: expected ' || $expected_rows || ', got ' || $actual_rows;
ELSE
    COMMIT;
END IF;
```

**Recovery:**
- Failed bulk loads leave no partial data (transactional)
- Re-run COPY INTO command after fixing issue
- Monitor warehouse size - upgrade if timeout persists

---

### 10.2 dbt Transformation Error Handling

#### Model Failures

**Error Scenarios:**
1. Source table missing/empty
2. Schema changes breaking transformations
3. Test failures (data quality issues)
4. Resource exhaustion (warehouse size)

**Handling Strategy:**

**`dbt_project.yml` Configuration:**
```yaml
on-run-start:
  - "{{ log('dbt run started at ' ~ run_started_at, info=True) }}"
  - "INSERT INTO observability.pipeline_run_metadata (run_id, run_timestamp, status) VALUES ('{{ invocation_id }}', '{{ run_started_at }}', 'STARTED')"

on-run-end:
  - "{{ log('dbt run completed', info=True) }}"
  - "INSERT INTO observability.pipeline_run_metadata (run_id, run_timestamp, status, models_run, models_failed)
     VALUES ('{{ invocation_id }}', CURRENT_TIMESTAMP(),
             CASE WHEN {{ results | selectattr('status', 'equalto', 'error') | list | length }} > 0 THEN 'FAILED' ELSE 'SUCCESS' END,
             {{ results | length }},
             {{ results | selectattr('status', 'equalto', 'error') | list | length }})"

models:
  customer_analytics:
    +on_schema_change: "fail"  # Explicit schema change detection
    +pre-hook:
      - "{{ log('Running model: ' ~ this, info=True) }}"
    +post-hook:
      - "INSERT INTO observability.layer_record_counts (run_id, model_name, record_count, run_timestamp)
         VALUES ('{{ invocation_id }}', '{{ this }}', (SELECT COUNT(*) FROM {{ this }}), CURRENT_TIMESTAMP())"
```

**Model-Level Error Handling:**
```sql
-- models/staging/stg_transactions.sql
{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    on_schema_change='fail'
) }}

-- Defensive checks
{% if execute %}
    {% set source_count = run_query("SELECT COUNT(*) as cnt FROM " ~ source('bronze', 'bronze_transactions')).columns[0].values()[0] %}
    {% if source_count == 0 %}
        {{ exceptions.raise_compiler_error("Source table bronze_transactions is empty!") }}
    {% endif %}
{% endif %}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'bronze_transactions') }}
    {% if is_incremental() %}
    WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

-- Validation: Check for required fields
validated AS (
    SELECT *,
        CASE
            WHEN transaction_id IS NULL THEN 'MISSING_ID'
            WHEN customer_id IS NULL THEN 'MISSING_CUSTOMER'
            WHEN transaction_amount IS NULL THEN 'MISSING_AMOUNT'
            WHEN transaction_amount < 0 THEN 'NEGATIVE_AMOUNT'
            ELSE 'VALID'
        END AS validation_status
    FROM source
),

-- Log validation failures
{{ log_validation_failures('validated', 'validation_status') }}

-- Filter to valid records only
final AS (
    SELECT * FROM validated WHERE validation_status = 'VALID'
)

SELECT * FROM final
```

**Custom Macro for Validation Logging:**
```sql
-- macros/log_validation_failures.sql
{% macro log_validation_failures(model_ref, status_column) %}
    {% if execute %}
        INSERT INTO observability.data_quality_metrics (
            run_id, run_timestamp, layer, table_name, check_type, records_checked, records_failed, failure_details
        )
        SELECT
            '{{ invocation_id }}' AS run_id,
            CURRENT_TIMESTAMP() AS run_timestamp,
            'staging' AS layer,
            '{{ this }}' AS table_name,
            {{ status_column }} AS check_type,
            COUNT(*) AS records_checked,
            SUM(CASE WHEN {{ status_column }} != 'VALID' THEN 1 ELSE 0 END) AS records_failed,
            OBJECT_CONSTRUCT('failed_ids', ARRAY_AGG(transaction_id)) AS failure_details
        FROM {{ model_ref }}
        GROUP BY {{ status_column }}
        HAVING {{ status_column }} != 'VALID';
    {% endif %}
{% endmacro %}
```

**Recovery Procedures:**
1. **Model Failure:** dbt run continues with other models (default behavior)
2. **Test Failure:** Use `dbt test --warn-error` to treat as warnings during development
3. **Full Failure:** Run `dbt run --full-refresh` to rebuild from scratch (Bronze layer intact)
4. **Partial Failure:** `dbt run --select model_name+` to re-run failed model and downstream dependencies

---

### 10.3 ML Model Error Handling

#### Cortex ML Training Failures

**Error Scenarios:**
1. Insufficient training data
2. Feature null values
3. Class imbalance too extreme
4. Model training timeout

**Handling Strategy:**
```sql
-- Pre-training validation
CREATE OR REPLACE PROCEDURE validate_training_data()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    row_count INT;
    null_feature_count INT;
    class_balance FLOAT;
BEGIN
    -- Check row count
    SELECT COUNT(*) INTO row_count FROM ml_training_data;
    IF (row_count < 1000) THEN
        RETURN 'ERROR: Insufficient training data (' || row_count || ' rows). Need at least 1000.';
    END IF;

    -- Check for null features
    SELECT COUNT(*) INTO null_feature_count
    FROM ml_training_data
    WHERE avg_monthly_spend IS NULL OR mom_spend_change_pct IS NULL;

    IF (null_feature_count > 0) THEN
        RETURN 'ERROR: Found ' || null_feature_count || ' rows with null features.';
    END IF;

    -- Check class balance
    SELECT
        MIN(class_count)::FLOAT / MAX(class_count)::FLOAT INTO class_balance
    FROM (
        SELECT churned, COUNT(*) as class_count
        FROM ml_training_data
        GROUP BY churned
    );

    IF (class_balance < 0.05) THEN
        RETURN 'WARNING: Severe class imbalance (' || (class_balance * 100)::STRING || '%). Consider resampling.';
    END IF;

    RETURN 'PASS: Training data validated';
END;
$$;

-- Execute validation before training
CALL validate_training_data();

-- Train with error handling
CREATE OR REPLACE PROCEDURE train_churn_model()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Drop existing model if retraining
    DROP SNOWFLAKE.ML.CLASSIFICATION IF EXISTS churn_model;

    -- Train model
    CREATE SNOWFLAKE.ML.CLASSIFICATION churn_model(
        INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'ml_training_data'),
        TARGET_COLNAME => 'churned',
        CONFIG_OBJECT => {
            'EVALUATION_METRIC': 'F1',
            'ON_ERROR': 'SKIP_ROW'  -- Skip rows with issues
        }
    );

    -- Validate model performance
    LET model_f1 := (SELECT F1_SCORE FROM TABLE(churn_model!SHOW_EVALUATION_METRICS()));

    IF (model_f1 < 0.5) THEN
        RETURN 'ERROR: Model F1 score too low (' || model_f1 || '). Investigate feature engineering.';
    END IF;

    RETURN 'SUCCESS: Model trained with F1 score ' || model_f1;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: Model training failed - ' || SQLERRM;
END;
$$;
```

**Recovery:**
- Training failures don't impact production (existing predictions remain)
- Retrain manually after fixing data issues
- Alert data science team via observability dashboard

---

### 10.4 Streamlit Application Error Handling

#### Query Failures

**Error Scenarios:**
1. Snowflake connection timeout
2. Query timeout (large result sets)
3. Invalid user input (SQL injection risk)
4. Session expiration

**Handling Strategy:**
```python
import streamlit as st
import snowflake.connector
from snowflake.connector.errors import DatabaseError, ProgrammingError
import pandas as pd

# Connection with retry logic
@st.cache_resource
def get_connection():
    try:
        conn = snowflake.connector.connect(
            user=st.secrets["snowflake"]["user"],
            account=st.secrets["snowflake"]["account"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            database="customer_analytics",
            schema="gold",
            client_session_keep_alive=True,  # Prevent timeout
            connection_timeout=10,
            network_timeout=30
        )
        return conn
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {e}")
        st.stop()

# Safe query execution
def execute_query(query, params=None):
    """Execute query with error handling and timeout"""
    conn = get_connection()

    try:
        cursor = conn.cursor()

        # Use parameterized queries to prevent SQL injection
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        # Set query timeout
        cursor.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 60")

        # Fetch results with size limit
        results = cursor.fetchmany(10000)  # Limit to 10K rows for UI performance

        df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
        return df

    except ProgrammingError as e:
        st.error(f"Query error: {e}")
        st.error("Please contact support if this issue persists.")
        return pd.DataFrame()

    except DatabaseError as e:
        if "timeout" in str(e).lower():
            st.warning("Query timed out. Try filtering to a smaller date range.")
        else:
            st.error(f"Database error: {e}")
        return pd.DataFrame()

    except Exception as e:
        st.error(f"Unexpected error: {e}")
        # Log to monitoring system
        logging.error(f"Streamlit query error: {e}", exc_info=True)
        return pd.DataFrame()

    finally:
        cursor.close()

# Safe user input handling
def get_customer_profile(customer_id):
    """Fetch customer profile with input validation"""

    # Validate input format
    if not customer_id.startswith("CUST") or not customer_id[4:].isdigit():
        st.error("Invalid customer ID format. Expected: CUST########")
        return None

    # Use parameterized query (SQL injection protection)
    query = """
        SELECT * FROM gold.customer_analytics.customer_360_profile
        WHERE customer_id = %s
    """

    df = execute_query(query, params=(customer_id,))

    if df.empty:
        st.warning(f"Customer {customer_id} not found.")
        return None

    return df.iloc[0]

# Global error boundary
def main():
    try:
        # App code here
        render_app()
    except Exception as e:
        st.error("An unexpected error occurred. Please refresh the page.")
        logging.error(f"Streamlit app crashed: {e}", exc_info=True)

        # Show error details in expander (for debugging)
        with st.expander("Error Details (for support)"):
            st.code(str(e))

if __name__ == "__main__":
    main()
```

**Recovery:**
- User can retry failed queries with refresh button
- Connection errors auto-reconnect on next query
- Session state preserved across errors where possible

---

### 10.5 Monitoring & Alerting Thresholds

#### Critical Alerts (Immediate Response)

| Metric | Threshold | Action |
|--------|-----------|--------|
| Snowpipe failure rate | >10% of files in 1 hour | Page on-call engineer |
| dbt run failure | Any model in Gold layer | Slack alert to data team |
| Streamlit app down | >3 consecutive health check failures | Page on-call engineer |
| ML model prediction failure | Unable to score customers | Email data science team |

#### Warning Alerts (Review within 4 hours)

| Metric | Threshold | Action |
|--------|-----------|--------|
| Data quality - duplicates | >5% of hourly batch | Slack alert to data team |
| Snowpipe latency | >15 minutes average | Email data engineering |
| Query performance | >10 second dashboard load | Email analytics team |
| Storage growth | >50GB/day unexpected increase | Email infrastructure team |

#### Informational Monitoring (Daily Review)

| Metric | Threshold | Action |
|--------|-----------|--------|
| dbt test warnings | Any test fails in staging | Daily digest email |
| Customer segment shifts | >10% customers change segment | Dashboard notification |
| Warehouse credit consumption | >$100/day | Daily cost report |

**Alert Implementation:**
```sql
-- Create alert for Snowpipe failures
CREATE OR REPLACE ALERT snowpipe_failure_alert
  WAREHOUSE = compute_wh
  SCHEDULE = '5 MINUTE'
  IF (EXISTS (
    SELECT 1
    FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
        DATE_RANGE_START => DATEADD(hour, -1, CURRENT_TIMESTAMP()),
        PIPE_NAME => 'bronze.transaction_pipe'
    ))
    WHERE ERROR_COUNT > 0
  ))
  THEN CALL SYSTEM$SEND_EMAIL(
    'data-alerts@company.com',
    'CRITICAL: Snowpipe Failure Detected',
    'Transaction pipe has encountered errors. Check COPY_HISTORY for details.'
  );

-- Create alert for dbt failures
CREATE OR REPLACE ALERT dbt_failure_alert
  WAREHOUSE = compute_wh
  SCHEDULE = '10 MINUTE'
  IF (EXISTS (
    SELECT 1
    FROM observability.pipeline_run_metadata
    WHERE status = 'FAILED'
      AND run_timestamp > DATEADD(minute, -10, CURRENT_TIMESTAMP())
  ))
  THEN CALL SYSTEM$SEND_EMAIL(
    'data-team@company.com',
    'WARNING: dbt Run Failed',
    'Recent dbt run failed. Check observability.pipeline_run_metadata for details.'
  );
```

---

## 11. Comprehensive Testing Plan

### 11.1 Unit Testing

#### Python Data Generators

**Test Framework:** pytest

**Test File:** `tests/test_data_generation.py`

```python
import pytest
import pandas as pd
from data_generation.generate_customers import generate_customers, validate_customer_data
from data_generation.generate_hourly_batch import generate_hourly_batch

class TestCustomerGeneration:

    def test_generates_correct_row_count(self):
        """Test that generator produces specified number of customers"""
        df = generate_customers(n=1000)
        assert len(df) == 1000

    def test_customer_id_format(self):
        """Test customer ID follows CUST######## pattern"""
        df = generate_customers(n=100)
        assert all(df['customer_id'].str.match(r'^CUST\d{8}$'))

    def test_segment_distribution(self):
        """Test customer segments match expected distribution"""
        df = generate_customers(n=10000)
        segment_pct = df['customer_segment'].value_counts(normalize=True)

        # Allow 5% tolerance
        assert 0.10 <= segment_pct['High-Value Travelers'] <= 0.20
        assert 0.35 <= segment_pct['Stable Mid-Spenders'] <= 0.45
        assert 0.20 <= segment_pct['Budget-Conscious'] <= 0.30
        assert 0.05 <= segment_pct['Declining'] <= 0.15

    def test_no_null_required_fields(self):
        """Test required fields are not null"""
        df = generate_customers(n=100)
        required_fields = ['customer_id', 'email', 'state', 'card_type', 'credit_limit']

        for field in required_fields:
            assert df[field].notna().all(), f"Found nulls in {field}"

    def test_credit_limit_ranges(self):
        """Test credit limits are realistic"""
        df = generate_customers(n=1000)
        assert df['credit_limit'].min() >= 5000
        assert df['credit_limit'].max() <= 50000

    def test_email_format(self):
        """Test emails are valid format"""
        df = generate_customers(n=100)
        assert all(df['email'].str.contains('@'))

class TestTransactionGeneration:

    def test_transaction_count_reasonable(self):
        """Test hourly batch generates realistic transaction count"""
        df = generate_hourly_batch()
        assert 500 <= len(df) <= 1000  # 750 +/- variance

    def test_transaction_id_unique(self):
        """Test transaction IDs are unique"""
        df = generate_hourly_batch()
        assert df['transaction_id'].is_unique

    def test_transaction_amounts_positive(self):
        """Test all transaction amounts are positive"""
        df = generate_hourly_batch()
        assert (df['transaction_amount'] > 0).all()

    def test_valid_merchant_categories(self):
        """Test merchant categories are from valid list"""
        df = generate_hourly_batch()
        valid_categories = ['Grocery', 'Gas', 'Dining', 'Retail', 'Travel',
                           'Entertainment', 'Healthcare', 'Utilities']
        assert df['merchant_category'].isin(valid_categories).all()

# Run tests
# pytest tests/test_data_generation.py -v --cov=data_generation
```

#### dbt Model Unit Tests

**Test File:** `tests/assert_no_duplicate_transactions.sql`

```sql
-- Test: No duplicate transaction IDs in silver layer
SELECT
    transaction_id,
    COUNT(*) as duplicate_count
FROM {{ ref('stg_transactions') }}
GROUP BY transaction_id
HAVING COUNT(*) > 1
```

**Test File:** `tests/assert_all_customers_have_segment.sql`

```sql
-- Test: All customers assigned a segment
SELECT
    customer_id,
    customer_segment
FROM {{ ref('dim_customer') }}
WHERE customer_segment IS NULL
  AND is_current = TRUE
```

**Test File:** `tests/assert_transaction_amounts_positive.sql`

```sql
-- Test: All transaction amounts are positive
SELECT
    transaction_id,
    transaction_amount
FROM {{ ref('fact_transaction') }}
WHERE transaction_amount <= 0
```

**Schema Tests:** `models/schema.yml`

```yaml
models:
  - name: stg_transactions
    description: Cleaned and deduplicated transactions
    columns:
      - name: transaction_id
        description: Unique transaction identifier
        tests:
          - unique
          - not_null
      - name: customer_id
        description: Customer identifier
        tests:
          - not_null
          - relationships:
              to: ref('stg_customers')
              field: customer_id
      - name: transaction_amount
        description: Transaction amount in USD
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "> 0"

  - name: dim_customer
    columns:
      - name: customer_key
        tests:
          - unique
          - not_null
      - name: customer_id
        tests:
          - not_null
```

**Run Tests:**
```bash
# Run all dbt tests
dbt test

# Run specific test
dbt test --select test_name:assert_no_duplicate_transactions

# Run tests for specific model
dbt test --select dim_customer

# Treat warnings as errors (strict mode)
dbt test --warn-error
```

---

### 11.2 Integration Testing

#### End-to-End Pipeline Test

**Test Scenario:** Validate full data flow from S3 → Bronze → Silver → Gold → Application

**Test Script:** `tests/integration/test_end_to_end_pipeline.sh`

```bash
#!/bin/bash
set -e

echo "=== Starting End-to-End Integration Test ==="

# Step 1: Generate test data (small batch)
echo "Step 1: Generating test data..."
python data_generation/generate_customers.py --count 100 --output test_customers.csv
python data_generation/generate_hourly_batch.py --count 50 --output test_transactions.csv

# Step 2: Upload to S3
echo "Step 2: Uploading to S3..."
aws s3 cp test_customers.csv s3://snowflake-customer-analytics-data/customers/test/
aws s3 cp test_transactions.csv s3://snowflake-customer-analytics-data/transactions/streaming/test/

# Step 3: Bulk load to Bronze
echo "Step 3: Loading to Bronze layer..."
snowsql -f snowflake/test_bulk_load.sql

# Step 4: Wait for Snowpipe (if testing streaming)
echo "Step 4: Waiting for Snowpipe ingestion (30 seconds)..."
sleep 30

# Step 5: Check Bronze layer
echo "Step 5: Validating Bronze layer..."
BRONZE_COUNT=$(snowsql -q "SELECT COUNT(*) FROM bronze.bronze_transactions WHERE source_file LIKE '%test%';" -o output_format=tsv -o header=false)
echo "Bronze layer row count: $BRONZE_COUNT"

if [ "$BRONZE_COUNT" -lt 50 ]; then
    echo "ERROR: Expected at least 50 rows in Bronze, got $BRONZE_COUNT"
    exit 1
fi

# Step 6: Run dbt transformations
echo "Step 6: Running dbt transformations..."
cd dbt_customer_analytics
dbt run --select +fact_transaction
dbt test --select +fact_transaction

# Step 7: Validate Gold layer
echo "Step 7: Validating Gold layer..."
GOLD_COUNT=$(snowsql -q "SELECT COUNT(*) FROM gold.fact_transaction WHERE source_file LIKE '%test%';" -o output_format=tsv -o header=false)
echo "Gold layer row count: $GOLD_COUNT"

if [ "$GOLD_COUNT" -lt 50 ]; then
    echo "ERROR: Expected at least 50 rows in Gold, got $GOLD_COUNT"
    exit 1
fi

# Step 8: Test Streamlit query
echo "Step 8: Testing Streamlit query..."
python tests/integration/test_streamlit_query.py

# Step 9: Cleanup test data
echo "Step 9: Cleaning up test data..."
snowsql -q "DELETE FROM bronze.bronze_transactions WHERE source_file LIKE '%test%';"
snowsql -q "DELETE FROM gold.fact_transaction WHERE source_file LIKE '%test%';"
aws s3 rm s3://snowflake-customer-analytics-data/customers/test/ --recursive
aws s3 rm s3://snowflake-customer-analytics-data/transactions/streaming/test/ --recursive

echo "=== End-to-End Integration Test PASSED ==="
```

**Python Test Helper:** `tests/integration/test_streamlit_query.py`

```python
import snowflake.connector
import os

def test_streamlit_connectivity():
    """Test that Streamlit can connect and query Gold layer"""
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse='compute_wh',
        database='customer_analytics',
        schema='gold'
    )

    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM customer_analytics.customer_360_profile")
    count = cursor.fetchone()[0]

    assert count > 0, "customer_360_profile is empty"

    print(f"✓ Streamlit query test passed ({count} rows in customer_360_profile)")
    conn.close()

if __name__ == '__main__':
    test_streamlit_connectivity()
```

---

### 11.3 Performance Testing

#### Query Performance Benchmarks

**Test Script:** `tests/performance/benchmark_queries.sql`

```sql
-- Benchmark 1: Customer 360 profile load (target: <2 seconds)
SET query_start = CURRENT_TIMESTAMP();

SELECT * FROM gold.customer_analytics.customer_360_profile
WHERE customer_id = 'CUST00012345';

SET query_end = CURRENT_TIMESTAMP();
SELECT 'Customer 360 Lookup' AS test_name,
       DATEDIFF('millisecond', $query_start, $query_end) AS duration_ms,
       CASE WHEN DATEDIFF('millisecond', $query_start, $query_end) < 2000
            THEN 'PASS' ELSE 'FAIL' END AS status;

-- Benchmark 2: Segment filter (target: <3 seconds)
SET query_start = CURRENT_TIMESTAMP();

SELECT customer_id, full_name, lifetime_value, churn_risk_category
FROM gold.customer_analytics.customer_360_profile
WHERE customer_segment = 'High-Value Travelers'
  AND state IN ('CA', 'NY', 'TX')
  AND churn_risk_category = 'High Risk';

SET query_end = CURRENT_TIMESTAMP();
SELECT 'Segment Filter Query' AS test_name,
       DATEDIFF('millisecond', $query_start, $query_end) AS duration_ms,
       CASE WHEN DATEDIFF('millisecond', $query_start, $query_end) < 3000
            THEN 'PASS' ELSE 'FAIL' END AS status;

-- Benchmark 3: Aggregation query (target: <5 seconds)
SET query_start = CURRENT_TIMESTAMP();

SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    AVG(lifetime_value) AS avg_ltv,
    SUM(lifetime_value) AS total_ltv
FROM gold.customer_analytics.customer_360_profile
GROUP BY customer_segment
ORDER BY total_ltv DESC;

SET query_end = CURRENT_TIMESTAMP();
SELECT 'Segment Aggregation' AS test_name,
       DATEDIFF('millisecond', $query_start, $query_end) AS duration_ms,
       CASE WHEN DATEDIFF('millisecond', $query_start, $query_end) < 5000
            THEN 'PASS' ELSE 'FAIL' END AS status;

-- Benchmark 4: dbt full refresh (target: <15 minutes for all models)
-- Run separately via: time dbt run --full-refresh
```

**Load Testing for Streamlit:**

Use Locust to simulate concurrent users:

`tests/performance/locustfile.py`:

```python
from locust import HttpUser, task, between
import random

class StreamlitUser(HttpUser):
    wait_time = between(1, 3)

    @task(3)
    def load_segment_explorer(self):
        """Simulate user loading segment explorer"""
        self.client.get("/")

    @task(2)
    def view_customer_360(self):
        """Simulate user viewing customer profile"""
        customer_id = f"CUST{random.randint(1, 50000):08d}"
        self.client.get(f"/?customer_id={customer_id}")

    @task(1)
    def ask_ai_question(self):
        """Simulate AI assistant query"""
        questions = [
            "What is the average spend in California?",
            "Show me high-value customers",
            "Which customers are at risk of churning?"
        ]
        self.client.post("/ai", json={"question": random.choice(questions)})

# Run: locust -f tests/performance/locustfile.py --host=https://your-streamlit-app.snowflakecomputing.com
```

---

### 11.4 Data Quality Testing

#### Statistical Validation Tests

**Test Script:** `tests/data_quality/validate_synthetic_data.sql`

```sql
-- Test 1: Segment distribution matches specification
WITH segment_dist AS (
    SELECT
        customer_segment,
        COUNT(*) AS count,
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS pct
    FROM gold.dim_customer
    WHERE is_current = TRUE
    GROUP BY customer_segment
)
SELECT
    customer_segment,
    pct AS actual_pct,
    CASE customer_segment
        WHEN 'High-Value Travelers' THEN 15.0
        WHEN 'Stable Mid-Spenders' THEN 40.0
        WHEN 'Budget-Conscious' THEN 25.0
        WHEN 'Declining' THEN 10.0
        WHEN 'New & Growing' THEN 10.0
    END AS expected_pct,
    ABS(pct - expected_pct) AS deviation,
    CASE WHEN ABS(pct - expected_pct) < 5.0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM segment_dist;

-- Test 2: Transaction volume distribution (expect ~750/hour average)
WITH hourly_volume AS (
    SELECT
        DATE_TRUNC('hour', transaction_date) AS hour,
        COUNT(*) AS txn_count
    FROM gold.fact_transaction
    WHERE transaction_date >= DATEADD('day', -7, CURRENT_DATE())
    GROUP BY DATE_TRUNC('hour', transaction_date)
)
SELECT
    AVG(txn_count) AS avg_hourly_txns,
    STDDEV(txn_count) AS stddev,
    MIN(txn_count) AS min_hourly,
    MAX(txn_count) AS max_hourly,
    CASE WHEN AVG(txn_count) BETWEEN 650 AND 850 THEN 'PASS' ELSE 'FAIL' END AS status
FROM hourly_volume;

-- Test 3: Declining segment shows actual decline
WITH declining_customers AS (
    SELECT customer_id
    FROM gold.dim_customer
    WHERE customer_segment = 'Declining' AND is_current = TRUE
),
monthly_spend AS (
    SELECT
        c.customer_id,
        DATE_TRUNC('month', t.transaction_date) AS month,
        SUM(t.transaction_amount) AS monthly_spend
    FROM gold.fact_transaction t
    JOIN gold.dim_customer c ON t.customer_key = c.customer_key
    WHERE c.customer_id IN (SELECT customer_id FROM declining_customers)
      AND c.is_current = TRUE
    GROUP BY c.customer_id, DATE_TRUNC('month', t.transaction_date)
),
decline_check AS (
    SELECT
        customer_id,
        (MAX(monthly_spend) - MIN(monthly_spend)) / NULLIF(MAX(monthly_spend), 0) * 100 AS decline_pct
    FROM monthly_spend
    GROUP BY customer_id
)
SELECT
    COUNT(*) AS declining_customer_count,
    AVG(decline_pct) AS avg_decline_pct,
    CASE WHEN AVG(decline_pct) > 30 THEN 'PASS' ELSE 'FAIL' END AS status
FROM decline_check;

-- Test 4: No data gaps in streaming ingestion
WITH date_hours AS (
    SELECT
        DATEADD('hour', SEQ4(), DATEADD('day', -7, CURRENT_DATE())) AS hour
    FROM TABLE(GENERATOR(ROWCOUNT => 168))  -- 7 days * 24 hours
),
actual_hours AS (
    SELECT DISTINCT DATE_TRUNC('hour', transaction_date) AS hour
    FROM gold.fact_transaction
    WHERE transaction_date >= DATEADD('day', -7, CURRENT_DATE())
)
SELECT
    dh.hour AS missing_hour
FROM date_hours dh
LEFT JOIN actual_hours ah ON dh.hour = ah.hour
WHERE ah.hour IS NULL
ORDER BY dh.hour;
-- Expect 0 rows (no gaps)
```

---

### 11.5 User Acceptance Testing (UAT)

#### UAT Test Cases

**Test Case 1: Marketing Manager - Segment Export**

| Step | Action | Expected Result | Status |
|------|--------|-----------------|--------|
| 1 | Open Streamlit app → "Segment Explorer" tab | App loads without errors | ☐ |
| 2 | Select "High-Value Travelers" segment | Filter applied, customer list displayed | ☐ |
| 3 | Apply state filter: "CA, NY" | List filtered to those states only | ☐ |
| 4 | Verify metrics: Customer count, Total LTV, Avg LTV | Metrics display reasonable values | ☐ |
| 5 | Click "Export Segment to CSV" | CSV file downloads successfully | ☐ |
| 6 | Open CSV file | Contains correct columns and data | ☐ |

**Test Case 2: Data Analyst - Customer 360 Deep Dive**

| Step | Action | Expected Result | Status |
|------|--------|-----------------|--------|
| 1 | Navigate to "Customer 360" tab | Tab loads | ☐ |
| 2 | Enter customer ID: "CUST00012345" | Customer profile loads | ☐ |
| 3 | Verify profile card shows: Name, Segment, LTV, Churn Risk | All fields populated | ☐ |
| 4 | Review monthly spend trend chart | Chart displays 18 months of data | ☐ |
| 5 | Review category breakdown chart | Top 10 categories shown | ☐ |
| 6 | Scroll to transaction history table | Recent 100 transactions displayed | ☐ |
| 7 | Test filter on transaction table | Table filters correctly | ☐ |

**Test Case 3: Marketing Manager - AI Assistant**

| Step | Action | Expected Result | Status |
|------|--------|-----------------|--------|
| 1 | Navigate to "AI Assistant" tab | Chat interface loads | ☐ |
| 2 | Ask: "What is the average spend of customers in California?" | AI returns answer with data | ☐ |
| 3 | Verify answer includes numeric value and explanation | Answer is clear and correct | ☐ |
| 4 | Click "View SQL Query" | Generated SQL is displayed | ☐ |
| 5 | Ask: "Show me customers spending over $5K/month without premium cards" | AI returns filtered list | ☐ |
| 6 | Verify data table shows correct customers | Data matches criteria | ☐ |

**Test Case 4: Marketing Manager - Campaign ROI**

| Step | Action | Expected Result | Status |
|------|--------|-----------------|--------|
| 1 | Navigate to "Campaign Performance" tab | Tab loads | ☐ |
| 2 | Select campaign: "Declining Segment Retention Offer" | Campaign data loads | ☐ |
| 3 | Review treatment vs. control comparison | Both groups display side-by-side | ☐ |
| 4 | Verify ROI calculation is positive | ROI > 0% displayed | ☐ |
| 5 | Review pre/post campaign spend chart | Chart shows clear difference | ☐ |

**Test Case 5: Data Engineer - Snowpipe Monitoring**

| Step | Action | Expected Result | Status |
|------|--------|-----------------|--------|
| 1 | Upload test hourly batch to S3 | File uploaded successfully | ☐ |
| 2 | Wait 5 minutes | - | ☐ |
| 3 | Query Bronze table for new file | New records appear | ☐ |
| 4 | Check Snowpipe status: `SELECT SYSTEM$PIPE_STATUS('bronze.transaction_pipe')` | Status shows "RUNNING" | ☐ |
| 5 | Query COPY_HISTORY for errors | No errors logged | ☐ |
| 6 | Check observability.data_quality_metrics | Duplicate count logged | ☐ |

**Test Case 6: Data Engineer - dbt Run**

| Step | Action | Expected Result | Status |
|------|--------|-----------------|--------|
| 1 | Run `dbt run` | All models execute successfully | ☐ |
| 2 | Review output logs | No errors or warnings | ☐ |
| 3 | Run `dbt test` | All tests pass | ☐ |
| 4 | Check observability.pipeline_run_metadata | Run logged with status "SUCCESS" | ☐ |
| 5 | Verify record counts in Gold layer | Counts match Bronze layer (accounting for dedup) | ☐ |

---

### 11.6 Regression Testing

**Regression Test Suite:** Run before each release

```bash
#!/bin/bash
# tests/regression/run_regression_suite.sh

echo "=== Running Regression Test Suite ==="

# 1. Unit tests
echo "Step 1: Running Python unit tests..."
pytest tests/test_data_generation.py -v

# 2. dbt tests
echo "Step 2: Running dbt tests..."
cd dbt_customer_analytics
dbt test --warn-error

# 3. Data quality validation
echo "Step 3: Running data quality tests..."
snowsql -f tests/data_quality/validate_synthetic_data.sql

# 4. Performance benchmarks
echo "Step 4: Running performance benchmarks..."
snowsql -f tests/performance/benchmark_queries.sql

# 5. Integration test
echo "Step 5: Running end-to-end integration test..."
./tests/integration/test_end_to_end_pipeline.sh

# 6. UAT smoke test (critical paths only)
echo "Step 6: Running UAT smoke tests..."
python tests/uat/smoke_test.py

echo "=== Regression Test Suite Complete ==="
```

---

### 11.7 Test Environment Setup

**Test Data Fixtures:**

```sql
-- Create test schema (isolated from prod)
CREATE SCHEMA IF NOT EXISTS customer_analytics.test;

-- Copy production structure to test
CREATE TABLE test.bronze_transactions CLONE bronze.bronze_transactions;
CREATE TABLE test.silver_transactions CLONE silver.silver_transactions;

-- Load fixture data (100 customers, 10K transactions)
COPY INTO test.bronze_transactions
FROM @bronze.transaction_stage/fixtures/test_transactions.csv;

-- Configure dbt for test environment
-- dbt_project.yml
models:
  customer_analytics:
    +schema: test  # Override schema for testing
```

**CI/CD Integration (GitHub Actions):**

`.github/workflows/test.yml`:

```yaml
name: Test Suite

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v2

      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest pytest-cov

      - name: Run Python unit tests
        run: pytest tests/ -v --cov=data_generation

      - name: Set up dbt
        run: |
          pip install dbt-snowflake
          cd dbt_customer_analytics
          dbt deps

      - name: Run dbt tests
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
        run: |
          cd dbt_customer_analytics
          dbt test --target test
```

---

## 12. Operational Runbooks

### 12.1 Daily Operations Checklist

**Morning Checklist (15 minutes):**

```bash
#!/bin/bash
# Daily health check script

echo "=== Daily Platform Health Check ===$(date)"

# 1. Check Snowpipe status
echo "\n1. Snowpipe Status:"
snowsql -q "SELECT SYSTEM\$PIPE_STATUS('bronze.transaction_pipe');"

# 2. Check for failed loads in last 24 hours
echo "\n2. Failed Loads (last 24h):"
snowsql -q "
SELECT COUNT(*) AS failed_loads
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'bronze_transactions',
    START_TIME => DATEADD(hours, -24, CURRENT_TIMESTAMP())
))
WHERE STATUS = 'LOAD_FAILED';"

# 3. Check dbt run status
echo "\n3. Last dbt Run Status:"
snowsql -q "
SELECT run_timestamp, status, models_run, models_failed
FROM observability.pipeline_run_metadata
ORDER BY run_timestamp DESC
LIMIT 1;"

# 4. Check data quality metrics
echo "\n4. Data Quality Issues (last 24h):"
snowsql -q "
SELECT check_type, SUM(records_failed) AS total_failures
FROM observability.data_quality_metrics
WHERE run_timestamp > DATEADD(hours, -24, CURRENT_TIMESTAMP())
GROUP BY check_type
HAVING SUM(records_failed) > 0;"

# 5. Check warehouse credit consumption
echo "\n5. Warehouse Credit Usage (last 24h):"
snowsql -q "
SELECT warehouse_name, SUM(credits_used) AS total_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time > DATEADD(hours, -24, CURRENT_TIMESTAMP())
GROUP BY warehouse_name
ORDER BY total_credits DESC;"

# 6. Check storage growth
echo "\n6. Storage Growth:"
snowsql -q "
SELECT
    database_name,
    ROUND(SUM(average_database_bytes) / POWER(1024, 3), 2) AS storage_gb
FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY
WHERE usage_date = CURRENT_DATE() - 1
GROUP BY database_name;"

echo "\n=== Health Check Complete ==="
```

**Action Items Based on Results:**
- ❌ Snowpipe not running → Escalate to on-call engineer
- ❌ Failed loads > 10 → Investigate source files, fix and reprocess
- ❌ dbt run failed → Check logs, re-run failed models
- ⚠️ Duplicate rate > 5% → Investigate data source, alert upstream team
- ⚠️ Credits > $100/day → Review query performance, optimize warehouse sizing

---

### 12.2 Incident Response Procedures

#### Incident: Snowpipe Stopped Ingesting

**Symptoms:**
- No new records in Bronze layer for >1 hour
- `SYSTEM$PIPE_STATUS()` returns "PAUSED" or error

**Diagnosis Steps:**
1. Check Snowpipe status:
   ```sql
   SELECT SYSTEM$PIPE_STATUS('bronze.transaction_pipe');
   ```
2. Check S3 event notifications (SNS/SQS):
   ```bash
   aws sqs get-queue-attributes --queue-url <QUEUE_URL> --attribute-names ApproximateNumberOfMessages
   ```
3. Check IAM role permissions:
   ```bash
   aws sts assume-role --role-arn <SNOWFLAKE_ROLE_ARN> --role-session-name test
   ```
4. Check COPY_HISTORY for errors:
   ```sql
   SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(...)) WHERE STATUS = 'LOAD_FAILED' LIMIT 10;
   ```

**Resolution:**
1. **If pipe is PAUSED:**
   ```sql
   ALTER PIPE bronze.transaction_pipe REFRESH;
   ```
2. **If IAM issue:**
   - Verify IAM role trust relationship in AWS
   - Verify storage integration in Snowflake references correct role
3. **If S3 event issue:**
   - Check SNS topic subscription is active
   - Verify SQS queue is not full
   - Re-create notification if needed (Terraform)
4. **If file format issue:**
   - Download sample failed file from S3
   - Test COPY INTO manually with VALIDATION_MODE
   - Fix source data generation script

**Escalation:** If unresolved in 30 minutes → Page platform lead

---

#### Incident: dbt Run Failed

**Symptoms:**
- `observability.pipeline_run_metadata` shows status = 'FAILED'
- Email alert received

**Diagnosis Steps:**
1. Check dbt logs:
   ```bash
   cd dbt_customer_analytics
   cat logs/dbt.log | grep ERROR
   ```
2. Identify failed model:
   ```bash
   dbt run --select <model_name> --debug
   ```
3. Check source data availability:
   ```sql
   SELECT COUNT(*) FROM bronze.bronze_transactions;
   ```

**Common Failures & Fixes:**

| Error | Cause | Fix |
|-------|-------|-----|
| "Relation does not exist" | Source table missing | Check Snowpipe, bulk load may have failed |
| "Compilation error" | SQL syntax error | Review model code, fix syntax |
| "Test failure" | Data quality issue | Investigate data, may need to skip test temporarily |
| "Warehouse timeout" | Query too large | Increase warehouse size temporarily |
| "Schema change detected" | Column added/removed in source | Update model to handle new schema |

**Resolution:**
1. Fix root cause (see table above)
2. Re-run failed model and downstream dependencies:
   ```bash
   dbt run --select <failed_model>+
   ```
3. Run tests:
   ```bash
   dbt test --select <failed_model>+
   ```
4. Verify observability tables updated

**Escalation:** If model logic issue → Escalate to analytics engineer

---

#### Incident: Streamlit App Down

**Symptoms:**
- Users report "Unable to connect"
- App returns 500 error

**Diagnosis Steps:**
1. Check app status in Snowsight (Streamlit section)
2. Check Snowflake warehouse status:
   ```sql
   SHOW WAREHOUSES LIKE 'compute_wh';
   ```
3. Review Streamlit logs (if accessible)
4. Test Snowflake connection manually:
   ```python
   snowflake.connector.connect(...)
   ```

**Resolution:**
1. **If warehouse suspended:**
   ```sql
   ALTER WAREHOUSE compute_wh RESUME;
   ```
2. **If Streamlit app error:**
   - Restart app in Snowsight
   - If restart fails, check code for recent changes
   - Rollback to last working version if needed
3. **If Snowflake connection issue:**
   - Check user/role permissions
   - Verify `secrets.toml` configuration
4. **If query timeout:**
   - Increase warehouse size
   - Add query result caching

**Escalation:** If unresolved in 15 minutes → Page platform lead

---

### 12.3 Weekly Maintenance Tasks

**Every Monday (30 minutes):**

1. **Review Observability Dashboard:**
   - Pipeline run success rate (target: >95%)
   - Data quality trends (duplicate rate, null rate)
   - Query performance (P95 latency)

2. **Optimize Warehouse Sizing:**
   ```sql
   -- Check average query queueing
   SELECT
       warehouse_name,
       AVG(queued_overload_time) / 1000 AS avg_queue_seconds
   FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
   WHERE start_time > DATEADD('day', -7, CURRENT_TIMESTAMP())
   GROUP BY warehouse_name;
   ```
   - If queue time > 5 seconds → Consider larger warehouse
   - If warehouse utilization <50% → Consider smaller warehouse

3. **Review Table Clustering:**
   ```sql
   -- Check clustering depth (higher = worse clustering)
   SELECT
       table_name,
       average_depth,
       average_overlaps
   FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
   WHERE start_time > DATEADD('day', -7, CURRENT_TIMESTAMP())
     AND average_depth > 10;
   ```
   - If depth >10 → Review clustering keys

4. **Cleanup Old Data (if needed):**
   ```sql
   -- Archive transactions older than 24 months
   CREATE TABLE IF NOT EXISTS bronze.bronze_transactions_archive AS
   SELECT * FROM bronze.bronze_transactions
   WHERE transaction_date < DATEADD('month', -24, CURRENT_DATE());

   DELETE FROM bronze.bronze_transactions
   WHERE transaction_date < DATEADD('month', -24, CURRENT_DATE());
   ```

5. **Update Documentation:**
   - Document any incidents and resolutions
   - Update runbooks with new procedures

---

### 12.4 Monthly Maintenance Tasks

**First Day of Month (1-2 hours):**

1. **Recalculate Customer Segments:**
   ```bash
   # Re-run segmentation model with rolling 90-day window
   cd dbt_customer_analytics
   dbt run --select customer_segments --full-refresh
   ```

2. **Retrain Churn ML Model:**
   ```sql
   -- Refresh training data with latest month
   CALL prepare_ml_training_data();

   -- Retrain model
   CALL train_churn_model();

   -- Validate performance
   SELECT * FROM TABLE(churn_model!SHOW_EVALUATION_METRICS());
   ```

3. **Cost Review:**
   ```sql
   -- Monthly cost breakdown
   SELECT
       DATE_TRUNC('month', start_time) AS month,
       warehouse_name,
       SUM(credits_used) AS total_credits,
       SUM(credits_used) * 3 AS estimated_cost_usd  -- Adjust based on your rate
   FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
   WHERE start_time >= DATEADD('month', -3, CURRENT_TIMESTAMP())
   GROUP BY DATE_TRUNC('month', start_time), warehouse_name
   ORDER BY month DESC, total_credits DESC;
   ```
   - Compare to budget
   - Identify optimization opportunities

4. **Security Review:**
   - Review user access logs
   - Audit role permissions
   - Check for unused users/roles

5. **Backup Verification:**
   - Verify Time Travel availability (90 days for critical tables)
   - Test restoration procedure on sample table

---

### 12.5 Troubleshooting Guide

#### Problem: Duplicate Rate Suddenly Increased

**Possible Causes:**
1. Source system sending duplicate files
2. Snowpipe processing same file twice
3. Data generation script malfunctioning

**Investigation:**
```sql
-- Check duplicate patterns
SELECT
    source_file,
    COUNT(*) AS total_records,
    COUNT(DISTINCT transaction_id) AS unique_records,
    COUNT(*) - COUNT(DISTINCT transaction_id) AS duplicate_count
FROM bronze.bronze_transactions
WHERE ingestion_timestamp > DATEADD('day', -1, CURRENT_TIMESTAMP())
GROUP BY source_file
HAVING duplicate_count > 0
ORDER BY duplicate_count DESC;
```

**Resolution:**
- If specific files → Remove duplicates, investigate source
- If widespread → Check data generator logic, fix and regenerate

---

#### Problem: Query Performance Degraded

**Symptoms:**
- Dashboard load time >10 seconds
- User complaints

**Investigation:**
```sql
-- Find slow queries
SELECT
    query_text,
    execution_time / 1000 AS execution_seconds,
    rows_produced,
    bytes_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE execution_time > 10000  -- >10 seconds
  AND start_time > DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY execution_time DESC
LIMIT 10;
```

**Optimization Steps:**
1. Add clustering keys to large fact tables
2. Materialize frequently-queried CTEs as tables
3. Add caching to Streamlit queries
4. Increase warehouse size during peak hours

---

#### Problem: ML Model Predictions Look Wrong

**Symptoms:**
- Churn scores don't match expected patterns
- All customers have similar scores

**Investigation:**
```sql
-- Check score distribution
SELECT
    CASE
        WHEN churn_risk_score BETWEEN 0 AND 20 THEN '0-20'
        WHEN churn_risk_score BETWEEN 21 AND 40 THEN '21-40'
        WHEN churn_risk_score BETWEEN 41 AND 60 THEN '41-60'
        WHEN churn_risk_score BETWEEN 61 AND 80 THEN '61-80'
        ELSE '81-100'
    END AS score_bucket,
    COUNT(*) AS customer_count
FROM gold.customer_analytics.churn_risk_features
GROUP BY score_bucket
ORDER BY score_bucket;
```

**Expected:** Normal distribution with peak around 30-50

**Resolution:**
- If all scores clustered → Model needs retraining with more diverse data
- If scores unrealistic → Check feature engineering logic
- Retrain model with validated training data

---

## 13. Risks & Mitigation

### Technical Risks

**Risk 1: Snowflake Trial Account Limitations**
- **Impact:** Trial accounts have credit limits and feature restrictions
- **Probability:** Medium
- **Mitigation:**
  - Confirm trial account includes Streamlit in Snowflake, Cortex ML, Cortex Analyst
  - Request extended trial or upgrade if needed
  - Optimize queries to minimize compute usage

**Risk 2: Synthetic Data Realism**
- **Impact:** Unrealistic data patterns may undermine demo credibility
- **Probability:** Low
- **Mitigation:**
  - Validate data distributions against industry benchmarks
  - Include randomness and edge cases (outliers, nulls)
  - Conduct peer review of generated data

**Risk 3: Cortex Analyst Semantic Model Complexity**
- **Impact:** Semantic model may not answer all natural language questions correctly
- **Probability:** Medium
- **Mitigation:**
  - Start with well-defined sample questions
  - Iteratively refine semantic model based on testing
  - Provide fallback: Show generated SQL for transparency

**Risk 4: Performance Issues at Scale**
- **Impact:** Queries may be slow with 13.5M transactions
- **Probability:** Low
- **Mitigation:**
  - Use clustering keys on large fact tables
  - Pre-aggregate metrics in mart tables
  - Implement caching in Streamlit
  - Load test with full dataset before delivery

**Risk 5: SCD Type 2 Complexity**
- **Impact:** SCD Type 2 logic may introduce bugs or query complexity
- **Probability:** Medium
- **Mitigation:**
  - Limit SCD Type 2 to 2 attributes (card_type, credit_limit)
  - Use dbt snapshots (proven pattern)
  - Test with multiple change scenarios
  - Document query patterns for joining to current records

### Project Risks

**Risk 6: Scope Creep**
- **Impact:** Additional features requested mid-project delay delivery
- **Probability:** High
- **Mitigation:**
  - Fixed specification with formal change request process
  - Weekly demos to validate direction
  - Reserve contingency budget for minor changes only

**Risk 7: Resource Availability**
- **Impact:** Team members unavailable due to conflicts
- **Probability:** Medium
- **Mitigation:**
  - Cross-train team members on multiple components
  - Maintain documentation to enable handoffs
  - Identify backup resources in advance

**Risk 8: Client Feedback Delays**
- **Impact:** Slow feedback blocks progress on dependent tasks
- **Probability:** Medium
- **Mitigation:**
  - Establish 2-business-day SLA for feedback
  - Escalation path to steering committee
  - Continue with best assumptions if feedback delayed

### Business Risks

**Risk 9: Demo Not Compelling Enough**
- **Impact:** Platform doesn't resonate with target customers
- **Probability:** Low
- **Mitigation:**
  - Validate business narrative with sales team early
  - Include ROI calculations and business value metrics
  - Practice demo with internal stakeholders before client delivery

**Risk 10: Maintenance Burden Post-Delivery**
- **Impact:** Client unable to maintain platform independently
- **Probability:** Medium
- **Mitigation:**
  - Comprehensive documentation and training
  - Offer support packages (see Section 8.4)
  - Design for simplicity (manual dbt runs, clear code structure)

---

## Appendices

### Appendix A: Technology Versions
- Snowflake: Enterprise Edition (trial)
- dbt Core: 1.7.x
- Python: 3.10+
- Streamlit: 1.30+
- Terraform: 1.6+
- Faker: 20.0+
- Pandas: 2.1+
- Boto3: 1.29+

### Appendix B: Sample Queries

**Query 1: Get High-Value Travelers without Premium Cards**
```sql
SELECT
    customer_id,
    full_name,
    email,
    state,
    lifetime_value,
    card_type
FROM gold.customer_analytics.customer_360_profile
WHERE customer_segment = 'High-Value Travelers'
  AND card_type = 'Standard'
  AND lifetime_value > 60000
ORDER BY lifetime_value DESC
LIMIT 100;
```

**Query 2: Identify Declining Customers (30%+ Drop)**
```sql
SELECT
    cp.customer_id,
    cp.full_name,
    cp.customer_segment,
    mm.mom_change_pct,
    cp.churn_risk_score
FROM gold.customer_analytics.customer_360_profile cp
JOIN gold.marketing.metric_mom_spend_change mm
  ON cp.customer_id = mm.customer_id
WHERE mm.month = DATE_TRUNC('month', CURRENT_DATE())
  AND mm.mom_change_pct < -30
ORDER BY mm.mom_change_pct ASC;
```

**Query 3: Campaign ROI Calculation**
```sql
SELECT
    treatment_group,
    COUNT(*) AS customers,
    AVG(pre_campaign_avg_spend) AS avg_spend_before,
    AVG(post_campaign_avg_spend) AS avg_spend_after,
    SUM(post_campaign_avg_spend - pre_campaign_avg_spend) AS total_lift,
    (SUM(post_campaign_avg_spend - pre_campaign_avg_spend) -
     (COUNT(*) * 50)) / (COUNT(*) * 50) * 100 AS roi_pct
FROM gold.marketing.campaign_performance
WHERE campaign_name = 'retention_offer_month16'
GROUP BY treatment_group;
```

### Appendix C: Git Repository Structure

```
snowflake-customer-analytics/
├── README.md
├── .gitignore
├── LICENSE
├── docs/
│   ├── architecture/
│   │   ├── architecture_diagram.png
│   │   ├── component_diagram.png
│   │   └── data_model_erd.png
│   ├── setup_guides/
│   │   ├── 01_terraform_setup.md
│   │   ├── 02_snowflake_setup.md
│   │   ├── 03_dbt_setup.md
│   │   ├── 04_data_generation.md
│   │   └── 05_streamlit_deployment.md
│   ├── user_guides/
│   │   ├── app_user_manual.md
│   │   ├── ai_assistant_guide.md
│   │   └── sample_queries.md
│   ├── operational_guides/
│   │   ├── monitoring.md
│   │   ├── troubleshooting.md
│   │   └── maintenance.md
│   └── demo/
│       ├── demo_script.md
│       └── talking_points.md
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   ├── modules/
│   │   ├── s3/
│   │   ├── iam/
│   │   └── sns_sqs/
│   └── README.md
├── snowflake/
│   ├── setup/
│   │   ├── 01_create_database_schemas.sql
│   │   ├── 02_create_roles_grants.sql
│   │   ├── 03_create_stages.sql
│   │   ├── 04_create_bronze_tables.sql
│   │   └── 05_create_snowpipe.sql
│   └── README.md
├── dbt_customer_analytics/
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   ├── packages.yml
│   ├── models/
│   ├── tests/
│   ├── macros/
│   ├── snapshots/
│   └── README.md
├── data_generation/
│   ├── generate_customers.py
│   ├── generate_transactions.sql
│   ├── generate_hourly_batch.py
│   ├── requirements.txt
│   └── README.md
├── ml/
│   ├── train_churn_model.sql
│   ├── create_features.sql
│   ├── apply_predictions.sql
│   └── README.md
├── semantic_layer/
│   ├── semantic_model.yaml
│   └── README.md
├── streamlit/
│   ├── customer_analytics_app.py
│   ├── requirements.txt
│   ├── secrets.toml.example
│   └── README.md
└── PROJECT_SPECIFICATION.md  # This document
```

### Appendix D: Glossary

- **ATV (Average Transaction Value):** Mean transaction amount per customer
- **CLV (Customer Lifetime Value):** Total spend by a customer over their entire relationship
- **Cortex Analyst:** Snowflake's natural language query interface powered by LLM
- **Cortex ML:** Snowflake's managed machine learning functions
- **MoM (Month-over-Month):** Percentage change from one month to the next
- **SCD Type 2 (Slowly Changing Dimension):** History-tracking method that creates new records for attribute changes
- **Snowpipe:** Snowflake's continuous data ingestion service
- **SiS (Streamlit in Snowflake):** Native Streamlit deployment within Snowflake

### Appendix E: Contact & Support

**Project Team:**
- Lead Solutions Architect: [Name] - [email]
- Data Engineering Lead: [Name] - [email]
- Analytics Engineering Lead: [Name] - [email]

**Communication Channels:**
- Project Slack: #snowflake-customer-360
- Weekly Status Meeting: Thursdays 2pm ET
- Steering Committee: Bi-weekly Fridays 10am ET

**Escalation Path:**
1. Issue raised in Slack → Response within 4 hours
2. If unresolved → Escalate to Lead Solutions Architect
3. If critical → Escalate to Project Sponsor

---

## Document Approval

**Prepared By:**
Claude Code AI Assistant
Date: 2025-11-11

**Reviewed By:**
[Client Product Owner Name]
Date: ___________
Signature: ___________

**Approved By:**
[Client Executive Sponsor Name]
Date: ___________
Signature: ___________

---

**END OF SPECIFICATION**
