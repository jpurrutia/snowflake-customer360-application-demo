# Snowflake Customer 360 Analytics Platform

## Overview

A comprehensive Customer 360 Analytics Platform built on Snowflake, demonstrating end-to-end data engineering, analytics, machine learning, and application development for a post-acquisition credit card customer integration scenario. This project showcases Snowflake's four pillars: Data Engineering, Data Warehousing, Data Science/ML, and Data Applications.

## Business Context

After acquiring a regional credit card portfolio, this platform enables:
- Unified customer view across legacy and acquired systems
- Customer segmentation and behavior analysis
- Churn prediction using Snowflake Cortex ML
- Natural language analytics via Cortex Analyst
- Campaign performance simulation and ROI analysis

## Architecture Overview

The platform implements a **medallion architecture** (Bronze → Silver → Gold) with synthetic data generation, dbt transformations, star schema modeling, ML-powered churn prediction, a semantic layer for natural language queries, and an interactive Streamlit dashboard. Infrastructure is provisioned via Terraform, with all components running natively in Snowflake.

## Prerequisites

Before getting started, ensure you have:

- **Python 3.10+** installed
- **UV package manager** (`pip install uv` or `curl -LsSf https://astral.sh/uv/install.sh | sh`)
- **Git** for version control
- **Snowflake account** (trial or production) with appropriate permissions
- **AWS account** with permissions to create S3 buckets, IAM roles, and SNS/SQS resources
- **AWS CLI** configured with credentials
- **Terraform** (v1.5+) for infrastructure provisioning
- **Make** utility (usually pre-installed on macOS/Linux)

## Quick Start

### Option 1: Automated Setup (Recommended)

```bash
# 1. Clone and setup
git clone <repository-url>
cd snowflake-panel-demo
uv sync

# 2. Configure AWS and Snowflake credentials
aws configure
# Follow Snowflake setup in docs/SETUP_GUIDE.md

# 3. Run automated setup
./scripts/setup_end_to_end.sh
```

See **[docs/SETUP_GUIDE.md](docs/SETUP_GUIDE.md)** for detailed instructions.

### Option 2: Manual Setup

```bash
# 1. Install dependencies
uv sync

# 2. Generate customer data
uv run python -m data_generation generate-customers \
    --count 50000 \
    --output data/customers.csv

# 3. Apply Terraform
cd terraform
./deploy.sh

# 4. Create Snowflake storage integration
# Follow instructions in snowflake/setup/04_create_storage_integration.sql

# 5. Upload to S3
uv run python -m data_generation upload-customers \
    --file data/customers.csv \
    --bucket <your-s3-bucket>

# 8. Run dbt transformations
# (Instructions will be added in later iterations)

# 9. Launch Streamlit app
# (Instructions will be added in later iterations)
```

## Project Structure

```
snowflake-panel-demo/
├── README.md                           # This file
├── .gitignore                          # Git ignore patterns
├── .env.example                        # Environment variable template
├── requirements.txt                    # Python dependencies (pip format)
├── pyproject.toml                      # UV package manager configuration
├── Makefile                            # Common development tasks
│
├── terraform/                          # AWS infrastructure as code
│   ├── main.tf                         # Provider and backend config
│   ├── variables.tf                    # Input variables
│   ├── s3.tf                           # S3 bucket for data lake
│   ├── iam.tf                          # IAM roles for Snowflake access
│   └── outputs.tf                      # Infrastructure outputs
│
├── snowflake/                          # Snowflake SQL scripts
│   └── setup/                          # Database, schema, role setup
│
├── dbt_customer_analytics/             # dbt project for transformations
│   ├── models/                         # SQL transformation models
│   ├── tests/                          # dbt tests
│   └── dbt_project.yml                 # dbt configuration
│
├── data_generation/                    # Synthetic data generators
│   ├── customer_generator.py          # Generate 50K customers
│   └── transaction_generator.sql      # Generate 13.5M transactions
│
├── ml/                                 # Machine learning scripts
│   ├── churn_model.sql                 # Cortex ML churn prediction
│   └── model_evaluation.py            # Model performance analysis
│
├── semantic_layer/                     # Cortex Analyst configuration
│   └── semantic_model.yaml            # Semantic layer definition
│
├── streamlit/                          # Streamlit dashboard application
│   ├── app.py                          # Main application entry point
│   └── tabs/                           # Individual dashboard tabs
│
└── tests/                              # Test suite
    ├── unit/                           # Unit tests
    ├── integration/                    # Integration tests
    ├── performance/                    # Performance tests
    └── data_quality/                   # Data quality tests
```

## Development Workflow

### Phase 1: Foundation & Infrastructure (✅ COMPLETE)
1. **Project Structure**: Set up directory structure and package configuration
2. **AWS Infrastructure**: Provision S3 and IAM with Terraform
3. **Snowflake Foundation**: Create database, schemas, roles, and observability tables

### Phase 2: Data Generation & Ingestion (🚧 IN PROGRESS)
4. **Customer Data Generator**: Generate 50K synthetic customers (✅ COMPLETE)
5. **S3 Integration & Upload**: Upload customer data to S3 (✅ COMPLETE)
6. **Bronze Layer - Customers**: Load customer data into Snowflake (✅ SCRIPTS READY)
7. **Transaction Generator**: Generate 13.5M transactions with GENERATOR()
8. **Bronze Layer - Transactions**: Load and cluster transaction data

### Phase 3: dbt Transformations (📋 PLANNED)
9. **Silver Layer**: Clean and standardize data with dbt staging models
10. **Gold Layer**: Create dimensional models (SCD Type 2) and fact tables
11. **Customer Segmentation**: Implement rolling 90-day window calculations
12. **Mart Layer**: Build customer_360_profile and hero metrics

### Phase 4: Machine Learning & Semantic Layer (✅ COMPLETE)
13. **ML Training Data**: Prepare churn prediction features (✅ COMPLETE)
14. **Cortex ML**: Train and evaluate churn prediction model (✅ COMPLETE)
15. **Semantic Layer**: Define metrics for Cortex Analyst (✅ COMPLETE)

### Phase 5: Application Development (✅ COMPLETE)
16. **Streamlit Foundation & Segment Explorer**: App foundation with first tab (✅ COMPLETE - Iteration 5.1)
17. **Customer 360 Deep Dive**: Individual customer profile view (✅ COMPLETE - Iteration 5.2)
18. **AI Assistant**: Natural language query interface (✅ COMPLETE - Iteration 5.3)
19. **Campaign Performance Simulator**: Marketing ROI calculator (✅ COMPLETE - Iteration 5.4)

## Technology Stack

- **Data Warehouse**: Snowflake
- **Infrastructure**: Terraform, AWS (S3, IAM, SNS, SQS)
- **Transformation**: dbt (data build tool)
- **Data Generation**: Python (Faker), Snowflake SQL (GENERATOR function)
- **Machine Learning**: Snowflake Cortex ML
- **Natural Language Analytics**: Snowflake Cortex Analyst
- **Application**: Streamlit in Snowflake
- **Testing**: pytest, dbt tests
- **Package Management**: UV (Python)

## Data Model

The platform uses a **star schema dimensional model** in the Gold layer:

- **Fact Table**: `fct_transactions` (~13.5M rows, clustered by transaction_date)
- **Dimensions**:
  - `dim_customer` (SCD Type 2 for card_type and credit_limit changes)
  - `dim_date` (580 days of calendar attributes)
  - `dim_merchant_category` (category hierarchies and discretionary flags)

For detailed schema design, query patterns, and best practices, see:
- **[docs/star_schema_design.md](docs/star_schema_design.md)** - Complete star schema documentation

### Customer Segmentation

The platform classifies customers into **5 behavioral segments** using rolling 90-day windows:

1. **High-Value Travelers** (10-15%): Premium customers, $5K+/month, 25%+ travel spend
2. **Declining** (5-10%): Churn risk, -30%+ spend decrease
3. **New & Growing** (10-15%): Recent customers (<6 months), +50%+ growth
4. **Budget-Conscious** (20-25%): <$1.5K/month, 60%+ necessities
5. **Stable Mid-Spenders** (40-50%): Consistent, moderate behavior

For complete segmentation logic, campaign strategies, and query examples, see:
- **[docs/customer_segmentation_guide.md](docs/customer_segmentation_guide.md)** - Customer segmentation guide

### Hero Metrics & Customer 360

Pre-aggregated metrics optimized for dashboard performance:

1. **Customer Lifetime Value (LTV)**: Total spending from account opening
2. **Month-over-Month Spend Change**: Spending trend analysis
3. **Average Transaction Value (ATV)**: Mean transaction amount with consistency metrics

**Customer 360 Profile**: Denormalized view combining demographics, segmentation, and all hero metrics for fast application queries (<1 second single customer lookup).

For metric definitions and query patterns, see:
- **[docs/aggregate_marts_guide.md](docs/aggregate_marts_guide.md)** - Hero metrics and Customer 360 guide

### ML Model - Churn Prediction (✅ DEPLOYED)

Production-ready churn prediction model using Snowflake Cortex ML:

**Model Type**: Binary Classification (Cortex ML)
**Status**: Deployed to Production
**Performance**: F1 ≥ 0.50, Precision ≥ 0.60, Recall ≥ 0.40

**Churn Definition**: Customer churned if **either**:
- No transactions for 60+ days (inactivity)
- Recent spending < 30% of baseline (significant decline)

**Pipeline**:
1. **Training Data**: 40K-45K labeled customers with 35+ features
2. **Model Training**: Snowflake Cortex ML CLASSIFICATION (`CHURN_MODEL`)
3. **Batch Predictions**: Daily/weekly scoring of all ~45-50K customers
4. **Integration**: Predictions joined to `customer_360_profile` for dashboard

**Churn Risk Categories**:
- **Low Risk (0-39)**: 70-80% of customers - Normal marketing
- **Medium Risk (40-69)**: 15-25% of customers - Engagement campaigns
- **High Risk (70-100)**: 5-10% of customers - Retention offers

**Retraining**: Automated monthly retraining via `RETRAIN_CHURN_MODEL()` stored procedure

**Usage**:
```sql
-- View high-risk customers
SELECT customer_id, full_name, churn_risk_score, lifetime_value
FROM GOLD.CUSTOMER_360_PROFILE
WHERE churn_risk_category = 'High Risk'
ORDER BY churn_risk_score DESC
LIMIT 100;

-- Retrain model
CALL RETRAIN_CHURN_MODEL();

-- Refresh predictions
CALL REFRESH_CHURN_PREDICTIONS();
```

For complete documentation, see:
- **[snowflake/ml/README.md](snowflake/ml/README.md)** - ML pipeline guide
- **[docs/ml_model_card.md](docs/ml_model_card.md)** - Comprehensive model card
- **[docs/prompt_4.1_completion_summary.md](docs/prompt_4.1_completion_summary.md)** - Training data iteration
- **[docs/prompt_4.2_completion_summary.md](docs/prompt_4.2_completion_summary.md)** - Model training iteration

### Semantic Layer - Cortex Analyst (✅ DEPLOYED)

Natural language query interface using Snowflake Cortex Analyst:

**Status**: Ready for Deployment
**Tables**: 4 (customer_360_profile, fct_transactions, dim_merchant_category, customer_segments)
**Metrics**: 30+ (lifetime_value, churn_risk_score, transaction_amount, etc.)
**Dimensions**: 40+ (state, segment, churn_risk_category, card_type, etc.)

**Example Questions**:
- "Which customers are at highest risk of churning?"
- "What is the average spend in California?"
- "Show me Premium cardholders with declining spend"
- "Compare lifetime value across customer segments"
- "Which states have the highest churn risk?"

**Deployment**:
```bash
cd semantic_layer
./deploy_semantic_model.sh
```

**Testing**:
```bash
# Test SQL queries
snowsql -f semantic_layer/test_semantic_model.sql

# Integration tests
pytest tests/integration/test_semantic_layer.py -v
```

**Architecture**:
```
User asks: "Show me high-risk customers"
    ↓
Cortex Analyst (semantic_model.yaml)
    ↓
Generates SQL from semantic definitions
    ↓
Executes on Snowflake
    ↓
Returns results in natural language
```

For complete documentation, see:
- **[semantic_layer/README.md](semantic_layer/README.md)** - Semantic layer guide
- **[semantic_layer/semantic_model.yaml](semantic_layer/semantic_model.yaml)** - Model definition
- **[docs/prompt_4.3_completion_summary.md](docs/prompt_4.3_completion_summary.md)** - Semantic layer iteration

### Streamlit Application (✅ PHASE 5 COMPLETE)

Interactive dashboard for business users:

**Status**: All 4 tabs deployed
**Tech Stack**: Streamlit, Snowflake Connector for Python, Plotly
**Deployment**: Local development + Streamlit in Snowflake (SiS)

**Tabs**:
1. **Segment Explorer (✅ Complete)**: Customer segmentation and export
   - Multi-select filters (segment, state, churn risk, LTV, card type)
   - Summary metrics (customer count, total/avg LTV, avg churn risk)
   - Interactive visualizations (pie charts, bar charts)
   - CSV export for marketing campaigns

2. **Customer 360 Deep Dive (✅ Complete)**: Individual customer analysis
   - Customer search (ID, name, email with partial match)
   - Profile with demographics, segment, churn risk alerts
   - Key metrics (LTV, avg transaction, 90d spend, days since last transaction)
   - Transaction history (1,000 most recent) with filters (date range, category, status)
   - Visualizations (daily spending line chart, category pie chart)
   - Transaction summary metrics and CSV export

3. **AI Assistant (✅ Complete)**: Natural language queries
   - 5 question categories (Churn, Segmentation, Spending, Geographic, Campaign)
   - 20+ suggested questions with clickable buttons
   - Natural language input with mock Cortex Analyst (ready for production integration)
   - Generated SQL display in collapsible expander
   - Results table with summary metrics and CSV export
   - Query history (last 5 queries with timestamps)

4. **Campaign Performance Simulator (✅ Complete)**: Marketing ROI
   - Target audience selection (segment, churn risk, card type, LTV filters)
   - Campaign parameters (incentive amount, retention rate, campaign cost)
   - ROI calculation with detailed metrics (cost, retained customers, value, net benefit)
   - Cost breakdown pie chart and sensitivity analysis line chart
   - Breakeven calculation (minimum retention rate for positive ROI)
   - Top 10 highest risk customers display
   - Campaign recommendations (messaging, timing, success metrics)
   - CSV export for target customer list

**Quick Start**:
```bash
cd streamlit
pip install -r requirements.txt
cp .env.example .env
# Edit .env with Snowflake credentials
streamlit run app.py
```

**Architecture**:
```
User → Streamlit App → Snowflake (CUSTOMER_ANALYTICS.GOLD)
         ↓
    Cached Connection
         ↓
    execute_query() with error handling
         ↓
    Pandas DataFrame → Plotly Charts
```

For complete documentation, see:
- **[streamlit/README.md](streamlit/README.md)** - Streamlit app guide
- **[IMPLEMENTATION_PROMPTS.md](IMPLEMENTATION_PROMPTS.md)** - Phase 5 implementation prompts
- **[docs/prompt_5_completion_summary.md](docs/prompt_5_completion_summary.md)** - Phase 5 completion summary

## Key Features

- **Customer Segmentation**: 5 behavioral segments with rolling 90-day window calculations
- **Churn Prediction**: Production ML model (F1 ≥0.50, deployed with automated retraining)
- **Hero Metrics**: LTV, MoM Spend Change, ATV - pre-aggregated for performance
- **SCD Type 2**: Historical tracking of customer dimension changes
- **Rolling Metrics**: Dynamic 90-day window calculations
- **Natural Language Queries**: Cortex Analyst semantic layer with 30+ metrics (deployed)
- **Campaign Simulation**: ROI analysis for retention campaigns (planned)

## Implementation Guide

### Phase 1: AWS Infrastructure Setup

The first step is provisioning AWS infrastructure for data storage and Snowflake integration.

#### What Gets Created

- **S3 Bucket**: Data lake storage with versioning, encryption, and lifecycle policies
- **IAM Role**: Secure access for Snowflake to read from S3
- **Folder Structure**: Organized paths for customers/ and transactions/

#### Prerequisites

- AWS account with S3 and IAM permissions
- AWS CLI configured (`aws configure`)
- Terraform installed (v1.5.0+)

#### Setup Steps

1. **Navigate to terraform directory**:
   ```bash
   cd terraform
   ```

2. **Review the configuration**:
   - See [terraform/README.md](terraform/README.md) for detailed instructions

3. **Create terraform.tfvars**:
   ```bash
   cp terraform.tfvars.example terraform.tfvars
   # Edit with your Snowflake account ID and external ID
   ```

4. **Initialize and apply**:
   ```bash
   terraform init
   terraform plan
   terraform apply
   ```

5. **Capture outputs**:
   ```bash
   terraform output
   ```
   Save the `iam_role_arn` and `s3_bucket_name` for Snowflake configuration.

#### Testing

Run the test suite to validate Terraform configuration:

```bash
# Unit tests (Python)
uv run pytest tests/unit/test_terraform_variables.py -v

# Integration tests (Shell)
./tests/integration/test_terraform_config.sh
```

#### Detailed Documentation

For complete setup instructions, troubleshooting, and Snowflake integration steps, see:
- **[terraform/README.md](terraform/README.md)** - Comprehensive Terraform guide

#### Next Steps

After infrastructure is provisioned:
- **Phase 2**: Snowflake foundation setup (databases, schemas, roles)
- **Phase 3**: Data generation and ingestion
- **Phase 4**: dbt transformations

## Contributing

This is a demonstration project. For production use, consider:
- Implementing proper secrets management (e.g., HashiCorp Vault, AWS Secrets Manager)
- Adding CI/CD pipelines for automated testing and deployment
- Implementing data governance and access controls
- Adding monitoring and alerting (e.g., Snowflake resource monitors)
- Optimizing warehouse sizing and query performance
- Implementing data retention and archival policies

## License

[Add your license here]

## Contact

[Add contact information here]
