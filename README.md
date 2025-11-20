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
uv run python -m data_generation generating newly introducing repo closing