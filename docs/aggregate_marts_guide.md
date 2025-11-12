# Aggregate Marts Guide - Hero Metrics & Customer 360

**Version**: 1.0
**Date**: 2025-11-11
**Models**: Hero metrics (LTV, MoM, ATV) + Customer 360 Profile

---

## Overview

This document describes the **aggregate mart models** that pre-calculate hero metrics and provide denormalized Customer 360 profiles for dashboard consumption. These marts optimize query performance by pre-aggregating complex calculations.

### Purpose

Enable fast, efficient analytics for:
- Executive dashboards (KPIs and segment summaries)
- Customer detail views (single customer lookups)
- Marketing campaign targeting (high-value,

 churn risk)
- Application consumption (Streamlit, Cortex Analyst)
- Operational reporting (segment performance)

---

## Hero Metrics (3)

### 1. Customer Lifetime Value (LTV)

**Model**: `metric_customer_ltv`
**Grain**: One row per customer
**Rows**: ~50,000

**Business Definition**:
> Total spending from account opening to present (approved transactions only)

**Formula**:
```sql
lifetime_value = SUM(transaction_amount) WHERE status = 'approved'
```

**Key Metrics**:
- `lifetime_value`: Total spending
- `total_transactions`: Transaction count
- `customer_age_days`: Days between first and last transaction
- `avg_spend_per_day`: LTV / age

**Usage Example**:
```sql
-- Top 100 customers by LTV
SELECT
    customer_id,
    customer_segment,
    lifetime_value,
    total_transactions,
    avg_spend_per_day
FROM metric_customer_ltv
ORDER BY lifetime_value DESC
LIMIT 100;

-- Average LTV by segment
SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    AVG(lifetime_value) AS avg_ltv,
    SUM(lifetime_value) AS total_segment_value
FROM metric_customer_ltv
GROUP BY customer_segment
ORDER BY total_segment_value DESC;

-- LTV distribution (tiering)
SELECT
    CASE
        WHEN lifetime_value < 10000 THEN '<$10K'
        WHEN lifetime_value < 50000 THEN '$10K-$50K'
        WHEN lifetime_value < 100000 THEN '$50K-$100K'
        ELSE '$100K+'
    END AS ltv_tier,
    COUNT(*) AS customer_count
FROM metric_customer_ltv
GROUP BY ltv_tier;
```

---

### 2. Month-over-Month Spend Change (MoM)

**Model**: `metric_mom_spend_change`
**Grain**: One row per customer per month
**Rows**: ~50,000 customers × ~18 months = ~900,000

**Business Definition**:
> Percentage change in spending from prior month to current month

**Formula**:
```sql
mom_change_pct = ((monthly_spend - prior_month_spend) / prior_month_spend) * 100
```

**Key Metrics**:
- `monthly_spend`: Total spending for the month
- `prior_month_spend`: Previous month's spending (NULL for first month)
- `mom_change_pct`: Percentage change
- `mom_trend_category`: Categorical classification (High Growth, Growth, Flat, Decline, High Decline)
- `month_number`: Sequential month number for each customer

**Usage Example**:
```sql
-- Latest month's MoM change by segment
WITH latest_month AS (
    SELECT MAX(month) AS max_month
    FROM metric_mom_spend_change
)
SELECT
    seg.customer_segment,
    AVG(m.mom_change_pct) AS avg_mom_change,
    COUNT(*) AS customer_count
FROM metric_mom_spend_change m
JOIN customer_segments seg ON m.customer_id = seg.customer_id
CROSS JOIN latest_month
WHERE m.month = latest_month.max_month
  AND m.mom_change_pct IS NOT NULL
GROUP BY seg.customer_segment;

-- Customers with biggest MoM decline (churn risk)
SELECT
    customer_id,
    month,
    monthly_spend,
    prior_month_spend,
    mom_change_pct,
    mom_trend_category
FROM metric_mom_spend_change
WHERE month = DATE_TRUNC('month', CURRENT_DATE() - INTERVAL '1 month')
  AND mom_change_pct < -30
ORDER BY mom_change_pct ASC
LIMIT 100;

-- MoM trend over time for specific customer
SELECT
    month,
    monthly_spend,
    prior_month_spend,
    mom_change_pct,
    mom_trend_category
FROM metric_mom_spend_change
WHERE customer_id = 'CUST00000001'
ORDER BY month;
```

---

### 3. Average Transaction Value (ATV)

**Model**: `metric_avg_transaction_value`
**Grain**: One row per customer
**Rows**: ~50,000

**Business Definition**:
> Average dollar amount per transaction

**Formula**:
```sql
avg_transaction_value = AVG(transaction_amount) WHERE status = 'approved'
```

**Key Metrics**:
- `avg_transaction_value`: Mean transaction amount
- `transaction_value_stddev`: Standard deviation (spending consistency)
- `min_transaction_value`: Minimum transaction
- `max_transaction_value`: Maximum transaction
- `median_transaction_value`: 50th percentile (less sensitive to outliers)
- `spending_consistency`: Categorical (Consistent, Moderate, Variable)

**Usage Example**:
```sql
-- Customers with highest ATV
SELECT
    customer_id,
    customer_segment,
    avg_transaction_value,
    transaction_value_stddev,
    spending_consistency
FROM metric_avg_transaction_value
ORDER BY avg_transaction_value DESC
LIMIT 100;

-- ATV by segment and consistency
SELECT
    customer_segment,
    spending_consistency,
    COUNT(*) AS customer_count,
    AVG(avg_transaction_value) AS avg_atv
FROM metric_avg_transaction_value
GROUP BY customer_segment, spending_consistency
ORDER BY avg_atv DESC;

-- Consistent vs variable spenders
SELECT
    CASE
        WHEN transaction_value_stddev < 50 THEN 'Consistent'
        WHEN transaction_value_stddev < 200 THEN 'Moderate'
        ELSE 'Variable'
    END AS spending_pattern,
    COUNT(*) AS customer_count,
    AVG(avg_transaction_value) AS avg_atv
FROM metric_avg_transaction_value
GROUP BY spending_pattern;
```

---

## Customer 360 Profile

**Model**: `customer_360_profile`
**Grain**: One row per customer (current state)
**Rows**: ~50,000

**Business Definition**:
> Denormalized customer view combining demographics, segmentation, and all hero metrics

**Purpose**:
- Single source of truth for customer profiles
- Optimized for fast application queries (denormalized)
- Ready for Streamlit dashboard, Cortex Analyst, ML workflows
- Campaign targeting and operational reporting

**Contains**:
- **Demographics**: Name, email, age, location, employment
- **Account**: Card type, credit limit, account age
- **Segmentation**: Behavioral segment with rolling 90-day metrics
- **Hero Metrics**: LTV, ATV, MoM spend change
- **Activity**: Recency status, days since last transaction
- **Category Preferences**: Travel vs necessities spending
- **Campaign Flags**: Eligibility for targeted campaigns
- **Churn Risk**: Placeholder for ML model scores (Prompt 4.x)

---

## Customer 360 Schema

### Identifiers
- `customer_id`: Natural key
- `customer_key`: Surrogate key (FK to dim_customer)

### Demographics
- `full_name`: First + last name
- `first_name`, `last_name`
- `email`
- `age`
- `state`, `city`
- `employment_status`

### Account Details
- `card_type`: Standard, Premium
- `credit_limit`: $5K-$50K
- `account_open_date`
- `account_age_days`

### Segmentation
- `customer_segment`: 5 behavioral segments
- `segment_assigned_date`
- `tenure_months`

### Lifetime Metrics
- `lifetime_value`: Total spending (LTV)
- `total_transactions`
- `customer_age_days`: First to last transaction
- `avg_spend_per_day`: LTV / age

### Average Transaction Value
- `avg_transaction_value`: Mean transaction
- `transaction_value_stddev`: Spending consistency
- `min_transaction_value`, `max_transaction_value`
- `median_transaction_value`
- `spending_consistency`: Consistent, Moderate, Variable

### Recent Activity (Rolling 90-Day Window)
- `spend_last_90_days`: Current period
- `spend_prior_90_days`: Prior period
- `spend_change_pct`: MoM trend
- `avg_monthly_spend`: Monthly average

### Activity Timeline
- `first_transaction_date`
- `last_transaction_date`
- `days_since_last_transaction`
- `recency_status`: Active (30d), Recent (60d), At Risk (90d), Inactive (90+d)

### Category Preferences
- `travel_spend_pct`: % on Travel, Airlines, Hotels
- `necessities_spend_pct`: % on Grocery, Gas, Utilities
- `spending_profile`: Travel-Focused, Necessity-Focused, Balanced

### Campaign Flags
- `eligible_for_retention_campaign`: TRUE if Declining segment
- `eligible_for_onboarding_campaign`: TRUE if New & Growing
- `eligible_for_premium_campaign`: TRUE if High-Value Travelers

### Churn Risk (Placeholder)
- `churn_risk_score`: NULL (to be populated in Prompt 4.x)
- `churn_risk_category`: NULL (Low, Medium, High, Very High)

### Metadata
- `profile_updated_date`: Last refresh date

---

## Customer 360 Query Patterns

### Single Customer Lookup

```sql
-- Retrieve complete profile for specific customer
SELECT *
FROM customer_360_profile
WHERE customer_id = 'CUST00000001';
```

**Performance**: <1 second

---

### Segment Dashboard

```sql
-- Executive dashboard: Segment summary
SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    AVG(lifetime_value) AS avg_ltv,
    AVG(avg_transaction_value) AS avg_atv,
    AVG(spend_change_pct) AS avg_mom_change
FROM customer_360_profile
GROUP BY customer_segment
ORDER BY avg_ltv DESC;
```

**Performance**: <3 seconds

---

### High-Value Customers

```sql
-- High-value travelers for premium campaigns
SELECT
    customer_id,
    full_name,
    email,
    lifetime_value,
    avg_transaction_value,
    travel_spend_pct,
    days_since_last_transaction,
    recency_status
FROM customer_360_profile
WHERE customer_segment = 'High-Value Travelers'
ORDER BY lifetime_value DESC
LIMIT 100;
```

---

### Churn Risk Monitoring

```sql
-- Declining segment (churn risk)
SELECT
    customer_id,
    full_name,
    email,
    customer_segment,
    lifetime_value,
    spend_change_pct,
    days_since_last_transaction,
    recency_status
FROM customer_360_profile
WHERE customer_segment = 'Declining'
  OR recency_status = 'Inactive (90+ days)'
ORDER BY spend_change_pct ASC;
```

---

### Campaign Targeting

```sql
-- Retention campaign: Declining + high LTV
SELECT
    customer_id,
    full_name,
    email,
    lifetime_value,
    spend_change_pct
FROM customer_360_profile
WHERE eligible_for_retention_campaign = TRUE
  AND lifetime_value >= 50000
ORDER BY spend_change_pct ASC
LIMIT 1000;

-- Onboarding campaign: New & Growing
SELECT
    customer_id,
    full_name,
    email,
    tenure_months,
    spend_change_pct,
    avg_monthly_spend
FROM customer_360_profile
WHERE eligible_for_onboarding_campaign = TRUE
ORDER BY spend_change_pct DESC
LIMIT 2000;

-- Premium campaign: High-Value Travelers
SELECT
    customer_id,
    full_name,
    email,
    lifetime_value,
    travel_spend_pct
FROM customer_360_profile
WHERE eligible_for_premium_campaign = TRUE
ORDER BY lifetime_value DESC
LIMIT 5000;
```

---

## Build & Refresh

### Initial Build

```bash
cd dbt_customer_analytics

# Build all hero metrics
dbt run --models marts.marketing

# Build customer 360 profile
dbt run --models customer_360_profile

# OR build all marts at once
dbt run --models marts
```

**Duration**: 60-120 seconds (SMALL warehouse)

### Daily Refresh

```bash
# Refresh all marts (recommended for daily batch)
dbt run --models marts

# OR selective refresh
dbt run --models marts.marketing  # Hero metrics only
dbt run --models customer_360_profile  # Customer 360 only
```

### Automated Refresh (Snowflake Task)

```sql
CREATE OR REPLACE TASK refresh_aggregate_marts
WAREHOUSE = SMALL
SCHEDULE = 'USING CRON 0 3 * * * America/New_York'  -- 3 AM daily
AS
CALL SYSTEM$RUN_DBT_COMMAND('run --models marts');
```

---

## Testing & Validation

### dbt Tests

```bash
# Test all mart models
dbt test --models marts

# Test specific metric
dbt test --models metric_customer_ltv
dbt test --models customer_360_profile
```

### Integration Tests

```bash
# Run all aggregate mart tests
uv run pytest tests/integration/test_aggregate_marts.py -v

# Run specific test
uv run pytest tests/integration/test_aggregate_marts.py::test_metric_customer_ltv -v
```

**Tests include**:
1. All marts build successfully
2. Metric calculations accurate
3. Customer 360 profile completeness
4. Query performance benchmarks
5. Metrics refresh correctly
6. Join integrity across marts

---

## Performance Optimization

### Build Performance

| Warehouse | Build Time | Cost |
|-----------|-----------|------|
| SMALL | 60-120s | Low |
| MEDIUM | 30-60s | Medium |
| LARGE | 15-30s | High |

**Recommendation**: SMALL warehouse sufficient for daily refresh

### Query Performance

| Query Type | Expected Time |
|------------|--------------|
| Single customer lookup | <1 second |
| Segment aggregation | <3 seconds |
| Campaign targeting | <5 seconds |

**Optimizations**:
- Denormalized schema (no runtime joins)
- Pre-aggregated metrics
- Fact table clustering by transaction_date

---

## Dashboard Use Cases

### Executive Dashboard

**KPIs**:
```sql
SELECT
    COUNT(*) AS total_customers,
    SUM(lifetime_value) AS total_ltv,
    AVG(lifetime_value) AS avg_ltv,
    AVG(avg_transaction_value) AS avg_atv
FROM customer_360_profile;
```

**Segment Breakdown**:
```sql
SELECT
    customer_segment,
    COUNT(*) AS customers,
    AVG(lifetime_value) AS avg_ltv
FROM customer_360_profile
GROUP BY customer_segment;
```

---

### Marketing Campaign Manager

**Target Lists**:
- High-Value Travelers (premium offers)
- Declining (retention campaigns)
- New & Growing (onboarding incentives)

**Export for Email/CRM**:
```sql
SELECT
    customer_id,
    email,
    full_name,
    customer_segment
FROM customer_360_profile
WHERE eligible_for_retention_campaign = TRUE;
```

---

### Customer Service Dashboard

**Single Customer View**:
```sql
SELECT * FROM customer_360_profile
WHERE customer_id = :customer_id;
```

Displays:
- Demographics and contact info
- Account details (card type, credit limit)
- Spending patterns (LTV, ATV, MoM)
- Segment and recency status
- Campaign eligibility

---

## Future Enhancements

### Additional Metrics

1. **Retention Rate**: % of customers still active after 12 months
2. **CLV Prediction**: Forecasted future value using ML
3. **RFM Score**: Recency, Frequency, Monetary segmentation
4. **Cross-Sell Propensity**: Likelihood to adopt new products

### ML Integration (Prompt 4.x)

1. **Churn Risk Score**: Populate `churn_risk_score` and `churn_risk_category`
2. **Next Best Action**: Recommended offer for each customer
3. **Lifetime Value Prediction**: Forecasted LTV at 12/24/36 months

### Real-Time Updates

1. **Streaming Metrics**: Update LTV/ATV in real-time via Snowpipe
2. **Dynamic Segmentation**: Recalculate segments hourly instead of monthly

---

## References

**dbt Models**:
- `models/marts/marketing/metric_customer_ltv.sql`
- `models/marts/marketing/metric_mom_spend_change.sql`
- `models/marts/marketing/metric_avg_transaction_value.sql`
- `models/marts/customer_analytics/customer_360_profile.sql`

**Schema Documentation**:
- `models/marts/marketing/schema.yml`
- `models/marts/customer_analytics/schema.yml`

**Tests**:
- `tests/integration/test_aggregate_marts.py`

**Documentation**:
- `docs/aggregate_marts_guide.md` (this file)
- `docs/prompt_3.5_completion_summary.md`

---

**End of Aggregate Marts Guide**
