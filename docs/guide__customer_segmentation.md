# Customer Segmentation Guide

**Version**: 1.0
**Date**: 2025-11-11
**Model**: `customer_segments`

---

## Overview

This document describes the **customer segmentation model** that classifies 50,000 customers into 5 behavioral segments based on spending patterns using a **rolling 90-day window**.

### Purpose

Enable targeted marketing, retention, and product strategies by:
- Identifying high-value customers for premium offerings
- Detecting churn risk (Declining segment) for retention campaigns
- Nurturing new customers showing growth potential
- Tailoring rewards programs to spending behavior
- Optimizing resource allocation across segments

---

## 5 Behavioral Segments

### 1. High-Value Travelers (10-15% of customers)

**Profile**:
- Premium customers with high spending and travel focus
- Avg monthly spend ≥ $5,000
- Travel spending (Travel, Airlines, Hotels) ≥ 25% of total

**Characteristics**:
- Highest lifetime value
- Premium card holders
- Frequent business/leisure travelers
- Low price sensitivity

**Marketing Strategy**:
- Premium rewards programs
- Travel perks (lounge access, miles multipliers)
- Concierge services
- Exclusive experiences

**Example SQL**:
```sql
SELECT
    customer_id,
    lifetime_value,
    avg_monthly_spend,
    travel_spend_pct
FROM customer_segments
WHERE customer_segment = 'High-Value Travelers'
ORDER BY lifetime_value DESC
LIMIT 100;
```

---

### 2. Declining (5-10% of customers)

**Profile**:
- **CHURN RISK**: Customers with significant spending decrease
- Spend decreased ≥ 30% from prior 90 days
- Prior period spend ≥ $2,000 (were significant spenders)

**Characteristics**:
- Negative spending trend
- Previously engaged customers
- At risk of account closure
- High priority for retention

**Marketing Strategy**:
- Win-back campaigns
- Targeted offers and incentives
- Personalized outreach
- Fee waivers or bonus rewards

**Example SQL**:
```sql
-- Top 100 churn risks
SELECT
    customer_id,
    lifetime_value,
    spend_last_90_days,
    spend_prior_90_days,
    spend_change_pct
FROM customer_segments
WHERE customer_segment = 'Declining'
ORDER BY spend_change_pct ASC  -- Most severe decline first
LIMIT 100;
```

---

### 3. New & Growing (10-15% of customers)

**Profile**:
- Recent customers showing strong growth
- Tenure ≤ 6 months
- Spend increased ≥ 50% from prior period

**Characteristics**:
- Early adopters
- Positive engagement trajectory
- High growth potential
- Opportunity for lifetime value expansion

**Marketing Strategy**:
- Onboarding incentives
- Educational content (maximize card benefits)
- Credit limit increase offers
- Upgrade to premium card

**Example SQL**:
```sql
SELECT
    customer_id,
    tenure_months,
    spend_change_pct,
    avg_monthly_spend
FROM customer_segments
WHERE customer_segment = 'New & Growing'
ORDER BY spend_change_pct DESC
LIMIT 100;
```

---

### 4. Budget-Conscious (20-25% of customers)

**Profile**:
- Price-sensitive customers focused on necessities
- Avg monthly spend < $1,500
- Necessities (Grocery, Gas, Utilities) ≥ 60% of spend

**Characteristics**:
- Essential spending focus
- Lower transaction amounts
- High price sensitivity
- Value rewards programs

**Marketing Strategy**:
- Cashback on everyday purchases
- Gas/grocery rewards multipliers
- No annual fee options
- Budget management tools

**Example SQL**:
```sql
SELECT
    customer_id,
    avg_monthly_spend,
    necessities_spend_pct,
    lifetime_value
FROM customer_segments
WHERE customer_segment = 'Budget-Conscious'
ORDER BY lifetime_value DESC
LIMIT 100;
```

---

### 5. Stable Mid-Spenders (40-50% of customers)

**Profile**:
- Default segment for consistent, moderate behavior
- Steady spending without extreme trends
- No specific category focus

**Characteristics**:
- Reliable revenue base
- Predictable behavior
- Moderate lifetime value
- Core customer segment

**Marketing Strategy**:
- General rewards programs
- Cross-sell opportunities
- Engagement campaigns (increase usage)
- Upgrade paths to high-value segment

**Example SQL**:
```sql
SELECT
    customer_id,
    avg_monthly_spend,
    lifetime_value,
    spend_change_pct
FROM customer_segments
WHERE customer_segment = 'Stable Mid-Spenders'
ORDER BY lifetime_value DESC
LIMIT 100;
```

---

## Segmentation Logic

### Rolling 90-Day Window

**Current Period**: Last 90 days
```sql
spend_last_90_days = SUM(transaction_amount)
WHERE transaction_date >= CURRENT_DATE - 90
```

**Prior Period**: Days 91-180
```sql
spend_prior_90_days = SUM(transaction_amount)
WHERE transaction_date BETWEEN CURRENT_DATE - 180 AND CURRENT_DATE - 91
```

**Trend Calculation**:
```sql
spend_change_pct = ((spend_last_90_days - spend_prior_90_days) / spend_prior_90_days) * 100
```

**Monthly Average**:
```sql
avg_monthly_spend = spend_last_90_days / 3
```

### Segment Assignment Rules

Applied in **priority order** (top to bottom):

```sql
CASE
    -- 1. High-Value Travelers (highest priority)
    WHEN avg_monthly_spend >= 5000 AND travel_spend_pct >= 25
    THEN 'High-Value Travelers'

    -- 2. Declining (churn risk - high priority)
    WHEN spend_change_pct <= -30 AND spend_prior_90_days >= 2000
    THEN 'Declining'

    -- 3. New & Growing (growth opportunity)
    WHEN tenure_months <= 6 AND spend_change_pct >= 50
    THEN 'New & Growing'

    -- 4. Budget-Conscious (necessity focus)
    WHEN avg_monthly_spend < 1500 AND necessities_spend_pct >= 60
    THEN 'Budget-Conscious'

    -- 5. Stable Mid-Spenders (default)
    ELSE 'Stable Mid-Spenders'
END
```

### Category Definitions

**Travel Categories**:
- Travel
- Airlines
- Hotels

**Necessities Categories**:
- Grocery
- Gas
- Utilities

**Percentage Calculations**:
```sql
travel_spend_pct = SUM(travel_amount) / SUM(total_amount) * 100
necessities_spend_pct = SUM(necessities_amount) / SUM(total_amount) * 100
```

---

## Segment Metrics

### Key Metrics by Segment

| Metric | High-Value | Declining | New & Growing | Budget-Conscious | Stable |
|--------|-----------|-----------|---------------|------------------|--------|
| **Avg Monthly Spend** | ≥ $5,000 | Varies | Varies | < $1,500 | Moderate |
| **Spend Trend** | Positive/Stable | ≤ -30% | ≥ +50% | Stable | Stable |
| **Tenure** | Established | Established | ≤ 6 months | Varies | Varies |
| **Travel %** | ≥ 25% | Varies | Varies | Low | Moderate |
| **Necessities %** | Low | Varies | Varies | ≥ 60% | Moderate |
| **Lifetime Value** | Highest | High (declining) | Growing | Low | Moderate |

### Expected Distribution

Based on typical credit card portfolios:

| Segment | Expected % | Target Count (50K customers) |
|---------|-----------|------------------------------|
| High-Value Travelers | 10-15% | 5,000 - 7,500 |
| Declining | 5-10% | 2,500 - 5,000 |
| New & Growing | 10-15% | 5,000 - 7,500 |
| Budget-Conscious | 20-25% | 10,000 - 12,500 |
| Stable Mid-Spenders | 40-50% | 20,000 - 25,000 |

---

## Usage Patterns

### Segment Distribution Analysis

```sql
SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    ROUND(AVG(lifetime_value), 2) AS avg_lifetime_value,
    ROUND(AVG(avg_monthly_spend), 2) AS avg_monthly_spend,
    ROUND(SUM(lifetime_value), 2) AS total_segment_value,
    ROUND(SUM(lifetime_value) * 100.0 / SUM(SUM(lifetime_value)) OVER (), 2) AS value_percentage
FROM customer_segments
GROUP BY customer_segment
ORDER BY total_segment_value DESC;
```

### Churn Risk Monitoring

```sql
-- Declining segment: Prioritize by LTV and decline severity
SELECT
    customer_id,
    lifetime_value,
    spend_last_90_days,
    spend_prior_90_days,
    spend_change_pct,
    RANK() OVER (ORDER BY lifetime_value DESC) AS ltv_rank,
    RANK() OVER (ORDER BY spend_change_pct ASC) AS decline_rank
FROM customer_segments
WHERE customer_segment = 'Declining'
ORDER BY ltv_rank + decline_rank ASC  -- Combined priority
LIMIT 100;
```

### Growth Opportunity Analysis

```sql
-- New & Growing: Identify highest potential
SELECT
    customer_id,
    tenure_months,
    spend_change_pct,
    avg_monthly_spend,
    lifetime_value,
    -- Projected 12-month value (if growth continues)
    avg_monthly_spend * 12 AS projected_annual_value
FROM customer_segments
WHERE customer_segment = 'New & Growing'
ORDER BY projected_annual_value DESC
LIMIT 100;
```

### Segment Migration Analysis

```sql
-- Track customers moving between segments over time
-- (Requires historical snapshot of customer_segments)

WITH current_segments AS (
    SELECT customer_id, customer_segment AS current_segment
    FROM customer_segments
),
prior_segments AS (
    SELECT customer_id, customer_segment AS prior_segment
    FROM customer_segments_snapshot_2024_10  -- Previous month
)

SELECT
    p.prior_segment,
    c.current_segment,
    COUNT(*) AS customer_count
FROM current_segments c
JOIN prior_segments p ON c.customer_id = p.customer_id
WHERE p.prior_segment != c.current_segment
GROUP BY p.prior_segment, c.current_segment
ORDER BY customer_count DESC;
```

---

## Recalculation & Maintenance

### Monthly Recalculation

**Recommended Schedule**: 1st of each month

**Option 1: dbt Macro**
```bash
dbt run-operation recalculate_segments
```

**Option 2: dbt Run**
```bash
dbt run --models customer_segments --full-refresh
```

**Option 3: Snowflake Task** (automated)
```sql
CREATE OR REPLACE TASK recalculate_customer_segments
WAREHOUSE = SMALL
SCHEDULE = 'USING CRON 0 2 1 * * America/New_York'  -- 2 AM on 1st of month
AS
CALL SYSTEM$RUN_DBT_COMMAND('run --models customer_segments --full-refresh');
```

### Historical Snapshots

**Why**: Track segment changes over time for migration analysis

**Implementation**:
```sql
-- Create monthly snapshot
CREATE TABLE customer_segments_snapshot_2024_11 AS
SELECT *, CURRENT_DATE() AS snapshot_date
FROM customer_segments;

-- OR use dbt snapshot (SCD Type 2)
-- See: dbt_customer_analytics/snapshots/customer_segments_snapshot.sql
```

---

## Campaign Targeting

### High-Value Travelers Campaign

**Objective**: Retain and expand premium customers

**SQL**:
```sql
SELECT
    c.customer_id,
    c.email,
    s.lifetime_value,
    s.avg_monthly_spend,
    s.travel_spend_pct
FROM customer_segments s
JOIN dim_customer c ON s.customer_key = c.customer_key AND c.is_current = TRUE
WHERE s.customer_segment = 'High-Value Travelers'
  AND s.avg_monthly_spend >= 7500  -- Top tier
ORDER BY s.lifetime_value DESC
LIMIT 5000;
```

**Actions**:
- Send premium travel rewards offer
- Invite to exclusive lounge network
- Offer concierge service upgrade

---

### Declining Customers Retention Campaign

**Objective**: Win back customers at risk of churn

**SQL**:
```sql
SELECT
    c.customer_id,
    c.email,
    c.first_name,
    s.lifetime_value,
    s.spend_change_pct,
    s.spend_last_90_days,
    s.spend_prior_90_days
FROM customer_segments s
JOIN dim_customer c ON s.customer_key = c.customer_key AND c.is_current = TRUE
WHERE s.customer_segment = 'Declining'
  AND s.lifetime_value >= 50000  -- High-value at-risk customers
ORDER BY s.spend_change_pct ASC
LIMIT 1000;
```

**Actions**:
- Personalized retention offer (bonus rewards)
- Annual fee waiver
- Customer service outreach call
- Exclusive promotion access

---

### New & Growing Engagement Campaign

**Objective**: Accelerate onboarding and increase usage

**SQL**:
```sql
SELECT
    c.customer_id,
    c.email,
    s.tenure_months,
    s.spend_change_pct,
    s.avg_monthly_spend
FROM customer_segments s
JOIN dim_customer c ON s.customer_key = c.customer_key AND c.is_current = TRUE
WHERE s.customer_segment = 'New & Growing'
  AND s.avg_monthly_spend >= 3000  -- High-potential new customers
ORDER BY s.spend_change_pct DESC
LIMIT 2000;
```

**Actions**:
- Educational email series (card benefits)
- Bonus rewards for hitting spending milestones
- Credit limit increase offer
- Upgrade to premium card invitation

---

## Testing & Validation

### dbt Tests

**Run all segmentation tests**:
```bash
dbt test --models customer_segments
```

**Tests included**:
- `unique`: customer_id
- `not_null`: customer_segment, spend metrics
- `accepted_values`: customer_segment (5 valid values)
- `relationships`: customer_key → dim_customer
- `expression_is_true`: Segment-specific criteria validation

### Custom Tests

**Segment Distribution Test**:
```bash
dbt test --select assert_segment_distribution
```

Verifies each segment has ≥ 5% of customers.

### Integration Tests

**Run Python integration tests**:
```bash
uv run pytest tests/integration/test_customer_segmentation.py -v
```

**Tests include**:
1. Model builds successfully
2. All customers assigned segments
3. Segment distribution balanced
4. High-Value Travelers criteria validated
5. Declining segment criteria validated
6. New & Growing criteria validated
7. Budget-Conscious criteria validated
8. Rolling window calculations accurate
9. Segment recalculation works
10. Performance benchmarks met

---

## Performance Considerations

### Build Time

| Warehouse Size | Expected Duration |
|---------------|-------------------|
| SMALL | 60-90 seconds |
| MEDIUM | 30-45 seconds |
| LARGE | 15-25 seconds |

**Tested**: 50,000 customers, 13.5M transactions

### Optimization Tips

1. **Use clustering on fact table** (already implemented)
   - Clustered by `transaction_date`
   - Optimizes 90-day window queries

2. **Consider incremental approach** (advanced)
   - For very large datasets (millions of customers)
   - Recalculate only customers with new transactions

3. **Warehouse sizing**
   - SMALL sufficient for monthly recalculation
   - MEDIUM for faster ad-hoc analysis

---

## Future Enhancements

### Additional Segmentation Dimensions

1. **Lifecycle Stage**:
   - Onboarding (0-3 months)
   - Active (3-12 months)
   - Mature (12+ months)

2. **Credit Utilization**:
   - Low utilization (<30%)
   - Medium utilization (30-70%)
   - High utilization (>70%)

3. **Channel Preference**:
   - Online shoppers
   - In-store spenders
   - Mobile-first users

### Predictive Segmentation

1. **Churn Probability Score**:
   - ML model predicting churn likelihood
   - Combine with Declining segment for prioritization

2. **Lifetime Value Prediction**:
   - Forecast future LTV for New & Growing customers
   - Prioritize highest potential customers

3. **Next Best Action**:
   - Recommend optimal offer for each customer
   - Based on segment + behavior + predicted response

---

## References

**dbt Model**:
- `models/marts/customer_analytics/customer_segments.sql`
- `models/marts/customer_analytics/customer_segments.yml`

**Tests**:
- `tests/assert_segment_distribution.sql`
- `tests/integration/test_customer_segmentation.py`

**Macros**:
- `macros/recalculate_segments.sql`

**Documentation**:
- `docs/customer_segmentation_guide.md` (this file)
- `docs/prompt_3.4_completion_summary.md`

---

**End of Customer Segmentation Guide**
