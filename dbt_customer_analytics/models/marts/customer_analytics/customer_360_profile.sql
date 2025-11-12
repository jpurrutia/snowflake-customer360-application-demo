{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'mart', 'customer_360']
    )
}}

{#
============================================================================
Gold Layer: Customer 360 Profile
============================================================================
Purpose: Denormalized customer view for dashboard and application consumption

Grain: One row per customer (current state)
Row Count: ~50,000 customers

Business Purpose:
- Single source of truth for customer profiles
- Combines demographics, segmentation, and metrics
- Optimized for application queries (denormalized)
- Ready for Streamlit dashboard consumption

Contains:
- Customer demographics (from dim_customer)
- Behavioral segment (from customer_segments)
- Lifetime metrics (LTV, ATV)
- Recent activity (90-day window)
- Category preferences
- Churn risk score (placeholder for ML iteration)

Usage:
  -- Single customer lookup
  SELECT * FROM customer_360_profile
  WHERE customer_id = 'CUST00000001';

  -- High-value customers
  SELECT customer_id, full_name, customer_segment, lifetime_value
  FROM customer_360_profile
  WHERE customer_segment = 'High-Value Travelers'
  ORDER BY lifetime_value DESC
  LIMIT 100;

  -- Churn risk dashboard
  SELECT customer_id, full_name, customer_segment,
         spend_change_pct, days_since_last_transaction
  FROM customer_360_profile
  WHERE customer_segment = 'Declining'
  ORDER BY spend_change_pct ASC;

  -- Segment summary
  SELECT
      customer_segment,
      COUNT(*) AS customer_count,
      AVG(lifetime_value) AS avg_ltv,
      AVG(avg_transaction_value) AS avg_atv
  FROM customer_360_profile
  GROUP BY customer_segment;
============================================================================
#}

SELECT
    -- Customer identifiers
    c.customer_id,
    c.customer_key,

    -- Demographics
    c.first_name || ' ' || c.last_name AS full_name,
    c.first_name,
    c.last_name,
    c.email,
    c.age,
    c.state,
    c.city,
    c.employment_status,

    -- Account details
    c.card_type,
    c.credit_limit,
    c.account_open_date,
    DATEDIFF('day', c.account_open_date, CURRENT_DATE()) AS account_age_days,

    -- Segmentation
    seg.customer_segment,
    seg.segment_assigned_date,
    seg.tenure_months,

    -- Lifetime metrics
    ltv.lifetime_value,
    ltv.total_transactions,
    ltv.customer_age_days,
    ltv.avg_spend_per_day,

    -- Average transaction value
    atv.avg_transaction_value,
    atv.transaction_value_stddev,
    atv.min_transaction_value,
    atv.max_transaction_value,
    atv.median_transaction_value,
    atv.spending_consistency,

    -- Recent activity (rolling 90-day window)
    seg.spend_last_90_days,
    seg.spend_prior_90_days,
    seg.spend_change_pct,
    seg.avg_monthly_spend,

    -- Activity timeline
    ltv.first_transaction_date,
    ltv.last_transaction_date,
    DATEDIFF('day', ltv.last_transaction_date, CURRENT_DATE()) AS days_since_last_transaction,

    -- Recency flags
    CASE
        WHEN DATEDIFF('day', ltv.last_transaction_date, CURRENT_DATE()) <= 30 THEN 'Active (30 days)'
        WHEN DATEDIFF('day', ltv.last_transaction_date, CURRENT_DATE()) <= 60 THEN 'Recent (60 days)'
        WHEN DATEDIFF('day', ltv.last_transaction_date, CURRENT_DATE()) <= 90 THEN 'At Risk (90 days)'
        ELSE 'Inactive (90+ days)'
    END AS recency_status,

    -- Category preferences
    seg.travel_spend_pct,
    seg.necessities_spend_pct,

    -- Calculated: Discretionary vs Necessities
    CASE
        WHEN seg.travel_spend_pct >= 25 THEN 'Travel-Focused'
        WHEN seg.necessities_spend_pct >= 60 THEN 'Necessity-Focused'
        ELSE 'Balanced'
    END AS spending_profile,

    -- Credit utilization (calculated from monthly spend and credit limit)
    CASE
        WHEN c.credit_limit > 0
        THEN (seg.spend_last_90_days / 3) / c.credit_limit * 100
        ELSE 0
    END AS credit_utilization_pct,

    -- Churn risk (from ML model predictions - Iteration 4.2)
    pred.churn_risk_score,
    CASE
        WHEN pred.churn_risk_score >= 70 THEN 'High Risk'
        WHEN pred.churn_risk_score >= 40 THEN 'Medium Risk'
        WHEN pred.churn_risk_score < 40 THEN 'Low Risk'
        ELSE 'Not Scored'  -- For customers with <5 transactions
    END AS churn_risk_category,

    -- Campaign eligibility flags (for marketing automation)
    CASE
        WHEN seg.customer_segment = 'Declining' THEN TRUE
        WHEN pred.churn_risk_score >= 70 THEN TRUE  -- ML-based eligibility
        ELSE FALSE
    END AS eligible_for_retention_campaign,
    CASE WHEN seg.customer_segment = 'New & Growing' THEN TRUE ELSE FALSE END AS eligible_for_onboarding_campaign,
    CASE WHEN seg.customer_segment = 'High-Value Travelers' THEN TRUE ELSE FALSE END AS eligible_for_premium_campaign,

    -- Metadata
    CURRENT_DATE() AS profile_updated_date

FROM {{ ref('dim_customer') }} c

-- Join segmentation
JOIN {{ ref('customer_segments') }} seg
    ON c.customer_id = seg.customer_id

-- Join lifetime value
JOIN {{ ref('metric_customer_ltv') }} ltv
    ON c.customer_id = ltv.customer_id

-- Join average transaction value
JOIN {{ ref('metric_avg_transaction_value') }} atv
    ON c.customer_id = atv.customer_id

-- Join churn predictions (LEFT JOIN to handle customers not scored)
LEFT JOIN {{ source('gold', 'churn_predictions') }} pred
    ON c.customer_id = pred.customer_id

WHERE c.is_current = TRUE
