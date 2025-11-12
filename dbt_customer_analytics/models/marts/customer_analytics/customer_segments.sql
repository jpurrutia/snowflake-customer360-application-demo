{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'mart', 'segmentation']
    )
}}

{#
============================================================================
Gold Layer: Customer Segmentation Model
============================================================================
Purpose: Classify customers into 5 behavioral segments using rolling 90-day window

Grain: One row per customer (current state only)
Row Count: ~50,000 customers

Segments:
1. High-Value Travelers: High spend (≥$5K/month) + travel focus (≥25%)
2. Declining: Significant spend drop (≤-30%) from prior period
3. New & Growing: Recent customers (≤6 months) with growth (≥50%)
4. Budget-Conscious: Low spend (<$1.5K/month) + necessity focus (≥60%)
5. Stable Mid-Spenders: Default for consistent behavior

Rolling Window:
- Last 90 days: Current period spending
- Prior 90 days: Previous period (days 91-180) for trend analysis
- Recalculate monthly to capture behavior changes

Usage:
  -- Segment distribution
  SELECT customer_segment, COUNT(*) AS customer_count
  FROM customer_segments
  GROUP BY customer_segment;

  -- High-value customers
  SELECT * FROM customer_segments
  WHERE customer_segment = 'High-Value Travelers'
  ORDER BY lifetime_value DESC;

  -- Declining customers (churn risk)
  SELECT * FROM customer_segments
  WHERE customer_segment = 'Declining'
  ORDER BY spend_change_pct ASC;
============================================================================
#}

WITH customer_spending AS (
    SELECT
        c.customer_id,
        c.customer_key,

        -- Overall metrics (all-time)
        COUNT(f.transaction_key) AS total_transactions,
        SUM(f.transaction_amount) AS lifetime_value,
        AVG(f.transaction_amount) AS avg_transaction_value,
        MIN(f.transaction_date) AS first_transaction_date,
        MAX(f.transaction_date) AS last_transaction_date,

        -- Rolling 90-day metrics
        SUM(CASE
            WHEN f.transaction_date >= DATEADD('day', -90, CURRENT_DATE())
            THEN f.transaction_amount ELSE 0
        END) AS spend_last_90_days,

        SUM(CASE
            WHEN f.transaction_date >= DATEADD('day', -180, CURRENT_DATE())
                 AND f.transaction_date < DATEADD('day', -90, CURRENT_DATE())
            THEN f.transaction_amount ELSE 0
        END) AS spend_prior_90_days,

        -- Category analysis
        SUM(CASE
            WHEN cat.category_name IN ('Travel', 'Airlines', 'Hotels')
            THEN f.transaction_amount ELSE 0
        END) / NULLIF(SUM(f.transaction_amount), 0) * 100 AS travel_spend_pct,

        SUM(CASE
            WHEN cat.category_name IN ('Grocery', 'Gas', 'Utilities')
            THEN f.transaction_amount ELSE 0
        END) / NULLIF(SUM(f.transaction_amount), 0) * 100 AS necessities_spend_pct,

        -- Tenure
        DATEDIFF('month', MIN(f.transaction_date), CURRENT_DATE()) AS tenure_months

    FROM {{ ref('dim_customer') }} c
    LEFT JOIN {{ ref('fct_transactions') }} f
        ON c.customer_key = f.customer_key
    LEFT JOIN {{ ref('dim_merchant_category') }} cat
        ON f.merchant_category_key = cat.category_key
    WHERE c.is_current = TRUE
    GROUP BY c.customer_id, c.customer_key
),

spending_trends AS (
    SELECT
        *,
        -- Calculate month-over-month change
        CASE
            WHEN spend_prior_90_days > 0
            THEN ((spend_last_90_days - spend_prior_90_days) / spend_prior_90_days) * 100
            ELSE 0
        END AS spend_change_pct,

        -- Monthly average
        spend_last_90_days / 3 AS avg_monthly_spend

    FROM customer_spending
),

segment_assignment AS (
    SELECT
        *,
        CASE
            -- High-Value Travelers: High spend + travel focus
            WHEN avg_monthly_spend >= 5000
                 AND travel_spend_pct >= 25
            THEN 'High-Value Travelers'

            -- Declining: Significant spend drop
            WHEN spend_change_pct <= -30
                 AND spend_prior_90_days >= 2000
            THEN 'Declining'

            -- New & Growing: Recent customers with growth
            WHEN tenure_months <= 6
                 AND spend_change_pct >= 50
            THEN 'New & Growing'

            -- Budget-Conscious: Low spend + necessity focus
            WHEN avg_monthly_spend < 1500
                 AND necessities_spend_pct >= 60
            THEN 'Budget-Conscious'

            -- Stable Mid-Spenders: Default for consistent behavior
            ELSE 'Stable Mid-Spenders'

        END AS customer_segment,

        CURRENT_DATE() AS segment_assigned_date

    FROM spending_trends
)

SELECT
    -- Keys
    customer_id,
    customer_key,

    -- Segment assignment
    customer_segment,
    segment_assigned_date,

    -- All-time metrics
    total_transactions,
    lifetime_value,
    avg_transaction_value,
    first_transaction_date,
    last_transaction_date,
    tenure_months,

    -- Rolling 90-day metrics
    spend_last_90_days,
    spend_prior_90_days,
    spend_change_pct,
    avg_monthly_spend,

    -- Category breakdown
    travel_spend_pct,
    necessities_spend_pct

FROM segment_assignment
