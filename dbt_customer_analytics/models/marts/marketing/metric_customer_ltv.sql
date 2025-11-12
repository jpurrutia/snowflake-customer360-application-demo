{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'mart', 'metrics', 'ltv']
    )
}}

{#
============================================================================
Gold Layer: Customer Lifetime Value (LTV) Metric
============================================================================
Purpose: Calculate total lifetime value for each customer

Grain: One row per customer (current state only)
Row Count: ~50,000 customers

Business Definition:
- Lifetime Value (LTV): Total spending from account opening to present
- Includes approved transactions only
- Updated when fact table refreshes

Usage:
  -- Top 100 customers by LTV
  SELECT customer_id, customer_segment, lifetime_value
  FROM metric_customer_ltv
  ORDER BY lifetime_value DESC
  LIMIT 100;

  -- Average LTV by segment
  SELECT customer_segment, AVG(lifetime_value) AS avg_ltv
  FROM metric_customer_ltv
  GROUP BY customer_segment;

  -- LTV distribution
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
============================================================================
#}

SELECT
    c.customer_id,
    c.customer_key,
    seg.customer_segment,

    -- Lifetime Value: Total spending all-time
    COALESCE(SUM(f.transaction_amount), 0) AS lifetime_value,

    -- Transaction counts
    COUNT(f.transaction_key) AS total_transactions,

    -- Activity timeline
    MIN(f.transaction_date) AS first_transaction_date,
    MAX(f.transaction_date) AS last_transaction_date,

    -- Customer age (days between first and last transaction)
    COALESCE(
        DATEDIFF('day', MIN(f.transaction_date), MAX(f.transaction_date)),
        0
    ) AS customer_age_days,

    -- Average spending per day (LTV / age)
    CASE
        WHEN DATEDIFF('day', MIN(f.transaction_date), MAX(f.transaction_date)) > 0
        THEN COALESCE(SUM(f.transaction_amount), 0) /
             DATEDIFF('day', MIN(f.transaction_date), MAX(f.transaction_date))
        ELSE 0
    END AS avg_spend_per_day,

    -- Metadata
    CURRENT_DATE() AS metric_calculated_date

FROM {{ ref('dim_customer') }} c

LEFT JOIN {{ ref('fct_transactions') }} f
    ON c.customer_key = f.customer_key
    AND f.status = 'approved'  -- Only approved transactions

LEFT JOIN {{ ref('customer_segments') }} seg
    ON c.customer_id = seg.customer_id

WHERE c.is_current = TRUE

GROUP BY
    c.customer_id,
    c.customer_key,
    seg.customer_segment
