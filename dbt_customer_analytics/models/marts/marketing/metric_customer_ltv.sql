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

-- Use pre-aggregated intermediate model (much faster!)
SELECT
    i.customer_id,
    i.customer_key,
    seg.customer_segment,

    -- All metrics pre-calculated in int_customer_transaction_summary
    i.lifetime_value,
    i.total_transactions,
    i.first_transaction_date,
    i.last_transaction_date,
    i.customer_age_days,
    i.avg_spend_per_day,

    -- Metadata
    i.metric_calculated_date

FROM {{ ref('int_customer_transaction_summary') }} i

LEFT JOIN {{ ref('customer_segments') }} seg
    ON i.customer_id = seg.customer_id
