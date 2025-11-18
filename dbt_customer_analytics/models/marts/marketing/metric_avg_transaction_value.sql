{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'mart', 'metrics', 'atv']
    )
}}

{#
============================================================================
Gold Layer: Average Transaction Value (ATV) Metric
============================================================================
Purpose: Calculate average transaction size per customer

Grain: One row per customer (current state only)
Row Count: ~50,000 customers

Business Definition:
- ATV: AVG(transaction_amount) across all transactions
- Standard deviation shows spending consistency
- Min/Max show transaction range
- Used for customer value analysis and segmentation

Usage:
  -- Customers with highest ATV
  SELECT customer_id, customer_segment, avg_transaction_value
  FROM metric_avg_transaction_value
  ORDER BY avg_transaction_value DESC
  LIMIT 100;

  -- ATV by segment
  SELECT
      customer_segment,
      AVG(avg_transaction_value) AS avg_atv,
      AVG(transaction_value_stddev) AS avg_stddev
  FROM metric_avg_transaction_value
  GROUP BY customer_segment;

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
============================================================================
#}

-- Use pre-aggregated intermediate model (much faster!)
SELECT
    i.customer_id,
    i.customer_key,
    seg.customer_segment,

    -- All metrics pre-calculated in int_customer_transaction_summary
    i.avg_transaction_value,
    i.transaction_value_stddev,
    i.min_transaction_value,
    i.max_transaction_value,
    i.total_transactions AS transaction_count,
    i.median_transaction_value,
    i.spending_consistency,

    -- Metadata
    i.metric_calculated_date

FROM {{ ref('int_customer_transaction_summary') }} i

LEFT JOIN {{ ref('customer_segments') }} seg
    ON i.customer_id = seg.customer_id
