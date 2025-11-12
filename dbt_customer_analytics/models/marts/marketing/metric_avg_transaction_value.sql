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

SELECT
    c.customer_id,
    c.customer_key,
    seg.customer_segment,

    -- Average Transaction Value
    COALESCE(AVG(f.transaction_amount), 0) AS avg_transaction_value,

    -- Spending variability
    COALESCE(STDDEV(f.transaction_amount), 0) AS transaction_value_stddev,

    -- Transaction range
    COALESCE(MIN(f.transaction_amount), 0) AS min_transaction_value,
    COALESCE(MAX(f.transaction_amount), 0) AS max_transaction_value,

    -- Transaction count (for context)
    COUNT(f.transaction_key) AS transaction_count,

    -- Median transaction value (using PERCENTILE_CONT)
    COALESCE(
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.transaction_amount),
        0
    ) AS median_transaction_value,

    -- Spending consistency flag
    CASE
        WHEN STDDEV(f.transaction_amount) IS NULL THEN 'No Transactions'
        WHEN STDDEV(f.transaction_amount) < 50 THEN 'Consistent'
        WHEN STDDEV(f.transaction_amount) < 200 THEN 'Moderate'
        ELSE 'Variable'
    END AS spending_consistency,

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
