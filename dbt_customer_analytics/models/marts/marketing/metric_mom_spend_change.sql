{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'mart', 'metrics', 'mom']
    )
}}

{#
============================================================================
Gold Layer: Month-over-Month Spend Change Percentage
============================================================================
Purpose: Track spending trends for each customer by month

Grain: One row per customer per month
Row Count: ~50,000 customers × ~18 months = ~900,000 rows

Business Definition:
- MoM Change %: ((Current Month - Prior Month) / Prior Month) × 100
- Positive = spending increased
- Negative = spending decreased
- NULL = first month (no prior period)

Usage:
  -- Latest month's MoM change by segment
  WITH latest_month AS (
      SELECT MAX(month) AS max_month
      FROM metric_mom_spend_change
  )
  SELECT
      seg.customer_segment,
      AVG(m.mom_change_pct) AS avg_mom_change
  FROM metric_mom_spend_change m
  JOIN customer_segments seg ON m.customer_id = seg.customer_id
  CROSS JOIN latest_month
  WHERE m.month = latest_month.max_month
    AND m.mom_change_pct IS NOT NULL
  GROUP BY seg.customer_segment;

  -- Customers with biggest MoM decline
  SELECT customer_id, month, monthly_spend, prior_month_spend, mom_change_pct
  FROM metric_mom_spend_change
  WHERE month = DATE_TRUNC('month', CURRENT_DATE() - INTERVAL '1 month')
  ORDER BY mom_change_pct ASC
  LIMIT 100;

  -- MoM trend over time for specific customer
  SELECT month, monthly_spend, mom_change_pct
  FROM metric_mom_spend_change
  WHERE customer_id = 'CUST00000001'
  ORDER BY month;
============================================================================
#}

WITH monthly_spend AS (
    SELECT
        c.customer_id,
        DATE_TRUNC('month', f.transaction_date) AS month,
        SUM(f.transaction_amount) AS monthly_spend
    FROM {{ ref('dim_customer') }} c
    LEFT JOIN {{ ref('fct_transactions') }} f
        ON c.customer_key = f.customer_key
        AND f.status = 'approved'  -- Only approved transactions
    WHERE c.is_current = TRUE
      AND f.transaction_date IS NOT NULL  -- Exclude NULL dates
    GROUP BY
        c.customer_id,
        DATE_TRUNC('month', f.transaction_date)
),

mom_calculations AS (
    SELECT
        customer_id,
        month,
        monthly_spend,

        -- Prior month spend
        LAG(monthly_spend) OVER (
            PARTITION BY customer_id
            ORDER BY month
        ) AS prior_month_spend,

        -- Month-over-month change percentage
        CASE
            WHEN LAG(monthly_spend) OVER (PARTITION BY customer_id ORDER BY month) > 0
            THEN (
                (monthly_spend - LAG(monthly_spend) OVER (PARTITION BY customer_id ORDER BY month))
                / LAG(monthly_spend) OVER (PARTITION BY customer_id ORDER BY month)
            ) * 100
            ELSE NULL
        END AS mom_change_pct,

        -- Month number (for analysis)
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY month) AS month_number

    FROM monthly_spend
)

SELECT
    customer_id,
    month,
    monthly_spend,
    prior_month_spend,
    mom_change_pct,
    month_number,

    -- Additional context
    CASE
        WHEN mom_change_pct IS NULL THEN 'First Month'
        WHEN mom_change_pct > 50 THEN 'High Growth'
        WHEN mom_change_pct > 0 THEN 'Growth'
        WHEN mom_change_pct = 0 THEN 'Flat'
        WHEN mom_change_pct > -30 THEN 'Decline'
        ELSE 'High Decline'
    END AS mom_trend_category,

    -- Metadata
    CURRENT_DATE() AS metric_calculated_date

FROM mom_calculations
ORDER BY customer_id, month
