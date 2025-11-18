{{
    config(
        materialized='table',
        tags=['intermediate', 'aggregations', 'performance']
    )
}}

{#
============================================================================
Intermediate Model: Customer Transaction Summary
============================================================================
Purpose: Single source of truth for all customer transaction aggregations

WHY THIS MODEL EXISTS:
Previously, customer_segments, metric_customer_ltv, and metric_avg_transaction_value
each independently scanned the 13.5M transaction fact table, performing redundant
aggregations. This model consolidates ALL transaction aggregations into ONE scan,
providing a 4x performance improvement.

Grain: One row per customer (current state)
Row Count: ~50,000 customers
Schema: SILVER

Aggregations Provided:
1. Lifetime metrics (for metric_customer_ltv)
2. Average transaction value metrics (for metric_avg_transaction_value)
3. Rolling 90-day metrics (for customer_segments)
4. Category analysis (for customer_segments)
5. Tenure calculations

Downstream Dependencies:
- customer_segments (uses rolling 90-day and category metrics)
- metric_customer_ltv (uses lifetime aggregations)
- metric_avg_transaction_value (uses ATV aggregations)
- customer_360_profile (can optionally use directly)

Performance Impact:
- Before: 4 full scans of fct_transactions (54M rows processed)
- After: 1 full scan of fct_transactions (13.5M rows processed)
- Improvement: ~4x faster Gold layer build time

============================================================================
#}

WITH customer_transactions AS (
    SELECT
        c.customer_id,
        c.customer_key,
        f.transaction_amount,
        f.transaction_date,
        cat.category_name,
        CASE
            WHEN cat.category_name IN ('Travel', 'Airlines', 'Hotels') THEN 1
            ELSE 0
        END AS is_travel,
        CASE
            WHEN cat.category_name IN ('Grocery', 'Gas', 'Utilities') THEN 1
            ELSE 0
        END AS is_necessity
    FROM {{ ref('dim_customer') }} c
    INNER JOIN {{ ref('fct_transactions') }} f
        ON c.customer_key = f.customer_key
    INNER JOIN {{ ref('dim_merchant_category') }} cat
        ON f.merchant_category_key = cat.category_key
    WHERE c.is_current = TRUE
),

base_aggregations AS (
    SELECT
        customer_id,
        customer_key,

        -- =================================================================
        -- LIFETIME METRICS (for metric_customer_ltv)
        -- =================================================================
        COUNT(*) AS total_transactions,
        SUM(transaction_amount) AS lifetime_value,
        MIN(transaction_date) AS first_transaction_date,
        MAX(transaction_date) AS last_transaction_date,
        DATEDIFF('day', MIN(transaction_date), MAX(transaction_date)) AS customer_age_days,

        -- =================================================================
        -- AVERAGE TRANSACTION VALUE METRICS (for metric_avg_transaction_value)
        -- =================================================================
        AVG(transaction_amount) AS avg_transaction_value,
        STDDEV(transaction_amount) AS transaction_value_stddev,
        MIN(transaction_amount) AS min_transaction_value,
        MAX(transaction_amount) AS max_transaction_value,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY transaction_amount) AS median_transaction_value,

        -- =================================================================
        -- ROLLING 90-DAY METRICS (for customer_segments)
        -- =================================================================
        SUM(CASE
            WHEN transaction_date >= DATEADD('day', -90, CURRENT_DATE())
            THEN transaction_amount
            ELSE 0
        END) AS spend_last_90_days,

        SUM(CASE
            WHEN transaction_date >= DATEADD('day', -180, CURRENT_DATE())
                AND transaction_date < DATEADD('day', -90, CURRENT_DATE())
            THEN transaction_amount
            ELSE 0
        END) AS spend_prior_90_days,

        -- =================================================================
        -- CATEGORY ANALYSIS (for customer_segments)
        -- =================================================================
        SUM(CASE WHEN is_travel = 1 THEN transaction_amount ELSE 0 END) AS travel_spend,
        SUM(CASE WHEN is_necessity = 1 THEN transaction_amount ELSE 0 END) AS necessities_spend,

        -- =================================================================
        -- TENURE (for customer_segments)
        -- =================================================================
        DATEDIFF('month', MIN(transaction_date), CURRENT_DATE()) AS tenure_months

    FROM customer_transactions
    GROUP BY customer_id, customer_key
),

final AS (
    SELECT
        customer_id,
        customer_key,

        -- Lifetime metrics
        total_transactions,
        lifetime_value,
        first_transaction_date,
        last_transaction_date,
        customer_age_days,
        CASE
            WHEN customer_age_days > 0
            THEN lifetime_value / customer_age_days
            ELSE 0
        END AS avg_spend_per_day,

        -- ATV metrics
        avg_transaction_value,
        transaction_value_stddev,
        min_transaction_value,
        max_transaction_value,
        median_transaction_value,
        CASE
            WHEN transaction_value_stddev IS NULL OR transaction_value_stddev = 0 THEN 'No Transactions'
            WHEN transaction_value_stddev < 50 THEN 'Consistent'
            WHEN transaction_value_stddev < 200 THEN 'Moderate'
            ELSE 'Variable'
        END AS spending_consistency,

        -- Rolling 90-day metrics
        spend_last_90_days,
        spend_prior_90_days,
        CASE
            WHEN spend_prior_90_days > 0
            THEN ((spend_last_90_days - spend_prior_90_days) / spend_prior_90_days) * 100
            ELSE 0
        END AS spend_change_pct,
        spend_last_90_days / 3 AS avg_monthly_spend,

        -- Category percentages
        CASE
            WHEN lifetime_value > 0
            THEN (travel_spend / lifetime_value) * 100
            ELSE 0
        END AS travel_spend_pct,
        CASE
            WHEN lifetime_value > 0
            THEN (necessities_spend / lifetime_value) * 100
            ELSE 0
        END AS necessities_spend_pct,

        -- Tenure
        tenure_months,

        -- Metadata
        CURRENT_DATE() AS metric_calculated_date

    FROM base_aggregations
)

SELECT * FROM final
