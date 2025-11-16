/*
============================================================================
ML: Create Churn Labels
============================================================================
Purpose: Generate churn labels based on actual customer behavior

Churn Definition:
- Churned = TRUE if either:
  1. No transactions for 60+ days (inactivity)
  2. Recent spending < 30% of baseline (significant decline)
- Churned = FALSE otherwise (active, engaged customers)

Process:
1. Calculate baseline spending (first 12 months average)
2. Calculate recent behavior (last 3 months)
3. Apply churn rules to label customers

Output: GOLD.CHURN_LABELS table

Usage:
  snowflake-sql -f snowflake/ml/01_create_churn_labels.sql

Validation:
  SELECT churned, COUNT(*) FROM GOLD.CHURN_LABELS GROUP BY churned;
============================================================================
*/

-- Set context
USE ROLE DATA_ENGINEER;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA GOLD;

-- Drop existing table if re-running
DROP TABLE IF EXISTS GOLD.CHURN_LABELS;

-- Create churn labels based on actual customer behavior
CREATE TABLE GOLD.CHURN_LABELS AS
WITH customer_baseline AS (
    /*
    Calculate baseline spending (average monthly spend in first 12 months).

    This represents "normal" spending behavior to compare against.
    Only includes customers with at least 12 months of history.
    */
    SELECT
        c.customer_id,
        COUNT(DISTINCT m.month) AS baseline_months,
        AVG(m.monthly_spend) AS baseline_avg_spend,
        SUM(m.monthly_spend) AS baseline_total_spend
    FROM GOLD.DIM_CUSTOMER c
    JOIN (
        SELECT
            c2.customer_id,
            DATE_TRUNC('month', f.transaction_date) AS month,
            SUM(f.transaction_amount) AS monthly_spend
        FROM GOLD.FCT_TRANSACTIONS f
        JOIN GOLD.DIM_CUSTOMER c2 ON f.customer_key = c2.customer_key
        WHERE c2.is_current = TRUE
          AND f.status = 'approved'
          -- First 12 months only (exclude last 6 months for training/validation)
          AND f.transaction_date >= DATEADD('month', -18, CURRENT_DATE())
          AND f.transaction_date < DATEADD('month', -6, CURRENT_DATE())
        GROUP BY c2.customer_id, DATE_TRUNC('month', f.transaction_date)
    ) m ON c.customer_id = m.customer_id
    WHERE c.is_current = TRUE
    GROUP BY c.customer_id
    -- Require at least 6 months of baseline data
    HAVING COUNT(DISTINCT m.month) >= 6
),

recent_behavior AS (
    /*
    Analyze recent behavior (last 3 months).

    Used to detect churn:
    - days_since_last_transaction: Inactivity indicator
    - recent_avg_spend: Compare against baseline
    */
    SELECT
        c.customer_id,
        MAX(f.transaction_date) AS last_transaction_date,
        COUNT(DISTINCT DATE_TRUNC('month', f.transaction_date)) AS recent_months_active,
        AVG(m.monthly_spend) AS recent_avg_spend,
        SUM(f.transaction_amount) AS recent_total_spend,
        COUNT(f.transaction_key) AS recent_transaction_count
    FROM GOLD.DIM_CUSTOMER c
    LEFT JOIN GOLD.FCT_TRANSACTIONS f
        ON c.customer_key = f.customer_key
        AND f.status = 'approved'
        -- Last 3 months
        AND f.transaction_date >= DATEADD('month', -3, CURRENT_DATE())
        AND f.transaction_date <= CURRENT_DATE()
    LEFT JOIN (
        SELECT
            c2.customer_id,
            DATE_TRUNC('month', f2.transaction_date) AS month,
            SUM(f2.transaction_amount) AS monthly_spend
        FROM GOLD.FCT_TRANSACTIONS f2
        JOIN GOLD.DIM_CUSTOMER c2 ON f2.customer_key = c2.customer_key
        WHERE c2.is_current = TRUE
          AND f2.status = 'approved'
          AND f2.transaction_date >= DATEADD('month', -3, CURRENT_DATE())
        GROUP BY c2.customer_id, DATE_TRUNC('month', f2.transaction_date)
    ) m ON c.customer_id = m.customer_id
    WHERE c.is_current = TRUE
    GROUP BY c.customer_id
)

SELECT
    b.customer_id,

    -- Baseline metrics (first 12 months)
    b.baseline_months,
    b.baseline_avg_spend,
    b.baseline_total_spend,

    -- Recent metrics (last 3 months)
    COALESCE(r.recent_months_active, 0) AS recent_months_active,
    COALESCE(r.recent_avg_spend, 0) AS recent_avg_spend,
    COALESCE(r.recent_total_spend, 0) AS recent_total_spend,
    COALESCE(r.recent_transaction_count, 0) AS recent_transaction_count,
    r.last_transaction_date,

    -- Recency metric
    CASE
        WHEN r.last_transaction_date IS NULL
        THEN 999  -- No recent transactions (definitely inactive)
        ELSE DATEDIFF('day', r.last_transaction_date, CURRENT_DATE())
    END AS days_since_last_transaction,

    -- Spend change percentage
    CASE
        WHEN b.baseline_avg_spend > 0 AND r.recent_avg_spend > 0
        THEN ((r.recent_avg_spend - b.baseline_avg_spend) / b.baseline_avg_spend) * 100
        WHEN r.recent_avg_spend IS NULL OR r.recent_avg_spend = 0
        THEN -100.0  -- Complete decline
        ELSE 0.0
    END AS spend_change_pct,

    -- Churn label (target variable)
    CASE
        -- Rule 1: No transactions for 60+ days (inactivity churn)
        WHEN r.last_transaction_date IS NULL
             OR DATEDIFF('day', r.last_transaction_date, CURRENT_DATE()) > 60
        THEN TRUE

        -- Rule 2: Recent spending < 30% of baseline (decline churn)
        WHEN r.recent_avg_spend < (b.baseline_avg_spend * 0.30)
        THEN TRUE

        -- Active customers (not churned)
        ELSE FALSE
    END AS churned,

    -- Churn reason (for analysis)
    CASE
        WHEN r.last_transaction_date IS NULL
        THEN 'No recent transactions'
        WHEN DATEDIFF('day', r.last_transaction_date, CURRENT_DATE()) > 60
        THEN 'Inactive (60+ days)'
        WHEN r.recent_avg_spend < (b.baseline_avg_spend * 0.30)
        THEN 'Significant decline (< 30% baseline)'
        ELSE 'Active'
    END AS churn_reason,

    -- Metadata
    CURRENT_TIMESTAMP() AS label_created_at

FROM customer_baseline b
LEFT JOIN recent_behavior r ON b.customer_id = r.customer_id;

-- Create index for performance
CREATE INDEX IF NOT EXISTS idx_churn_labels_customer_id
    ON GOLD.CHURN_LABELS(customer_id);

-- Validation: Display summary statistics
SELECT
    '=== CHURN LABELS SUMMARY ===' AS summary;

SELECT
    'Total customers labeled' AS metric,
    COUNT(*) AS value
FROM GOLD.CHURN_LABELS;

SELECT
    'Class distribution' AS metric,
    churned,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM GOLD.CHURN_LABELS
GROUP BY churned
ORDER BY churned;

SELECT
    'Churn reasons breakdown' AS metric,
    churn_reason,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM GOLD.CHURN_LABELS
GROUP BY churn_reason
ORDER BY count DESC;

SELECT
    'Average baseline spend' AS metric,
    'Churned' AS customer_type,
    ROUND(AVG(baseline_avg_spend), 2) AS avg_baseline_spend,
    ROUND(AVG(recent_avg_spend), 2) AS avg_recent_spend,
    ROUND(AVG(spend_change_pct), 2) AS avg_spend_change_pct
FROM GOLD.CHURN_LABELS
WHERE churned = TRUE

UNION ALL

SELECT
    'Average baseline spend' AS metric,
    'Active' AS customer_type,
    ROUND(AVG(baseline_avg_spend), 2) AS avg_baseline_spend,
    ROUND(AVG(recent_avg_spend), 2) AS avg_recent_spend,
    ROUND(AVG(spend_change_pct), 2) AS avg_spend_change_pct
FROM GOLD.CHURN_LABELS
WHERE churned = FALSE;

-- Success message
SELECT '✓ Churn labels created successfully' AS status;
