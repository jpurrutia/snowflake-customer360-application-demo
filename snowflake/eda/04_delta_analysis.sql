-- ============================================================================
-- Delta Analysis - Compare Before and After Transaction Generation
-- ============================================================================
-- Purpose: Compare metrics before and after running generate_transactions.sql
-- Run this AFTER 03_post_generation_validation.sql
-- Requires: metrics_baseline table created by 01_baseline_metrics.sql
-- ============================================================================

USE ROLE DATA_ENGINEER;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA BRONZE;

-- ============================================================================
-- 1. CAPTURE AFTER-GENERATION METRICS
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'CAPTURING POST-GENERATION METRICS'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

-- Insert AFTER metrics
INSERT INTO metrics_baseline
SELECT
    CURRENT_TIMESTAMP() AS snapshot_timestamp,
    'AFTER_TRANSACTION_GENERATION' AS snapshot_type,
    (SELECT COUNT(*) FROM bronze_customers) AS customer_count,
    (SELECT COUNT(*) FROM bronze_transactions) AS transaction_count,
    (SELECT COUNT(DISTINCT customer_segment) FROM bronze_customers) AS segment_count,
    (SELECT COALESCE(SUM(transaction_amount), 0) FROM bronze_transactions) AS total_transaction_amount,
    (SELECT COALESCE(AVG(transaction_amount), 0) FROM bronze_transactions) AS avg_transaction_amount,
    (SELECT MIN(transaction_date) FROM bronze_transactions) AS min_transaction_date,
    (SELECT MAX(transaction_date) FROM bronze_transactions) AS max_transaction_date,
    (SELECT COALESCE(DATEDIFF('month', MIN(transaction_date), MAX(transaction_date)), 0)
     FROM bronze_transactions) AS months_of_data
;

SELECT '✓ Post-generation metrics captured' AS status;

-- ============================================================================
-- 2. BEFORE/AFTER COMPARISON
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'BEFORE/AFTER METRICS COMPARISON'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

WITH latest_snapshots AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY snapshot_type ORDER BY snapshot_timestamp DESC) AS rn
        FROM metrics_baseline
    )
    WHERE rn = 1
)
SELECT
    snapshot_type,
    customer_count,
    transaction_count,
    segment_count,
    ROUND(total_transaction_amount, 2) AS total_amount,
    ROUND(avg_transaction_amount, 2) AS avg_amount,
    TO_CHAR(min_transaction_date, 'YYYY-MM-DD') AS min_date,
    TO_CHAR(max_transaction_date, 'YYYY-MM-DD') AS max_date,
    months_of_data,
    TO_CHAR(snapshot_timestamp, 'YYYY-MM-DD HH24:MI:SS') AS captured_at
FROM latest_snapshots
ORDER BY snapshot_type;

-- ============================================================================
-- 3. DELTA CALCULATIONS
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'DELTA CALCULATIONS'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

WITH latest_snapshots AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY snapshot_type ORDER BY snapshot_timestamp DESC) AS rn
        FROM metrics_baseline
    )
    WHERE rn = 1
),
before AS (
    SELECT * FROM latest_snapshots WHERE snapshot_type = 'BEFORE_TRANSACTION_GENERATION'
),
after AS (
    SELECT * FROM latest_snapshots WHERE snapshot_type = 'AFTER_TRANSACTION_GENERATION'
)
SELECT
    'Customer Count' AS metric,
    b.customer_count AS before_value,
    a.customer_count AS after_value,
    a.customer_count - b.customer_count AS delta,
    CASE
        WHEN b.customer_count = 0 THEN 'N/A'
        ELSE TO_CHAR(ROUND((a.customer_count - b.customer_count) * 100.0 / b.customer_count, 2), '999990.00') || '%'
    END AS pct_change
FROM before b, after a
UNION ALL
SELECT
    'Transaction Count' AS metric,
    b.transaction_count AS before_value,
    a.transaction_count AS after_value,
    a.transaction_count - b.transaction_count AS delta,
    CASE
        WHEN b.transaction_count = 0 THEN 'NEW DATA'
        ELSE TO_CHAR(ROUND((a.transaction_count - b.transaction_count) * 100.0 / b.transaction_count, 2), '999990.00') || '%'
    END AS pct_change
FROM before b, after a
UNION ALL
SELECT
    'Total Transaction Amount' AS metric,
    ROUND(b.total_transaction_amount, 2) AS before_value,
    ROUND(a.total_transaction_amount, 2) AS after_value,
    ROUND(a.total_transaction_amount - b.total_transaction_amount, 2) AS delta,
    CASE
        WHEN b.total_transaction_amount = 0 THEN 'NEW DATA'
        ELSE TO_CHAR(ROUND((a.total_transaction_amount - b.total_transaction_amount) * 100.0 / b.total_transaction_amount, 2), '999990.00') || '%'
    END AS pct_change
FROM before b, after a
UNION ALL
SELECT
    'Avg Transaction Amount' AS metric,
    ROUND(b.avg_transaction_amount, 2) AS before_value,
    ROUND(a.avg_transaction_amount, 2) AS after_value,
    ROUND(a.avg_transaction_amount - b.avg_transaction_amount, 2) AS delta,
    CASE
        WHEN b.avg_transaction_amount = 0 THEN 'NEW DATA'
        ELSE TO_CHAR(ROUND((a.avg_transaction_amount - b.avg_transaction_amount) * 100.0 / b.avg_transaction_amount, 2), '999990.00') || '%'
    END AS pct_change
FROM before b, after a
UNION ALL
SELECT
    'Months of Data' AS metric,
    b.months_of_data AS before_value,
    a.months_of_data AS after_value,
    a.months_of_data - b.months_of_data AS delta,
    CASE
        WHEN b.months_of_data = 0 THEN 'NEW DATA'
        ELSE TO_CHAR(ROUND((a.months_of_data - b.months_of_data) * 100.0 / b.months_of_data, 2), '999990.00') || '%'
    END AS pct_change
FROM before b, after a
;

-- ============================================================================
-- 4. SEGMENT-LEVEL TRANSACTION ANALYSIS
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'TRANSACTION DISTRIBUTION BY SEGMENT'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

SELECT
    c.customer_segment,
    COUNT(DISTINCT c.customer_id) AS total_customers,
    COUNT(t.transaction_id) AS total_transactions,
    ROUND(COUNT(t.transaction_id) * 1.0 / NULLIF(COUNT(DISTINCT c.customer_id), 0), 2) AS avg_txns_per_customer,
    ROUND(MIN(t.transaction_amount), 2) AS min_amount,
    ROUND(AVG(t.transaction_amount), 2) AS avg_amount,
    ROUND(MAX(t.transaction_amount), 2) AS max_amount,
    ROUND(SUM(t.transaction_amount), 2) AS total_spend,
    ROUND(SUM(t.transaction_amount) / NULLIF(COUNT(DISTINCT c.customer_id), 0), 2) AS spend_per_customer
FROM bronze_customers c
LEFT JOIN bronze_transactions t ON c.customer_id = t.customer_id
GROUP BY c.customer_segment
ORDER BY total_spend DESC;

-- ============================================================================
-- 5. MONTHLY TRANSACTION GROWTH
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'MONTHLY TRANSACTION TREND'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

WITH monthly_metrics AS (
    SELECT
        DATE_TRUNC('month', transaction_date) AS month,
        COUNT(*) AS txn_count,
        COUNT(DISTINCT customer_id) AS active_customers,
        ROUND(AVG(transaction_amount), 2) AS avg_amount,
        ROUND(SUM(transaction_amount), 2) AS total_amount
    FROM bronze_transactions
    GROUP BY DATE_TRUNC('month', transaction_date)
)
SELECT
    TO_CHAR(month, 'YYYY-MM') AS month,
    txn_count,
    active_customers,
    avg_amount,
    total_amount,
    ROUND(txn_count * 1.0 / active_customers, 2) AS txns_per_customer,
    LAG(txn_count) OVER (ORDER BY month) AS prev_month_txns,
    CASE
        WHEN LAG(txn_count) OVER (ORDER BY month) IS NULL THEN NULL
        ELSE ROUND((txn_count - LAG(txn_count) OVER (ORDER BY month)) * 100.0 /
             NULLIF(LAG(txn_count) OVER (ORDER BY month), 0), 2)
    END AS pct_change_mom
FROM monthly_metrics
ORDER BY month;

-- ============================================================================
-- 6. DECLINING SEGMENT PATTERN VALIDATION
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'DECLINING SEGMENT PATTERN ANALYSIS'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

WITH monthly_spend AS (
    SELECT
        t.customer_id,
        c.decline_type,
        DATE_TRUNC('month', t.transaction_date) AS month,
        SUM(t.transaction_amount) AS monthly_total,
        COUNT(t.transaction_id) AS monthly_txns
    FROM bronze_transactions t
    JOIN bronze_customers c ON t.customer_id = c.customer_id
    WHERE c.customer_segment = 'Declining'
    GROUP BY t.customer_id, c.decline_type, DATE_TRUNC('month', t.transaction_date)
),
monthly_avg AS (
    SELECT
        decline_type,
        month,
        ROUND(AVG(monthly_total), 2) AS avg_monthly_spend,
        ROUND(AVG(monthly_txns), 2) AS avg_monthly_txns,
        COUNT(DISTINCT customer_id) AS customers
    FROM monthly_spend
    GROUP BY decline_type, month
)
SELECT
    decline_type,
    TO_CHAR(month, 'YYYY-MM') AS month,
    avg_monthly_spend,
    avg_monthly_txns,
    customers,
    LAG(avg_monthly_spend) OVER (PARTITION BY decline_type ORDER BY month) AS prev_month_spend,
    CASE
        WHEN LAG(avg_monthly_spend) OVER (PARTITION BY decline_type ORDER BY month) IS NULL THEN NULL
        ELSE ROUND((avg_monthly_spend - LAG(avg_monthly_spend) OVER (PARTITION BY decline_type ORDER BY month)) * 100.0 /
             NULLIF(LAG(avg_monthly_spend) OVER (PARTITION BY decline_type ORDER BY month), 0), 2)
    END AS pct_change_mom
FROM monthly_avg
ORDER BY decline_type, month;

-- ============================================================================
-- 7. TOP 10 CUSTOMERS BY SPEND
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'TOP 10 CUSTOMERS BY TOTAL SPEND'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS full_name,
    c.customer_segment,
    c.card_type,
    c.credit_limit,
    COUNT(t.transaction_id) AS txn_count,
    ROUND(AVG(t.transaction_amount), 2) AS avg_txn_amount,
    ROUND(SUM(t.transaction_amount), 2) AS total_spend,
    TO_CHAR(MIN(t.transaction_date), 'YYYY-MM-DD') AS first_txn,
    TO_CHAR(MAX(t.transaction_date), 'YYYY-MM-DD') AS last_txn
FROM bronze_customers c
JOIN bronze_transactions t ON c.customer_id = t.customer_id
GROUP BY c.customer_id, full_name, c.customer_segment, c.card_type, c.credit_limit
ORDER BY total_spend DESC
LIMIT 10;

-- ============================================================================
-- 8. CHANNEL AND CATEGORY DISTRIBUTION
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'TRANSACTION CHANNEL DISTRIBUTION'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

SELECT
    channel,
    COUNT(*) AS txn_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    ROUND(AVG(transaction_amount), 2) AS avg_amount,
    ROUND(SUM(transaction_amount), 2) AS total_amount
FROM bronze_transactions
GROUP BY channel
ORDER BY txn_count DESC;

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'TOP 10 MERCHANT CATEGORIES'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

SELECT
    merchant_category,
    COUNT(*) AS txn_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    ROUND(AVG(transaction_amount), 2) AS avg_amount,
    ROUND(SUM(transaction_amount), 2) AS total_amount
FROM bronze_transactions
GROUP BY merchant_category
ORDER BY txn_count DESC
LIMIT 10;

-- ============================================================================
-- 9. TRANSACTION STATUS ANALYSIS
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'TRANSACTION STATUS DISTRIBUTION'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

SELECT
    status,
    COUNT(*) AS txn_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    ROUND(AVG(transaction_amount), 2) AS avg_amount,
    ROUND(SUM(transaction_amount), 2) AS total_amount
FROM bronze_transactions
GROUP BY status
ORDER BY txn_count DESC;

-- ============================================================================
-- 10. DATA QUALITY SUMMARY
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'DATA QUALITY SUMMARY'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

SELECT
    'NULL transaction_id' AS quality_check,
    COUNT_IF(transaction_id IS NULL) AS issue_count,
    CASE WHEN COUNT_IF(transaction_id IS NULL) = 0 THEN '✓ PASS' ELSE '✗ FAIL' END AS status
FROM bronze_transactions
UNION ALL
SELECT
    'NULL customer_id' AS quality_check,
    COUNT_IF(customer_id IS NULL) AS issue_count,
    CASE WHEN COUNT_IF(customer_id IS NULL) = 0 THEN '✓ PASS' ELSE '✗ FAIL' END AS status
FROM bronze_transactions
UNION ALL
SELECT
    'Invalid amounts (<= 0)' AS quality_check,
    COUNT_IF(transaction_amount <= 0) AS issue_count,
    CASE WHEN COUNT_IF(transaction_amount <= 0) = 0 THEN '✓ PASS' ELSE '✗ FAIL' END AS status
FROM bronze_transactions
UNION ALL
SELECT
    'Future dates' AS quality_check,
    COUNT_IF(transaction_date > CURRENT_TIMESTAMP()) AS issue_count,
    CASE WHEN COUNT_IF(transaction_date > CURRENT_TIMESTAMP()) = 0 THEN '✓ PASS' ELSE '✗ FAIL' END AS status
FROM bronze_transactions
UNION ALL
SELECT
    'Duplicate transaction_ids' AS quality_check,
    COUNT(*) - COUNT(DISTINCT transaction_id) AS issue_count,
    CASE WHEN COUNT(*) = COUNT(DISTINCT transaction_id) THEN '✓ PASS' ELSE '✗ FAIL' END AS status
FROM bronze_transactions
UNION ALL
SELECT
    'Orphan transactions' AS quality_check,
    (SELECT COUNT(*) FROM bronze_transactions t
     WHERE NOT EXISTS (SELECT 1 FROM bronze_customers c WHERE c.customer_id = t.customer_id)) AS issue_count,
    CASE
        WHEN (SELECT COUNT(*) FROM bronze_transactions t
              WHERE NOT EXISTS (SELECT 1 FROM bronze_customers c WHERE c.customer_id = t.customer_id)) = 0
        THEN '✓ PASS' ELSE '✗ FAIL'
    END AS status
;

-- ============================================================================
-- FINAL SUMMARY
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'DELTA ANALYSIS COMPLETE'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

SELECT
    '✓ Delta analysis complete' AS status,
    (SELECT COUNT(*) FROM bronze_transactions) AS total_transactions,
    (SELECT COUNT(DISTINCT customer_id) FROM bronze_transactions) AS unique_customers,
    (SELECT COUNT(DISTINCT customer_segment) FROM bronze_customers) AS segments,
    (SELECT DATEDIFF('month', MIN(transaction_date), MAX(transaction_date))
     FROM bronze_transactions) AS months_of_data,
    (SELECT TO_CHAR(ROUND(SUM(transaction_amount), 2), '999,999,999,999.99')
     FROM bronze_transactions) AS total_transaction_volume
;

SELECT 'Next: Run 05_telemetry_tracking.sql to set up ongoing monitoring' AS next_step;
