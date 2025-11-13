-- ============================================================================
-- Post-Generation Validation - Comprehensive Data Quality Checks
-- ============================================================================
-- Purpose: Validate transaction data after running generate_transactions.sql
-- Run this AFTER generate_transactions.sql completes
-- Expected: 10M-17M transactions with 18 months of data
-- ============================================================================

USE ROLE DATA_ENGINEER;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA BRONZE;

-- ============================================================================
-- 1. ROW COUNT VALIDATION
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'ROW COUNT VALIDATION'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

SELECT
    COUNT(*) AS total_transactions,
    CASE
        WHEN COUNT(*) BETWEEN 10000000 AND 17000000 THEN '✓ PASS: Within expected range (10M-17M)'
        WHEN COUNT(*) > 17000000 THEN '⚠ WARNING: More than expected (>17M)'
        WHEN COUNT(*) < 10000000 AND COUNT(*) > 0 THEN '✗ FAIL: Less than expected (<10M)'
        ELSE '✗ FAIL: No data generated'
    END AS validation_status
FROM bronze_transactions;

-- ============================================================================
-- 2. UNIQUE TRANSACTION ID VALIDATION
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'UNIQUE TRANSACTION ID VALIDATION'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT transaction_id) AS unique_transaction_ids,
    COUNT(*) - COUNT(DISTINCT transaction_id) AS duplicate_count,
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT transaction_id) THEN '✓ PASS: All transaction IDs are unique'
        ELSE '✗ FAIL: Duplicate transaction IDs found'
    END AS validation_status
FROM bronze_transactions;

-- ============================================================================
-- 3. NULL VALUE VALIDATION
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'NULL VALUE VALIDATION'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

SELECT
    'transaction_id' AS field_name,
    COUNT_IF(transaction_id IS NULL) AS null_count,
    CASE WHEN COUNT_IF(transaction_id IS NULL) = 0 THEN '✓ PASS' ELSE '✗ FAIL' END AS status
FROM bronze_transactions
UNION ALL
SELECT
    'customer_id' AS field_name,
    COUNT_IF(customer_id IS NULL) AS null_count,
    CASE WHEN COUNT_IF(customer_id IS NULL) = 0 THEN '✓ PASS' ELSE '✗ FAIL' END AS status
FROM bronze_transactions
UNION ALL
SELECT
    'transaction_date' AS field_name,
    COUNT_IF(transaction_date IS NULL) AS null_count,
    CASE WHEN COUNT_IF(transaction_date IS NULL) = 0 THEN '✓ PASS' ELSE '✗ FAIL' END AS status
FROM bronze_transactions
UNION ALL
SELECT
    'transaction_amount' AS field_name,
    COUNT_IF(transaction_amount IS NULL) AS null_count,
    CASE WHEN COUNT_IF(transaction_amount IS NULL) = 0 THEN '✓ PASS' ELSE '✗ FAIL' END AS status
FROM bronze_transactions
UNION ALL
SELECT
    'merchant_name' AS field_name,
    COUNT_IF(merchant_name IS NULL) AS null_count,
    CASE WHEN COUNT_IF(merchant_name IS NULL) = 0 THEN '✓ PASS' ELSE '✗ FAIL' END AS status
FROM bronze_transactions
UNION ALL
SELECT
    'merchant_category' AS field_name,
    COUNT_IF(merchant_category IS NULL) AS null_count,
    CASE WHEN COUNT_IF(merchant_category IS NULL) = 0 THEN '✓ PASS' ELSE '✗ FAIL' END AS status
FROM bronze_transactions
UNION ALL
SELECT
    'channel' AS field_name,
    COUNT_IF(channel IS NULL) AS null_count,
    CASE WHEN COUNT_IF(channel IS NULL) = 0 THEN '✓ PASS' ELSE '✗ FAIL' END AS status
FROM bronze_transactions
UNION ALL
SELECT
    'status' AS field_name,
    COUNT_IF(status IS NULL) AS null_count,
    CASE WHEN COUNT_IF(status IS NULL) = 0 THEN '✓ PASS' ELSE '✗ FAIL' END AS status
FROM bronze_transactions
;

-- ============================================================================
-- 4. CUSTOMER REPRESENTATION VALIDATION
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'CUSTOMER REPRESENTATION VALIDATION'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

WITH customer_txn_counts AS (
    SELECT
        c.customer_id,
        c.customer_segment,
        COUNT(t.transaction_id) AS txn_count
    FROM bronze_customers c
    LEFT JOIN bronze_transactions t ON c.customer_id = t.customer_id
    GROUP BY c.customer_id, c.customer_segment
)
SELECT
    COUNT(DISTINCT customer_id) AS customers_with_transactions,
    (SELECT COUNT(*) FROM bronze_customers) AS total_customers,
    CASE
        WHEN COUNT(DISTINCT customer_id) = (SELECT COUNT(*) FROM bronze_customers)
        THEN '✓ PASS: All customers have transactions'
        ELSE '✗ FAIL: Some customers missing transactions'
    END AS validation_status,
    MIN(txn_count) AS min_txns_per_customer,
    ROUND(AVG(txn_count), 2) AS avg_txns_per_customer,
    MAX(txn_count) AS max_txns_per_customer
FROM customer_txn_counts
WHERE txn_count > 0;

-- ============================================================================
-- 5. REFERENTIAL INTEGRITY VALIDATION
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'REFERENTIAL INTEGRITY VALIDATION'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

SELECT
    COUNT(*) AS orphan_transactions,
    CASE
        WHEN COUNT(*) = 0 THEN '✓ PASS: All transactions have valid customer_id'
        ELSE '✗ FAIL: Orphan transactions found (customer_id not in bronze_customers)'
    END AS validation_status
FROM bronze_transactions t
WHERE NOT EXISTS (
    SELECT 1 FROM bronze_customers c WHERE c.customer_id = t.customer_id
);

-- ============================================================================
-- 6. DATE RANGE VALIDATION
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'DATE RANGE VALIDATION'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

SELECT
    TO_CHAR(MIN(transaction_date), 'YYYY-MM-DD') AS earliest_date,
    TO_CHAR(MAX(transaction_date), 'YYYY-MM-DD') AS latest_date,
    DATEDIFF('month', MIN(transaction_date), MAX(transaction_date)) AS months_of_data,
    DATEDIFF('day', MIN(transaction_date), MAX(transaction_date)) AS days_of_data,
    CASE
        WHEN DATEDIFF('month', MIN(transaction_date), MAX(transaction_date)) BETWEEN 17 AND 19
        THEN '✓ PASS: Date range ~18 months as expected'
        ELSE '⚠ WARNING: Date range outside expected 17-19 months'
    END AS validation_status
FROM bronze_transactions;

-- Check for future dates
SELECT
    COUNT_IF(transaction_date > CURRENT_TIMESTAMP()) AS future_date_count,
    CASE
        WHEN COUNT_IF(transaction_date > CURRENT_TIMESTAMP()) = 0 THEN '✓ PASS: No future dates'
        ELSE '✗ FAIL: Future dates found'
    END AS validation_status
FROM bronze_transactions;

-- ============================================================================
-- 7. TRANSACTION AMOUNT VALIDATION
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'TRANSACTION AMOUNT VALIDATION'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

SELECT
    ROUND(MIN(transaction_amount), 2) AS min_amount,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY transaction_amount), 2) AS q1_amount,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY transaction_amount), 2) AS median_amount,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY transaction_amount), 2) AS q3_amount,
    ROUND(MAX(transaction_amount), 2) AS max_amount,
    ROUND(AVG(transaction_amount), 2) AS avg_amount,
    COUNT_IF(transaction_amount <= 0) AS invalid_amounts,
    COUNT_IF(transaction_amount > 10000) AS suspiciously_high,
    CASE
        WHEN COUNT_IF(transaction_amount <= 0) = 0 AND COUNT_IF(transaction_amount > 10000) = 0
        THEN '✓ PASS: All amounts positive and reasonable'
        ELSE '⚠ WARNING: Some amounts outside expected range'
    END AS validation_status
FROM bronze_transactions;

-- ============================================================================
-- 8. STATUS DISTRIBUTION VALIDATION
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'STATUS DISTRIBUTION VALIDATION'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

WITH status_dist AS (
    SELECT
        status,
        COUNT(*) AS txn_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
    FROM bronze_transactions
    GROUP BY status
)
SELECT
    status,
    txn_count,
    percentage,
    CASE
        WHEN status = 'approved' AND percentage BETWEEN 95 AND 99 THEN '✓ PASS'
        WHEN status = 'declined' AND percentage BETWEEN 1 AND 5 THEN '✓ PASS'
        ELSE '⚠ Check distribution'
    END AS validation_status
FROM status_dist
ORDER BY txn_count DESC;

-- ============================================================================
-- 9. CHANNEL DISTRIBUTION VALIDATION
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'CHANNEL DISTRIBUTION VALIDATION'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

SELECT
    channel,
    COUNT(*) AS txn_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    ROUND(AVG(transaction_amount), 2) AS avg_amount
FROM bronze_transactions
GROUP BY channel
ORDER BY txn_count DESC;

-- ============================================================================
-- 10. MERCHANT CATEGORY DISTRIBUTION VALIDATION
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'MERCHANT CATEGORY DISTRIBUTION VALIDATION'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

SELECT
    merchant_category,
    COUNT(*) AS txn_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    ROUND(SUM(transaction_amount), 2) AS total_amount,
    ROUND(AVG(transaction_amount), 2) AS avg_amount
FROM bronze_transactions
GROUP BY merchant_category
ORDER BY txn_count DESC;

-- ============================================================================
-- 11. SEGMENT-SPECIFIC PATTERN VALIDATION
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'SEGMENT-SPECIFIC PATTERN VALIDATION'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

SELECT
    c.customer_segment,
    COUNT(DISTINCT c.customer_id) AS customers,
    COUNT(t.transaction_id) AS transactions,
    ROUND(COUNT(t.transaction_id) * 1.0 / COUNT(DISTINCT c.customer_id), 2) AS avg_txns_per_customer,
    ROUND(MIN(t.transaction_amount), 2) AS min_amount,
    ROUND(AVG(t.transaction_amount), 2) AS avg_amount,
    ROUND(MAX(t.transaction_amount), 2) AS max_amount,
    ROUND(SUM(t.transaction_amount), 2) AS total_spend
FROM bronze_customers c
JOIN bronze_transactions t ON c.customer_id = t.customer_id
GROUP BY c.customer_segment
ORDER BY total_spend DESC;

-- Expected patterns:
-- High-Value Travelers: Highest avg_amount ($50-500), travel-heavy
-- Stable Mid-Spenders: Medium avg_amount ($30-150), consistent
-- Budget-Conscious: Lower avg_amount ($10-80), frequent small purchases
-- Declining: Declining trend over time
-- New & Growing: Growing trend over time

-- ============================================================================
-- 12. MONTHLY TRANSACTION TREND
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'MONTHLY TRANSACTION TREND'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

SELECT
    DATE_TRUNC('month', transaction_date) AS month,
    COUNT(*) AS txn_count,
    COUNT(DISTINCT customer_id) AS active_customers,
    ROUND(AVG(transaction_amount), 2) AS avg_amount,
    ROUND(SUM(transaction_amount), 2) AS total_amount
FROM bronze_transactions
GROUP BY DATE_TRUNC('month', transaction_date)
ORDER BY month;

-- ============================================================================
-- VALIDATION SUMMARY
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'VALIDATION SUMMARY'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

SELECT
    '✓ Validation complete - Review results above' AS status,
    (SELECT COUNT(*) FROM bronze_transactions) AS total_transactions,
    (SELECT COUNT(DISTINCT customer_id) FROM bronze_transactions) AS unique_customers,
    (SELECT TO_CHAR(MIN(transaction_date), 'YYYY-MM-DD') FROM bronze_transactions) AS earliest_date,
    (SELECT TO_CHAR(MAX(transaction_date), 'YYYY-MM-DD') FROM bronze_transactions) AS latest_date
;

SELECT 'Next: Run 04_delta_analysis.sql to compare before/after metrics' AS next_step;
