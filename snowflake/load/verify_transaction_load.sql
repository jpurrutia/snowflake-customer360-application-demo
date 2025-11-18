-- ============================================================================
-- Verify Transaction Bulk Load
-- ============================================================================
-- Purpose: Comprehensive validation of BRONZE.RAW_TRANSACTIONS data
-- Run After: snowflake/load/load_transactions_bulk.sql
-- Expected Rows: ~13.5M (10M - 17M acceptable range)
-- ============================================================================

USE ROLE DATA_ENGINEER;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA BRONZE;

-- ============================================================================
-- Validation Overview
-- ============================================================================

SELECT '========================================' AS check;
SELECT 'Transaction Load Validation Report' AS check;
SELECT 'Generated: ' || CURRENT_TIMESTAMP()::STRING AS check;
SELECT '========================================' AS check;

-- ============================================================================
-- Check 1: Row Count
-- ============================================================================

SELECT '1. Row Count Validation' AS check_name;

WITH row_count AS (
    SELECT COUNT(*) AS actual_count
    FROM BRONZE.RAW_TRANSACTIONS
)
SELECT
    'Row Count' AS validation,
    actual_count,
    CASE
        WHEN actual_count BETWEEN 10000000 AND 17000000 THEN '✓ PASS'
        WHEN actual_count BETWEEN 5000000 AND 20000000 THEN '⚠️  WARNING - Outside target range but acceptable'
        ELSE '✗ FAIL - Row count significantly off target'
    END AS status,
    '10M - 17M expected (target: 13.5M)' AS expected_range
FROM row_count;

-- ============================================================================
-- Check 2: Unique Transaction IDs
-- ============================================================================

SELECT '2. Transaction ID Uniqueness' AS check_name;

WITH txn_id_check AS (
    SELECT
        COUNT(*) AS total_count,
        COUNT(DISTINCT transaction_id) AS unique_count
    FROM BRONZE.RAW_TRANSACTIONS
)
SELECT
    'Unique transaction_ids' AS validation,
    total_count AS total_rows,
    unique_count AS unique_txn_ids,
    total_count - unique_count AS duplicates,
    CASE
        WHEN total_count = unique_count THEN '✓ PASS'
        ELSE '✗ FAIL - Duplicate transaction IDs found'
    END AS status
FROM txn_id_check;

-- ============================================================================
-- Check 3: Null Values in Critical Fields
-- ============================================================================

SELECT '3. Null Value Checks' AS check_name;

SELECT
    'Null transaction_ids' AS validation,
    COUNT(*) AS null_count,
    CASE
        WHEN COUNT(*) = 0 THEN '✓ PASS'
        ELSE '✗ FAIL - NULL transaction IDs found'
    END AS status
FROM BRONZE.RAW_TRANSACTIONS
WHERE transaction_id IS NULL;

SELECT
    'Null customer_ids' AS validation,
    COUNT(*) AS null_count,
    CASE
        WHEN COUNT(*) = 0 THEN '✓ PASS'
        ELSE '✗ FAIL - NULL customer IDs found'
    END AS status
FROM BRONZE.RAW_TRANSACTIONS
WHERE customer_id IS NULL;

SELECT
    'Null transaction_dates' AS validation,
    COUNT(*) AS null_count,
    CASE
        WHEN COUNT(*) = 0 THEN '✓ PASS'
        ELSE '✗ FAIL - NULL transaction dates found'
    END AS status
FROM BRONZE.RAW_TRANSACTIONS
WHERE transaction_date IS NULL;

SELECT
    'Null transaction_amounts' AS validation,
    COUNT(*) AS null_count,
    CASE
        WHEN COUNT(*) = 0 THEN '✓ PASS'
        ELSE '✗ FAIL - NULL transaction amounts found'
    END AS status
FROM BRONZE.RAW_TRANSACTIONS
WHERE transaction_amount IS NULL;

-- ============================================================================
-- Check 4: All Customers Represented
-- ============================================================================

SELECT '4. Customer Representation' AS check_name;

WITH customer_check AS (
    SELECT
        COUNT(DISTINCT customer_id) AS distinct_customers
    FROM BRONZE.RAW_TRANSACTIONS
)
SELECT
    'Unique customers in transactions' AS validation,
    distinct_customers,
    CASE
        WHEN distinct_customers = 50000 THEN '✓ PASS'
        WHEN distinct_customers >= 49000 THEN '⚠️  WARNING - Some customers missing transactions'
        ELSE '✗ FAIL - Significant customers missing'
    END AS status,
    '50,000 expected' AS expected
FROM customer_check;

-- ============================================================================
-- Check 5: Customers Without Transactions
-- ============================================================================

SELECT '5. Customers Without Transactions' AS check_name;

WITH missing_customers AS (
    SELECT
        c.customer_id
    FROM BRONZE.RAW_CUSTOMERS c
    WHERE NOT EXISTS (
        SELECT 1
        FROM BRONZE.RAW_TRANSACTIONS t
        WHERE t.customer_id = c.customer_id
    )
)
SELECT
    'Customers without transactions' AS validation,
    COUNT(*) AS missing_count,
    CASE
        WHEN COUNT(*) = 0 THEN '✓ PASS'
        WHEN COUNT(*) < 100 THEN '⚠️  WARNING - Few customers missing'
        ELSE '✗ FAIL - Many customers have no transactions'
    END AS status
FROM missing_customers;

-- Show sample of missing customers (if any)
SELECT
    'Sample missing customers' AS info,
    customer_id,
    customer_segment
FROM BRONZE.RAW_CUSTOMERS c
WHERE NOT EXISTS (
    SELECT 1
    FROM BRONZE.RAW_TRANSACTIONS t
    WHERE t.customer_id = c.customer_id
)
LIMIT 10;

-- ============================================================================
-- Check 6: Referential Integrity (Customer IDs exist)
-- ============================================================================

SELECT '6. Referential Integrity Check' AS check_name;

WITH orphaned_txns AS (
    SELECT
        t.customer_id
    FROM BRONZE.RAW_TRANSACTIONS t
    WHERE NOT EXISTS (
        SELECT 1
        FROM BRONZE.RAW_CUSTOMERS c
        WHERE c.customer_id = t.customer_id
    )
)
SELECT
    'Transactions with invalid customer_id' AS validation,
    COUNT(DISTINCT customer_id) AS orphaned_customers,
    CASE
        WHEN COUNT(*) = 0 THEN '✓ PASS'
        ELSE '✗ FAIL - Transactions reference non-existent customers'
    END AS status
FROM orphaned_txns;

-- ============================================================================
-- Check 7: Date Range Validation
-- ============================================================================

SELECT '7. Date Range Validation' AS check_name;

WITH date_range AS (
    SELECT
        MIN(transaction_date) AS earliest_date,
        MAX(transaction_date) AS latest_date,
        DATEDIFF('month', MIN(transaction_date), MAX(transaction_date)) AS months_span,
        DATEDIFF('day', MIN(transaction_date), MAX(transaction_date)) AS days_span
    FROM BRONZE.RAW_TRANSACTIONS
)
SELECT
    'Date range' AS validation,
    earliest_date,
    latest_date,
    months_span || ' months' AS span,
    days_span || ' days' AS days,
    CASE
        WHEN months_span BETWEEN 17 AND 19 THEN '✓ PASS'
        WHEN months_span BETWEEN 15 AND 21 THEN '⚠️  WARNING - Date range slightly off'
        ELSE '✗ FAIL - Date range incorrect'
    END AS status,
    '17-19 months expected (~18 months)' AS expected
FROM date_range;

-- Check for future dates
SELECT
    'Future transaction dates' AS validation,
    COUNT(*) AS future_count,
    CASE
        WHEN COUNT(*) = 0 THEN '✓ PASS'
        ELSE '✗ FAIL - Found transactions in the future'
    END AS status
FROM BRONZE.RAW_TRANSACTIONS
WHERE transaction_date > CURRENT_TIMESTAMP();

-- ============================================================================
-- Check 8: Transaction Amount Validation
-- ============================================================================

SELECT '8. Transaction Amount Validation' AS check_name;

WITH amount_stats AS (
    SELECT
        MIN(transaction_amount) AS min_amount,
        MAX(transaction_amount) AS max_amount,
        AVG(transaction_amount) AS avg_amount,
        COUNT_IF(transaction_amount <= 0) AS zero_or_negative,
        COUNT_IF(transaction_amount > 10000) AS extremely_high
    FROM BRONZE.RAW_TRANSACTIONS
)
SELECT
    'Transaction amounts' AS validation,
    min_amount,
    max_amount,
    ROUND(avg_amount, 2) AS avg_amount,
    zero_or_negative,
    extremely_high,
    CASE
        WHEN zero_or_negative = 0 AND max_amount <= 10000 THEN '✓ PASS'
        WHEN zero_or_negative = 0 AND max_amount <= 15000 THEN '⚠️  WARNING - Some high amounts'
        ELSE '✗ FAIL - Invalid amounts detected'
    END AS status,
    'All amounts > 0 and < $10,000 expected' AS expected
FROM amount_stats;

-- ============================================================================
-- Check 9: Metadata Fields Populated
-- ============================================================================

SELECT '9. Metadata Fields Validation' AS check_name;

SELECT
    'Null ingestion_timestamps' AS validation,
    COUNT(*) AS null_count,
    CASE
        WHEN COUNT(*) = 0 THEN '✓ PASS'
        ELSE '✗ FAIL - Missing ingestion timestamps'
    END AS status
FROM BRONZE.RAW_TRANSACTIONS
WHERE ingestion_timestamp IS NULL;

SELECT
    'Null source_files' AS validation,
    COUNT(*) AS null_count,
    CASE
        WHEN COUNT(*) = 0 THEN '✓ PASS'
        ELSE '✗ FAIL - Missing source file metadata'
    END AS status
FROM BRONZE.RAW_TRANSACTIONS
WHERE source_file IS NULL;

SELECT
    'Null file_row_numbers' AS validation,
    COUNT(*) AS null_count,
    CASE
        WHEN COUNT(*) = 0 THEN '✓ PASS'
        ELSE '✗ FAIL - Missing file row number metadata'
    END AS status
FROM BRONZE.RAW_TRANSACTIONS
WHERE _metadata_file_row_number IS NULL;

-- Check source file naming
SELECT
    'Source file naming' AS validation,
    COUNT(DISTINCT source_file) AS distinct_files,
    CASE
        WHEN MIN(source_file) LIKE '%transactions_historical%' THEN '✓ PASS'
        ELSE '⚠️  WARNING - Unexpected source file names'
    END AS status
FROM BRONZE.RAW_TRANSACTIONS;

-- ============================================================================
-- Check 10: Status Distribution
-- ============================================================================

SELECT '10. Status Distribution Validation' AS check_name;

WITH status_dist AS (
    SELECT
        status,
        COUNT(*) AS txn_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
    FROM BRONZE.RAW_TRANSACTIONS
    GROUP BY status
)
SELECT
    'Status distribution' AS validation,
    status,
    txn_count,
    percentage || '%' AS pct,
    CASE
        WHEN status = 'approved' AND percentage BETWEEN 95 AND 99 THEN '✓ PASS'
        WHEN status = 'declined' AND percentage BETWEEN 1 AND 5 THEN '✓ PASS'
        ELSE '⚠️  WARNING - Distribution unexpected'
    END AS status_check,
    'Expected: ~97% approved, ~3% declined' AS expected
FROM status_dist
ORDER BY txn_count DESC;

-- ============================================================================
-- Check 11: Channel Distribution
-- ============================================================================

SELECT '11. Channel Distribution' AS check_name;

SELECT
    'Channel distribution' AS validation,
    channel,
    COUNT(*) AS txn_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM BRONZE.RAW_TRANSACTIONS
GROUP BY channel
ORDER BY txn_count DESC;

-- ============================================================================
-- Check 12: Merchant Category Distribution
-- ============================================================================

SELECT '12. Merchant Category Distribution' AS check_name;

SELECT
    'Merchant categories' AS validation,
    merchant_category,
    COUNT(*) AS txn_count,
    ROUND(AVG(transaction_amount), 2) AS avg_amount
FROM BRONZE.RAW_TRANSACTIONS
GROUP BY merchant_category
ORDER BY txn_count DESC;

-- ============================================================================
-- Summary Statistics
-- ============================================================================

SELECT '========================================' AS summary;
SELECT 'Summary Statistics' AS summary;
SELECT '========================================' AS summary;

-- Overall summary
SELECT
    'Overall Statistics' AS metric_group,
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT customer_id) AS unique_customers,
    MIN(transaction_date) AS earliest_date,
    MAX(transaction_date) AS latest_date,
    ROUND(AVG(transaction_amount), 2) AS avg_amount,
    ROUND(SUM(transaction_amount), 2) AS total_volume
FROM BRONZE.RAW_TRANSACTIONS;

-- Monthly volume
SELECT
    'Monthly Transaction Volume' AS metric_group,
    DATE_TRUNC('month', transaction_date) AS month,
    COUNT(*) AS txn_count,
    ROUND(AVG(transaction_amount), 2) AS avg_amount,
    ROUND(SUM(transaction_amount), 2) AS total_amount
FROM BRONZE.RAW_TRANSACTIONS
GROUP BY DATE_TRUNC('month', transaction_date)
ORDER BY month;

-- Customer transaction distribution
SELECT
    'Customer Transaction Distribution' AS metric_group,
    MIN(txn_count) AS min_txns_per_customer,
    MAX(txn_count) AS max_txns_per_customer,
    ROUND(AVG(txn_count), 2) AS avg_txns_per_customer,
    ROUND(STDDEV(txn_count), 2) AS stddev_txns
FROM (
    SELECT
        customer_id,
        COUNT(*) AS txn_count
    FROM BRONZE.RAW_TRANSACTIONS
    GROUP BY customer_id
);

-- ============================================================================
-- Sample Data Preview
-- ============================================================================

SELECT '========================================' AS preview;
SELECT 'Sample Data Preview' AS preview;
SELECT '========================================' AS preview;

SELECT
    'First 10 transactions' AS info,
    transaction_id,
    customer_id,
    transaction_date,
    transaction_amount,
    merchant_category,
    channel,
    status
FROM BRONZE.RAW_TRANSACTIONS
ORDER BY transaction_date, transaction_id
LIMIT 10;

-- ============================================================================
-- Data Quality Issues Summary
-- ============================================================================

SELECT '========================================' AS issues;
SELECT 'Data Quality Issues (if any)' AS issues;
SELECT '========================================' AS issues;

-- Compile all issues
WITH all_checks AS (
    SELECT 'Duplicate transaction IDs' AS issue_type,
           COUNT(*) - COUNT(DISTINCT transaction_id) AS issue_count
    FROM BRONZE.RAW_TRANSACTIONS
    HAVING COUNT(*) - COUNT(DISTINCT transaction_id) > 0

    UNION ALL

    SELECT 'NULL transaction IDs' AS issue_type,
           COUNT(*) AS issue_count
    FROM BRONZE.RAW_TRANSACTIONS
    WHERE transaction_id IS NULL
    HAVING COUNT(*) > 0

    UNION ALL

    SELECT 'NULL customer IDs' AS issue_type,
           COUNT(*) AS issue_count
    FROM BRONZE.RAW_TRANSACTIONS
    WHERE customer_id IS NULL
    HAVING COUNT(*) > 0

    UNION ALL

    SELECT 'Invalid customer IDs' AS issue_type,
           COUNT(DISTINCT t.customer_id) AS issue_count
    FROM BRONZE.RAW_TRANSACTIONS t
    WHERE NOT EXISTS (
        SELECT 1 FROM BRONZE.RAW_CUSTOMERS c
        WHERE c.customer_id = t.customer_id
    )
    HAVING COUNT(*) > 0

    UNION ALL

    SELECT 'Future transaction dates' AS issue_type,
           COUNT(*) AS issue_count
    FROM BRONZE.RAW_TRANSACTIONS
    WHERE transaction_date > CURRENT_TIMESTAMP()
    HAVING COUNT(*) > 0

    UNION ALL

    SELECT 'Zero or negative amounts' AS issue_type,
           COUNT(*) AS issue_count
    FROM BRONZE.RAW_TRANSACTIONS
    WHERE transaction_amount <= 0
    HAVING COUNT(*) > 0
)
SELECT
    issue_type,
    issue_count,
    '✗ REQUIRES ATTENTION' AS status
FROM all_checks
ORDER BY issue_count DESC;

-- If no issues
SELECT
    CASE
        WHEN NOT EXISTS (SELECT 1 FROM all_checks) THEN '✓ NO DATA QUALITY ISSUES FOUND'
        ELSE 'Issues detected - see above'
    END AS overall_status
FROM (SELECT 1) dummy;

-- ============================================================================
-- Validation Summary
-- ============================================================================

SELECT '========================================' AS summary;
SELECT '✅ Validation Complete' AS summary;
SELECT '========================================' AS summary;

SELECT
    'Next Steps' AS info,
    'Review validation results above' AS step_1,
    'Check for any FAIL or WARNING statuses' AS step_2,
    'Investigate any data quality issues' AS step_3,
    'If all validations pass, proceed to Phase 3 (dbt transformations)' AS step_4;
