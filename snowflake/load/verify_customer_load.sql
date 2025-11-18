-- ============================================================================
-- Verify Customer Bulk Load Data Quality
-- ============================================================================
-- Purpose: Validate data quality after loading customers into Bronze layer
-- Run after: load_customers_bulk.sql
-- ============================================================================

USE ROLE DATA_ENGINEER;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA BRONZE;

-- ============================================================================
-- Validation 1: Row Count
-- ============================================================================

SELECT 'Row Count Validation' AS validation_name,
       COUNT(*) AS actual_count,
       50000 AS expected_count,
       CASE
           WHEN COUNT(*) = 50000 THEN '✓ PASS'
           ELSE '✗ FAIL'
       END AS status
FROM RAW_CUSTOMERS;

-- ============================================================================
-- Validation 2: Null Customer IDs
-- ============================================================================

SELECT 'Null Customer IDs' AS validation_name,
       COUNT_IF(customer_id IS NULL) AS null_count,
       0 AS expected_null_count,
       CASE
           WHEN COUNT_IF(customer_id IS NULL) = 0 THEN '✓ PASS'
           ELSE '✗ FAIL'
       END AS status
FROM RAW_CUSTOMERS;

-- ============================================================================
-- Validation 3: Duplicate Customer IDs
-- ============================================================================

WITH duplicates AS (
    SELECT customer_id,
           COUNT(*) AS occurrence_count
    FROM RAW_CUSTOMERS
    GROUP BY customer_id
    HAVING COUNT(*) > 1
)
SELECT 'Duplicate Customer IDs' AS validation_name,
       COUNT(*) AS duplicate_count,
       0 AS expected_duplicate_count,
       CASE
           WHEN COUNT(*) = 0 THEN '✓ PASS'
           ELSE '✗ FAIL'
       END AS status
FROM duplicates;

-- ============================================================================
-- Validation 4: Segment Distribution
-- ============================================================================

WITH segment_stats AS (
    SELECT customer_segment,
           COUNT(*) AS row_count,
           ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
    FROM RAW_CUSTOMERS
    GROUP BY customer_segment
)
SELECT 'Segment Distribution' AS validation_name,
       customer_segment,
       row_count,
       percentage,
       CASE customer_segment
           WHEN 'High-Value Travelers' THEN
               CASE WHEN percentage BETWEEN 10 AND 20 THEN '✓ PASS' ELSE '✗ FAIL' END
           WHEN 'Stable Mid-Spenders' THEN
               CASE WHEN percentage BETWEEN 35 AND 45 THEN '✓ PASS' ELSE '✗ FAIL' END
           WHEN 'Budget-Conscious' THEN
               CASE WHEN percentage BETWEEN 20 AND 30 THEN '✓ PASS' ELSE '✗ FAIL' END
           WHEN 'Declining' THEN
               CASE WHEN percentage BETWEEN 5 AND 15 THEN '✓ PASS' ELSE '✗ FAIL' END
           WHEN 'New & Growing' THEN
               CASE WHEN percentage BETWEEN 5 AND 15 THEN '✓ PASS' ELSE '✗ FAIL' END
           ELSE '? UNKNOWN'
       END AS status
FROM segment_stats
ORDER BY row_count DESC;

-- ============================================================================
-- Validation 5: Date Range
-- ============================================================================

SELECT 'Account Open Date Range' AS validation_name,
       MIN(account_open_date) AS earliest_date,
       MAX(account_open_date) AS latest_date,
       DATEDIFF(day, MIN(account_open_date), MAX(account_open_date)) AS date_range_days,
       CASE
           WHEN MIN(account_open_date) >= DATEADD(year, -6, CURRENT_DATE())
                AND MAX(account_open_date) <= CURRENT_DATE()
           THEN '✓ PASS'
           ELSE '✗ FAIL'
       END AS status
FROM RAW_CUSTOMERS;

-- ============================================================================
-- Validation 6: Email Format
-- ============================================================================

SELECT 'Email Format' AS validation_name,
       COUNT_IF(email LIKE '%@%' AND email LIKE '%.%') AS valid_email_count,
       COUNT(*) AS total_count,
       ROUND(COUNT_IF(email LIKE '%@%' AND email LIKE '%.%') * 100.0 / COUNT(*), 2) AS valid_percentage,
       CASE
           WHEN COUNT_IF(email LIKE '%@%' AND email LIKE '%.%') = COUNT(*) THEN '✓ PASS'
           ELSE '✗ FAIL'
       END AS status
FROM RAW_CUSTOMERS;

-- ============================================================================
-- Validation 7: Credit Limit Range
-- ============================================================================

SELECT 'Credit Limit Range' AS validation_name,
       MIN(credit_limit) AS min_credit_limit,
       MAX(credit_limit) AS max_credit_limit,
       ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
       CASE
           WHEN MIN(credit_limit) >= 5000
                AND MAX(credit_limit) <= 50000
                AND MIN(credit_limit) % 1000 = 0
                AND MAX(credit_limit) % 1000 = 0
           THEN '✓ PASS'
           ELSE '✗ FAIL'
       END AS status
FROM RAW_CUSTOMERS;

-- ============================================================================
-- Validation 8: Age Range
-- ============================================================================

SELECT 'Age Range' AS validation_name,
       MIN(age) AS min_age,
       MAX(age) AS max_age,
       ROUND(AVG(age), 1) AS avg_age,
       CASE
           WHEN MIN(age) >= 22 AND MAX(age) <= 75 THEN '✓ PASS'
           ELSE '✗ FAIL'
       END AS status
FROM RAW_CUSTOMERS;

-- ============================================================================
-- Validation 9: Metadata Fields
-- ============================================================================

SELECT 'Metadata Fields Populated' AS validation_name,
       COUNT_IF(ingestion_timestamp IS NOT NULL) AS ingestion_timestamp_count,
       COUNT_IF(source_file IS NOT NULL) AS source_file_count,
       COUNT_IF(source_file LIKE '%customers.csv%') AS source_file_match_count,
       COUNT(*) AS total_count,
       CASE
           WHEN COUNT_IF(ingestion_timestamp IS NOT NULL) = COUNT(*)
                AND COUNT_IF(source_file LIKE '%customers.csv%') = COUNT(*)
           THEN '✓ PASS'
           ELSE '✗ FAIL'
       END AS status
FROM RAW_CUSTOMERS;

-- ============================================================================
-- Validation 10: Decline Type Logic
-- ============================================================================

WITH decline_validation AS (
    SELECT
        customer_segment,
        decline_type,
        COUNT(*) AS count
    FROM RAW_CUSTOMERS
    GROUP BY customer_segment, decline_type
)
SELECT 'Decline Type Logic' AS validation_name,
       customer_segment,
       decline_type,
       count,
       CASE
           WHEN customer_segment = 'Declining' AND decline_type IN ('gradual', 'sudden') THEN '✓ PASS'
           WHEN customer_segment != 'Declining' AND decline_type IS NULL THEN '✓ PASS'
           ELSE '✗ FAIL'
       END AS status
FROM decline_validation
ORDER BY customer_segment, decline_type;

-- ============================================================================
-- Overall Validation Summary
-- ============================================================================

SELECT
    '========== VALIDATION SUMMARY ==========' AS summary,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT_IF(customer_id IS NULL) AS null_ids,
    COUNT(*) - COUNT(DISTINCT customer_id) AS duplicate_ids,
    MIN(ingestion_timestamp) AS first_ingestion,
    MAX(ingestion_timestamp) AS last_ingestion,
    COUNT(DISTINCT source_file) AS source_files,
    CASE
        WHEN COUNT(*) = 50000
             AND COUNT(DISTINCT customer_id) = 50000
             AND COUNT_IF(customer_id IS NULL) = 0
        THEN '✓ ALL VALIDATIONS PASSED'
        ELSE '✗ SOME VALIDATIONS FAILED - REVIEW ABOVE'
    END AS overall_status
FROM RAW_CUSTOMERS;

-- ============================================================================
-- Sample Data Preview
-- ============================================================================

SELECT 'Sample Data (First 5 Rows)' AS preview;

SELECT *
FROM RAW_CUSTOMERS
ORDER BY customer_id
LIMIT 5;

-- ============================================================================
-- Completion Message
-- ============================================================================

SELECT '✓ Validation queries completed' AS status;
SELECT 'Review results above for any FAIL statuses' AS action;
