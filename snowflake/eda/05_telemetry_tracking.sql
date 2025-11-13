-- ============================================================================
-- Telemetry Tracking - Set Up Ongoing Monitoring
-- ============================================================================
-- Purpose: Create telemetry tables for tracking data generation performance,
--          data quality metrics, and warehouse usage
-- Run this AFTER transaction generation is complete
-- ============================================================================

USE ROLE DATA_ENGINEER;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA BRONZE;

-- ============================================================================
-- 1. CREATE GENERATION TELEMETRY TABLE
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'CREATING TELEMETRY TABLES'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

CREATE TABLE IF NOT EXISTS generation_telemetry (
    process_name STRING,
    execution_timestamp TIMESTAMP,
    total_transactions NUMBER,
    unique_customers NUMBER,
    min_date DATE,
    max_date DATE,
    months_of_data INT,
    total_transaction_volume NUMBER(15,2),
    avg_transaction_amount NUMBER(10,2),
    execution_duration_seconds NUMBER,
    warehouse_name STRING,
    warehouse_size STRING,
    credits_used NUMBER(10,4)
);

SELECT '✓ generation_telemetry table created' AS status;

-- ============================================================================
-- 2. CREATE DATA QUALITY TELEMETRY TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS data_quality_telemetry (
    check_timestamp TIMESTAMP,
    check_name STRING,
    check_category STRING,
    records_checked NUMBER,
    issues_found NUMBER,
    issue_percentage NUMBER(5,2),
    check_status STRING,
    check_details STRING
);

SELECT '✓ data_quality_telemetry table created' AS status;

-- ============================================================================
-- 3. CREATE SEGMENT TELEMETRY TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS segment_telemetry (
    snapshot_timestamp TIMESTAMP,
    customer_segment STRING,
    total_customers NUMBER,
    customers_with_transactions NUMBER,
    total_transactions NUMBER,
    avg_txns_per_customer NUMBER(10,2),
    avg_txn_amount NUMBER(10,2),
    total_spend NUMBER(15,2),
    spend_per_customer NUMBER(12,2)
);

SELECT '✓ segment_telemetry table created' AS status;

-- ============================================================================
-- 4. POPULATE GENERATION TELEMETRY
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'POPULATING TELEMETRY DATA'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

INSERT INTO generation_telemetry
SELECT
    'transaction_generation' AS process_name,
    CURRENT_TIMESTAMP() AS execution_timestamp,
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT customer_id) AS unique_customers,
    MIN(transaction_date::DATE) AS min_date,
    MAX(transaction_date::DATE) AS max_date,
    DATEDIFF('month', MIN(transaction_date), MAX(transaction_date)) AS months_of_data,
    SUM(transaction_amount) AS total_transaction_volume,
    AVG(transaction_amount) AS avg_transaction_amount,
    NULL AS execution_duration_seconds,
    'COMPUTE_WH' AS warehouse_name,
    NULL AS warehouse_size,
    NULL AS credits_used
FROM bronze_transactions;

SELECT '✓ Generation telemetry populated' AS status;

-- ============================================================================
-- 5. POPULATE DATA QUALITY TELEMETRY
-- ============================================================================

-- NULL checks
INSERT INTO data_quality_telemetry
SELECT
    CURRENT_TIMESTAMP() AS check_timestamp,
    'NULL_TRANSACTION_ID' AS check_name,
    'NULL_CHECKS' AS check_category,
    COUNT(*) AS records_checked,
    COUNT_IF(transaction_id IS NULL) AS issues_found,
    ROUND(COUNT_IF(transaction_id IS NULL) * 100.0 / COUNT(*), 2) AS issue_percentage,
    CASE WHEN COUNT_IF(transaction_id IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_status,
    'Transaction IDs should never be NULL' AS check_details
FROM bronze_transactions;

INSERT INTO data_quality_telemetry
SELECT
    CURRENT_TIMESTAMP() AS check_timestamp,
    'NULL_CUSTOMER_ID' AS check_name,
    'NULL_CHECKS' AS check_category,
    COUNT(*) AS records_checked,
    COUNT_IF(customer_id IS NULL) AS issues_found,
    ROUND(COUNT_IF(customer_id IS NULL) * 100.0 / COUNT(*), 2) AS issue_percentage,
    CASE WHEN COUNT_IF(customer_id IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_status,
    'Customer IDs should never be NULL' AS check_details
FROM bronze_transactions;

INSERT INTO data_quality_telemetry
SELECT
    CURRENT_TIMESTAMP() AS check_timestamp,
    'NULL_TRANSACTION_DATE' AS check_name,
    'NULL_CHECKS' AS check_category,
    COUNT(*) AS records_checked,
    COUNT_IF(transaction_date IS NULL) AS issues_found,
    ROUND(COUNT_IF(transaction_date IS NULL) * 100.0 / COUNT(*), 2) AS issue_percentage,
    CASE WHEN COUNT_IF(transaction_date IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_status,
    'Transaction dates should never be NULL' AS check_details
FROM bronze_transactions;

INSERT INTO data_quality_telemetry
SELECT
    CURRENT_TIMESTAMP() AS check_timestamp,
    'NULL_TRANSACTION_AMOUNT' AS check_name,
    'NULL_CHECKS' AS check_category,
    COUNT(*) AS records_checked,
    COUNT_IF(transaction_amount IS NULL) AS issues_found,
    ROUND(COUNT_IF(transaction_amount IS NULL) * 100.0 / COUNT(*), 2) AS issue_percentage,
    CASE WHEN COUNT_IF(transaction_amount IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_status,
    'Transaction amounts should never be NULL' AS check_details
FROM bronze_transactions;

-- Duplicate checks
INSERT INTO data_quality_telemetry
SELECT
    CURRENT_TIMESTAMP() AS check_timestamp,
    'DUPLICATE_TRANSACTION_IDS' AS check_name,
    'DUPLICATE_CHECKS' AS check_category,
    COUNT(*) AS records_checked,
    COUNT(*) - COUNT(DISTINCT transaction_id) AS issues_found,
    ROUND((COUNT(*) - COUNT(DISTINCT transaction_id)) * 100.0 / COUNT(*), 2) AS issue_percentage,
    CASE WHEN COUNT(*) = COUNT(DISTINCT transaction_id) THEN 'PASS' ELSE 'FAIL' END AS check_status,
    'All transaction IDs should be unique' AS check_details
FROM bronze_transactions;

-- Value range checks
INSERT INTO data_quality_telemetry
SELECT
    CURRENT_TIMESTAMP() AS check_timestamp,
    'INVALID_AMOUNTS' AS check_name,
    'VALUE_RANGE_CHECKS' AS check_category,
    COUNT(*) AS records_checked,
    COUNT_IF(transaction_amount <= 0) AS issues_found,
    ROUND(COUNT_IF(transaction_amount <= 0) * 100.0 / COUNT(*), 2) AS issue_percentage,
    CASE WHEN COUNT_IF(transaction_amount <= 0) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_status,
    'Transaction amounts should be positive' AS check_details
FROM bronze_transactions;

INSERT INTO data_quality_telemetry
SELECT
    CURRENT_TIMESTAMP() AS check_timestamp,
    'FUTURE_DATES' AS check_name,
    'VALUE_RANGE_CHECKS' AS check_category,
    COUNT(*) AS records_checked,
    COUNT_IF(transaction_date > CURRENT_TIMESTAMP()) AS issues_found,
    ROUND(COUNT_IF(transaction_date > CURRENT_TIMESTAMP()) * 100.0 / COUNT(*), 2) AS issue_percentage,
    CASE WHEN COUNT_IF(transaction_date > CURRENT_TIMESTAMP()) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_status,
    'Transaction dates should not be in the future' AS check_details
FROM bronze_transactions;

INSERT INTO data_quality_telemetry
SELECT
    CURRENT_TIMESTAMP() AS check_timestamp,
    'SUSPICIOUSLY_HIGH_AMOUNTS' AS check_name,
    'VALUE_RANGE_CHECKS' AS check_category,
    COUNT(*) AS records_checked,
    COUNT_IF(transaction_amount > 10000) AS issues_found,
    ROUND(COUNT_IF(transaction_amount > 10000) * 100.0 / COUNT(*), 2) AS issue_percentage,
    CASE WHEN COUNT_IF(transaction_amount > 10000) = 0 THEN 'PASS' ELSE 'WARNING' END AS check_status,
    'Very few transactions should exceed $10,000' AS check_details
FROM bronze_transactions;

-- Referential integrity check
INSERT INTO data_quality_telemetry
SELECT
    CURRENT_TIMESTAMP() AS check_timestamp,
    'ORPHAN_TRANSACTIONS' AS check_name,
    'REFERENTIAL_INTEGRITY' AS check_category,
    (SELECT COUNT(*) FROM bronze_transactions) AS records_checked,
    COUNT(*) AS issues_found,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM bronze_transactions), 2) AS issue_percentage,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_status,
    'All transactions should link to existing customers' AS check_details
FROM bronze_transactions t
WHERE NOT EXISTS (
    SELECT 1 FROM bronze_customers c WHERE c.customer_id = t.customer_id
);

SELECT '✓ Data quality telemetry populated' AS status;

-- ============================================================================
-- 6. POPULATE SEGMENT TELEMETRY
-- ============================================================================

INSERT INTO segment_telemetry
SELECT
    CURRENT_TIMESTAMP() AS snapshot_timestamp,
    c.customer_segment,
    COUNT(DISTINCT c.customer_id) AS total_customers,
    COUNT(DISTINCT CASE WHEN t.transaction_id IS NOT NULL THEN c.customer_id END) AS customers_with_transactions,
    COUNT(t.transaction_id) AS total_transactions,
    ROUND(COUNT(t.transaction_id) * 1.0 / NULLIF(COUNT(DISTINCT c.customer_id), 0), 2) AS avg_txns_per_customer,
    ROUND(AVG(t.transaction_amount), 2) AS avg_txn_amount,
    ROUND(SUM(t.transaction_amount), 2) AS total_spend,
    ROUND(SUM(t.transaction_amount) / NULLIF(COUNT(DISTINCT c.customer_id), 0), 2) AS spend_per_customer
FROM bronze_customers c
LEFT JOIN bronze_transactions t ON c.customer_id = t.customer_id
GROUP BY c.customer_segment;

SELECT '✓ Segment telemetry populated' AS status;

-- ============================================================================
-- 7. VIEW TELEMETRY SUMMARIES
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'GENERATION TELEMETRY SUMMARY'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

SELECT * FROM generation_telemetry ORDER BY execution_timestamp DESC LIMIT 5;

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'DATA QUALITY SUMMARY'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

SELECT
    check_category,
    check_name,
    records_checked,
    issues_found,
    issue_percentage,
    check_status
FROM data_quality_telemetry
WHERE check_timestamp = (SELECT MAX(check_timestamp) FROM data_quality_telemetry)
ORDER BY check_category, check_name;

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'SEGMENT TELEMETRY SUMMARY'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

SELECT
    customer_segment,
    total_customers,
    customers_with_transactions,
    total_transactions,
    avg_txns_per_customer,
    avg_txn_amount,
    total_spend,
    spend_per_customer
FROM segment_telemetry
WHERE snapshot_timestamp = (SELECT MAX(snapshot_timestamp) FROM segment_telemetry)
ORDER BY total_spend DESC;

-- ============================================================================
-- 8. CREATE TELEMETRY MONITORING VIEW
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'CREATING MONITORING VIEWS'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

CREATE OR REPLACE VIEW v_data_quality_dashboard AS
SELECT
    check_timestamp,
    check_category,
    COUNT(*) AS total_checks,
    SUM(CASE WHEN check_status = 'PASS' THEN 1 ELSE 0 END) AS passed_checks,
    SUM(CASE WHEN check_status = 'FAIL' THEN 1 ELSE 0 END) AS failed_checks,
    SUM(CASE WHEN check_status = 'WARNING' THEN 1 ELSE 0 END) AS warning_checks,
    ROUND(SUM(CASE WHEN check_status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pass_rate
FROM data_quality_telemetry
GROUP BY check_timestamp, check_category
ORDER BY check_timestamp DESC, check_category;

SELECT '✓ v_data_quality_dashboard view created' AS status;

CREATE OR REPLACE VIEW v_segment_performance AS
SELECT
    s.snapshot_timestamp,
    s.customer_segment,
    s.total_customers,
    s.total_transactions,
    s.avg_txns_per_customer,
    s.avg_txn_amount,
    s.total_spend,
    s.spend_per_customer,
    ROUND(s.total_spend * 100.0 / SUM(s.total_spend) OVER (PARTITION BY s.snapshot_timestamp), 2) AS pct_of_total_spend
FROM segment_telemetry s
ORDER BY s.snapshot_timestamp DESC, s.total_spend DESC;

SELECT '✓ v_segment_performance view created' AS status;

-- ============================================================================
-- 9. WAREHOUSE PERFORMANCE METRICS (from ACCOUNT_USAGE)
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'WAREHOUSE PERFORMANCE METRICS'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

-- Recent queries related to transaction generation
SELECT
    query_id,
    TO_CHAR(start_time, 'YYYY-MM-DD HH24:MI:SS') AS start_time,
    warehouse_name,
    warehouse_size,
    execution_status,
    ROUND(execution_time / 1000, 2) AS execution_seconds,
    rows_produced,
    ROUND(bytes_scanned / 1024 / 1024 / 1024, 2) AS gb_scanned,
    ROUND(credits_used_cloud_services, 4) AS credits_used,
    LEFT(query_text, 80) AS query_preview
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_text ILIKE '%bronze_transactions%'
  AND start_time >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
  AND execution_status = 'SUCCESS'
ORDER BY start_time DESC
LIMIT 10;

-- ============================================================================
-- 10. SCHEDULED MONITORING QUERIES
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'RECOMMENDED MONITORING QUERIES'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

-- Display recommended monitoring queries as documentation
SELECT
    '1. Daily Data Quality Check' AS query_name,
    'SELECT * FROM v_data_quality_dashboard ORDER BY check_timestamp DESC LIMIT 1;' AS query_text
UNION ALL
SELECT
    '2. Segment Performance Trend',
    'SELECT * FROM v_segment_performance WHERE snapshot_timestamp >= DATEADD(day, -7, CURRENT_TIMESTAMP());'
UNION ALL
SELECT
    '3. Failed Quality Checks',
    'SELECT * FROM data_quality_telemetry WHERE check_status = ''FAIL'' ORDER BY check_timestamp DESC;'
UNION ALL
SELECT
    '4. Transaction Volume Trend',
    'SELECT DATE(transaction_date), COUNT(*) FROM bronze_transactions GROUP BY 1 ORDER BY 1;'
UNION ALL
SELECT
    '5. Customer Activity Rate',
    'SELECT COUNT(DISTINCT customer_id) * 100.0 / (SELECT COUNT(*) FROM bronze_customers) AS active_pct FROM bronze_transactions WHERE transaction_date >= DATEADD(month, -1, CURRENT_DATE());'
;

-- ============================================================================
-- FINAL SUMMARY
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'TELEMETRY SETUP COMPLETE'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

SELECT
    '✓ Telemetry tracking setup complete' AS status,
    (SELECT COUNT(*) FROM generation_telemetry) AS generation_records,
    (SELECT COUNT(*) FROM data_quality_telemetry) AS quality_check_records,
    (SELECT COUNT(*) FROM segment_telemetry) AS segment_snapshot_records
;

SELECT
    'Tables Created' AS category,
    'generation_telemetry, data_quality_telemetry, segment_telemetry' AS details
UNION ALL
SELECT
    'Views Created',
    'v_data_quality_dashboard, v_segment_performance'
UNION ALL
SELECT
    'Next Steps',
    'Use monitoring views to track data quality and segment performance over time'
;
