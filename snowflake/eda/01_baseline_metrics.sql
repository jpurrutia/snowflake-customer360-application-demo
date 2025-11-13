-- ============================================================================
-- Baseline Metrics Capture - Run BEFORE Transaction Generation
-- ============================================================================
-- Purpose: Capture current state of data to compare before/after generation
-- Run this script before executing generate_transactions.sql
-- ============================================================================

USE ROLE DATA_ENGINEER;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA BRONZE;

-- ============================================================================
-- 1. Create Baseline Metrics Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS metrics_baseline (
    snapshot_timestamp TIMESTAMP,
    snapshot_type STRING,
    customer_count INT,
    transaction_count INT,
    segment_count INT,
    total_transaction_amount NUMBER(15,2),
    avg_transaction_amount NUMBER(10,2),
    min_transaction_date DATE,
    max_transaction_date DATE,
    months_of_data INT
);

-- ============================================================================
-- 2. Capture Current State
-- ============================================================================

INSERT INTO metrics_baseline
SELECT
    CURRENT_TIMESTAMP() AS snapshot_timestamp,
    'BEFORE_TRANSACTION_GENERATION' AS snapshot_type,
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

-- ============================================================================
-- 3. Display Baseline Snapshot
-- ============================================================================

SELECT
    '=' || REPEAT('=', 78) || '=' AS separator
UNION ALL
SELECT 'BASELINE METRICS CAPTURED - ' || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS')
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

SELECT * FROM metrics_baseline WHERE snapshot_type = 'BEFORE_TRANSACTION_GENERATION' ORDER BY snapshot_timestamp DESC LIMIT 1;

-- ============================================================================
-- Display confirmation
-- ============================================================================

SELECT '✓ Baseline metrics captured successfully' AS status;
SELECT 'Next: Run 02_pre_generation_eda.sql to explore current data' AS next_step;
