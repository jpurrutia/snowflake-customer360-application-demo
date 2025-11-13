-- ============================================================================
-- Pre-Generation EDA - Explore Current Data State
-- ============================================================================
-- Purpose: Comprehensive exploratory analysis of existing data
-- Run this AFTER 01_baseline_metrics.sql and BEFORE generate_transactions.sql
-- ============================================================================

USE ROLE DATA_ENGINEER;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA BRONZE;

-- ============================================================================
-- 1. CUSTOMER DATA EXPLORATION
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'CUSTOMER DATA EXPLORATION'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

-- Total customer count
SELECT 'Total Customers' AS metric, COUNT(*) AS value
FROM bronze_customers;

-- Customer segment distribution
SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    ROUND(AVG(age), 1) AS avg_age
FROM bronze_customers
GROUP BY customer_segment
ORDER BY customer_count DESC;

-- Card type distribution
SELECT
    card_type,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    ROUND(MIN(credit_limit), 2) AS min_credit_limit,
    ROUND(MAX(credit_limit), 2) AS max_credit_limit
FROM bronze_customers
GROUP BY card_type
ORDER BY customer_count DESC;

-- Decline type distribution (for Declining segment)
SELECT
    decline_type,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM bronze_customers
WHERE customer_segment = 'Declining'
GROUP BY decline_type
ORDER BY customer_count DESC;

-- Age distribution by segment
SELECT
    customer_segment,
    MIN(age) AS min_age,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY age), 0) AS q1_age,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY age), 0) AS median_age,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY age), 0) AS q3_age,
    MAX(age) AS max_age,
    ROUND(AVG(age), 1) AS avg_age
FROM bronze_customers
GROUP BY customer_segment
ORDER BY avg_age DESC;

-- Top 10 states by customer count
SELECT
    state,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM bronze_customers
GROUP BY state
ORDER BY customer_count DESC
LIMIT 10;

-- Employment status distribution
SELECT
    employment_status,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit
FROM bronze_customers
GROUP BY employment_status
ORDER BY customer_count DESC;

-- Credit limit distribution by segment
SELECT
    customer_segment,
    ROUND(MIN(credit_limit), 2) AS min_limit,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY credit_limit), 2) AS q1_limit,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY credit_limit), 2) AS median_limit,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY credit_limit), 2) AS q3_limit,
    ROUND(MAX(credit_limit), 2) AS max_limit,
    ROUND(AVG(credit_limit), 2) AS avg_limit
FROM bronze_customers
GROUP BY customer_segment
ORDER BY avg_limit DESC;

-- Account age distribution
SELECT
    customer_segment,
    ROUND(AVG(DATEDIFF('day', account_open_date, CURRENT_DATE()) / 365.25), 1) AS avg_account_age_years,
    MIN(account_open_date) AS oldest_account,
    MAX(account_open_date) AS newest_account
FROM bronze_customers
GROUP BY customer_segment
ORDER BY avg_account_age_years DESC;

-- ============================================================================
-- 2. TRANSACTION DATA EXPLORATION (if any exists)
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'TRANSACTION DATA EXPLORATION'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

-- Check if transactions exist
SELECT
    'Total Transactions' AS metric,
    COUNT(*) AS value
FROM bronze_transactions;

-- Transaction date range (if transactions exist)
SELECT
    'Transaction Date Range' AS metric,
    TO_CHAR(MIN(transaction_date), 'YYYY-MM-DD') AS earliest_date,
    TO_CHAR(MAX(transaction_date), 'YYYY-MM-DD') AS latest_date,
    DATEDIFF('month', MIN(transaction_date), MAX(transaction_date)) AS months_of_data,
    DATEDIFF('day', MIN(transaction_date), MAX(transaction_date)) AS days_of_data
FROM bronze_transactions
WHERE EXISTS (SELECT 1 FROM bronze_transactions LIMIT 1);

-- Transaction amount statistics (if transactions exist)
SELECT
    'Transaction Amount Statistics' AS metric,
    ROUND(MIN(transaction_amount), 2) AS min_amount,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY transaction_amount), 2) AS q1_amount,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY transaction_amount), 2) AS median_amount,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY transaction_amount), 2) AS q3_amount,
    ROUND(MAX(transaction_amount), 2) AS max_amount,
    ROUND(AVG(transaction_amount), 2) AS avg_amount,
    ROUND(SUM(transaction_amount), 2) AS total_amount
FROM bronze_transactions
WHERE EXISTS (SELECT 1 FROM bronze_transactions LIMIT 1);

-- Transaction status distribution (if transactions exist)
SELECT
    status,
    COUNT(*) AS txn_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    ROUND(AVG(transaction_amount), 2) AS avg_amount
FROM bronze_transactions
WHERE EXISTS (SELECT 1 FROM bronze_transactions LIMIT 1)
GROUP BY status
ORDER BY txn_count DESC;

-- Transaction channel distribution (if transactions exist)
SELECT
    channel,
    COUNT(*) AS txn_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    ROUND(AVG(transaction_amount), 2) AS avg_amount
FROM bronze_transactions
WHERE EXISTS (SELECT 1 FROM bronze_transactions LIMIT 1)
GROUP BY channel
ORDER BY txn_count DESC;

-- Top merchant categories (if transactions exist)
SELECT
    merchant_category,
    COUNT(*) AS txn_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    ROUND(SUM(transaction_amount), 2) AS total_amount,
    ROUND(AVG(transaction_amount), 2) AS avg_amount
FROM bronze_transactions
WHERE EXISTS (SELECT 1 FROM bronze_transactions LIMIT 1)
GROUP BY merchant_category
ORDER BY txn_count DESC
LIMIT 10;

-- ============================================================================
-- 3. DATA QUALITY CHECKS
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'DATA QUALITY CHECKS'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

-- Customer data quality
SELECT
    'Customers with NULL customer_id' AS check_name,
    COUNT_IF(customer_id IS NULL) AS issue_count
FROM bronze_customers
UNION ALL
SELECT
    'Customers with NULL email' AS check_name,
    COUNT_IF(email IS NULL) AS issue_count
FROM bronze_customers
UNION ALL
SELECT
    'Customers with invalid age (<18 or >100)' AS check_name,
    COUNT_IF(age < 18 OR age > 100) AS issue_count
FROM bronze_customers
UNION ALL
SELECT
    'Customers with NULL credit_limit' AS check_name,
    COUNT_IF(credit_limit IS NULL) AS issue_count
FROM bronze_customers
UNION ALL
SELECT
    'Customers with credit_limit <= 0' AS check_name,
    COUNT_IF(credit_limit <= 0) AS issue_count
FROM bronze_customers
UNION ALL
SELECT
    'Duplicate customer_ids' AS check_name,
    COUNT(*) - COUNT(DISTINCT customer_id) AS issue_count
FROM bronze_customers
;

-- Transaction data quality (if transactions exist)
SELECT
    'Transactions with NULL transaction_id' AS check_name,
    COUNT_IF(transaction_id IS NULL) AS issue_count
FROM bronze_transactions
WHERE EXISTS (SELECT 1 FROM bronze_transactions LIMIT 1)
UNION ALL
SELECT
    'Transactions with NULL customer_id' AS check_name,
    COUNT_IF(customer_id IS NULL) AS issue_count
FROM bronze_transactions
WHERE EXISTS (SELECT 1 FROM bronze_transactions LIMIT 1)
UNION ALL
SELECT
    'Transactions with invalid amount (<= 0)' AS check_name,
    COUNT_IF(transaction_amount <= 0) AS issue_count
FROM bronze_transactions
WHERE EXISTS (SELECT 1 FROM bronze_transactions LIMIT 1)
UNION ALL
SELECT
    'Transactions with future dates' AS check_name,
    COUNT_IF(transaction_date > CURRENT_TIMESTAMP()) AS issue_count
FROM bronze_transactions
WHERE EXISTS (SELECT 1 FROM bronze_transactions LIMIT 1)
UNION ALL
SELECT
    'Duplicate transaction_ids' AS check_name,
    COUNT(*) - COUNT(DISTINCT transaction_id) AS issue_count
FROM bronze_transactions
WHERE EXISTS (SELECT 1 FROM bronze_transactions LIMIT 1)
;

-- ============================================================================
-- 4. CUSTOMER-TRANSACTION JOIN ANALYSIS (if transactions exist)
-- ============================================================================

SELECT '=' || REPEAT('=', 78) || '=' AS section_header
UNION ALL
SELECT 'CUSTOMER-TRANSACTION JOIN ANALYSIS'
UNION ALL
SELECT '=' || REPEAT('=', 78) || '='
;

-- Customers with and without transactions
SELECT
    CASE
        WHEN transaction_count > 0 THEN 'With Transactions'
        ELSE 'No Transactions'
    END AS customer_status,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM (
    SELECT
        c.customer_id,
        COUNT(t.transaction_id) AS transaction_count
    FROM bronze_customers c
    LEFT JOIN bronze_transactions t ON c.customer_id = t.customer_id
    GROUP BY c.customer_id
)
GROUP BY customer_status
ORDER BY customer_count DESC;

-- Transaction count by segment (if transactions exist)
SELECT
    c.customer_segment,
    COUNT(DISTINCT c.customer_id) AS customers,
    COUNT(t.transaction_id) AS transactions,
    ROUND(COUNT(t.transaction_id) * 1.0 / NULLIF(COUNT(DISTINCT c.customer_id), 0), 2) AS avg_txns_per_customer,
    ROUND(AVG(t.transaction_amount), 2) AS avg_txn_amount,
    ROUND(SUM(t.transaction_amount), 2) AS total_spend
FROM bronze_customers c
LEFT JOIN bronze_transactions t ON c.customer_id = t.customer_id
WHERE EXISTS (SELECT 1 FROM bronze_transactions LIMIT 1)
GROUP BY c.customer_segment
ORDER BY transactions DESC;

-- ============================================================================
-- Display confirmation
-- ============================================================================

SELECT '✓ Pre-generation EDA complete' AS status;
SELECT 'Next: Review the results, then run ../data_generation/generate_transactions.sql' AS next_step;
