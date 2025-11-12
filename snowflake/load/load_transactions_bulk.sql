-- ============================================================================
-- Bulk Load Transactions from S3 to Bronze Layer
-- ============================================================================
-- Purpose: Load ~13.5M transactions from S3 with transactional validation
-- Source: @CUSTOMER_ANALYTICS.BRONZE.transaction_stage_historical
-- Target: BRONZE.BRONZE_TRANSACTIONS
-- Method: COPY INTO with transaction-based validation
-- ============================================================================

USE ROLE DATA_ENGINEER;
USE WAREHOUSE COMPUTE_WH;  -- Consider using MEDIUM for better performance
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA BRONZE;

-- ============================================================================
-- Pre-Load Validation
-- ============================================================================

SELECT 'Starting transaction bulk load process...' AS step;

-- Verify target table exists
SELECT
    'Verifying BRONZE_TRANSACTIONS table exists' AS check_name,
    COUNT(*) AS table_exists
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'BRONZE'
    AND TABLE_NAME = 'BRONZE_TRANSACTIONS';

-- Verify stage exists and has files
SELECT 'Checking for files in transaction stage...' AS step;

LIST @CUSTOMER_ANALYTICS.BRONZE.transaction_stage_historical;

-- ============================================================================
-- Transactional Load with Validation
-- ============================================================================

SELECT 'Beginning transactional load...' AS step;

BEGIN TRANSACTION;

    -- ========================================================================
    -- Step 1: Truncate table if reloading (optional safety measure)
    -- ========================================================================

    -- Uncomment if this is a reload and you want to clear existing data
    -- TRUNCATE TABLE BRONZE.BRONZE_TRANSACTIONS;
    -- SELECT 'Table truncated for reload' AS step;

    -- ========================================================================
    -- Step 2: COPY INTO from S3 Stage
    -- ========================================================================

    SELECT 'Loading transactions from S3...' AS step;

    COPY INTO BRONZE.BRONZE_TRANSACTIONS (
        transaction_id,
        customer_id,
        transaction_date,
        transaction_amount,
        merchant_name,
        merchant_category,
        channel,
        status,
        -- Metadata columns
        source_file,
        _metadata_file_row_number
    )
    FROM (
        SELECT
            $1::STRING AS transaction_id,
            $2::STRING AS customer_id,
            $3::TIMESTAMP AS transaction_date,
            $4::NUMBER(10,2) AS transaction_amount,
            $5::STRING AS merchant_name,
            $6::STRING AS merchant_category,
            $7::STRING AS channel,
            $8::STRING AS status,
            -- Metadata from Snowflake
            METADATA$FILENAME AS source_file,
            METADATA$FILE_ROW_NUMBER AS _metadata_file_row_number
        FROM @CUSTOMER_ANALYTICS.BRONZE.transaction_stage_historical
    )
    FILE_FORMAT = (
        TYPE = 'CSV'
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        TRIM_SPACE = TRUE
        COMPRESSION = 'GZIP'
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        NULL_IF = ('NULL', 'null', '')
    )
    PATTERN = '.*transactions_historical.*\.csv.*'
    ON_ERROR = 'ABORT_STATEMENT'
    FORCE = FALSE;  -- Skip files already loaded

    SELECT 'COPY INTO completed' AS step;

    -- ========================================================================
    -- Step 3: Validate Row Count
    -- ========================================================================

    SELECT 'Validating row count...' AS step;

    -- Define expected row count (approximate)
    SET expected_rows = 13500000;  -- 13.5M
    SET tolerance_pct = 0.25;      -- Allow ±25% variance due to randomization

    -- Calculate min/max acceptable range
    SET min_expected = $expected_rows * (1 - $tolerance_pct);  -- 10.125M
    SET max_expected = $expected_rows * (1 + $tolerance_pct);  -- 16.875M

    -- Get actual row count
    SET actual_rows = (SELECT COUNT(*) FROM BRONZE.BRONZE_TRANSACTIONS);

    -- Display validation results
    SELECT
        'Row Count Validation' AS validation,
        $expected_rows AS expected_approx,
        $min_expected AS min_acceptable,
        $max_expected AS max_acceptable,
        $actual_rows AS actual_count,
        CASE
            WHEN $actual_rows BETWEEN $min_expected AND $max_expected THEN '✓ PASS'
            ELSE '✗ FAIL - Row count out of expected range'
        END AS status;

    -- Validate or rollback
    IF ($actual_rows < $min_expected OR $actual_rows > $max_expected) THEN
        ROLLBACK;
        -- Note: RAISE is not available in all Snowflake editions
        -- Instead, we'll rely on the ROLLBACK and manual inspection
        SELECT
            '✗ TRANSACTION ROLLED BACK' AS error,
            'Row count out of expected range' AS reason,
            'Expected: ' || $expected_rows || ' ± ' || ($tolerance_pct * 100) || '%' AS expected_range,
            'Actual: ' || $actual_rows AS actual,
            'Run verification queries to diagnose issue' AS next_step;
        -- Exit with error (this won't raise but will show the issue)
        RETURN;
    END IF;

    -- ========================================================================
    -- Step 4: Additional Validation Checks
    -- ========================================================================

    SELECT 'Running additional validation checks...' AS step;

    -- Check for NULL transaction IDs
    SET null_txn_ids = (
        SELECT COUNT(*)
        FROM BRONZE.BRONZE_TRANSACTIONS
        WHERE transaction_id IS NULL
    );

    -- Check for NULL customer IDs
    SET null_customer_ids = (
        SELECT COUNT(*)
        FROM BRONZE.BRONZE_TRANSACTIONS
        WHERE customer_id IS NULL
    );

    -- Check for duplicate transaction IDs
    SET duplicate_txn_ids = (
        SELECT COUNT(*)
        FROM (
            SELECT transaction_id
            FROM BRONZE.BRONZE_TRANSACTIONS
            GROUP BY transaction_id
            HAVING COUNT(*) > 1
        )
    );

    -- Display validation results
    SELECT
        'Additional Validations' AS validation,
        $null_txn_ids AS null_transaction_ids,
        $null_customer_ids AS null_customer_ids,
        $duplicate_txn_ids AS duplicate_transaction_ids,
        CASE
            WHEN $null_txn_ids = 0
                AND $null_customer_ids = 0
                AND $duplicate_txn_ids = 0
            THEN '✓ PASS'
            ELSE '✗ FAIL - Data quality issues detected'
        END AS status;

    -- Rollback if validation fails
    IF ($null_txn_ids > 0 OR $null_customer_ids > 0 OR $duplicate_txn_ids > 0) THEN
        ROLLBACK;
        SELECT
            '✗ TRANSACTION ROLLED BACK' AS error,
            'Data quality validation failed' AS reason,
            'NULL transaction IDs: ' || $null_txn_ids AS check_1,
            'NULL customer IDs: ' || $null_customer_ids AS check_2,
            'Duplicate transaction IDs: ' || $duplicate_txn_ids AS check_3;
        RETURN;
    END IF;

    -- ========================================================================
    -- Step 5: Commit Transaction
    -- ========================================================================

    SELECT 'All validations passed. Committing transaction...' AS step;

COMMIT;

SELECT '✓ Transaction committed successfully' AS final_status;

-- ============================================================================
-- Post-Load Summary Statistics
-- ============================================================================

SELECT 'Generating summary statistics...' AS step;

-- Overall statistics
SELECT
    'Transaction Load Summary' AS summary,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT transaction_id) AS unique_transactions,
    COUNT(DISTINCT customer_id) AS unique_customers,
    MIN(transaction_date) AS earliest_date,
    MAX(transaction_date) AS latest_date,
    DATEDIFF('day', MIN(transaction_date), MAX(transaction_date)) AS date_range_days,
    ROUND(AVG(transaction_amount), 2) AS avg_amount,
    ROUND(MIN(transaction_amount), 2) AS min_amount,
    ROUND(MAX(transaction_amount), 2) AS max_amount,
    ROUND(SUM(transaction_amount), 2) AS total_volume
FROM BRONZE.BRONZE_TRANSACTIONS;

-- Status breakdown
SELECT
    'Status Distribution' AS breakdown,
    status,
    COUNT(*) AS txn_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM BRONZE.BRONZE_TRANSACTIONS
GROUP BY status
ORDER BY txn_count DESC;

-- Channel breakdown
SELECT
    'Channel Distribution' AS breakdown,
    channel,
    COUNT(*) AS txn_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM BRONZE.BRONZE_TRANSACTIONS
GROUP BY channel
ORDER BY txn_count DESC;

-- Top merchant categories
SELECT
    'Top Merchant Categories' AS breakdown,
    merchant_category,
    COUNT(*) AS txn_count,
    ROUND(AVG(transaction_amount), 2) AS avg_amount
FROM BRONZE.BRONZE_TRANSACTIONS
GROUP BY merchant_category
ORDER BY txn_count DESC
LIMIT 10;

-- Monthly transaction volume
SELECT
    'Monthly Transaction Volume' AS breakdown,
    DATE_TRUNC('month', transaction_date) AS month,
    COUNT(*) AS txn_count,
    ROUND(AVG(transaction_amount), 2) AS avg_amount,
    ROUND(SUM(transaction_amount), 2) AS total_amount
FROM BRONZE.BRONZE_TRANSACTIONS
GROUP BY DATE_TRUNC('month', transaction_date)
ORDER BY month;

-- ============================================================================
-- Log to Observability Layer
-- ============================================================================

SELECT 'Logging to observability layer...' AS step;

INSERT INTO OBSERVABILITY.LAYER_RECORD_COUNTS
SELECT
    'BULK_LOAD_TRANSACTIONS_' || CURRENT_TIMESTAMP()::STRING AS run_id,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'bronze' AS layer,
    'BRONZE' AS schema_name,
    'BRONZE_TRANSACTIONS' AS table_name,
    COUNT(*) AS record_count,
    COUNT(DISTINCT transaction_id) AS distinct_keys,
    COUNT_IF(transaction_id IS NULL) AS null_key_count,
    COUNT(*) - COUNT(DISTINCT transaction_id) AS duplicate_key_count
FROM BRONZE.BRONZE_TRANSACTIONS;

SELECT '✓ Observability record created' AS step;

-- ============================================================================
-- Display Load History
-- ============================================================================

SELECT 'Displaying load history...' AS step;

-- Show COPY INTO history for this table
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'CUSTOMER_ANALYTICS.BRONZE.BRONZE_TRANSACTIONS',
    START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;

-- ============================================================================
-- Completion Summary
-- ============================================================================

SELECT '========================================' AS summary;
SELECT '✅ Transaction Bulk Load Complete' AS summary;
SELECT '========================================' AS summary;

SELECT
    'Summary' AS info,
    'Transactions loaded successfully' AS message,
    (SELECT COUNT(*) FROM BRONZE.BRONZE_TRANSACTIONS) AS total_transactions,
    (SELECT COUNT(DISTINCT customer_id) FROM BRONZE.BRONZE_TRANSACTIONS) AS unique_customers;

SELECT 'Next Steps:' AS info;
SELECT '1. Run snowflake/load/verify_transaction_load.sql for detailed validation' AS step;
SELECT '2. Review summary statistics above' AS step;
SELECT '3. Check COPY_HISTORY for any warnings' AS step;
SELECT '4. Proceed to Phase 3: dbt Silver layer transformations' AS step;

-- ============================================================================
-- Performance Notes
-- ============================================================================

/*
PERFORMANCE EXPECTATIONS:

Warehouse Size vs Load Time:
- XSMALL: ~30-45 minutes (not recommended)
- SMALL: ~10-20 minutes
- MEDIUM: ~5-10 minutes (recommended)
- LARGE: ~3-5 minutes

Optimization Tips:
1. Use MEDIUM warehouse for initial load
2. Scale down to SMALL for subsequent queries
3. Consider adding clustering key after load (see 07_create_bronze_transaction_table.sql)
4. Monitor with: SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(...))

Troubleshooting:
- If load fails, check error message in output
- Verify files exist in stage: LIST @transaction_stage_historical
- Check file format matches CSV spec
- Ensure GZIP compression is correct
- Review COPY_HISTORY for detailed error messages
*/
