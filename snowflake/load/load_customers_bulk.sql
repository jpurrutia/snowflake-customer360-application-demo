-- ============================================================================
-- Bulk Load Customers from S3 to Bronze Layer
-- ============================================================================
-- Purpose: Load customer data from S3 into BRONZE_CUSTOMERS table using COPY INTO
-- Prerequisites:
--   - Bronze table created (06_create_bronze_tables.sql)
--   - S3 stage configured (05_create_stages.sql)
--   - Customer CSV uploaded to S3
-- ============================================================================

USE ROLE DATA_ENGINEER;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA BRONZE;

-- ============================================================================
-- Pre-Load Checks
-- ============================================================================

-- Verify stage is accessible
LIST @customer_stage;

-- Check current row count (before load)
SELECT 'Current row count before load' AS step,
       COUNT(*) AS row_count
FROM BRONZE_CUSTOMERS;

-- ============================================================================
-- Bulk Load using COPY INTO
-- ============================================================================

COPY INTO BRONZE_CUSTOMERS (
    customer_id,
    first_name,
    last_name,
    email,
    age,
    state,
    city,
    employment_status,
    card_type,
    credit_limit,
    account_open_date,
    customer_segment,
    decline_type,
    source_file,
    _metadata_file_row_number
)
FROM (
    SELECT
        $1::STRING AS customer_id,
        $2::STRING AS first_name,
        $3::STRING AS last_name,
        $4::STRING AS email,
        $5::INT AS age,
        $6::STRING AS state,
        $7::STRING AS city,
        $8::STRING AS employment_status,
        $9::STRING AS card_type,
        $10::NUMBER(10,2) AS credit_limit,
        $11::DATE AS account_open_date,
        $12::STRING AS customer_segment,
        $13::STRING AS decline_type,
        METADATA$FILENAME AS source_file,
        METADATA$FILE_ROW_NUMBER AS _metadata_file_row_number
    FROM @customer_stage
)
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    NULL_IF = ('NULL', 'null', '')
)
PATTERN = '.*customers\.csv'
ON_ERROR = 'ABORT_STATEMENT'  -- Fail fast for bulk loads
FORCE = FALSE;  -- Skip files already loaded (prevents duplicates)

-- Note: FORCE = FALSE prevents re-loading the same file
-- To reload, either:
--   1. Truncate the table first, OR
--   2. Set FORCE = TRUE (will load duplicates)

-- ============================================================================
-- Post-Load Summary
-- ============================================================================

-- Total rows loaded
SELECT 'Total rows after load' AS step,
       COUNT(*) AS row_count
FROM BRONZE_CUSTOMERS;

-- Distinct customer count
SELECT 'Distinct customers' AS step,
       COUNT(DISTINCT customer_id) AS distinct_count
FROM BRONZE_CUSTOMERS;

-- Rows per segment
SELECT customer_segment,
       COUNT(*) AS row_count,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM BRONZE_CUSTOMERS
GROUP BY customer_segment
ORDER BY row_count DESC;

-- Card type distribution
SELECT card_type,
       COUNT(*) AS row_count,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM BRONZE_CUSTOMERS
GROUP BY card_type
ORDER BY row_count DESC;

-- ============================================================================
-- Log Load to Observability
-- ============================================================================

INSERT INTO CUSTOMER_ANALYTICS.OBSERVABILITY.LAYER_RECORD_COUNTS (
    run_id,
    run_timestamp,
    layer,
    schema_name,
    table_name,
    record_count,
    distinct_keys,
    null_key_count,
    duplicate_key_count
)
SELECT
    'BULK_LOAD_CUSTOMERS_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS') AS run_id,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'bronze' AS layer,
    'BRONZE' AS schema_name,
    'BRONZE_CUSTOMERS' AS table_name,
    COUNT(*) AS record_count,
    COUNT(DISTINCT customer_id) AS distinct_keys,
    COUNT_IF(customer_id IS NULL) AS null_key_count,
    COUNT(*) - COUNT(DISTINCT customer_id) AS duplicate_key_count
FROM BRONZE_CUSTOMERS;

-- Verify observability logging
SELECT 'Observability logging' AS step,
       'Check OBSERVABILITY.LAYER_RECORD_COUNTS' AS location;

SELECT *
FROM CUSTOMER_ANALYTICS.OBSERVABILITY.LAYER_RECORD_COUNTS
WHERE table_name = 'BRONZE_CUSTOMERS'
ORDER BY run_timestamp DESC
LIMIT 5;

-- ============================================================================
-- Load History
-- ============================================================================

-- View load history (files loaded, rows processed, errors)
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'CUSTOMER_ANALYTICS.BRONZE.BRONZE_CUSTOMERS',
    START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;

-- ============================================================================
-- Completion Message
-- ============================================================================

SELECT '✓ Customer bulk load completed successfully' AS status;
SELECT 'Next: Run verification queries using snowflake/load/verify_customer_load.sql' AS next_step;
