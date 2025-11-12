-- ============================================================================
-- Create Bronze Layer Tables
-- ============================================================================
-- Purpose: Create tables in Bronze layer for raw data ingestion
-- Bronze layer: Raw data exactly as received, no transformations
-- Requires: DATA_ENGINEER role
-- ============================================================================

USE ROLE DATA_ENGINEER;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA BRONZE;

-- ============================================================================
-- Create BRONZE_CUSTOMERS Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS BRONZE_CUSTOMERS (
    -- Customer data columns (raw, as received from CSV)
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    age INT,
    state STRING,
    city STRING,
    employment_status STRING,
    card_type STRING,
    credit_limit NUMBER(10,2),
    account_open_date DATE,
    customer_segment STRING,
    decline_type STRING,

    -- Metadata columns (for lineage and auditing)
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING,
    _metadata_file_row_number INT
)
COMMENT = 'Bronze Layer: Raw customer data loaded from S3. No transformations or data quality rules applied. Source of truth for customer master data ingestion.';

-- ============================================================================
-- Verify Table Creation
-- ============================================================================

-- Show table structure
DESC TABLE BRONZE_CUSTOMERS;

-- Display table information
SELECT
    'BRONZE_CUSTOMERS' AS table_name,
    COUNT(*) AS current_row_count
FROM BRONZE_CUSTOMERS;

-- Show table comment
SHOW TABLES LIKE 'BRONZE_CUSTOMERS' IN SCHEMA CUSTOMER_ANALYTICS.BRONZE;

-- ============================================================================
-- Grant Permissions
-- ============================================================================

-- DATA_ENGINEER already has full access as creator
-- Grant read access to DATA_ANALYST
GRANT SELECT ON TABLE BRONZE_CUSTOMERS TO ROLE DATA_ANALYST;

-- Grant read access to MARKETING_MANAGER (they can see raw data for validation)
GRANT SELECT ON TABLE BRONZE_CUSTOMERS TO ROLE MARKETING_MANAGER;

-- ============================================================================
-- Table Design Notes
-- ============================================================================

-- 1. NO PRIMARY KEY: Bronze layer stores raw data, duplicates are possible
-- 2. NO NOT NULL CONSTRAINTS: We accept data as-is for maximum flexibility
-- 3. NO FOREIGN KEYS: References are validated in Silver/Gold layers
-- 4. STRING types for IDs: Preserve original format, no implicit conversions
-- 5. Metadata columns track data lineage:
--    - ingestion_timestamp: When row was loaded
--    - source_file: Which S3 file provided the data
--    - _metadata_file_row_number: Row number in source file (for debugging)

-- ============================================================================
-- Expected Row Count
-- ============================================================================

-- After bulk load from S3, expect exactly 50,000 rows
-- Source: data/customers.csv generated with seed=42

-- ============================================================================
-- Next Steps
-- ============================================================================

SELECT '✓ Bronze table BRONZE_CUSTOMERS created successfully' AS status;
SELECT 'Next: Load data using snowflake/load/load_customers_bulk.sql' AS next_step;
