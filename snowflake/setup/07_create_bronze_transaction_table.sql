-- ============================================================================
-- Create Bronze Layer Transaction Table
-- ============================================================================
-- Purpose: Create table to store raw transaction data loaded from S3
-- Layer: Bronze (raw data as-is from source)
-- Expected Rows: ~13.5 million transactions
-- ============================================================================

USE ROLE DATA_ENGINEER;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA BRONZE;

-- ============================================================================
-- Create RAW_TRANSACTIONS Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS BRONZE.RAW_TRANSACTIONS (
    -- Transaction attributes from source data
    transaction_id STRING NOT NULL,
    customer_id STRING NOT NULL,
    transaction_date TIMESTAMP NOT NULL,
    transaction_amount NUMBER(10,2) NOT NULL,
    merchant_name STRING,
    merchant_category STRING,
    channel STRING,
    status STRING,

    -- Metadata columns for lineage and debugging
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING,
    _metadata_file_row_number INT
)
COMMENT = 'Bronze layer table for raw transaction data loaded from S3.

PURPOSE:
This table stores raw, unprocessed transaction data exactly as it appears in
source CSV files. It serves as the foundation for downstream transformations
in the Silver and Gold layers.

DATA CHARACTERISTICS:
- Row Count: ~13.5 million transactions
- Time Period: 18 months of historical data
- Source: S3 staged files from transaction generation process
- File Format: GZIP compressed CSV files

SCHEMA DESIGN:
- No PRIMARY KEY constraint (Bronze accepts data as-is)
- No NOT NULL enforcement on most columns (accept dirty data)
- No FOREIGN KEY constraints (validated in Silver/Gold layers)
- Metadata columns for data lineage and debugging

COLUMNS:
- transaction_id: Unique transaction identifier (TXN00000000001 format)
- customer_id: Foreign key to RAW_CUSTOMERS (CUST00000001 format)
- transaction_date: Transaction timestamp
- transaction_amount: Transaction amount in USD
- merchant_name: Merchant identifier
- merchant_category: Category (Travel, Dining, Retail, etc.)
- channel: Transaction channel (Online, In-Store, Mobile)
- status: Transaction status (approved, declined)
- ingestion_timestamp: When record was loaded into Snowflake
- source_file: S3 file path for data lineage
- _metadata_file_row_number: Row number in source file for debugging

USAGE:
- Bronze layer is read-only after initial load
- Used as source for dbt Silver layer transformations
- Enables full data lineage from source to Gold layer
- Supports data quality validation and auditing

CLUSTERING:
- No clustering key initially (can be added later for performance)
- Recommended clustering key: CLUSTER BY (transaction_date, customer_id)
- Add clustering after initial load if query performance requires it

NEXT STEPS:
1. Load data using snowflake/load/load_transactions_bulk.sql
2. Validate using snowflake/load/verify_transaction_load.sql
3. Transform to Silver layer using dbt models
';

-- ============================================================================
-- Display Table Information
-- ============================================================================

SELECT 'Bronze transactions table created successfully' AS status;

-- Show table structure
DESC TABLE BRONZE.RAW_TRANSACTIONS;

-- Show table comment
SHOW TABLES LIKE 'RAW_TRANSACTIONS' IN SCHEMA BRONZE;

-- ============================================================================
-- Grant Permissions
-- ============================================================================

-- Grant read access to analysts
GRANT SELECT ON TABLE BRONZE.RAW_TRANSACTIONS TO ROLE DATA_ANALYST;

-- Grant read access to marketing team
GRANT SELECT ON TABLE BRONZE.RAW_TRANSACTIONS TO ROLE MARKETING_MANAGER;

SELECT 'Permissions granted to DATA_ANALYST and MARKETING_MANAGER roles' AS status;

-- ============================================================================
-- Optional: Add Clustering Key (Uncomment if needed for performance)
-- ============================================================================

/*
-- Add clustering key on transaction_date for time-series queries
-- This improves performance for date-range queries and monthly aggregations
-- Recommended to add AFTER initial bulk load completes

ALTER TABLE BRONZE.RAW_TRANSACTIONS
CLUSTER BY (transaction_date);

SELECT 'Clustering key added on transaction_date' AS status;
*/

/*
-- Alternative: Cluster by both transaction_date and customer_id
-- Use this if queries frequently filter by both date range and customer
-- Cost: Higher clustering maintenance overhead

ALTER TABLE BRONZE.RAW_TRANSACTIONS
CLUSTER BY (transaction_date, customer_id);

SELECT 'Clustering key added on (transaction_date, customer_id)' AS status;
*/

-- ============================================================================
-- Expected Usage Patterns
-- ============================================================================

/*
COMMON QUERIES:

1. Count total transactions:
   SELECT COUNT(*) FROM BRONZE.RAW_TRANSACTIONS;

2. Date range analysis:
   SELECT
       MIN(transaction_date) AS earliest,
       MAX(transaction_date) AS latest
   FROM BRONZE.RAW_TRANSACTIONS;

3. Customer transaction count:
   SELECT
       customer_id,
       COUNT(*) AS txn_count
   FROM BRONZE.BRONZE_TRANSACTIONS
   GROUP BY customer_id;

4. Monthly transaction volume:
   SELECT
       DATE_TRUNC('month', transaction_date) AS month,
       COUNT(*) AS txn_count,
       SUM(transaction_amount) AS total_amount
   FROM BRONZE.BRONZE_TRANSACTIONS
   GROUP BY month
   ORDER BY month;

5. Check for duplicates:
   SELECT
       transaction_id,
       COUNT(*) AS duplicate_count
   FROM BRONZE.BRONZE_TRANSACTIONS
   GROUP BY transaction_id
   HAVING COUNT(*) > 1;
*/

-- ============================================================================
-- Performance Considerations
-- ============================================================================

/*
PERFORMANCE TIPS:

1. Initial Load:
   - Use SMALL or MEDIUM warehouse for 13.5M row load
   - Expected load time: 5-15 minutes depending on warehouse size
   - Monitor with: SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(...))

2. Query Performance:
   - Add clustering key if queries are slow (see optional section above)
   - Use partitioning in downstream dbt models
   - Consider materialized views for frequently-used aggregations

3. Storage Optimization:
   - Transaction data compresses well (expect ~10:1 compression)
   - Monitor storage with: SHOW TABLE BRONZE.RAW_TRANSACTIONS

4. Warehouse Sizing:
   - XSMALL: Acceptable for ad-hoc queries (< 30 seconds)
   - SMALL: Recommended for regular analytics workloads
   - MEDIUM+: Use for time-sensitive reporting or large joins

5. Cost Optimization:
   - Bronze layer is loaded once, queried infrequently
   - Most queries should run against Silver/Gold layers
   - Consider time travel retention (default 1 day is usually sufficient)
*/

-- ============================================================================
-- Data Quality Expectations
-- ============================================================================

/*
EXPECTED DATA QUALITY (from generation process):

Row Count:
- Expected: ~13.5M transactions
- Tolerance: 10M - 17M (due to randomization)

Transaction IDs:
- Format: TXN00000000001 (11-digit zero-padded)
- Uniqueness: 100% (no duplicates)

Customer IDs:
- Format: CUST00000001 (8-digit zero-padded)
- Distinct count: 50,000 (all customers)
- Referential integrity: All customer_ids exist in BRONZE_CUSTOMERS

Transaction Dates:
- Range: ~18 months from current date
- No NULL values
- No future dates

Transaction Amounts:
- Range: $10 - $500 (varies by customer segment)
- All positive values
- Precision: 2 decimal places

Merchant Categories:
- Values: Travel, Dining, Retail, Entertainment, Grocery, Gas, Healthcare, Utilities
- No NULL values (but accepts if present)

Channels:
- Values: Online, In-Store, Mobile
- No NULL values (but accepts if present)

Status:
- Values: approved, declined
- Distribution: ~97% approved, ~3% declined

VALIDATION:
Run snowflake/load/verify_transaction_load.sql after load to validate all expectations.
*/

-- ============================================================================
-- Completion
-- ============================================================================

SELECT '✓ Bronze transactions table ready for bulk load' AS final_status;
SELECT 'Next step: Run snowflake/load/load_transactions_bulk.sql' AS next_step;
