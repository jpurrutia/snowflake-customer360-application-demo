-- ============================================================================
-- Create Snowflake External Stages for S3 Data Access
-- ============================================================================
-- Purpose: Create external stages pointing to S3 for data ingestion
-- Requires: DATA_ENGINEER role, storage integration created (04_create_storage_integration.sql)
-- ============================================================================

USE ROLE DATA_ENGINEER;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA BRONZE;

-- ============================================================================
-- BEFORE RUNNING THIS SCRIPT
-- ============================================================================
-- 1. Complete Terraform deployment (S3 bucket and IAM role created)
-- 2. Create storage integration (04_create_storage_integration.sql)
-- 3. Update Terraform with external ID and re-apply
-- 4. Replace <S3_BUCKET_NAME> below with your actual bucket name

-- ============================================================================
-- Create File Format for CSV Files
-- ============================================================================

CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  ESCAPE = 'NONE'
  ESCAPE_UNENCLOSED_FIELD = 'NONE'
  DATE_FORMAT = 'AUTO'
  TIMESTAMP_FORMAT = 'AUTO'
  NULL_IF = ('NULL', 'null', '')
  COMMENT = 'CSV file format with header row for customer and transaction data';

-- ============================================================================
-- Create External Stage: CUSTOMER_STAGE
-- ============================================================================

CREATE OR REPLACE STAGE customer_stage
  URL = 's3://snowflake-customer-analytics-data-demo/customers/'
  STORAGE_INTEGRATION = s3_customer_analytics_integration
  FILE_FORMAT = csv_format
  COMMENT = 'External stage for customer CSV files from S3';

-- Example (DO NOT USE THIS VALUE):
-- URL = 's3://customer360-analytics-data-20250111/customers/'



-- ============================================================================
-- Create External Stage: TRANSACTION_STAGE_HISTORICAL
-- ============================================================================

CREATE OR REPLACE STAGE transaction_stage_historical
  URL = 's3://snowflake-customer-analytics-data-demo/transactions/historical/'
  STORAGE_INTEGRATION = s3_customer_analytics_integration
  FILE_FORMAT = csv_format
  COMMENT = 'External stage for historical transaction CSV files from S3';

-- ============================================================================
-- Create External Stage: TRANSACTION_STAGE_STREAMING
-- ============================================================================

CREATE OR REPLACE STAGE transaction_stage_streaming
  URL = 's3://snowflake-customer-analytics-data-demo/transactions/streaming/'
  STORAGE_INTEGRATION = s3_customer_analytics_integration
  FILE_FORMAT = csv_format
  COMMENT = 'External stage for streaming/incremental transaction data from S3';

-- ============================================================================
-- Verify Stages
-- ============================================================================

-- Show all stages in current schema
SHOW STAGES IN SCHEMA CUSTOMER_ANALYTICS.BRONZE;

-- ============================================================================
-- Test Stage Access
-- ============================================================================

-- Try to list files in customer stage
-- This will succeed if:
-- 1. Storage integration is properly configured
-- 2. IAM trust relationship is correct
-- 3. IAM role has S3 permissions
LIST @customer_stage;

-- If no files yet (before upload), you'll see: "No files found"
-- After uploading customers.csv, you should see the file listed

-- Test other stages
LIST @transaction_stage_historical;
LIST @transaction_stage_streaming;

-- ============================================================================
-- Grant Stage Usage to Roles
-- ============================================================================

-- Grant usage to DATA_ENGINEER (already has access as creator)
-- Grant read-only access to DATA_ANALYST
GRANT USAGE ON STAGE customer_stage TO ROLE DATA_ANALYST;
GRANT USAGE ON STAGE transaction_stage_historical TO ROLE DATA_ANALYST;
GRANT USAGE ON STAGE transaction_stage_streaming TO ROLE DATA_ANALYST;

-- ============================================================================
-- Test File Format
-- ============================================================================

-- View file format details
DESC FILE FORMAT csv_format;

-- Show file formats
SHOW FILE FORMATS IN SCHEMA CUSTOMER_ANALYTICS.BRONZE;

-- ============================================================================
-- Example: Preview Data from Stage (after upload)
-- ============================================================================

-- Once customers.csv is uploaded, you can preview it with:
-- SELECT $1, $2, $3, $4, $5
-- FROM @customer_stage/customers.csv
-- LIMIT 10;

-- Or use METADATA$FILENAME to see which file data comes from:
-- SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1, $2, $3
-- FROM @customer_stage
-- LIMIT 10;

-- ============================================================================
-- Verification Queries
-- ============================================================================

-- Count stages created
SELECT COUNT(*) AS stage_count
FROM INFORMATION_SCHEMA.STAGES
WHERE STAGE_SCHEMA = 'BRONZE'
  AND STAGE_NAME IN ('CUSTOMER_STAGE', 'TRANSACTION_STAGE_HISTORICAL', 'TRANSACTION_STAGE_STREAMING');

-- Display confirmation
SELECT '✓ Stages Created Successfully' AS status;
SELECT 'customer_stage, transaction_stage_historical, transaction_stage_streaming' AS stages_created;
SELECT 'Next: Upload customer data to S3 using upload-customers CLI command' AS next_step;

-- ============================================================================
-- Troubleshooting
-- ============================================================================

-- Error: "Integration 'CUSTOMER360_S3_INTEGRATION' does not exist"
-- Solution: Run 04_create_storage_integration.sql first

-- Error: "Not authorized to perform sts:AssumeRole"
-- Solution: Update terraform.tfvars with external ID from DESC STORAGE INTEGRATION
--          Then re-run terraform apply

-- Error: "Access Denied" when listing stage
-- Solution: Verify IAM role has s3:ListBucket and s3:GetObject permissions
--          Check that bucket name in URL matches your actual bucket

-- Error: "No files found" when listing stage
-- Solution: This is expected before uploading files. Upload data using:
--          python -m data_generation upload-customers --file data/customers.csv --bucket <BUCKET>
