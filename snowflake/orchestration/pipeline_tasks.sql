-- ============================================================================
-- Complete Pipeline Task Orchestration
-- ============================================================================
-- Purpose: Automate end-to-end data pipeline with Snowflake Tasks
-- Orchestrates: Data Generation → Transformations → ML → Analytics
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CUSTOMER_ANALYTICS;

-- ============================================================================
-- TASK DEPENDENCY DIAGRAM
-- ============================================================================

/*
                    generate_customer_data (Task 1)
                              ↓
                    generate_transaction_data (Task 2)
                              ↓
                    run_dbt_transformations (Task 3)
                              ↓
                    train_churn_model (Task 4)
                              ↓
                    refresh_analytics_views (Task 5)
*/

-- ============================================================================
-- STEP 1: Create Root Task - Generate Customer Data
-- ============================================================================

CREATE OR REPLACE TASK BRONZE.generate_customer_data
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 2 * * SUN UTC'  -- Weekly on Sunday at 2 AM UTC
  COMMENT = 'Generate synthetic customer data using Snowpark stored procedure'
AS
  CALL BRONZE.GENERATE_CUSTOMERS(50000, 42);

-- ============================================================================
-- STEP 2: Create Task - Generate Transaction Data
-- ============================================================================

CREATE OR REPLACE TASK BRONZE.generate_transaction_data
  WAREHOUSE = COMPUTE_WH
  AFTER BRONZE.generate_customer_data  -- Runs after Task 1 completes
  COMMENT = 'Generate synthetic transaction data for all customers'
AS
  -- Execute transaction generation SQL script from Git repository
  EXECUTE IMMEDIATE FROM @CUSTOMER_ANALYTICS.GOLD.snowflake_panel_demo_repo/branches/main/snowflake/data_generation/generate_transactions.sql;

-- ============================================================================
-- STEP 3: Create Task - Run dbt Transformations
-- ============================================================================

CREATE OR REPLACE TASK GOLD.run_dbt_transformations
  WAREHOUSE = COMPUTE_WH
  AFTER BRONZE.generate_transaction_data  -- Runs after Task 2 completes
  COMMENT = 'Execute dbt transformations to build Silver and Gold layer models'
AS
  EXECUTE DBT PROJECT CUSTOMER_ANALYTICS.GOLD.customer_analytics_dbt
    COMMAND = 'run'
    WAREHOUSE = COMPUTE_WH;

-- ============================================================================
-- STEP 4: Create Task - Train ML Churn Model
-- ============================================================================

CREATE OR REPLACE TASK GOLD.train_churn_model
  WAREHOUSE = COMPUTE_WH
  AFTER GOLD.run_dbt_transformations  -- Runs after Task 3 completes
  COMMENT = 'Train churn prediction model using Snowflake ML'
AS
  -- Execute ML model training from Git repository
  EXECUTE IMMEDIATE FROM @CUSTOMER_ANALYTICS.GOLD.snowflake_panel_demo_repo/branches/main/snowflake/ml/03_train_churn_model.sql;

-- ============================================================================
-- STEP 5: Create Task - Refresh Analytics Views
-- ============================================================================

CREATE OR REPLACE TASK GOLD.refresh_analytics_views
  WAREHOUSE = COMPUTE_WH
  AFTER GOLD.train_churn_model  -- Runs after Task 4 completes
  COMMENT = 'Refresh materialized views and update analytics tables'
AS
BEGIN
  -- Refresh customer 360 profile
  ALTER VIEW IF EXISTS CUSTOMER_ANALYTICS.GOLD.customer_360_profile REFRESH;

  -- Update observability metrics
  INSERT INTO CUSTOMER_ANALYTICS.BRONZE.pipeline_run_metadata (
    pipeline_name,
    run_timestamp,
    status,
    records_processed
  )
  SELECT
    'complete_pipeline',
    CURRENT_TIMESTAMP(),
    'SUCCESS',
    (SELECT COUNT(*) FROM CUSTOMER_ANALYTICS.BRONZE.BRONZE_TRANSACTIONS);
END;

-- ============================================================================
-- STEP 6: Create Stream for Incremental Processing
-- ============================================================================

USE SCHEMA BRONZE;

-- Create stream on transactions table to track changes
CREATE OR REPLACE STREAM bronze_transactions_stream
  ON TABLE BRONZE_TRANSACTIONS
  COMMENT = 'Track new transactions for incremental processing';

-- ============================================================================
-- STEP 7: Create Task for Incremental Updates (Stream Consumer)
-- ============================================================================

CREATE OR REPLACE TASK BRONZE.process_incremental_transactions
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '5 MINUTE'  -- Run every 5 minutes
  WHEN SYSTEM$STREAM_HAS_DATA('bronze_transactions_stream')  -- Only run if new data
  COMMENT = 'Process new transactions incrementally using Stream'
AS
  -- Insert new transactions into Gold layer
  MERGE INTO CUSTOMER_ANALYTICS.GOLD.fct_transactions AS target
  USING CUSTOMER_ANALYTICS.BRONZE.bronze_transactions_stream AS source
  ON target.transaction_id = source.transaction_id
  WHEN MATCHED AND source.METADATA$ACTION = 'DELETE' THEN DELETE
  WHEN MATCHED AND source.METADATA$ACTION = 'INSERT' THEN UPDATE SET
    target.transaction_amount = source.transaction_amount,
    target.transaction_date = source.transaction_date,
    target.merchant_category = source.merchant_category,
    target.status = source.status
  WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' THEN INSERT (
    transaction_id,
    customer_id,
    transaction_date,
    transaction_amount,
    merchant_name,
    merchant_category,
    channel,
    status
  ) VALUES (
    source.transaction_id,
    source.customer_id,
    source.transaction_date,
    source.transaction_amount,
    source.merchant_name,
    source.merchant_category,
    source.channel,
    source.status
  );

-- ============================================================================
-- STEP 8: Resume Tasks (Start the Pipeline)
-- ============================================================================

-- Important: Resume tasks in reverse dependency order (child before parent)
ALTER TASK GOLD.refresh_analytics_views RESUME;
ALTER TASK GOLD.train_churn_model RESUME;
ALTER TASK GOLD.run_dbt_transformations RESUME;
ALTER TASK BRONZE.generate_transaction_data RESUME;
ALTER TASK BRONZE.generate_customer_data RESUME;  -- Root task started last

-- Resume incremental processing task
ALTER TASK BRONZE.process_incremental_transactions RESUME;

-- ============================================================================
-- STEP 9: View Task Status
-- ============================================================================

-- Show all tasks
SHOW TASKS IN DATABASE CUSTOMER_ANALYTICS;

-- Check task state
SELECT
    name,
    database_name,
    schema_name,
    state,
    schedule,
    warehouse,
    predecessors,
    created_on,
    last_committed_on
FROM TABLE(INFORMATION_SCHEMA.TASKS())
WHERE database_name = 'CUSTOMER_ANALYTICS'
ORDER BY created_on;

-- ============================================================================
-- STEP 10: Monitor Task Execution History
-- ============================================================================

-- View recent task runs
SELECT
    name,
    database_name,
    schema_name,
    state,
    scheduled_time,
    query_start_time,
    completed_time,
    DATEDIFF('second', query_start_time, completed_time) AS duration_seconds,
    return_value,
    error_code,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP())
))
WHERE database_name = 'CUSTOMER_ANALYTICS'
ORDER BY scheduled_time DESC;

-- ============================================================================
-- STEP 11: Manual Task Execution (For Testing)
-- ============================================================================

-- Execute a specific task manually (without waiting for schedule)
-- EXECUTE TASK BRONZE.generate_customer_data;

-- Execute entire pipeline manually
-- EXECUTE TASK BRONZE.generate_customer_data;  -- Will trigger all downstream tasks

-- ============================================================================
-- STEP 12: Monitor Stream Status
-- ============================================================================

-- Check if stream has data
SELECT SYSTEM$STREAM_HAS_DATA('bronze_transactions_stream');

-- View stream statistics
SHOW STREAMS IN SCHEMA BRONZE;

-- Preview stream contents
SELECT *
FROM bronze_transactions_stream
LIMIT 10;

-- Count records in stream
SELECT
    COUNT(*) AS total_changes,
    SUM(CASE WHEN METADATA$ACTION = 'INSERT' THEN 1 ELSE 0 END) AS inserts,
    SUM(CASE WHEN METADATA$ACTION = 'DELETE' THEN 1 ELSE 0 END) AS deletes
FROM bronze_transactions_stream;

-- ============================================================================
-- STEP 13: Pause/Suspend Tasks (If Needed)
-- ============================================================================

-- Suspend all pipeline tasks (stops execution)
/*
ALTER TASK BRONZE.generate_customer_data SUSPEND;
ALTER TASK BRONZE.generate_transaction_data SUSPEND;
ALTER TASK GOLD.run_dbt_transformations SUSPEND;
ALTER TASK GOLD.train_churn_model SUSPEND;
ALTER TASK GOLD.refresh_analytics_views SUSPEND;
ALTER TASK BRONZE.process_incremental_transactions SUSPEND;
*/

-- ============================================================================
-- STEP 14: Delete Tasks (If Cleanup Needed)
-- ============================================================================

-- WARNING: This will delete all tasks
/*
DROP TASK IF EXISTS GOLD.refresh_analytics_views;
DROP TASK IF EXISTS GOLD.train_churn_model;
DROP TASK IF EXISTS GOLD.run_dbt_transformations;
DROP TASK IF EXISTS BRONZE.generate_transaction_data;
DROP TASK IF EXISTS BRONZE.generate_customer_data;
DROP TASK IF EXISTS BRONZE.process_incremental_transactions;
DROP STREAM IF EXISTS BRONZE.bronze_transactions_stream;
*/

-- ============================================================================
-- STEP 15: Cost Monitoring
-- ============================================================================

-- Check warehouse usage by tasks
SELECT
    warehouse_name,
    SUM(credits_used) AS total_credits_used,
    SUM(credits_used_cloud_services) AS cloud_services_credits,
    COUNT(*) AS execution_count
FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
    DATEADD('day', -7, CURRENT_TIMESTAMP())
))
WHERE warehouse_name = 'COMPUTE_WH'
GROUP BY warehouse_name;

-- ============================================================================
-- STEP 16: Error Handling and Notifications (Optional)
-- ============================================================================

-- Create task to check for failures and send notifications
/*
CREATE OR REPLACE TASK BRONZE.check_pipeline_failures
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '15 MINUTE'
  COMMENT = 'Monitor pipeline for failures and send alerts'
AS
BEGIN
  -- Check for failed tasks in last hour
  LET failed_tasks VARCHAR := (
    SELECT LISTAGG(name, ', ')
    FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
        SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
    ))
    WHERE state = 'FAILED'
      AND database_name = 'CUSTOMER_ANALYTICS'
  );

  -- If failures detected, log to observability table
  IF (failed_tasks IS NOT NULL) THEN
    INSERT INTO BRONZE.pipeline_alerts (
      alert_timestamp,
      alert_type,
      alert_message
    )
    VALUES (
      CURRENT_TIMESTAMP(),
      'TASK_FAILURE',
      'Failed tasks: ' || failed_tasks
    );
  END IF;
END;

ALTER TASK BRONZE.check_pipeline_failures RESUME;
*/

-- ============================================================================
-- Display confirmation
-- ============================================================================

SELECT '✓ Pipeline tasks created and started successfully' AS status;
SELECT 'Monitor with: SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(...))' AS monitoring;
SELECT 'Manual execution: EXECUTE TASK BRONZE.generate_customer_data;' AS manual_run;
