-- ============================================================================
-- dbt Native Snowflake Deployment
-- ============================================================================
-- Purpose: Deploy dbt project to run natively in Snowflake
-- Documentation: https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake
-- Requires: Git integration already set up (08_create_git_integration.sql)
-- ============================================================================

USE ROLE DATA_ENGINEER;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA GOLD;

-- ============================================================================
-- STEP 1: Verify Git Repository Integration
-- ============================================================================

-- Check that Git repository is accessible
SHOW GIT REPOSITORIES LIKE 'snowflake_panel_demo_repo';

-- List dbt project files in repository
LS @snowflake_panel_demo_repo/branches/main/dbt_customer_analytics/;

-- ============================================================================
-- STEP 2: Create DBT PROJECT Object
-- ============================================================================

CREATE OR REPLACE DBT PROJECT customer_analytics_dbt
  GIT_REPOSITORY = CUSTOMER_ANALYTICS.GOLD.snowflake_panel_demo_repo
  GIT_BRANCH = 'main'
  FOLDER = 'dbt_customer_analytics'
  COMMENT = 'Customer 360 Analytics dbt project - native Snowflake execution';

-- ============================================================================
-- STEP 3: Verify DBT PROJECT Creation
-- ============================================================================

-- Show dbt projects
SHOW DBT PROJECTS IN SCHEMA CUSTOMER_ANALYTICS.GOLD;

-- Describe the dbt project
DESC DBT PROJECT customer_analytics_dbt;

-- ============================================================================
-- STEP 4: Execute dbt Project - Test Connection
-- ============================================================================

-- Run dbt debug to verify configuration
EXECUTE DBT PROJECT customer_analytics_dbt
  COMMAND = 'debug'
  WAREHOUSE = COMPUTE_WH;

-- ============================================================================
-- STEP 5: Install dbt Dependencies (if using packages)
-- ============================================================================

-- Run dbt deps to install packages defined in packages.yml
EXECUTE DBT PROJECT customer_analytics_dbt
  COMMAND = 'deps'
  WAREHOUSE = COMPUTE_WH;

-- ============================================================================
-- STEP 6: Execute dbt Project - Build Models
-- ============================================================================

-- Run all dbt models
EXECUTE DBT PROJECT customer_analytics_dbt
  COMMAND = 'run'
  WAREHOUSE = COMPUTE_WH;

-- Run with full refresh (rebuilds all incremental models)
-- EXECUTE DBT PROJECT customer_analytics_dbt
--   COMMAND = 'run --full-refresh'
--   WAREHOUSE = COMPUTE_WH;

-- Run specific models
-- EXECUTE DBT PROJECT customer_analytics_dbt
--   COMMAND = 'run --select customer_360_profile'
--   WAREHOUSE = COMPUTE_WH;

-- ============================================================================
-- STEP 7: Execute dbt Tests
-- ============================================================================

-- Run all dbt tests
EXECUTE DBT PROJECT customer_analytics_dbt
  COMMAND = 'test'
  WAREHOUSE = COMPUTE_WH;

-- Run tests for specific models
-- EXECUTE DBT PROJECT customer_analytics_dbt
--   COMMAND = 'test --select customer_segments'
--   WAREHOUSE = COMPUTE_WH;

-- ============================================================================
-- STEP 8: Generate dbt Documentation
-- ============================================================================

-- Generate dbt docs
EXECUTE DBT PROJECT customer_analytics_dbt
  COMMAND = 'docs generate'
  WAREHOUSE = COMPUTE_WH;

-- ============================================================================
-- STEP 9: View Execution History
-- ============================================================================

-- View recent dbt executions
SELECT
    query_id,
    query_text,
    start_time,
    end_time,
    execution_status,
    error_message,
    DATEDIFF('second', start_time, end_time) AS duration_seconds
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE query_text ILIKE '%EXECUTE DBT PROJECT%'
ORDER BY start_time DESC
LIMIT 10;

-- ============================================================================
-- STEP 10: Monitor dbt Execution (Alternative Query)
-- ============================================================================

-- Check latest dbt execution status
SELECT
    query_id,
    database_name,
    schema_name,
    execution_status,
    error_message,
    start_time,
    end_time,
    rows_produced,
    bytes_scanned
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
    DATEADD('hours', -1, CURRENT_TIMESTAMP()),
    CURRENT_TIMESTAMP()
))
WHERE query_text ILIKE '%customer_analytics_dbt%'
ORDER BY start_time DESC
LIMIT 5;

-- ============================================================================
-- STEP 11: Verify dbt Models Created
-- ============================================================================

-- List all tables/views created by dbt in GOLD schema
SHOW TABLES IN SCHEMA CUSTOMER_ANALYTICS.GOLD;
SHOW VIEWS IN SCHEMA CUSTOMER_ANALYTICS.GOLD;

-- Check key dbt models exist
SELECT
    table_name,
    table_type,
    row_count,
    bytes,
    created,
    last_altered
FROM CUSTOMER_ANALYTICS.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'GOLD'
  AND table_name IN (
    'DIM_CUSTOMER',
    'DIM_DATE',
    'FCT_TRANSACTIONS',
    'CUSTOMER_SEGMENTS',
    'CUSTOMER_360_PROFILE',
    'METRIC_CUSTOMER_LTV'
  )
ORDER BY table_name;

-- ============================================================================
-- STEP 12: Grant Permissions on DBT PROJECT
-- ============================================================================

-- Grant execute permissions to roles
GRANT USAGE ON DBT PROJECT customer_analytics_dbt TO ROLE DATA_ENGINEER;
GRANT USAGE ON DBT PROJECT customer_analytics_dbt TO ROLE ACCOUNTADMIN;

-- ============================================================================
-- Common dbt Commands Reference
-- ============================================================================

/*
-- Run all models
EXECUTE DBT PROJECT customer_analytics_dbt COMMAND = 'run';

-- Run with full refresh
EXECUTE DBT PROJECT customer_analytics_dbt COMMAND = 'run --full-refresh';

-- Run specific model
EXECUTE DBT PROJECT customer_analytics_dbt COMMAND = 'run --select customer_360_profile';

-- Run tests
EXECUTE DBT PROJECT customer_analytics_dbt COMMAND = 'test';

-- Run specific test
EXECUTE DBT PROJECT customer_analytics_dbt COMMAND = 'test --select customer_segments';

-- Compile models (doesn't execute, just checks syntax)
EXECUTE DBT PROJECT customer_analytics_dbt COMMAND = 'compile';

-- List models
EXECUTE DBT PROJECT customer_analytics_dbt COMMAND = 'list';

-- Generate documentation
EXECUTE DBT PROJECT customer_analytics_dbt COMMAND = 'docs generate';

-- Debug connection
EXECUTE DBT PROJECT customer_analytics_dbt COMMAND = 'debug';

-- Install packages
EXECUTE DBT PROJECT customer_analytics_dbt COMMAND = 'deps';

-- Run specific tag
EXECUTE DBT PROJECT customer_analytics_dbt COMMAND = 'run --select tag:daily';

-- Run incremental models only
EXECUTE DBT PROJECT customer_analytics_dbt COMMAND = 'run --select config.materialized:incremental';
*/

-- ============================================================================
-- Troubleshooting
-- ============================================================================

/*
-- Issue: "DBT PROJECT not found"
-- Solution: Check Git repository is connected and dbt_customer_analytics folder exists

-- Issue: "dbt compilation error"
-- Solution: Check dbt_project.yml syntax and model SQL syntax

-- Issue: "Warehouse not available"
-- Solution: Ensure COMPUTE_WH warehouse is running and accessible

-- Issue: "Permission denied"
-- Solution: Grant USAGE on DBT PROJECT to appropriate roles

-- View error details from last execution
SELECT error_message
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE query_text ILIKE '%EXECUTE DBT PROJECT%'
  AND execution_status = 'FAIL'
ORDER BY start_time DESC
LIMIT 1;
*/

-- ============================================================================
-- Display confirmation
-- ============================================================================

SELECT '✓ DBT PROJECT customer_analytics_dbt deployed successfully' AS status;
SELECT 'Execute with: EXECUTE DBT PROJECT customer_analytics_dbt COMMAND = ''run'';' AS usage;
