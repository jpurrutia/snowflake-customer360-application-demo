-- ============================================================================
-- Deploy Semantic Model to Snowflake Stage
-- ============================================================================
-- Purpose: Upload semantic model YAML to Snowflake stage for Cortex Analyst
-- Prerequisites: cortex_analyst_setup.sql must be run first
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE SEMANTIC_MODELS;
USE SCHEMA DEFINITIONS;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================================
-- Upload semantic model from local file system
-- ============================================================================
-- Note: This command must be run from your local machine using SnowSQL
--
-- Usage:
--   snowsql -c default -f deploy_semantic_model.sql
--
-- Or manually run:
--   PUT file:///path/to/snowflake-panel-demo/semantic_models/customer_analytics.yaml
--       @SEMANTIC_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- ============================================================================

-- Remove existing semantic model file (if any)
REMOVE @SEMANTIC_STAGE/customer_analytics.yaml;

-- Upload new semantic model
-- NOTE: Update the file path to match your local directory
PUT file://../../semantic_models/customer_analytics.yaml
    @SEMANTIC_STAGE
    AUTO_COMPRESS=FALSE
    OVERWRITE=TRUE;

-- Verify upload
LIST @SEMANTIC_STAGE;

-- Verify file contents
SELECT $1 FROM @SEMANTIC_STAGE/customer_analytics.yaml LIMIT 10;

SELECT 'Semantic model deployed successfully!' AS status;

-- ============================================================================
-- Validation: Test Cortex Analyst with sample question
-- ============================================================================

-- Test query (uncomment to test)
-- SELECT SNOWFLAKE.CORTEX.COMPLETE(
--     'analyst',
--     '[{"role": "user", "content": [{"type": "text", "text": "How many customers are in each segment?"}]}]',
--     {
--         'semantic_model_file': '@SEMANTIC_MODELS.DEFINITIONS.SEMANTIC_STAGE/customer_analytics.yaml'
--     }
-- ) AS response;

-- ============================================================================
-- Troubleshooting
-- ============================================================================
-- If deployment fails:
--
-- 1. Check that semantic model file exists:
--    ls -la semantic_models/customer_analytics.yaml
--
-- 2. Verify you're running from project root directory
--
-- 3. Verify stage exists and has DIRECTORY enabled:
--    SHOW STAGES IN SCHEMA SEMANTIC_MODELS.DEFINITIONS;
--
-- 4. Check permissions:
--    SHOW GRANTS TO ROLE ACCOUNTADMIN;
--
-- 5. Manually specify full path:
--    PUT file:///Users/jpurrutia/projects/snowflake-panel-demo/semantic_models/customer_analytics.yaml
--        @SEMANTIC_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- ============================================================================
