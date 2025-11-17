-- ============================================================================
-- Cortex Analyst Setup Script
-- ============================================================================
-- Purpose: Configure Snowflake infrastructure for Cortex Analyst integration
-- Prerequisites: ACCOUNTADMIN role or sufficient privileges
-- Documentation: https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- 1. Create Database and Schema for Semantic Models
-- ============================================================================

CREATE DATABASE IF NOT EXISTS SEMANTIC_MODELS
    COMMENT = 'Storage for Cortex Analyst semantic model definitions';

CREATE SCHEMA IF NOT EXISTS SEMANTIC_MODELS.DEFINITIONS
    COMMENT = 'Schema containing semantic model YAML files and stages';

USE DATABASE SEMANTIC_MODELS;
USE SCHEMA DEFINITIONS;

-- ============================================================================
-- 2. Create Stage for Semantic Model YAML Files
-- ============================================================================

CREATE STAGE IF NOT EXISTS SEMANTIC_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for storing semantic model YAML files used by Cortex Analyst';

-- ============================================================================
-- 3. Create or Verify Cortex Analyst Role
-- ============================================================================
-- Note: Cortex Analyst requires either CORTEX_USER or a custom role with
-- CORTEX_ANALYST_USER privilege

-- Check if CORTEX_USER role exists (available in some Snowflake accounts)
SHOW ROLES LIKE 'CORTEX_USER';

-- If CORTEX_USER doesn't exist, create a custom role
CREATE ROLE IF NOT EXISTS CORTEX_ANALYST_ROLE
    COMMENT = 'Role for using Cortex Analyst features';

-- Grant Cortex Analyst usage privilege
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE CORTEX_ANALYST_ROLE;

-- Grant necessary database and schema permissions
GRANT USAGE ON DATABASE SEMANTIC_MODELS TO ROLE CORTEX_ANALYST_ROLE;
GRANT USAGE ON SCHEMA SEMANTIC_MODELS.DEFINITIONS TO ROLE CORTEX_ANALYST_ROLE;
GRANT READ ON STAGE SEMANTIC_MODELS.DEFINITIONS.SEMANTIC_STAGE TO ROLE CORTEX_ANALYST_ROLE;

-- Grant access to customer analytics data
GRANT USAGE ON DATABASE CUSTOMER_ANALYTICS TO ROLE CORTEX_ANALYST_ROLE;
GRANT USAGE ON SCHEMA CUSTOMER_ANALYTICS.GOLD TO ROLE CORTEX_ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA CUSTOMER_ANALYTICS.GOLD TO ROLE CORTEX_ANALYST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CUSTOMER_ANALYTICS.GOLD TO ROLE CORTEX_ANALYST_ROLE;

-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE CORTEX_ANALYST_ROLE;

-- ============================================================================
-- 4. Grant Cortex Analyst Role to Users
-- ============================================================================
-- Replace 'jpurrutia' with your username or grant to appropriate users

GRANT ROLE CORTEX_ANALYST_ROLE TO USER jpurrutia;

-- ============================================================================
-- 5. Grant Permissions to DATA_ANALYST Role (for Streamlit app)
-- ============================================================================

GRANT USAGE ON DATABASE SEMANTIC_MODELS TO ROLE DATA_ANALYST;
GRANT USAGE ON SCHEMA SEMANTIC_MODELS.DEFINITIONS TO ROLE DATA_ANALYST;
GRANT READ ON STAGE SEMANTIC_MODELS.DEFINITIONS.SEMANTIC_STAGE TO ROLE DATA_ANALYST;

-- Grant Cortex privileges to DATA_ANALYST role
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE DATA_ANALYST;

-- ============================================================================
-- 6. Validation Queries
-- ============================================================================

-- Verify database and schema exist
SHOW DATABASES LIKE 'SEMANTIC_MODELS';
SHOW SCHEMAS IN DATABASE SEMANTIC_MODELS;

-- Verify stage exists and is directory-enabled
SHOW STAGES IN SCHEMA SEMANTIC_MODELS.DEFINITIONS;

-- Verify role grants
SHOW GRANTS TO ROLE CORTEX_ANALYST_ROLE;
SHOW GRANTS TO ROLE DATA_ANALYST;

-- List files in stage (should be empty initially)
LIST @SEMANTIC_MODELS.DEFINITIONS.SEMANTIC_STAGE;

SELECT 'Cortex Analyst infrastructure setup complete!' AS status;

-- ============================================================================
-- Next Steps:
-- ============================================================================
-- 1. Create semantic model YAML file for customer analytics
-- 2. Upload YAML file to SEMANTIC_STAGE:
--    PUT file:///path/to/customer_analytics.yaml @SEMANTIC_STAGE AUTO_COMPRESS=FALSE;
-- 3. Test Cortex Analyst with sample queries:
--    SELECT SNOWFLAKE.CORTEX.COMPLETE('analyst', 'What are our top customers?',
--           OBJECT_CONSTRUCT('semantic_model_file', '@SEMANTIC_STAGE/customer_analytics.yaml'));
-- ============================================================================
