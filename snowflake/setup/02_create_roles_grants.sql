-- ============================================================================
-- Create Roles and Grant Permissions (RBAC)
-- ============================================================================
-- Purpose: Set up role-based access control for Data Engineers, Marketing Managers, and Data Analysts
-- Requires: ACCOUNTADMIN or SECURITYADMIN role
-- ============================================================================

-- Use ACCOUNTADMIN for role management
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CUSTOMER_ANALYTICS;

-- ============================================================================
-- Create Custom Roles
-- ============================================================================

-- DATA_ENGINEER Role: Full access to all layers for development and ETL
CREATE ROLE IF NOT EXISTS DATA_ENGINEER
    COMMENT = 'Data Engineers: Full access to all schemas and layers. Can create, modify, and delete objects. Responsible for ETL pipelines and data transformations.';

-- MARKETING_MANAGER Role: Read-only access to GOLD layer for business insights
CREATE ROLE IF NOT EXISTS MARKETING_MANAGER
    COMMENT = 'Marketing Managers: Read-only access to GOLD schema for business intelligence and reporting. No access to raw/intermediate data layers.';

-- DATA_ANALYST Role: Read-only access to all layers for analysis and troubleshooting
CREATE ROLE IF NOT EXISTS DATA_ANALYST
    COMMENT = 'Data Analysts: Read-only access to all schemas (BRONZE, SILVER, GOLD) for comprehensive analysis, data validation, and troubleshooting.';

-- ============================================================================
-- Grant Hierarchy (Inherit from SYSADMIN)
-- ============================================================================

GRANT ROLE DATA_ENGINEER TO ROLE SYSADMIN;
GRANT ROLE MARKETING_MANAGER TO ROLE SYSADMIN;
GRANT ROLE DATA_ANALYST TO ROLE SYSADMIN;

-- ============================================================================
-- DATA_ENGINEER Role Grants
-- ============================================================================

-- Database access
GRANT USAGE ON DATABASE CUSTOMER_ANALYTICS TO ROLE DATA_ENGINEER;

-- Schema access (all schemas)
GRANT USAGE ON SCHEMA CUSTOMER_ANALYTICS.BRONZE TO ROLE DATA_ENGINEER;
GRANT USAGE ON SCHEMA CUSTOMER_ANALYTICS.SILVER TO ROLE DATA_ENGINEER;
GRANT USAGE ON SCHEMA CUSTOMER_ANALYTICS.GOLD TO ROLE DATA_ENGINEER;
GRANT USAGE ON SCHEMA CUSTOMER_ANALYTICS.OBSERVABILITY TO ROLE DATA_ENGINEER;

-- Full privileges on all schemas
GRANT ALL PRIVILEGES ON SCHEMA CUSTOMER_ANALYTICS.BRONZE TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON SCHEMA CUSTOMER_ANALYTICS.SILVER TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON SCHEMA CUSTOMER_ANALYTICS.GOLD TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON SCHEMA CUSTOMER_ANALYTICS.OBSERVABILITY TO ROLE DATA_ENGINEER;

-- Full privileges on existing and future tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA CUSTOMER_ANALYTICS.BRONZE TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA CUSTOMER_ANALYTICS.SILVER TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA CUSTOMER_ANALYTICS.GOLD TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA CUSTOMER_ANALYTICS.OBSERVABILITY TO ROLE DATA_ENGINEER;

GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA CUSTOMER_ANALYTICS.BRONZE TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA CUSTOMER_ANALYTICS.SILVER TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA CUSTOMER_ANALYTICS.GOLD TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA CUSTOMER_ANALYTICS.OBSERVABILITY TO ROLE DATA_ENGINEER;

-- Warehouse access
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE DATA_ENGINEER;

-- Storage integration access (will be created in next iteration)
-- GRANT USAGE ON INTEGRATION s3_customer_analytics_integration TO ROLE DATA_ENGINEER;

-- ============================================================================
-- MARKETING_MANAGER Role Grants (GOLD Schema Only)
-- ============================================================================

-- Database access
GRANT USAGE ON DATABASE CUSTOMER_ANALYTICS TO ROLE MARKETING_MANAGER;

-- GOLD schema access only
GRANT USAGE ON SCHEMA CUSTOMER_ANALYTICS.GOLD TO ROLE MARKETING_MANAGER;

-- Read-only access to GOLD tables
GRANT SELECT ON ALL TABLES IN SCHEMA CUSTOMER_ANALYTICS.GOLD TO ROLE MARKETING_MANAGER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CUSTOMER_ANALYTICS.GOLD TO ROLE MARKETING_MANAGER;

-- Warehouse access
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE MARKETING_MANAGER;

-- Note: No access to BRONZE or SILVER schemas (enforced by absence of grants)

-- ============================================================================
-- DATA_ANALYST Role Grants (Read-Only All Schemas)
-- ============================================================================

-- Database access
GRANT USAGE ON DATABASE CUSTOMER_ANALYTICS TO ROLE DATA_ANALYST;

-- Schema access (all schemas)
GRANT USAGE ON SCHEMA CUSTOMER_ANALYTICS.BRONZE TO ROLE DATA_ANALYST;
GRANT USAGE ON SCHEMA CUSTOMER_ANALYTICS.SILVER TO ROLE DATA_ANALYST;
GRANT USAGE ON SCHEMA CUSTOMER_ANALYTICS.GOLD TO ROLE DATA_ANALYST;
GRANT USAGE ON SCHEMA CUSTOMER_ANALYTICS.OBSERVABILITY TO ROLE DATA_ANALYST;

-- Read-only access to all tables
GRANT SELECT ON ALL TABLES IN SCHEMA CUSTOMER_ANALYTICS.BRONZE TO ROLE DATA_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA CUSTOMER_ANALYTICS.SILVER TO ROLE DATA_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA CUSTOMER_ANALYTICS.GOLD TO ROLE DATA_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA CUSTOMER_ANALYTICS.OBSERVABILITY TO ROLE DATA_ANALYST;

GRANT SELECT ON FUTURE TABLES IN SCHEMA CUSTOMER_ANALYTICS.BRONZE TO ROLE DATA_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CUSTOMER_ANALYTICS.SILVER TO ROLE DATA_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CUSTOMER_ANALYTICS.GOLD TO ROLE DATA_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CUSTOMER_ANALYTICS.OBSERVABILITY TO ROLE DATA_ANALYST;

-- Warehouse access
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE DATA_ANALYST;

-- ============================================================================
-- Verify Role Creation and Grants
-- ============================================================================

-- Show all custom roles
SHOW ROLES LIKE '%ENGINEER%';
SHOW ROLES LIKE '%MANAGER%';
SHOW ROLES LIKE '%ANALYST%';

-- Show grants for each role
SHOW GRANTS TO ROLE DATA_ENGINEER;
SHOW GRANTS TO ROLE MARKETING_MANAGER;
SHOW GRANTS TO ROLE DATA_ANALYST;

-- Display confirmation
SELECT '✓ Roles and Grants Created Successfully' AS STATUS;
SELECT 'Roles: DATA_ENGINEER, MARKETING_MANAGER, DATA_ANALYST' AS CREATED;
SELECT 'RBAC configured with appropriate permissions' AS CONFIRMATION;
