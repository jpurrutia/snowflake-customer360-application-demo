-- ============================================================================
-- Snowflake Environment Check
-- ============================================================================
-- Purpose: Verify Snowflake connectivity and display current environment
-- Run this first to ensure you're connected to the right account/role
-- ============================================================================

-- Display current context
SELECT 'Current Environment' AS INFO_TYPE;
SELECT
    CURRENT_ACCOUNT() AS ACCOUNT,
    CURRENT_USER() AS USER,
    CURRENT_ROLE() AS ROLE,
    CURRENT_WAREHOUSE() AS WAREHOUSE,
    CURRENT_DATABASE() AS DATABASE,
    CURRENT_SCHEMA() AS SCHEMA,
    CURRENT_REGION() AS REGION;

-- Show available warehouses
SELECT 'Available Warehouses' AS INFO_TYPE;
SHOW WAREHOUSES;

-- Show current role grants
SELECT 'Current Role Grants' AS INFO_TYPE;
SHOW GRANTS TO ROLE IDENTIFIER(CURRENT_ROLE());

-- Show databases
SELECT 'Available Databases' AS INFO_TYPE;
SHOW DATABASES;

-- Check if we have ACCOUNTADMIN privileges (needed for setup)
SELECT 'Role Hierarchy Check' AS INFO_TYPE;
SELECT
    CASE
        WHEN CURRENT_ROLE() = 'ACCOUNTADMIN' THEN '✓ Running as ACCOUNTADMIN - Full setup possible'
        WHEN CURRENT_ROLE() = 'SYSADMIN' THEN '⚠ Running as SYSADMIN - Limited setup possible'
        ELSE '✗ Running as ' || CURRENT_ROLE() || ' - May need ACCOUNTADMIN or SYSADMIN'
    END AS STATUS_MESSAGE;

-- Environment check complete
SELECT '✓ Environment Check Complete' AS STATUS;
SELECT 'Ready to proceed with Snowflake foundation setup' AS NEXT_STEP;
