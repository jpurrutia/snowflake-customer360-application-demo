-- ============================================================================
-- Create Database and Schemas for Medallion Architecture
-- ============================================================================
-- Purpose: Set up CUSTOMER_ANALYTICS database with Bronze/Silver/Gold layers
-- Requires: SYSADMIN or ACCOUNTADMIN role
-- ============================================================================

-- Use appropriate role
USE ROLE SYSADMIN;

-- Set default warehouse (assumes COMPUTE_WH exists in trial account)
USE WAREHOUSE COMPUTE_WH;

-- ============================================================================
-- Create Database
-- ============================================================================

CREATE DATABASE IF NOT EXISTS CUSTOMER_ANALYTICS
    COMMENT = 'Customer 360 Analytics Platform - Post-acquisition credit card customer integration and analysis';

USE DATABASE CUSTOMER_ANALYTICS;

-- ============================================================================
-- Create Schemas (Medallion Architecture)
-- ============================================================================

-- BRONZE Schema: Raw data landing zone
CREATE SCHEMA IF NOT EXISTS BRONZE
    COMMENT = 'Bronze Layer: Raw data ingested from S3. Minimal transformation, preserves source format with metadata columns (_loaded_at, _source_file)';

-- SILVER Schema: Cleaned and deduplicated data
CREATE SCHEMA IF NOT EXISTS SILVER
    COMMENT = 'Silver Layer: Cleaned, deduplicated, and standardized data. Business logic applied, data types enforced, ready for modeling';

-- GOLD Schema: Analytics-ready dimensional models
CREATE SCHEMA IF NOT EXISTS GOLD
    COMMENT = 'Gold Layer: Analytics-ready star schema with facts and dimensions. Optimized for BI tools and end-user queries';

-- OBSERVABILITY Schema: Pipeline metadata and data quality metrics
CREATE SCHEMA IF NOT EXISTS OBSERVABILITY
    COMMENT = 'Observability Layer: Pipeline run metadata, data quality metrics, and monitoring tables for operational visibility';

-- ============================================================================
-- Verify Schema Creation
-- ============================================================================

SHOW SCHEMAS IN DATABASE CUSTOMER_ANALYTICS;

-- Display confirmation
SELECT '✓ Database and Schemas Created Successfully' AS STATUS;
SELECT 'Database: CUSTOMER_ANALYTICS' AS CREATED;
SELECT 'Schemas: BRONZE, SILVER, GOLD, OBSERVABILITY' AS CREATED;
