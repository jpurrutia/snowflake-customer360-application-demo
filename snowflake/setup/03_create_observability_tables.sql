-- ============================================================================
-- Create Observability and Monitoring Tables
-- ============================================================================
-- Purpose: Set up tables for pipeline metadata, data quality tracking, and operational monitoring
-- Requires: DATA_ENGINEER role or SYSADMIN
-- ============================================================================

USE ROLE DATA_ENGINEER;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA OBSERVABILITY;

-- ============================================================================
-- Pipeline Run Metadata Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS PIPELINE_RUN_METADATA (
    run_id STRING PRIMARY KEY,
    run_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    pipeline_name STRING NOT NULL,
    status STRING NOT NULL,  -- STARTED, SUCCESS, FAILED, RUNNING
    models_run INT DEFAULT 0,
    models_failed INT DEFAULT 0,
    error_message STRING,
    start_time TIMESTAMP_NTZ,
    end_time TIMESTAMP_NTZ,
    duration_seconds INT,
    triggered_by STRING,  -- MANUAL, SCHEDULED, EVENT
    dbt_version STRING,
    git_commit_sha STRING,
    metadata VARIANT  -- JSON for additional custom metadata
)
COMMENT = 'Tracks all pipeline runs (dbt, data ingestion, etc.) with status and error details for operational monitoring';

-- ============================================================================
-- Data Quality Metrics Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS DATA_QUALITY_METRICS (
    check_id STRING DEFAULT UUID_STRING(),
    run_id STRING NOT NULL,
    run_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    layer STRING NOT NULL,  -- bronze, silver, gold
    table_name STRING NOT NULL,
    check_type STRING NOT NULL,  -- duplicate, null, referential, range, schema, custom
    check_description STRING,
    records_checked INT NOT NULL,
    records_failed INT DEFAULT 0,
    failure_rate FLOAT AS (records_failed::FLOAT / NULLIF(records_checked, 0)),
    failure_details VARIANT,  -- JSON with specific failure records or patterns
    threshold_pct FLOAT,  -- Acceptable failure rate threshold
    status STRING AS (
        CASE
            WHEN failure_rate <= COALESCE(threshold_pct, 0.01) THEN 'PASS'
            WHEN failure_rate > COALESCE(threshold_pct, 0.01) AND failure_rate <= 0.05 THEN 'WARNING'
            ELSE 'FAIL'
        END
    ),
    PRIMARY KEY (check_id)
)
COMMENT = 'Tracks data quality checks across all layers with failure rates and detailed error information';

-- ============================================================================
-- Layer Record Counts Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS LAYER_RECORD_COUNTS (
    count_id STRING DEFAULT UUID_STRING(),
    run_id STRING NOT NULL,
    run_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    layer STRING NOT NULL,  -- bronze, silver, gold
    schema_name STRING NOT NULL,
    table_name STRING NOT NULL,
    record_count INT NOT NULL,
    distinct_keys INT,  -- Count of distinct primary/business keys
    null_key_count INT,  -- Count of records with null keys
    duplicate_key_count INT,  -- Count of duplicate keys
    min_timestamp TIMESTAMP_NTZ,  -- Earliest record timestamp
    max_timestamp TIMESTAMP_NTZ,  -- Latest record timestamp
    PRIMARY KEY (count_id)
)
COMMENT = 'Tracks record counts and key statistics for each table in every pipeline run for trend analysis and anomaly detection';

-- ============================================================================
-- Model Execution Log Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS MODEL_EXECUTION_LOG (
    execution_id STRING DEFAULT UUID_STRING(),
    run_id STRING NOT NULL,
    model_name STRING NOT NULL,
    execution_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    status STRING NOT NULL,  -- SUCCESS, FAILED, SKIPPED
    rows_affected INT,
    execution_time_seconds FLOAT,
    warehouse_used STRING,
    credits_used FLOAT,
    error_message STRING,
    metadata VARIANT,
    PRIMARY KEY (execution_id)
)
COMMENT = 'Detailed execution log for each model/transformation with performance metrics and error tracking';

-- ============================================================================
-- Create Views for Common Queries
-- ============================================================================

-- Latest pipeline run status
CREATE OR REPLACE VIEW V_LATEST_PIPELINE_RUNS AS
SELECT
    pipeline_name,
    run_id,
    run_timestamp,
    status,
    models_run,
    models_failed,
    duration_seconds,
    error_message
FROM PIPELINE_RUN_METADATA
QUALIFY ROW_NUMBER() OVER (PARTITION BY pipeline_name ORDER BY run_timestamp DESC) = 1
ORDER BY run_timestamp DESC;

-- Failed data quality checks (last 7 days)
CREATE OR REPLACE VIEW V_RECENT_DQ_FAILURES AS
SELECT
    run_timestamp,
    layer,
    table_name,
    check_type,
    check_description,
    records_checked,
    records_failed,
    failure_rate,
    status
FROM DATA_QUALITY_METRICS
WHERE status IN ('WARNING', 'FAIL')
  AND run_timestamp >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY run_timestamp DESC, failure_rate DESC;

-- Record count trends (daily)
CREATE OR REPLACE VIEW V_RECORD_COUNT_TRENDS AS
SELECT
    DATE_TRUNC('day', run_timestamp) AS run_date,
    layer,
    schema_name,
    table_name,
    AVG(record_count) AS avg_record_count,
    MIN(record_count) AS min_record_count,
    MAX(record_count) AS max_record_count,
    COUNT(DISTINCT run_id) AS num_runs
FROM LAYER_RECORD_COUNTS
GROUP BY DATE_TRUNC('day', run_timestamp), layer, schema_name, table_name
ORDER BY run_date DESC, layer, table_name;

-- ============================================================================
-- Insert Sample Metadata Row (for testing)
-- ============================================================================

INSERT INTO PIPELINE_RUN_METADATA (
    run_id,
    pipeline_name,
    status,
    models_run,
    models_failed,
    start_time,
    end_time,
    duration_seconds,
    triggered_by
)
VALUES (
    'SETUP_TEST_RUN',
    'SNOWFLAKE_FOUNDATION_SETUP',
    'SUCCESS',
    0,
    0,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    1,
    'MANUAL'
);

-- ============================================================================
-- Verify Table Creation
-- ============================================================================

SHOW TABLES IN SCHEMA CUSTOMER_ANALYTICS.OBSERVABILITY;

-- Display row counts
SELECT 'PIPELINE_RUN_METADATA' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM PIPELINE_RUN_METADATA
UNION ALL
SELECT 'DATA_QUALITY_METRICS', COUNT(*) FROM DATA_QUALITY_METRICS
UNION ALL
SELECT 'LAYER_RECORD_COUNTS', COUNT(*) FROM LAYER_RECORD_COUNTS
UNION ALL
SELECT 'MODEL_EXECUTION_LOG', COUNT(*) FROM MODEL_EXECUTION_LOG;

-- Display confirmation
SELECT '✓ Observability Tables Created Successfully' AS STATUS;
SELECT 'Tables: PIPELINE_RUN_METADATA, DATA_QUALITY_METRICS, LAYER_RECORD_COUNTS, MODEL_EXECUTION_LOG' AS CREATED;
SELECT 'Views: V_LATEST_PIPELINE_RUNS, V_RECENT_DQ_FAILURES, V_RECORD_COUNT_TRENDS' AS CREATED;
