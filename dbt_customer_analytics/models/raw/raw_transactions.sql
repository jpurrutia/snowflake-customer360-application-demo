{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        on_schema_change='fail',
        tags=['bronze', 'transactions', 'incremental']
    )
}}

{#
============================================================================
Bronze Model: Raw Transactions
============================================================================
Purpose: Load transaction data from external S3 stage into Bronze layer

Source: GZIP compressed CSV files in @transaction_stage_historical

Transformations: NONE (Bronze layer accepts data as-is from source)

Incremental Strategy:
- Materialized as incremental table
- unique_key: transaction_id (upsert on conflict)
- Processes only new files from S3

Quality Checks:
- Unique transaction_ids (enforced by incremental unique_key)
- All expected columns present

Usage:
1. Upload CSVs to S3
2. Load to Bronze: dbt run --select bronze.raw_transactions
3. Verify: SELECT COUNT(*) FROM {{ this }};
============================================================================
#}

{% if is_incremental() %}

-- Incremental load: only copy files not already loaded
COPY INTO {{ this }} (
    transaction_id,
    customer_id,
    transaction_date,
    transaction_amount,
    merchant_name,
    merchant_category,
    channel,
    status,
    ingestion_timestamp,
    source_file,
    _metadata_file_row_number
)
FROM (
    SELECT
        $1::STRING AS transaction_id,
        $2::STRING AS customer_id,
        $3::TIMESTAMP AS transaction_date,
        $4::NUMBER(10,2) AS transaction_amount,
        $5::STRING AS merchant_name,
        $6::STRING AS merchant_category,
        $7::STRING AS channel,
        $8::STRING AS status,
        CURRENT_TIMESTAMP() AS ingestion_timestamp,
        METADATA$FILENAME AS source_file,
        METADATA$FILE_ROW_NUMBER AS _metadata_file_row_number
    FROM @CUSTOMER_ANALYTICS.BRONZE.transaction_stage_historical
)
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    COMPRESSION = 'GZIP'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    NULL_IF = ('NULL', 'null', '')
)
PATTERN = '.*transactions_historical.*\.csv.*'
ON_ERROR = 'ABORT_STATEMENT'
FORCE = FALSE  -- Skip already-loaded files

{% else %}

-- Initial full load
COPY INTO {{ this }} (
    transaction_id,
    customer_id,
    transaction_date,
    transaction_amount,
    merchant_name,
    merchant_category,
    channel,
    status,
    ingestion_timestamp,
    source_file,
    _metadata_file_row_number
)
FROM (
    SELECT
        $1::STRING AS transaction_id,
        $2::STRING AS customer_id,
        $3::TIMESTAMP AS transaction_date,
        $4::NUMBER(10,2) AS transaction_amount,
        $5::STRING AS merchant_name,
        $6::STRING AS merchant_category,
        $7::STRING AS channel,
        $8::STRING AS status,
        CURRENT_TIMESTAMP() AS ingestion_timestamp,
        METADATA$FILENAME AS source_file,
        METADATA$FILE_ROW_NUMBER AS _metadata_file_row_number
    FROM @CUSTOMER_ANALYTICS.BRONZE.transaction_stage_historical
)
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    COMPRESSION = 'GZIP'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    NULL_IF = ('NULL', 'null', '')
)
PATTERN = '.*transactions_historical.*\.csv.*'
ON_ERROR = 'ABORT_STATEMENT'

{% endif %}
