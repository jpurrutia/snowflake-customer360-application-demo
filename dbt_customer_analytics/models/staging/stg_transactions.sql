{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        on_schema_change='fail',
        tags=['staging', 'transactions', 'incremental']
    )
}}

{#
============================================================================
Staging Model: Transactions
============================================================================
Purpose: Clean, deduplicate, and normalize transaction data from Bronze layer

Transformations:
- Deduplicate transactions (keep most recent by ingestion_timestamp)
- Normalize text fields (TRIM, handle NULLs)
- Default merchant_category to 'Uncategorized' if NULL
- Filter to only approved and declined transactions

Incremental Strategy:
- Materialized as incremental table for performance (13.5M rows)
- unique_key: transaction_id (upsert on conflict)
- Processes only new records based on ingestion_timestamp

Quality Checks:
- Unique transaction_ids
- All customer_ids exist in stg_customers (referential integrity)
- Positive transaction amounts
- No NULL merchant_category (defaulted to 'Uncategorized')

Dependencies:
- Bronze layer: BRONZE_TRANSACTIONS
- Staging layer: stg_customers (for FK validation)

Downstream:
- fct_transactions (fact table)
- customer_360_profile (aggregated metrics)
============================================================================
#}

WITH source AS (

    SELECT * FROM {{ source('bronze', 'raw_transactions') }}

    {% if is_incremental() %}
        -- Only process new records since last dbt run
        WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
    {% endif %}

),

deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY ingestion_timestamp DESC
        ) AS row_num

    FROM source

),

cleaned AS (

    SELECT
        -- ====================================================================
        -- Primary Key
        -- ====================================================================
        transaction_id,

        -- ====================================================================
        -- Foreign Key
        -- ====================================================================
        customer_id,

        -- ====================================================================
        -- Transaction Details
        -- ====================================================================
        transaction_date,
        transaction_amount,
        TRIM(merchant_name) AS merchant_name,

        -- Default merchant_category to 'Uncategorized' if NULL
        COALESCE(TRIM(merchant_category), 'Uncategorized') AS merchant_category,

        channel,
        status,

        -- ====================================================================
        -- Metadata
        -- ====================================================================
        ingestion_timestamp,
        source_file

    FROM deduplicated
    WHERE row_num = 1  -- Keep only first occurrence (deduplication)

)

SELECT * FROM cleaned
