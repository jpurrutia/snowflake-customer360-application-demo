{{
    config(
        materialized='incremental',
        unique_key='transaction_key',
        schema='gold',
        cluster_by=['transaction_date'],
        tags=['gold', 'fact', 'transactions']
    )
}}

{#
============================================================================
Gold Layer: Transaction Fact Table
============================================================================
Purpose: Central fact table for transaction analysis (star schema)

Grain: One row per transaction
Row Count: ~13.5 million
Materialization: Incremental for performance

Foreign Keys:
- customer_key → dim_customer (current version at time of transaction)
- date_key → dim_date
- merchant_category_key → dim_merchant_category

Measures:
- transaction_amount (additive)
- transaction_count (COUNT(*))

Clustering: By transaction_date for time-series query performance

Usage:
  -- Star schema query
  SELECT
      c.customer_segment,
      cat.category_group,
      d.year,
      d.month,
      COUNT(*) AS txn_count,
      SUM(f.transaction_amount) AS total_amount
  FROM fct_transactions f
  JOIN dim_customer c ON f.customer_key = c.customer_key
  JOIN dim_merchant_category cat ON f.merchant_category_key = cat.category_key
  JOIN dim_date d ON f.date_key = d.date_key
  GROUP BY 1, 2, 3, 4;
============================================================================
#}

WITH transactions AS (
    SELECT
        transaction_id,
        customer_id,
        transaction_date,
        transaction_amount,
        merchant_name,
        merchant_category,
        channel,
        status,
        ingestion_timestamp,
        source_file
    FROM {{ ref('stg_transactions') }}

    {% if is_incremental() %}
        -- Only process new records since last dbt run
        WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

enriched_transactions AS (
    SELECT
        t.transaction_id,

        -- Join to current customer dimension
        c.customer_key,

        -- Generate date key (YYYYMMDD format)
        TO_NUMBER(TO_CHAR(t.transaction_date, 'YYYYMMDD')) AS date_key,

        -- Join to merchant category dimension
        cat.category_key AS merchant_category_key,

        -- Transaction attributes
        t.transaction_date,
        t.transaction_amount,
        t.merchant_name,
        t.channel,
        t.status,

        -- Metadata
        t.ingestion_timestamp,
        t.source_file

    FROM transactions t

    -- LEFT JOIN to handle potential orphan transactions (shouldn't happen but defensive)
    LEFT JOIN {{ ref('dim_customer') }} c
        ON t.customer_id = c.customer_id
        AND c.is_current = TRUE  -- Join to current version only

    -- LEFT JOIN to merchant category dimension
    LEFT JOIN {{ ref('dim_merchant_category') }} cat
        ON t.merchant_category = cat.category_name
)

SELECT
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['transaction_id']) }} AS transaction_key,

    -- Natural key
    transaction_id,

    -- Foreign keys (dimensional model)
    customer_key,
    date_key,
    merchant_category_key,

    -- Degenerate dimensions (attributes stored in fact table)
    transaction_date,
    merchant_name,
    channel,
    status,

    -- Measures
    transaction_amount,

    -- Metadata
    ingestion_timestamp,
    source_file

FROM enriched_transactions

-- Quality filter: Exclude transactions with missing FK (shouldn't happen)
WHERE customer_key IS NOT NULL
  AND merchant_category_key IS NOT NULL
