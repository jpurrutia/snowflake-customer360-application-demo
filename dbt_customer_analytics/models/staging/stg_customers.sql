{{
    config(
        materialized='view',
        tags=['staging', 'customers']
    )
}}

{#
============================================================================
Staging Model: Customers
============================================================================
Purpose: Clean and normalize raw customer data from Bronze layer

Transformations:
- Normalize text fields (TRIM, case standardization)
- Standardize email addresses (lowercase)
- Standardize state codes (uppercase)
- Preserve all source columns for downstream use

Quality Checks:
- Unique customer_ids
- No NULL critical fields
- Credit limits within expected range
- Email addresses present

Dependencies:
- Bronze layer: BRONZE_CUSTOMERS

Downstream:
- dim_customer (SCD Type 2 dimension)
- customer_360_profile (mart)
============================================================================
#}

WITH source AS (

    SELECT * FROM {{ source('bronze', 'bronze_customers') }}

),

cleaned AS (

    SELECT
        -- ====================================================================
        -- Primary Key
        -- ====================================================================
        customer_id,

        -- ====================================================================
        -- Demographics (normalized)
        -- ====================================================================
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        LOWER(TRIM(email)) AS email,  -- Normalize email to lowercase
        age,
        UPPER(TRIM(state)) AS state,  -- Normalize state to uppercase
        TRIM(city) AS city,
        employment_status,

        -- ====================================================================
        -- Account Details
        -- ====================================================================
        card_type,
        credit_limit,
        account_open_date,
        customer_segment,
        decline_type,

        -- ====================================================================
        -- Metadata (for lineage and auditing)
        -- ====================================================================
        ingestion_timestamp,
        source_file

    FROM source

)

SELECT * FROM cleaned
