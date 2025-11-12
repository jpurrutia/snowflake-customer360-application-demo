{{
    config(
        materialized='table',
        unique_key='customer_key',
        schema='gold',
        tags=['gold', 'dimension', 'scd_type_2']
    )
}}

{#
============================================================================
Gold Layer: Customer Dimension with SCD Type 2
============================================================================
Purpose: Customer dimension table with Slowly Changing Dimension Type 2
         tracking for card_type and credit_limit changes

SCD Type 2 Attributes (track history):
- card_type: Standard → Premium upgrades/downgrades
- credit_limit: Credit limit increases/decreases

SCD Type 1 Attributes (overwrite, no history):
- first_name, last_name, email, age, state, city
- employment_status, customer_segment, decline_type

Key Design:
- customer_key: Surrogate key (unique for each version)
- customer_id: Natural key (same across all versions)
- valid_from, valid_to: Effective date range
- is_current: Flag for current version

Usage:
  -- Get current customer records
  SELECT * FROM {{ ref('dim_customer') }} WHERE is_current = TRUE

  -- Get customer history
  SELECT * FROM {{ ref('dim_customer') }}
  WHERE customer_id = 'CUST00000001'
  ORDER BY valid_from
============================================================================
#}

{% if is_incremental() %}

{#
============================================================================
INCREMENTAL MODE: Detect and track changes
============================================================================
#}

WITH current_dimension AS (
    -- Get currently active records
    SELECT *
    FROM {{ this }}
    WHERE is_current = TRUE
),

source_data AS (
    -- Get latest data from staging
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        age,
        state,
        city,
        employment_status,
        card_type,
        credit_limit,
        account_open_date,
        customer_segment,
        decline_type
    FROM {{ ref('stg_customers') }}
),

-- Detect changes in SCD Type 2 attributes
changes AS (
    SELECT
        s.customer_id,
        s.card_type AS new_card_type,
        s.credit_limit AS new_credit_limit,
        c.card_type AS old_card_type,
        c.credit_limit AS old_credit_limit,
        c.customer_key AS old_customer_key,
        CASE
            WHEN c.customer_id IS NULL THEN 'NEW'
            WHEN s.card_type != c.card_type OR s.credit_limit != c.credit_limit THEN 'CHANGED'
            ELSE 'NO_CHANGE'
        END AS change_type,
        s.* EXCEPT (customer_id)  -- All other source columns
    FROM source_data s
    LEFT JOIN current_dimension c
        ON s.customer_id = c.customer_id
),

-- Records that need to be expired (marked as historical)
records_to_expire AS (
    SELECT
        old_customer_key AS customer_key,
        CURRENT_DATE() - 1 AS new_valid_to,
        FALSE AS new_is_current
    FROM changes
    WHERE change_type = 'CHANGED'
),

-- Update existing records: Expire old versions
expired_records AS (
    SELECT
        c.customer_key,
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.age,
        c.state,
        c.city,
        c.employment_status,
        c.card_type,
        c.credit_limit,
        c.account_open_date,
        c.customer_segment,
        c.decline_type,
        c.valid_from,
        e.new_valid_to AS valid_to,  -- Set end date
        e.new_is_current AS is_current,  -- Set to FALSE
        c.created_timestamp,
        CURRENT_TIMESTAMP() AS updated_timestamp
    FROM current_dimension c
    INNER JOIN records_to_expire e
        ON c.customer_key = e.customer_key
),

-- New records: New customers or changed SCD Type 2 attributes
new_and_changed_records AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'CURRENT_TIMESTAMP()']) }} AS customer_key,
        customer_id,
        first_name,
        last_name,
        email,
        age,
        state,
        city,
        employment_status,
        new_card_type AS card_type,
        new_credit_limit AS credit_limit,
        account_open_date,
        customer_segment,
        decline_type,
        CURRENT_DATE() AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current,
        CURRENT_TIMESTAMP() AS created_timestamp,
        CURRENT_TIMESTAMP() AS updated_timestamp
    FROM changes
    WHERE change_type IN ('NEW', 'CHANGED')
),

-- Type 1 updates: Update attributes that don't track history
type_1_updates AS (
    SELECT
        c.customer_key,
        c.customer_id,
        s.first_name,  -- Updated Type 1 attributes
        s.last_name,
        s.email,
        s.age,
        s.state,
        s.city,
        s.employment_status,
        c.card_type,  -- Keep Type 2 attributes unchanged
        c.credit_limit,
        c.account_open_date,
        s.customer_segment,
        s.decline_type,
        c.valid_from,
        c.valid_to,
        c.is_current,
        c.created_timestamp,
        CURRENT_TIMESTAMP() AS updated_timestamp
    FROM current_dimension c
    INNER JOIN source_data s
        ON c.customer_id = s.customer_id
    INNER JOIN changes ch
        ON s.customer_id = ch.customer_id
    WHERE ch.change_type = 'NO_CHANGE'  -- Only process unchanged SCD Type 2
        AND (
            -- Check if any Type 1 attribute changed
            s.first_name != c.first_name
            OR s.last_name != c.last_name
            OR s.email != c.email
            OR s.age != c.age
            OR s.state != c.state
            OR s.city != c.city
            OR s.employment_status != c.employment_status
            OR s.customer_segment != c.customer_segment
            OR COALESCE(s.decline_type, '') != COALESCE(c.decline_type, '')
        )
),

-- Union all changes
all_changes AS (
    SELECT * FROM expired_records
    UNION ALL
    SELECT * FROM new_and_changed_records
    UNION ALL
    SELECT * FROM type_1_updates
)

SELECT * FROM all_changes

{% else %}

{#
============================================================================
FULL REFRESH MODE: Initial load - all records are current
============================================================================
#}

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'account_open_date']) }} AS customer_key,
    customer_id,
    first_name,
    last_name,
    email,
    age,
    state,
    city,
    employment_status,
    card_type,
    credit_limit,
    account_open_date,
    customer_segment,
    decline_type,

    -- SCD Type 2 metadata
    account_open_date AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current,

    -- Audit timestamps
    CURRENT_TIMESTAMP() AS created_timestamp,
    CURRENT_TIMESTAMP() AS updated_timestamp

FROM {{ ref('stg_customers') }}

{% endif %}
