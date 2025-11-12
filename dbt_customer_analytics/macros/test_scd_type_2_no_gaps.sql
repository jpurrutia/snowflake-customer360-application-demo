{#
============================================================================
Test Macro: SCD Type 2 No Date Gaps
============================================================================
Purpose: Verify no gaps in SCD Type 2 date ranges for each customer

Logic:
- For each customer, order versions by valid_from
- Check that valid_to of version N = valid_from of version N+1 - 1 day
- Ensures continuous timeline with no missing dates

Expected: No rows returned (all date ranges are contiguous)
Failure: Returns records where gaps exist

Usage:
  {{ test_scd_type_2_no_gaps(ref('dim_customer'), 'customer_id') }}

Parameters:
  - model: The dimension model to test
  - customer_id_column: Name of the customer ID column
============================================================================
#}

{% macro test_scd_type_2_no_gaps(model, customer_id_column='customer_id') %}

WITH customer_history AS (
    SELECT
        {{ customer_id_column }},
        valid_from,
        valid_to,
        is_current,
        -- Get the next version's valid_from for comparison
        LEAD(valid_from) OVER (
            PARTITION BY {{ customer_id_column }}
            ORDER BY valid_from
        ) AS next_valid_from
    FROM {{ model }}
    WHERE valid_to IS NOT NULL  -- Exclude current records (no next version yet)
),

gaps AS (
    SELECT
        {{ customer_id_column }},
        valid_from,
        valid_to,
        next_valid_from,
        -- Check if there's a gap
        CASE
            WHEN next_valid_from IS NULL THEN NULL  -- Last historical record (OK)
            WHEN valid_to != next_valid_from - INTERVAL '1 day' THEN 'GAP_FOUND'
            ELSE 'NO_GAP'
        END AS gap_status,
        -- Calculate gap size in days
        DATEDIFF('day', valid_to, next_valid_from) - 1 AS gap_days
    FROM customer_history
)

-- Return records where gaps exist
SELECT
    {{ customer_id_column }},
    valid_from,
    valid_to,
    next_valid_from,
    gap_days,
    'Expected valid_to = ' || next_valid_from || ' - 1 day' AS expected
FROM gaps
WHERE gap_status = 'GAP_FOUND'

{% endmacro %}
