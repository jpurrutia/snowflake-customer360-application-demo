{#
============================================================================
Custom Test: SCD Type 2 Integrity Check
============================================================================
Purpose: Verify that each customer has exactly ONE current record

Test Logic:
- Group by customer_id
- Count records where is_current = TRUE
- Fail if any customer has != 1 current record

Expected Result: No rows returned (all customers have exactly 1 current)
Failure: Returns customer_ids with wrong current_count

Usage:
  dbt test --select assert_scd_type_2_integrity
============================================================================
#}

-- Test fails if this query returns any rows
SELECT
    customer_id,
    SUM(CASE WHEN is_current = TRUE THEN 1 ELSE 0 END) AS current_count,
    COUNT(*) AS total_versions
FROM {{ ref('dim_customer') }}
GROUP BY customer_id
HAVING current_count != 1  -- Should be exactly 1
