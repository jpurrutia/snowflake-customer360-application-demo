{#
============================================================================
Custom Test: SCD Type 2 No Date Gaps
============================================================================
Purpose: Ensure no gaps in date ranges for customer history

This test uses the test_scd_type_2_no_gaps macro to verify that
valid_to dates align with next valid_from dates.

Expected Result: No rows returned (no gaps found)
Failure: Returns customer records with date gaps

Usage:
  dbt test --select assert_scd_type_2_no_gaps
============================================================================
#}

{{ test_scd_type_2_no_gaps(ref('dim_customer'), 'customer_id') }}
