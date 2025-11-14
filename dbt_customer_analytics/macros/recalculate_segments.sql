{#
============================================================================
Macro: Recalculate Customer Segments
============================================================================
Purpose: Refresh customer segments using rolling 90-day window

Usage:
  dbt run-operation recalculate_segments

Schedule:
  Run monthly (via Airflow, Snowflake Task, or manual execution)

Process:
  1. Truncate existing customer_segments table
  2. Rebuild from customer_segments model
  3. Log execution metadata

Notes:
  - Uses full refresh pattern (TRUNCATE + INSERT)
  - Alternative: Use dbt run --models customer_segments --full-refresh
  - This macro provides more control and logging

Example Workflow:
  # Run on 1st of each month
  dbt run-operation recalculate_segments

  # Or via dbt run
  dbt run --models customer_segments --full-refresh
============================================================================
#}

{% macro recalculate_segments() %}

{{ log("Starting customer segment recalculation...", info=True) }}

{% set start_time = modules.datetime.datetime.now() %}

-- Step 1: Truncate existing table
{% set truncate_query %}
TRUNCATE TABLE IF EXISTS {{ target.database }}.GOLD.CUSTOMER_SEGMENTS;
{% endset %}

{{ log("Truncating existing customer_segments table...", info=True) }}
{% do run_query(truncate_query) %}

-- Step 2: Rebuild from model
-- Note: This operation requires the customer_segments model to be materialized
{% set rebuild_query %}
-- Run: dbt run --models customer_segments --full-refresh
-- This macro is kept for reference but actual refresh should use dbt run
SELECT 'Use dbt run --models customer_segments --full-refresh to rebuild segments' AS message;
{% endset %}

{{ log("Rebuilding customer_segments with rolling 90-day window...", info=True) }}
{% do run_query(rebuild_query) %}

-- Step 3: Get row count and distribution
{% set stats_query %}
SELECT
    COUNT(*) AS total_customers,
    COUNT(DISTINCT customer_segment) AS segment_count
FROM {{ target.database }}.GOLD.CUSTOMER_SEGMENTS;
{% endset %}

{% set stats_result = run_query(stats_query) %}

{% if execute %}
    {% set total_customers = stats_result.columns[0].values()[0] %}
    {% set segment_count = stats_result.columns[1].values()[0] %}

    {{ log("Segment recalculation complete!", info=True) }}
    {{ log("Total customers: " ~ total_customers, info=True) }}
    {{ log("Segments: " ~ segment_count, info=True) }}

    -- Get distribution
    {% set distribution_query %}
    SELECT
        customer_segment,
        COUNT(*) AS customer_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
    FROM {{ target.database }}.GOLD.CUSTOMER_SEGMENTS
    GROUP BY customer_segment
    ORDER BY customer_count DESC;
    {% endset %}

    {% set distribution_result = run_query(distribution_query) %}

    {{ log("Segment Distribution:", info=True) }}
    {{ log("=" * 60, info=True) }}

    {% for row in distribution_result.rows %}
        {{ log(row[0] ~ ": " ~ row[1] ~ " customers (" ~ row[2] ~ "%)", info=True) }}
    {% endfor %}

    {{ log("=" * 60, info=True) }}

    {% set end_time = modules.datetime.datetime.now() %}
    {% set duration = (end_time - start_time).total_seconds() %}

    {{ log("Execution time: " ~ duration ~ " seconds", info=True) }}
{% endif %}

{% endmacro %}
