{#
============================================================================
Custom Test: Segment Distribution Balance
============================================================================
Purpose: Verify segment distribution is reasonable (no segment < 5%)

Expected Distribution:
- High-Value Travelers: 10-15%
- Declining: 5-10%
- New & Growing: 10-15%
- Budget-Conscious: 20-25%
- Stable Mid-Spenders: 40-50%

Test Fails If:
- Any segment has < 5% of customers (indicates segmentation logic issue)
- Returns rows for segments below threshold

Usage:
  dbt test --select assert_segment_distribution
============================================================================
#}

WITH segment_counts AS (
    SELECT
        customer_segment,
        COUNT(*) AS customer_count,
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS percentage
    FROM {{ ref('customer_segments') }}
    GROUP BY customer_segment
)

-- Return segments below 5% threshold (test fails if any rows returned)
SELECT
    customer_segment,
    customer_count,
    ROUND(percentage, 2) AS percentage
FROM segment_counts
WHERE percentage < 5.0
ORDER BY percentage ASC
