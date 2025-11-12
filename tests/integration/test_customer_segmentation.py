"""
Integration tests for customer segmentation model.

Tests verify:
- Segmentation model builds successfully
- All customers assigned to segments
- Segment distribution is balanced (no segment < 5%)
- Segment criteria are correctly applied
- Rolling window calculations are accurate
- Segment recalculation works

Run with:
    uv run pytest tests/integration/test_customer_segmentation.py -v
"""

import pytest
import time
from pathlib import Path
from tests.conftest import snowflake_connection, dbt_project_dir


@pytest.fixture(scope="module")
def segmentation_setup(snowflake_connection, dbt_project_dir):
    """
    Build customer segmentation model before running tests.

    Runs: dbt run --models customer_segments
    """
    import subprocess
    import os

    # Change to dbt project directory
    original_dir = Path.cwd()
    dbt_dir = Path(dbt_project_dir)

    try:
        os.chdir(dbt_dir)

        # Build customer segments model
        result = subprocess.run(
            ["dbt", "run", "--models", "customer_segments", "--profiles-dir", "."],
            capture_output=True,
            text=True,
            timeout=180  # 3 minutes max
        )

        if result.returncode != 0:
            pytest.fail(f"dbt run failed:\n{result.stderr}")

        yield snowflake_connection

    finally:
        os.chdir(original_dir)


# ============================================================================
# Test 1: Customer Segments Model Builds
# ============================================================================

def test_customer_segments_builds(segmentation_setup):
    """
    Verify customer_segments table created in GOLD schema.
    """
    cursor = segmentation_setup.cursor()

    # Query INFORMATION_SCHEMA for customer_segments table
    cursor.execute("""
        SELECT table_name
        FROM SNOWFLAKE_DEMO.INFORMATION_SCHEMA.TABLES
        WHERE table_schema = 'GOLD'
          AND table_name = 'CUSTOMER_SEGMENTS'
          AND table_type = 'BASE TABLE'
    """)

    tables = cursor.fetchall()

    assert len(tables) == 1, "customer_segments table not found in GOLD schema"

    print("✓ customer_segments table exists in GOLD schema")


# ============================================================================
# Test 2: All Customers Assigned Segment
# ============================================================================

def test_all_customers_assigned_segment(segmentation_setup):
    """
    Verify all customers have a non-NULL segment assigned.
    """
    cursor = segmentation_setup.cursor()

    # Count customers with NULL segment
    cursor.execute("""
        SELECT COUNT(*) AS null_segment_count
        FROM GOLD.CUSTOMER_SEGMENTS
        WHERE customer_segment IS NULL
    """)

    null_count = cursor.fetchone()[0]

    assert null_count == 0, f"Found {null_count} customers with NULL segment"

    # Get total customer count
    cursor.execute("""
        SELECT COUNT(*) AS total_customers
        FROM GOLD.CUSTOMER_SEGMENTS
    """)

    total_customers = cursor.fetchone()[0]

    print(f"✓ All {total_customers:,} customers assigned to segments (0 NULL values)")


# ============================================================================
# Test 3: Segment Distribution
# ============================================================================

def test_segment_distribution(segmentation_setup):
    """
    Verify segment distribution is balanced.

    Checks:
    - Each segment has at least 5% of customers
    - Total percentages sum to 100%
    - All 5 expected segments exist
    """
    cursor = segmentation_setup.cursor()

    # Get segment distribution
    cursor.execute("""
        SELECT
            customer_segment,
            COUNT(*) AS customer_count,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS percentage
        FROM GOLD.CUSTOMER_SEGMENTS
        GROUP BY customer_segment
        ORDER BY customer_count DESC
    """)

    segments = cursor.fetchall()

    # Expected segments
    expected_segments = {
        'High-Value Travelers',
        'Declining',
        'New & Growing',
        'Budget-Conscious',
        'Stable Mid-Spenders'
    }

    actual_segments = {row[0] for row in segments}

    # Test 1: All expected segments exist
    missing_segments = expected_segments - actual_segments
    assert len(missing_segments) == 0, f"Missing segments: {missing_segments}"

    # Test 2: No unexpected segments
    unexpected_segments = actual_segments - expected_segments
    assert len(unexpected_segments) == 0, f"Unexpected segments: {unexpected_segments}"

    # Test 3: Each segment has at least 5% of customers
    total_percentage = 0
    print("\nSegment Distribution:")
    print("=" * 80)

    for segment_name, count, percentage in segments:
        print(f"{segment_name:25} {count:>8,} customers ({percentage:>6.2f}%)")

        assert percentage >= 5.0, \
            f"Segment '{segment_name}' has only {percentage:.2f}% of customers (should be ≥ 5%)"

        total_percentage += percentage

    print("=" * 80)

    # Test 4: Total percentages sum to ~100%
    assert 99.9 <= total_percentage <= 100.1, \
        f"Total percentage is {total_percentage:.2f}% (should be ~100%)"

    print(f"✓ All segments have ≥ 5% of customers")
    print(f"✓ Total percentage: {total_percentage:.2f}%")


# ============================================================================
# Test 4: High-Value Travelers Criteria
# ============================================================================

def test_high_value_travelers_criteria(segmentation_setup):
    """
    Verify High-Value Travelers segment criteria.

    Criteria:
    - avg_monthly_spend >= 5000
    - travel_spend_pct >= 25
    """
    cursor = segmentation_setup.cursor()

    # Query High-Value Travelers
    cursor.execute("""
        SELECT
            COUNT(*) AS total_count,
            COUNT(CASE WHEN avg_monthly_spend >= 5000 THEN 1 END) AS meets_spend_criteria,
            COUNT(CASE WHEN travel_spend_pct >= 25 THEN 1 END) AS meets_travel_criteria,
            AVG(avg_monthly_spend) AS avg_spend,
            AVG(travel_spend_pct) AS avg_travel_pct
        FROM GOLD.CUSTOMER_SEGMENTS
        WHERE customer_segment = 'High-Value Travelers'
    """)

    row = cursor.fetchone()
    total_count, meets_spend, meets_travel, avg_spend, avg_travel_pct = row

    # All High-Value Travelers must meet criteria
    assert meets_spend == total_count, \
        f"Only {meets_spend}/{total_count} High-Value Travelers meet spend criteria (≥$5,000/month)"

    assert meets_travel == total_count, \
        f"Only {meets_travel}/{total_count} High-Value Travelers meet travel criteria (≥25%)"

    print(f"✓ All {total_count:,} High-Value Travelers meet criteria:")
    print(f"  - Average monthly spend: ${avg_spend:,.2f}")
    print(f"  - Average travel %: {avg_travel_pct:.2f}%")


# ============================================================================
# Test 5: Declining Segment Has Negative Growth
# ============================================================================

def test_declining_segment_has_negative_growth(segmentation_setup):
    """
    Verify Declining segment criteria.

    Criteria:
    - spend_change_pct <= -30
    - spend_prior_90_days >= 2000
    """
    cursor = segmentation_setup.cursor()

    # Query Declining segment
    cursor.execute("""
        SELECT
            COUNT(*) AS total_count,
            COUNT(CASE WHEN spend_change_pct <= -30 THEN 1 END) AS meets_decline_criteria,
            COUNT(CASE WHEN spend_prior_90_days >= 2000 THEN 1 END) AS meets_prior_spend_criteria,
            AVG(spend_change_pct) AS avg_change_pct,
            AVG(spend_prior_90_days) AS avg_prior_spend
        FROM GOLD.CUSTOMER_SEGMENTS
        WHERE customer_segment = 'Declining'
    """)

    row = cursor.fetchone()
    total_count, meets_decline, meets_prior_spend, avg_change_pct, avg_prior_spend = row

    # All Declining customers must meet criteria
    assert meets_decline == total_count, \
        f"Only {meets_decline}/{total_count} Declining customers have ≤-30% spend change"

    assert meets_prior_spend == total_count, \
        f"Only {meets_prior_spend}/{total_count} Declining customers have ≥$2,000 prior spend"

    print(f"✓ All {total_count:,} Declining customers meet criteria:")
    print(f"  - Average spend change: {avg_change_pct:.2f}%")
    print(f"  - Average prior 90-day spend: ${avg_prior_spend:,.2f}")


# ============================================================================
# Test 6: New & Growing Segment Criteria
# ============================================================================

def test_new_and_growing_segment_criteria(segmentation_setup):
    """
    Verify New & Growing segment criteria.

    Criteria:
    - tenure_months <= 6
    - spend_change_pct >= 50
    """
    cursor = segmentation_setup.cursor()

    # Query New & Growing segment
    cursor.execute("""
        SELECT
            COUNT(*) AS total_count,
            COUNT(CASE WHEN tenure_months <= 6 THEN 1 END) AS meets_tenure_criteria,
            COUNT(CASE WHEN spend_change_pct >= 50 THEN 1 END) AS meets_growth_criteria,
            AVG(tenure_months) AS avg_tenure,
            AVG(spend_change_pct) AS avg_growth
        FROM GOLD.CUSTOMER_SEGMENTS
        WHERE customer_segment = 'New & Growing'
    """)

    row = cursor.fetchone()
    total_count, meets_tenure, meets_growth, avg_tenure, avg_growth = row

    # All New & Growing customers must meet criteria
    assert meets_tenure == total_count, \
        f"Only {meets_tenure}/{total_count} New & Growing customers have ≤6 months tenure"

    assert meets_growth == total_count, \
        f"Only {meets_growth}/{total_count} New & Growing customers have ≥50% growth"

    print(f"✓ All {total_count:,} New & Growing customers meet criteria:")
    print(f"  - Average tenure: {avg_tenure:.1f} months")
    print(f"  - Average growth: {avg_growth:.2f}%")


# ============================================================================
# Test 7: Budget-Conscious Segment Criteria
# ============================================================================

def test_budget_conscious_segment_criteria(segmentation_setup):
    """
    Verify Budget-Conscious segment criteria.

    Criteria:
    - avg_monthly_spend < 1500
    - necessities_spend_pct >= 60
    """
    cursor = segmentation_setup.cursor()

    # Query Budget-Conscious segment
    cursor.execute("""
        SELECT
            COUNT(*) AS total_count,
            COUNT(CASE WHEN avg_monthly_spend < 1500 THEN 1 END) AS meets_spend_criteria,
            COUNT(CASE WHEN necessities_spend_pct >= 60 THEN 1 END) AS meets_necessity_criteria,
            AVG(avg_monthly_spend) AS avg_spend,
            AVG(necessities_spend_pct) AS avg_necessity_pct
        FROM GOLD.CUSTOMER_SEGMENTS
        WHERE customer_segment = 'Budget-Conscious'
    """)

    row = cursor.fetchone()
    total_count, meets_spend, meets_necessity, avg_spend, avg_necessity_pct = row

    # All Budget-Conscious customers must meet criteria
    assert meets_spend == total_count, \
        f"Only {meets_spend}/{total_count} Budget-Conscious customers have <$1,500/month spend"

    assert meets_necessity == total_count, \
        f"Only {meets_necessity}/{total_count} Budget-Conscious customers have ≥60% necessities"

    print(f"✓ All {total_count:,} Budget-Conscious customers meet criteria:")
    print(f"  - Average monthly spend: ${avg_spend:,.2f}")
    print(f"  - Average necessities %: {avg_necessity_pct:.2f}%")


# ============================================================================
# Test 8: Rolling Window Calculation
# ============================================================================

def test_rolling_window_calculation(segmentation_setup):
    """
    Verify rolling window calculations are accurate.

    Tests:
    - spend_last_90_days only includes transactions from last 90 days
    - spend_prior_90_days covers days 91-180
    """
    cursor = segmentation_setup.cursor()

    # Sample 10 customers and verify their rolling window calculations
    cursor.execute("""
        SELECT
            s.customer_id,
            s.spend_last_90_days,
            s.spend_prior_90_days,

            -- Manually calculate last 90 days
            (SELECT COALESCE(SUM(f.transaction_amount), 0)
             FROM GOLD.FCT_TRANSACTIONS f
             WHERE f.customer_key = s.customer_key
               AND f.transaction_date >= DATEADD('day', -90, CURRENT_DATE())
            ) AS manual_last_90,

            -- Manually calculate prior 90 days
            (SELECT COALESCE(SUM(f.transaction_amount), 0)
             FROM GOLD.FCT_TRANSACTIONS f
             WHERE f.customer_key = s.customer_key
               AND f.transaction_date >= DATEADD('day', -180, CURRENT_DATE())
               AND f.transaction_date < DATEADD('day', -90, CURRENT_DATE())
            ) AS manual_prior_90

        FROM GOLD.CUSTOMER_SEGMENTS s
        ORDER BY RANDOM()
        LIMIT 10
    """)

    results = cursor.fetchall()

    assert len(results) > 0, "No customers found for rolling window verification"

    mismatches = 0

    for customer_id, spend_last_90, spend_prior_90, manual_last_90, manual_prior_90 in results:
        # Allow small floating point differences (0.01)
        last_90_match = abs(spend_last_90 - manual_last_90) < 0.01
        prior_90_match = abs(spend_prior_90 - manual_prior_90) < 0.01

        if not last_90_match:
            print(f"  MISMATCH {customer_id}: last_90_days {spend_last_90} != {manual_last_90}")
            mismatches += 1

        if not prior_90_match:
            print(f"  MISMATCH {customer_id}: prior_90_days {spend_prior_90} != {manual_prior_90}")
            mismatches += 1

    assert mismatches == 0, f"Found {mismatches} rolling window calculation mismatches"

    print(f"✓ Rolling window calculations verified for {len(results)} customers")


# ============================================================================
# Test 9: Segment Recalculation
# ============================================================================

def test_segment_recalculation(segmentation_setup, dbt_project_dir):
    """
    Verify segment recalculation macro works.

    Process:
    1. Record initial segment distribution
    2. Run recalculate_segments macro
    3. Verify table still exists and has data
    """
    cursor = segmentation_setup.cursor()
    import subprocess
    import os

    # Get initial distribution
    cursor.execute("""
        SELECT customer_segment, COUNT(*) AS count
        FROM GOLD.CUSTOMER_SEGMENTS
        GROUP BY customer_segment
        ORDER BY customer_segment
    """)

    initial_distribution = {row[0]: row[1] for row in cursor.fetchall()}
    initial_total = sum(initial_distribution.values())

    print(f"\nInitial distribution (total: {initial_total:,}):")
    for segment, count in sorted(initial_distribution.items()):
        print(f"  {segment}: {count:,}")

    # Run recalculate_segments macro
    original_dir = Path.cwd()
    dbt_dir = Path(dbt_project_dir)

    try:
        os.chdir(dbt_dir)

        result = subprocess.run(
            ["dbt", "run-operation", "recalculate_segments", "--profiles-dir", "."],
            capture_output=True,
            text=True,
            timeout=120
        )

        if result.returncode != 0:
            pytest.fail(f"recalculate_segments macro failed:\n{result.stderr}")

        # Get new distribution
        cursor.execute("""
            SELECT customer_segment, COUNT(*) AS count
            FROM GOLD.CUSTOMER_SEGMENTS
            GROUP BY customer_segment
            ORDER BY customer_segment
        """)

        new_distribution = {row[0]: row[1] for row in cursor.fetchall()}
        new_total = sum(new_distribution.values())

        print(f"\nNew distribution (total: {new_total:,}):")
        for segment, count in sorted(new_distribution.items()):
            print(f"  {segment}: {count:,}")

        # Verify table still has data
        assert new_total > 0, "customer_segments table is empty after recalculation"

        # Total should be same (all customers still segmented)
        assert new_total == initial_total, \
            f"Customer count changed: {initial_total:,} → {new_total:,}"

        print(f"✓ Segment recalculation successful (customer count unchanged)")

    finally:
        os.chdir(original_dir)


# ============================================================================
# Test 10: Segmentation Query Performance
# ============================================================================

def test_segmentation_query_performance(segmentation_setup):
    """
    Verify customer segmentation model completes efficiently.

    Expected: < 2 minutes for 50K customers, 13.5M transactions
    """
    cursor = segmentation_setup.cursor()
    import subprocess
    import os

    # Rebuild customer_segments and measure time
    original_dir = Path.cwd()
    dbt_dir = Path("/Users/jpurrutia/projects/snowflake-panel-demo/dbt_customer_analytics")

    try:
        os.chdir(dbt_dir)

        start_time = time.time()

        result = subprocess.run(
            ["dbt", "run", "--models", "customer_segments", "--full-refresh", "--profiles-dir", "."],
            capture_output=True,
            text=True,
            timeout=180  # 3 minute timeout
        )

        execution_time = time.time() - start_time

        if result.returncode != 0:
            pytest.fail(f"dbt run failed:\n{result.stderr}")

        # Performance threshold: 2 minutes on SMALL warehouse
        max_execution_time = 120.0

        assert execution_time < max_execution_time, \
            f"Segmentation took {execution_time:.2f}s (expected <{max_execution_time}s)"

        print(f"✓ Segmentation model completed in {execution_time:.2f}s")

    finally:
        os.chdir(original_dir)


# ============================================================================
# Summary
# ============================================================================

def test_summary(segmentation_setup):
    """
    Print summary of customer segmentation.
    """
    cursor = segmentation_setup.cursor()

    # Get detailed segment statistics
    cursor.execute("""
        SELECT
            customer_segment,
            COUNT(*) AS customer_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
            ROUND(AVG(lifetime_value), 2) AS avg_ltv,
            ROUND(AVG(avg_monthly_spend), 2) AS avg_monthly_spend,
            ROUND(AVG(spend_change_pct), 2) AS avg_change_pct
        FROM GOLD.CUSTOMER_SEGMENTS
        GROUP BY customer_segment
        ORDER BY avg_ltv DESC
    """)

    results = cursor.fetchall()

    print("\n" + "="*100)
    print("CUSTOMER SEGMENTATION SUMMARY")
    print("="*100)
    print(f"{'Segment':<25} {'Customers':>12} {'%':>8} {'Avg LTV':>12} {'Avg Monthly':>12} {'Avg Change':>12}")
    print("-"*100)

    for segment, count, pct, avg_ltv, avg_monthly, avg_change in results:
        print(f"{segment:<25} {count:>12,} {pct:>7.2f}% ${avg_ltv:>11,.2f} ${avg_monthly:>11,.2f} {avg_change:>11.2f}%")

    print("="*100)
