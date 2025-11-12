"""
Integration tests for aggregate mart models (hero metrics and Customer 360).

Tests verify:
- All mart models build successfully
- Metric calculations are accurate
- Customer 360 profile has complete data
- Query performance meets targets
- Metrics refresh correctly

Run with:
    uv run pytest tests/integration/test_aggregate_marts.py -v
"""

import pytest
import time
from pathlib import Path
from tests.conftest import snowflake_connection, dbt_project_dir


@pytest.fixture(scope="module")
def aggregate_marts_setup(snowflake_connection, dbt_project_dir):
    """
    Build all aggregate mart models before running tests.

    Runs: dbt run --models marts
    """
    import subprocess
    import os

    # Change to dbt project directory
    original_dir = Path.cwd()
    dbt_dir = Path(dbt_project_dir)

    try:
        os.chdir(dbt_dir)

        # Build all mart models
        result = subprocess.run(
            ["dbt", "run", "--models", "marts", "--profiles-dir", "."],
            capture_output=True,
            text=True,
            timeout=300  # 5 minutes max
        )

        if result.returncode != 0:
            pytest.fail(f"dbt run failed:\n{result.stderr}")

        yield snowflake_connection

    finally:
        os.chdir(original_dir)


# ============================================================================
# Test 1: All Marts Build Successfully
# ============================================================================

def test_all_marts_build(aggregate_marts_setup):
    """
    Verify all aggregate mart tables created in GOLD schema.

    Expected tables:
    - metric_customer_ltv
    - metric_mom_spend_change
    - metric_avg_transaction_value
    - customer_360_profile
    """
    cursor = aggregate_marts_setup.cursor()

    # Query INFORMATION_SCHEMA for GOLD tables
    cursor.execute("""
        SELECT table_name
        FROM SNOWFLAKE_DEMO.INFORMATION_SCHEMA.TABLES
        WHERE table_schema = 'GOLD'
          AND table_type = 'BASE TABLE'
          AND table_name IN (
              'METRIC_CUSTOMER_LTV',
              'METRIC_MOM_SPEND_CHANGE',
              'METRIC_AVG_TRANSACTION_VALUE',
              'CUSTOMER_360_PROFILE'
          )
        ORDER BY table_name
    """)

    tables = {row[0] for row in cursor.fetchall()}

    # Expected tables
    expected_tables = {
        'METRIC_CUSTOMER_LTV',
        'METRIC_MOM_SPEND_CHANGE',
        'METRIC_AVG_TRANSACTION_VALUE',
        'CUSTOMER_360_PROFILE'
    }

    # Assert all expected tables exist
    missing_tables = expected_tables - tables
    assert len(missing_tables) == 0, f"Missing tables in GOLD schema: {missing_tables}"

    print(f"✓ All {len(expected_tables)} aggregate mart tables exist in GOLD schema")


# ============================================================================
# Test 2: Metric Customer LTV
# ============================================================================

def test_metric_customer_ltv(aggregate_marts_setup):
    """
    Verify LTV metric calculations.

    Tests:
    - All 50K customers have LTV calculated
    - LTV > 0 for customers with transactions
    - Specific customer: verify LTV = SUM of their transactions
    """
    cursor = aggregate_marts_setup.cursor()

    # Test 1: All customers have LTV calculated
    cursor.execute("""
        SELECT COUNT(*) AS customer_count
        FROM GOLD.METRIC_CUSTOMER_LTV
    """)

    customer_count = cursor.fetchone()[0]

    # Expected: ~50K customers
    assert customer_count >= 49000 and customer_count <= 51000, \
        f"Expected ~50K customers, got {customer_count:,}"

    # Test 2: All LTV values >= 0
    cursor.execute("""
        SELECT COUNT(*) AS invalid_ltv_count
        FROM GOLD.METRIC_CUSTOMER_LTV
        WHERE lifetime_value < 0
    """)

    invalid_ltv_count = cursor.fetchone()[0]
    assert invalid_ltv_count == 0, f"Found {invalid_ltv_count} customers with negative LTV"

    # Test 3: Verify specific customer LTV calculation
    cursor.execute("""
        WITH customer_ltv AS (
            SELECT customer_id, lifetime_value
            FROM GOLD.METRIC_CUSTOMER_LTV
            LIMIT 1
        ),
        manual_ltv AS (
            SELECT
                c.customer_id,
                COALESCE(SUM(f.transaction_amount), 0) AS manual_lifetime_value
            FROM GOLD.DIM_CUSTOMER c
            LEFT JOIN GOLD.FCT_TRANSACTIONS f
                ON c.customer_key = f.customer_key
                AND f.status = 'approved'
            WHERE c.is_current = TRUE
              AND c.customer_id = (SELECT customer_id FROM customer_ltv)
            GROUP BY c.customer_id
        )
        SELECT
            c.customer_id,
            c.lifetime_value AS calculated_ltv,
            m.manual_lifetime_value,
            ABS(c.lifetime_value - m.manual_lifetime_value) AS difference
        FROM customer_ltv c
        JOIN manual_ltv m ON c.customer_id = m.customer_id
    """)

    row = cursor.fetchone()
    customer_id, calculated_ltv, manual_ltv, difference = row

    assert difference < 0.01, \
        f"LTV mismatch for {customer_id}: {calculated_ltv} != {manual_ltv} (diff: {difference})"

    print(f"✓ All {customer_count:,} customers have valid LTV (verified calculation for {customer_id})")


# ============================================================================
# Test 3: Metric MoM Spend Change
# ============================================================================

def test_metric_mom_spend_change(aggregate_marts_setup):
    """
    Verify MoM spend change metric calculations.

    Tests:
    - Monthly records exist for 18 months
    - MoM change % calculated correctly
    - First month has NULL prior_month_spend
    """
    cursor = aggregate_marts_setup.cursor()

    # Test 1: Monthly records exist
    cursor.execute("""
        SELECT
            COUNT(DISTINCT month) AS month_count,
            MIN(month) AS earliest_month,
            MAX(month) AS latest_month
        FROM GOLD.METRIC_MOM_SPEND_CHANGE
    """)

    row = cursor.fetchone()
    month_count, earliest_month, latest_month = row

    # Expected: ~18 months of data
    assert month_count >= 15 and month_count <= 20, \
        f"Expected ~18 months, got {month_count}"

    # Test 2: First month has NULL prior_month_spend
    cursor.execute("""
        SELECT COUNT(*) AS first_month_with_prior
        FROM GOLD.METRIC_MOM_SPEND_CHANGE
        WHERE month_number = 1
          AND prior_month_spend IS NOT NULL
    """)

    first_month_with_prior = cursor.fetchone()[0]
    assert first_month_with_prior == 0, \
        f"Found {first_month_with_prior} first months with non-NULL prior_month_spend"

    # Test 3: Verify MoM calculation for random customer
    cursor.execute("""
        SELECT
            customer_id,
            month,
            monthly_spend,
            prior_month_spend,
            mom_change_pct,
            CASE
                WHEN prior_month_spend > 0
                THEN ((monthly_spend - prior_month_spend) / prior_month_spend) * 100
                ELSE NULL
            END AS manual_mom_change_pct
        FROM GOLD.METRIC_MOM_SPEND_CHANGE
        WHERE mom_change_pct IS NOT NULL
        ORDER BY RANDOM()
        LIMIT 1
    """)

    row = cursor.fetchone()
    customer_id, month, monthly_spend, prior_month_spend, mom_change_pct, manual_mom_change_pct = row

    difference = abs(mom_change_pct - manual_mom_change_pct)

    assert difference < 0.01, \
        f"MoM calculation mismatch for {customer_id} {month}: {mom_change_pct} != {manual_mom_change_pct}"

    print(f"✓ MoM spend change: {month_count} months of data, verified calculation for {customer_id}")


# ============================================================================
# Test 4: Metric Average Transaction Value
# ============================================================================

def test_metric_avg_transaction_value(aggregate_marts_setup):
    """
    Verify ATV metric calculations.

    Tests:
    - ATV > 0 for all customers with transactions
    - Verify calculation: ATV = SUM(amount) / COUNT(transactions)
    """
    cursor = aggregate_marts_setup.cursor()

    # Test 1: All ATV values >= 0
    cursor.execute("""
        SELECT COUNT(*) AS invalid_atv_count
        FROM GOLD.METRIC_AVG_TRANSACTION_VALUE
        WHERE avg_transaction_value < 0
    """)

    invalid_atv_count = cursor.fetchone()[0]
    assert invalid_atv_count == 0, f"Found {invalid_atv_count} customers with negative ATV"

    # Test 2: Verify ATV calculation for random customer
    cursor.execute("""
        WITH customer_atv AS (
            SELECT customer_id, avg_transaction_value, transaction_count
            FROM GOLD.METRIC_AVG_TRANSACTION_VALUE
            WHERE transaction_count > 0
            ORDER BY RANDOM()
            LIMIT 1
        ),
        manual_atv AS (
            SELECT
                c.customer_id,
                AVG(f.transaction_amount) AS manual_avg_transaction_value
            FROM GOLD.DIM_CUSTOMER c
            LEFT JOIN GOLD.FCT_TRANSACTIONS f
                ON c.customer_key = f.customer_key
                AND f.status = 'approved'
            WHERE c.is_current = TRUE
              AND c.customer_id = (SELECT customer_id FROM customer_atv)
            GROUP BY c.customer_id
        )
        SELECT
            a.customer_id,
            a.avg_transaction_value AS calculated_atv,
            m.manual_avg_transaction_value,
            ABS(a.avg_transaction_value - m.manual_avg_transaction_value) AS difference
        FROM customer_atv a
        JOIN manual_atv m ON a.customer_id = m.customer_id
    """)

    row = cursor.fetchone()
    customer_id, calculated_atv, manual_atv, difference = row

    assert difference < 0.01, \
        f"ATV mismatch for {customer_id}: {calculated_atv} != {manual_atv} (diff: {difference})"

    print(f"✓ ATV metric verified (calculation validated for {customer_id})")


# ============================================================================
# Test 5: Customer 360 Profile
# ============================================================================

def test_customer_360_profile(aggregate_marts_setup):
    """
    Verify Customer 360 profile completeness.

    Tests:
    - All 50K customers in profile
    - No NULL required fields
    - All metrics present for each customer
    - Churn risk score is NULL (placeholder)
    """
    cursor = aggregate_marts_setup.cursor()

    # Test 1: All customers in profile
    cursor.execute("""
        SELECT COUNT(*) AS customer_count
        FROM GOLD.CUSTOMER_360_PROFILE
    """)

    customer_count = cursor.fetchone()[0]

    assert customer_count >= 49000 and customer_count <= 51000, \
        f"Expected ~50K customers, got {customer_count:,}"

    # Test 2: No NULL required fields
    cursor.execute("""
        SELECT COUNT(*) AS null_required_fields
        FROM GOLD.CUSTOMER_360_PROFILE
        WHERE customer_id IS NULL
           OR full_name IS NULL
           OR email IS NULL
           OR customer_segment IS NULL
           OR lifetime_value IS NULL
           OR avg_transaction_value IS NULL
    """)

    null_required_fields = cursor.fetchone()[0]
    assert null_required_fields == 0, f"Found {null_required_fields} profiles with NULL required fields"

    # Test 3: All metrics present
    cursor.execute("""
        SELECT
            COUNT(*) AS total_profiles,
            COUNT(lifetime_value) AS has_ltv,
            COUNT(avg_transaction_value) AS has_atv,
            COUNT(customer_segment) AS has_segment
        FROM GOLD.CUSTOMER_360_PROFILE
    """)

    row = cursor.fetchone()
    total_profiles, has_ltv, has_atv, has_segment = row

    assert has_ltv == total_profiles, f"Only {has_ltv}/{total_profiles} profiles have LTV"
    assert has_atv == total_profiles, f"Only {has_atv}/{total_profiles} profiles have ATV"
    assert has_segment == total_profiles, f"Only {has_segment}/{total_profiles} profiles have segment"

    # Test 4: Churn risk score is NULL (placeholder)
    cursor.execute("""
        SELECT COUNT(*) AS has_churn_score
        FROM GOLD.CUSTOMER_360_PROFILE
        WHERE churn_risk_score IS NOT NULL
    """)

    has_churn_score = cursor.fetchone()[0]
    assert has_churn_score == 0, f"Expected churn_risk_score to be NULL (placeholder), but {has_churn_score} profiles have values"

    print(f"✓ Customer 360 profile: {customer_count:,} customers with complete data (churn risk placeholder verified)")


# ============================================================================
# Test 6: Metrics Refresh
# ============================================================================

def test_metrics_refresh(aggregate_marts_setup, dbt_project_dir):
    """
    Verify metrics update when upstream data changes.

    Process:
    1. Record initial LTV for sample customer
    2. Re-run mart models
    3. Verify LTV remains consistent (no data changes)
    """
    cursor = aggregate_marts_setup.cursor()
    import subprocess
    import os

    # Get initial LTV for sample customer
    cursor.execute("""
        SELECT customer_id, lifetime_value
        FROM GOLD.METRIC_CUSTOMER_LTV
        ORDER BY RANDOM()
        LIMIT 1
    """)

    initial_row = cursor.fetchone()
    customer_id, initial_ltv = initial_row

    # Re-run mart models
    original_dir = Path.cwd()
    dbt_dir = Path(dbt_project_dir)

    try:
        os.chdir(dbt_dir)

        result = subprocess.run(
            ["dbt", "run", "--models", "marts.marketing", "--profiles-dir", "."],
            capture_output=True,
            text=True,
            timeout=120
        )

        if result.returncode != 0:
            pytest.fail(f"Metrics refresh failed:\n{result.stderr}")

        # Get new LTV
        cursor.execute("""
            SELECT lifetime_value
            FROM GOLD.METRIC_CUSTOMER_LTV
            WHERE customer_id = %s
        """, (customer_id,))

        new_ltv = cursor.fetchone()[0]

        # LTV should be unchanged (no new transactions)
        assert abs(new_ltv - initial_ltv) < 0.01, \
            f"LTV changed unexpectedly: {initial_ltv} → {new_ltv}"

        print(f"✓ Metrics refresh successful ({customer_id} LTV stable: ${new_ltv:,.2f})")

    finally:
        os.chdir(original_dir)


# ============================================================================
# Test 7: Customer 360 Query Performance
# ============================================================================

def test_customer_360_query_performance(aggregate_marts_setup):
    """
    Verify Customer 360 profile query performance.

    Tests:
    - Single customer lookup < 1 second
    - Segment aggregation < 3 seconds
    """
    cursor = aggregate_marts_setup.cursor()

    # Get sample customer ID
    cursor.execute("""
        SELECT customer_id
        FROM GOLD.CUSTOMER_360_PROFILE
        ORDER BY RANDOM()
        LIMIT 1
    """)

    customer_id = cursor.fetchone()[0]

    # Test 1: Single customer lookup
    start_time = time.time()

    cursor.execute("""
        SELECT *
        FROM GOLD.CUSTOMER_360_PROFILE
        WHERE customer_id = %s
    """, (customer_id,))

    result = cursor.fetchone()
    single_query_time = time.time() - start_time

    assert result is not None, f"Customer {customer_id} not found"
    assert single_query_time < 1.0, \
        f"Single customer query took {single_query_time:.2f}s (expected <1s)"

    # Test 2: Segment aggregation
    start_time = time.time()

    cursor.execute("""
        SELECT
            customer_segment,
            COUNT(*) AS customer_count,
            AVG(lifetime_value) AS avg_ltv,
            AVG(avg_transaction_value) AS avg_atv
        FROM GOLD.CUSTOMER_360_PROFILE
        GROUP BY customer_segment
        ORDER BY avg_ltv DESC
    """)

    results = cursor.fetchall()
    agg_query_time = time.time() - start_time

    assert len(results) > 0, "Segment aggregation returned no results"
    assert agg_query_time < 3.0, \
        f"Segment aggregation took {agg_query_time:.2f}s (expected <3s)"

    print(f"✓ Query performance: Single lookup {single_query_time:.2f}s, Aggregation {agg_query_time:.2f}s")


# ============================================================================
# Test 8: Mart Join Integrity
# ============================================================================

def test_mart_join_integrity(aggregate_marts_setup):
    """
    Verify all mart models join correctly to customer_360_profile.

    Tests:
    - All LTV metrics in customer_360_profile
    - All ATV metrics in customer_360_profile
    - All segments in customer_360_profile
    """
    cursor = aggregate_marts_setup.cursor()

    # Test 1: All LTV customers in customer_360_profile
    cursor.execute("""
        SELECT COUNT(*) AS missing_in_360
        FROM GOLD.METRIC_CUSTOMER_LTV ltv
        WHERE NOT EXISTS (
            SELECT 1 FROM GOLD.CUSTOMER_360_PROFILE c360
            WHERE ltv.customer_id = c360.customer_id
        )
    """)

    missing_ltv = cursor.fetchone()[0]
    assert missing_ltv == 0, f"Found {missing_ltv} LTV customers missing from customer_360_profile"

    # Test 2: All ATV customers in customer_360_profile
    cursor.execute("""
        SELECT COUNT(*) AS missing_in_360
        FROM GOLD.METRIC_AVG_TRANSACTION_VALUE atv
        WHERE NOT EXISTS (
            SELECT 1 FROM GOLD.CUSTOMER_360_PROFILE c360
            WHERE atv.customer_id = c360.customer_id
        )
    """)

    missing_atv = cursor.fetchone()[0]
    assert missing_atv == 0, f"Found {missing_atv} ATV customers missing from customer_360_profile"

    # Test 3: All segment customers in customer_360_profile
    cursor.execute("""
        SELECT COUNT(*) AS missing_in_360
        FROM GOLD.CUSTOMER_SEGMENTS seg
        WHERE NOT EXISTS (
            SELECT 1 FROM GOLD.CUSTOMER_360_PROFILE c360
            WHERE seg.customer_id = c360.customer_id
        )
    """)

    missing_seg = cursor.fetchone()[0]
    assert missing_seg == 0, f"Found {missing_seg} segment customers missing from customer_360_profile"

    print("✓ Mart join integrity verified (all customers in customer_360_profile)")


# ============================================================================
# Summary
# ============================================================================

def test_summary(aggregate_marts_setup):
    """
    Print summary of aggregate marts.
    """
    cursor = aggregate_marts_setup.cursor()

    # Get mart row counts
    cursor.execute("""
        SELECT
            (SELECT COUNT(*) FROM GOLD.METRIC_CUSTOMER_LTV) AS ltv_count,
            (SELECT COUNT(*) FROM GOLD.METRIC_MOM_SPEND_CHANGE) AS mom_count,
            (SELECT COUNT(*) FROM GOLD.METRIC_AVG_TRANSACTION_VALUE) AS atv_count,
            (SELECT COUNT(*) FROM GOLD.CUSTOMER_360_PROFILE) AS c360_count
    """)

    row = cursor.fetchone()

    print("\n" + "="*80)
    print("AGGREGATE MARTS SUMMARY")
    print("="*80)
    print(f"metric_customer_ltv:            {row[0]:>12,} rows")
    print(f"metric_mom_spend_change:        {row[1]:>12,} rows")
    print(f"metric_avg_transaction_value:   {row[2]:>12,} rows")
    print(f"customer_360_profile:           {row[3]:>12,} rows")
    print("="*80)

    # Get segment distribution from customer_360_profile
    cursor.execute("""
        SELECT
            customer_segment,
            COUNT(*) AS customer_count,
            ROUND(AVG(lifetime_value), 2) AS avg_ltv,
            ROUND(AVG(avg_transaction_value), 2) AS avg_atv
        FROM GOLD.CUSTOMER_360_PROFILE
        GROUP BY customer_segment
        ORDER BY avg_ltv DESC
    """)

    results = cursor.fetchall()

    print("\nCUSTOMER 360 BY SEGMENT")
    print("="*80)
    print(f"{'Segment':<25} {'Customers':>12} {'Avg LTV':>12} {'Avg ATV':>12}")
    print("-"*80)

    for segment, count, avg_ltv, avg_atv in results:
        print(f"{segment:<25} {count:>12,} ${avg_ltv:>11,.2f} ${avg_atv:>11,.2f}")

    print("="*80)
