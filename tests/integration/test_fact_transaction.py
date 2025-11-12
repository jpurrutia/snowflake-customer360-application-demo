"""
Integration tests for Gold layer dimensional model (fact and dimensions).

Tests verify:
- Dimensional model builds successfully
- Fact table row counts
- Foreign key relationships
- No orphan transactions
- Clustering applied
- Star schema query performance
- Incremental loading behavior

Run with:
    uv run pytest tests/integration/test_fact_transaction.py -v
"""

import pytest
import time
from pathlib import Path
from tests.conftest import snowflake_connection, dbt_project_dir


@pytest.fixture(scope="module")
def fact_table_setup(snowflake_connection, dbt_project_dir):
    """
    Build dimensional model before running tests.

    Runs: dbt run --models marts.core
    """
    import subprocess

    # Change to dbt project directory
    original_dir = Path.cwd()
    dbt_dir = Path(dbt_project_dir)

    try:
        # Navigate to dbt project
        import os
        os.chdir(dbt_dir)

        # Build all mart models (dimensions + fact)
        result = subprocess.run(
            ["dbt", "run", "--models", "marts.core", "--profiles-dir", "."],
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
# Test 1: Dimensional Model Builds Successfully
# ============================================================================

def test_dimensional_model_builds(fact_table_setup):
    """
    Verify all dimension and fact tables created in GOLD schema.

    Expected tables:
    - dim_customer (SCD Type 2)
    - dim_date
    - dim_merchant_category
    - fct_transactions
    """
    cursor = fact_table_setup.cursor()

    # Query INFORMATION_SCHEMA for GOLD tables
    cursor.execute("""
        SELECT table_name
        FROM SNOWFLAKE_DEMO.INFORMATION_SCHEMA.TABLES
        WHERE table_schema = 'GOLD'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)

    tables = {row[0] for row in cursor.fetchall()}

    # Expected tables
    expected_tables = {
        'DIM_CUSTOMER',
        'DIM_DATE',
        'DIM_MERCHANT_CATEGORY',
        'FCT_TRANSACTIONS'
    }

    # Assert all expected tables exist
    missing_tables = expected_tables - tables
    assert len(missing_tables) == 0, f"Missing tables in GOLD schema: {missing_tables}"

    print(f"✓ All {len(expected_tables)} dimensional model tables exist in GOLD schema")


# ============================================================================
# Test 2: Fact Table Row Count
# ============================================================================

def test_fact_table_row_count(fact_table_setup):
    """
    Verify fact table contains approximately 13.5M rows.

    Allows ±5% variance from expected count.
    """
    cursor = fact_table_setup.cursor()

    # Count rows in fact table
    cursor.execute("""
        SELECT COUNT(*) AS row_count
        FROM GOLD.FCT_TRANSACTIONS
    """)

    actual_count = cursor.fetchone()[0]

    # Expected: ~13.5M rows (allow ±5% variance)
    expected_count = 13_500_000
    variance = 0.05
    min_expected = expected_count * (1 - variance)
    max_expected = expected_count * (1 + variance)

    assert min_expected <= actual_count <= max_expected, \
        f"Fact table row count {actual_count:,} outside expected range [{min_expected:,.0f}, {max_expected:,.0f}]"

    print(f"✓ Fact table contains {actual_count:,} rows (within expected range)")


# ============================================================================
# Test 3: All Foreign Key Relationships Valid
# ============================================================================

def test_all_fk_relationships_valid(fact_table_setup):
    """
    Verify all foreign keys in fact table reference valid dimension records.

    Checks:
    - customer_key → dim_customer.customer_key
    - date_key → dim_date.date_key
    - merchant_category_key → dim_merchant_category.category_key
    """
    cursor = fact_table_setup.cursor()

    # Test 1: customer_key FK
    cursor.execute("""
        SELECT COUNT(*) AS orphan_count
        FROM GOLD.FCT_TRANSACTIONS f
        WHERE NOT EXISTS (
            SELECT 1 FROM GOLD.DIM_CUSTOMER c
            WHERE f.customer_key = c.customer_key
        )
    """)

    orphan_customers = cursor.fetchone()[0]
    assert orphan_customers == 0, f"Found {orphan_customers:,} orphan customer_key values in fact table"

    # Test 2: date_key FK
    cursor.execute("""
        SELECT COUNT(*) AS orphan_count
        FROM GOLD.FCT_TRANSACTIONS f
        WHERE NOT EXISTS (
            SELECT 1 FROM GOLD.DIM_DATE d
            WHERE f.date_key = d.date_key
        )
    """)

    orphan_dates = cursor.fetchone()[0]
    assert orphan_dates == 0, f"Found {orphan_dates:,} orphan date_key values in fact table"

    # Test 3: merchant_category_key FK
    cursor.execute("""
        SELECT COUNT(*) AS orphan_count
        FROM GOLD.FCT_TRANSACTIONS f
        WHERE NOT EXISTS (
            SELECT 1 FROM GOLD.DIM_MERCHANT_CATEGORY cat
            WHERE f.merchant_category_key = cat.category_key
        )
    """)

    orphan_categories = cursor.fetchone()[0]
    assert orphan_categories == 0, f"Found {orphan_categories:,} orphan merchant_category_key values in fact table"

    print("✓ All foreign key relationships valid (0 orphan records)")


# ============================================================================
# Test 4: No Orphan Transactions (Quality Filter Works)
# ============================================================================

def test_no_orphan_transactions(fact_table_setup):
    """
    Verify quality filter successfully excluded transactions with missing FKs.

    Compares staging row count vs fact row count.
    """
    cursor = fact_table_setup.cursor()

    # Count staging transactions
    cursor.execute("""
        SELECT COUNT(*) AS staging_count
        FROM SILVER.STG_TRANSACTIONS
    """)

    staging_count = cursor.fetchone()[0]

    # Count fact transactions
    cursor.execute("""
        SELECT COUNT(*) AS fact_count
        FROM GOLD.FCT_TRANSACTIONS
    """)

    fact_count = cursor.fetchone()[0]

    # Calculate exclusion rate
    exclusion_rate = (staging_count - fact_count) / staging_count if staging_count > 0 else 0

    # Should exclude very few records (<1%)
    assert exclusion_rate < 0.01, \
        f"Excluded {exclusion_rate:.2%} of transactions (should be <1%). Check dimension joins."

    print(f"✓ Excluded {staging_count - fact_count:,} orphan transactions ({exclusion_rate:.4%})")


# ============================================================================
# Test 5: Clustering Applied
# ============================================================================

def test_clustering_applied(fact_table_setup):
    """
    Verify clustering key applied to fact table for time-series optimization.

    Expected: CLUSTER BY (transaction_date)
    """
    cursor = fact_table_setup.cursor()

    # Query clustering information
    cursor.execute("""
        SELECT clustering_key
        FROM SNOWFLAKE_DEMO.INFORMATION_SCHEMA.TABLES
        WHERE table_schema = 'GOLD'
          AND table_name = 'FCT_TRANSACTIONS'
    """)

    row = cursor.fetchone()
    clustering_key = row[0] if row else None

    # Verify clustering key exists and contains transaction_date
    assert clustering_key is not None, "No clustering key found on fact table"
    assert 'TRANSACTION_DATE' in clustering_key.upper(), \
        f"Expected clustering on TRANSACTION_DATE, got: {clustering_key}"

    print(f"✓ Clustering applied: {clustering_key}")


# ============================================================================
# Test 6: Star Schema Query Performance
# ============================================================================

def test_star_schema_query_performance(fact_table_setup):
    """
    Verify star schema query completes efficiently.

    Query: Customer segment spending by category and month
    Expected: < 10 seconds on SMALL warehouse
    """
    cursor = fact_table_setup.cursor()

    # Complex star schema query
    query = """
        SELECT
            c.customer_segment,
            cat.category_group,
            d.year,
            d.month,
            COUNT(*) AS txn_count,
            SUM(f.transaction_amount) AS total_spend,
            AVG(f.transaction_amount) AS avg_spend
        FROM GOLD.FCT_TRANSACTIONS f
        JOIN GOLD.DIM_CUSTOMER c
            ON f.customer_key = c.customer_key
        JOIN GOLD.DIM_MERCHANT_CATEGORY cat
            ON f.merchant_category_key = cat.category_key
        JOIN GOLD.DIM_DATE d
            ON f.date_key = d.date_key
        WHERE c.is_current = TRUE
        GROUP BY 1, 2, 3, 4
        ORDER BY total_spend DESC
        LIMIT 100
    """

    # Measure query execution time
    start_time = time.time()
    cursor.execute(query)
    results = cursor.fetchall()
    execution_time = time.time() - start_time

    # Verify results returned
    assert len(results) > 0, "Star schema query returned no results"

    # Performance threshold: 10 seconds on SMALL warehouse
    max_execution_time = 10.0
    assert execution_time < max_execution_time, \
        f"Star schema query took {execution_time:.2f}s (expected <{max_execution_time}s)"

    print(f"✓ Star schema query completed in {execution_time:.2f}s ({len(results)} rows)")


# ============================================================================
# Test 7: Incremental Load Fact Table
# ============================================================================

def test_incremental_load_fact_table(fact_table_setup, dbt_project_dir):
    """
    Verify incremental loading works for fact table.

    Tests:
    1. Initial row count
    2. Run dbt incrementally (should add 0 new rows if no staging changes)
    3. Verify row count unchanged
    """
    cursor = fact_table_setup.cursor()

    # Get initial row count
    cursor.execute("""
        SELECT COUNT(*) AS row_count
        FROM GOLD.FCT_TRANSACTIONS
    """)

    initial_count = cursor.fetchone()[0]

    # Run dbt incrementally
    import subprocess
    import os

    original_dir = Path.cwd()
    dbt_dir = Path(dbt_project_dir)

    try:
        os.chdir(dbt_dir)

        # Run incremental build
        result = subprocess.run(
            ["dbt", "run", "--models", "fct_transactions", "--profiles-dir", "."],
            capture_output=True,
            text=True,
            timeout=120
        )

        if result.returncode != 0:
            pytest.fail(f"Incremental dbt run failed:\n{result.stderr}")

        # Get new row count
        cursor.execute("""
            SELECT COUNT(*) AS row_count
            FROM GOLD.FCT_TRANSACTIONS
        """)

        final_count = cursor.fetchone()[0]

        # Should be same count (no new staging data)
        assert final_count == initial_count, \
            f"Row count changed unexpectedly: {initial_count:,} → {final_count:,}"

        print(f"✓ Incremental load successful (row count stable: {final_count:,})")

    finally:
        os.chdir(original_dir)


# ============================================================================
# Test 8: Star Schema Integrity
# ============================================================================

def test_star_schema_integrity(fact_table_setup):
    """
    Verify star schema design integrity.

    Checks:
    - All dimension tables have primary keys
    - Fact table has unique transaction_key
    - Date dimension covers transaction date range
    """
    cursor = fact_table_setup.cursor()

    # Test 1: dim_customer has unique customer_key
    cursor.execute("""
        SELECT COUNT(*) AS total_keys,
               COUNT(DISTINCT customer_key) AS unique_keys
        FROM GOLD.DIM_CUSTOMER
    """)

    row = cursor.fetchone()
    assert row[0] == row[1], f"dim_customer has duplicate customer_key values"

    # Test 2: dim_date has unique date_key
    cursor.execute("""
        SELECT COUNT(*) AS total_keys,
               COUNT(DISTINCT date_key) AS unique_keys
        FROM GOLD.DIM_DATE
    """)

    row = cursor.fetchone()
    assert row[0] == row[1], f"dim_date has duplicate date_key values"

    # Test 3: dim_merchant_category has unique category_key
    cursor.execute("""
        SELECT COUNT(*) AS total_keys,
               COUNT(DISTINCT category_key) AS unique_keys
        FROM GOLD.DIM_MERCHANT_CATEGORY
    """)

    row = cursor.fetchone()
    assert row[0] == row[1], f"dim_merchant_category has duplicate category_key values"

    # Test 4: fct_transactions has unique transaction_key
    cursor.execute("""
        SELECT COUNT(*) AS total_keys,
               COUNT(DISTINCT transaction_key) AS unique_keys
        FROM GOLD.FCT_TRANSACTIONS
    """)

    row = cursor.fetchone()
    assert row[0] == row[1], f"fct_transactions has duplicate transaction_key values"

    # Test 5: Date dimension covers transaction date range
    cursor.execute("""
        SELECT
            MIN(transaction_date) AS min_txn_date,
            MAX(transaction_date) AS max_txn_date,
            (SELECT MIN(date_day) FROM GOLD.DIM_DATE) AS min_dim_date,
            (SELECT MAX(date_day) FROM GOLD.DIM_DATE) AS max_dim_date
        FROM GOLD.FCT_TRANSACTIONS
    """)

    row = cursor.fetchone()
    min_txn_date, max_txn_date, min_dim_date, max_dim_date = row

    assert min_txn_date >= min_dim_date, \
        f"Date dimension missing dates before {min_txn_date}"
    assert max_txn_date <= max_dim_date, \
        f"Date dimension missing dates after {max_txn_date}"

    print("✓ Star schema integrity verified (unique keys, date coverage)")


# ============================================================================
# Summary
# ============================================================================

def test_summary(fact_table_setup):
    """
    Print summary of dimensional model.
    """
    cursor = fact_table_setup.cursor()

    # Get row counts for all tables
    cursor.execute("""
        SELECT
            (SELECT COUNT(*) FROM GOLD.DIM_CUSTOMER) AS dim_customer_count,
            (SELECT COUNT(*) FROM GOLD.DIM_DATE) AS dim_date_count,
            (SELECT COUNT(*) FROM GOLD.DIM_MERCHANT_CATEGORY) AS dim_category_count,
            (SELECT COUNT(*) FROM GOLD.FCT_TRANSACTIONS) AS fact_count
    """)

    row = cursor.fetchone()

    print("\n" + "="*80)
    print("DIMENSIONAL MODEL SUMMARY")
    print("="*80)
    print(f"dim_customer:            {row[0]:>12,} rows")
    print(f"dim_date:                {row[1]:>12,} rows")
    print(f"dim_merchant_category:   {row[2]:>12,} rows")
    print(f"fct_transactions:        {row[3]:>12,} rows")
    print("="*80)
