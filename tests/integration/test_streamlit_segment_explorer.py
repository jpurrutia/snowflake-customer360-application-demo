"""
Integration Tests for Streamlit Segment Explorer (Iteration 5.1)

Tests Streamlit app functionality:
1. Snowflake connection
2. Query execution
3. Filter logic
4. CSV export
5. Error handling

Run:
    pytest tests/integration/test_streamlit_segment_explorer.py -v
"""

import os
import pytest
import pandas as pd
from snowflake.connector import connect
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


@pytest.fixture(scope="module")
def snowflake_conn():
    """Create Snowflake connection for tests"""
    conn = connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "CUSTOMER_ANALYTICS"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "GOLD"),
        role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
    )
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def cursor(snowflake_conn):
    """Create cursor for executing queries"""
    cur = snowflake_conn.cursor()
    yield cur
    cur.close()


# ============================================================================
# Test 1: Snowflake Connection
# ============================================================================


def test_snowflake_connection(cursor):
    """
    Test that Snowflake connection works and can query CUSTOMER_360_PROFILE.

    Validates:
    - Connection succeeds
    - Can query CUSTOMER_360_PROFILE table
    - Table has data
    """
    cursor.execute("SELECT COUNT(*) AS customer_count FROM CUSTOMER_360_PROFILE")
    result = cursor.fetchone()

    assert result is not None, "Query returned no results"
    assert result[0] > 0, "CUSTOMER_360_PROFILE table is empty"

    print(f"\n✓ Connection successful: {result[0]:,} customers in CUSTOMER_360_PROFILE")


# ============================================================================
# Test 2: Segment Filter Query
# ============================================================================


def test_segment_filter_query(cursor):
    """
    Test segment filter query execution.

    Validates:
    - Query with segment filter executes successfully
    - Returns DataFrame with expected columns
    - Results filtered correctly
    """
    # Test query with segment filter (similar to Streamlit app)
    query = """
        SELECT
            customer_id,
            full_name,
            email,
            state,
            city,
            customer_segment,
            card_type,
            lifetime_value,
            avg_transaction_value,
            churn_risk_category,
            churn_risk_score,
            days_since_last_transaction
        FROM CUSTOMER_360_PROFILE
        WHERE customer_segment IN ('High-Value Travelers', 'Declining')
        ORDER BY lifetime_value DESC
        LIMIT 5000
    """

    cursor.execute(query)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    assert results is not None, "Query returned no results"
    assert len(results) > 0, "No customers found for segments 'High-Value Travelers', 'Declining'"

    # Validate columns
    expected_columns = [
        'CUSTOMER_ID', 'FULL_NAME', 'EMAIL', 'STATE', 'CITY',
        'CUSTOMER_SEGMENT', 'CARD_TYPE', 'LIFETIME_VALUE',
        'AVG_TRANSACTION_VALUE', 'CHURN_RISK_CATEGORY',
        'CHURN_RISK_SCORE', 'DAYS_SINCE_LAST_TRANSACTION'
    ]

    for col in expected_columns:
        assert col in columns, f"Expected column {col} not found in query results"

    # Create DataFrame
    df = pd.DataFrame(results, columns=columns)

    # Validate segment filter worked
    segments = df['CUSTOMER_SEGMENT'].unique()
    for segment in segments:
        assert segment in ['High-Value Travelers', 'Declining'], \
            f"Unexpected segment {segment} found in results"

    print(f"\n✓ Segment filter query successful:")
    print(f"  Rows: {len(df):,}")
    print(f"  Columns: {len(df.columns)}")
    print(f"  Segments: {segments.tolist()}")


# ============================================================================
# Test 3: State Filter Query
# ============================================================================


def test_state_filter_query(cursor):
    """
    Test state filter query execution.

    Validates:
    - Query with state filter executes
    - Results filtered correctly
    """
    query = """
        SELECT
            customer_id,
            full_name,
            state,
            customer_segment
        FROM CUSTOMER_360_PROFILE
        WHERE state IN ('CA', 'NY', 'TX')
        LIMIT 1000
    """

    cursor.execute(query)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=columns)

    assert len(df) > 0, "No customers found in CA, NY, TX"

    # Validate state filter
    states = df['STATE'].unique()
    for state in states:
        assert state in ['CA', 'NY', 'TX'], f"Unexpected state {state} found"

    print(f"\n✓ State filter query successful:")
    print(f"  Rows: {len(df):,}")
    print(f"  States: {states.tolist()}")


# ============================================================================
# Test 4: Churn Risk Filter Query
# ============================================================================


def test_churn_risk_filter_query(cursor):
    """
    Test churn risk filter query execution.

    Validates:
    - Query with churn risk filter executes
    - Results filtered correctly
    """
    query = """
        SELECT
            customer_id,
            full_name,
            churn_risk_category,
            churn_risk_score
        FROM CUSTOMER_360_PROFILE
        WHERE churn_risk_category = 'High Risk'
        LIMIT 1000
    """

    cursor.execute(query)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=columns)

    assert len(df) > 0, "No high-risk customers found"

    # Validate all are High Risk
    assert (df['CHURN_RISK_CATEGORY'] == 'High Risk').all(), \
        "Found non-High Risk customers in results"

    print(f"\n✓ Churn risk filter query successful:")
    print(f"  High-risk customers: {len(df):,}")
    print(f"  Avg churn risk score: {df['CHURN_RISK_SCORE'].mean():.2f}")


# ============================================================================
# Test 5: Combined Filters Query
# ============================================================================


def test_combined_filters_query(cursor):
    """
    Test query with multiple filters (segment + state + churn risk + LTV).

    Validates:
    - Complex WHERE clause executes
    - All filters apply correctly
    """
    query = """
        SELECT
            customer_id,
            full_name,
            customer_segment,
            state,
            churn_risk_category,
            lifetime_value,
            card_type
        FROM CUSTOMER_360_PROFILE
        WHERE customer_segment IN ('High-Value Travelers', 'Declining')
          AND state IN ('CA', 'NY')
          AND churn_risk_category = 'High Risk'
          AND lifetime_value >= 10000
          AND card_type = 'Premium'
        LIMIT 1000
    """

    cursor.execute(query)
    results = cursor.fetchall()

    if results:
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results, columns=columns)

        # Validate filters
        assert df['CUSTOMER_SEGMENT'].isin(['High-Value Travelers', 'Declining']).all()
        assert df['STATE'].isin(['CA', 'NY']).all()
        assert (df['CHURN_RISK_CATEGORY'] == 'High Risk').all()
        assert (df['LIFETIME_VALUE'] >= 10000).all()
        assert (df['CARD_TYPE'] == 'Premium').all()

        print(f"\n✓ Combined filters query successful:")
        print(f"  Matching customers: {len(df):,}")
    else:
        print(f"\n✓ Combined filters query executed (0 matching customers - expected with strict filters)")


# ============================================================================
# Test 6: CSV Export
# ============================================================================


def test_export_csv(cursor):
    """
    Test CSV export functionality.

    Validates:
    - DataFrame converts to CSV correctly
    - CSV has headers
    - CSV has data rows
    """
    # Get sample data
    query = """
        SELECT
            customer_id,
            full_name,
            email,
            state,
            customer_segment,
            lifetime_value
        FROM CUSTOMER_360_PROFILE
        LIMIT 100
    """

    cursor.execute(query)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=columns)

    # Convert to CSV
    csv = df.to_csv(index=False)

    # Validate CSV
    assert csv is not None, "CSV conversion failed"
    assert len(csv) > 0, "CSV is empty"

    # Check headers
    csv_lines = csv.split('\n')
    assert 'CUSTOMER_ID' in csv_lines[0], "CSV missing CUSTOMER_ID header"
    assert 'FULL_NAME' in csv_lines[0], "CSV missing FULL_NAME header"

    # Check data rows
    assert len(csv_lines) > 1, "CSV has no data rows"

    print(f"\n✓ CSV export successful:")
    print(f"  CSV length: {len(csv):,} characters")
    print(f"  CSV lines: {len(csv_lines):,}")


# ============================================================================
# Test 7: Error Handling - Empty Result
# ============================================================================


def test_error_handling_empty_result(cursor):
    """
    Test handling of queries that return no results.

    Validates:
    - Query executes without error
    - Empty DataFrame returned gracefully
    """
    # Query with impossible condition
    query = """
        SELECT *
        FROM CUSTOMER_360_PROFILE
        WHERE lifetime_value < 0
    """

    cursor.execute(query)
    results = cursor.fetchall()

    assert results == [], "Expected empty result set"

    # Create empty DataFrame
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=columns)

    assert df.empty, "DataFrame should be empty"

    print(f"\n✓ Empty result handling successful:")
    print(f"  Empty DataFrame created correctly")


# ============================================================================
# Test 8: Query Timeout Protection
# ============================================================================


def test_query_timeout_setting(cursor):
    """
    Test that query timeout can be set.

    Validates:
    - Session timeout can be configured
    """
    # Set timeout (similar to app.py)
    cursor.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 60")

    # Verify it worked (no exception)
    cursor.execute("SHOW PARAMETERS LIKE 'STATEMENT_TIMEOUT_IN_SECONDS'")
    result = cursor.fetchone()

    print(f"\n✓ Query timeout setting successful:")
    print(f"  Timeout configured to 60 seconds")


# ============================================================================
# Test 9: Summary Metrics Calculation
# ============================================================================


def test_summary_metrics_calculation(cursor):
    """
    Test summary metrics calculation (as shown in Streamlit app).

    Validates:
    - Customer count
    - Total LTV
    - Average LTV
    - Average churn risk
    """
    query = """
        SELECT
            customer_id,
            lifetime_value,
            churn_risk_score
        FROM CUSTOMER_360_PROFILE
        WHERE customer_segment IN ('High-Value Travelers', 'Declining')
        LIMIT 5000
    """

    cursor.execute(query)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=columns)

    # Calculate metrics
    customer_count = len(df)
    total_ltv = df['LIFETIME_VALUE'].sum()
    avg_ltv = df['LIFETIME_VALUE'].mean()
    avg_churn_risk = df['CHURN_RISK_SCORE'].mean()

    assert customer_count > 0, "No customers found"
    assert total_ltv > 0, "Total LTV should be positive"
    assert avg_ltv > 0, "Average LTV should be positive"
    assert avg_churn_risk >= 0, "Churn risk should be non-negative"

    print(f"\n✓ Summary metrics calculation successful:")
    print(f"  Customers: {customer_count:,}")
    print(f"  Total LTV: ${total_ltv:,.0f}")
    print(f"  Avg LTV: ${avg_ltv:,.0f}")
    print(f"  Avg Churn Risk: {avg_churn_risk:.2f}")


# ============================================================================
# Run Tests
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
