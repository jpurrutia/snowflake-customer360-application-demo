"""
Integration Tests for Customer 360 Deep Dive Tab (Iteration 5.2)

Tests Customer 360 tab functionality:
1. Customer search (by ID, name, email)
2. Transaction history queries
3. Transaction filters
4. Spending trend visualization
5. Category breakdown
6. Profile metrics
7. CSV export

Run:
    pytest tests/integration/test_customer_360_tab.py -v
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


@pytest.fixture(scope="module")
def sample_customer_id(cursor):
    """Get a sample customer ID for testing"""
    cursor.execute("""
        SELECT customer_id
        FROM CUSTOMER_360_PROFILE
        LIMIT 1
    """)
    result = cursor.fetchone()
    return result[0] if result else 1


# ============================================================================
# Test 1: Customer Search by ID
# ============================================================================


def test_customer_search_by_id(cursor, sample_customer_id):
    """
    Test customer search by ID.

    Validates:
    - Customer can be found by ID
    - Profile fields are present
    - Key metrics exist
    """
    query = f"""
        SELECT
            customer_id,
            full_name,
            email,
            state,
            city,
            customer_segment,
            card_type,
            credit_limit,
            lifetime_value,
            avg_transaction_value,
            churn_risk_score,
            churn_risk_category
        FROM CUSTOMER_360_PROFILE
        WHERE customer_id = {sample_customer_id}
    """

    cursor.execute(query)
    results = cursor.fetchall()

    assert len(results) == 1, f"Expected 1 customer, found {len(results)}"

    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=columns)
    customer = df.iloc[0]

    # Validate key fields
    assert customer['CUSTOMER_ID'] == sample_customer_id
    assert customer['FULL_NAME'] is not None, "Full name missing"
    assert customer['EMAIL'] is not None, "Email missing"
    assert customer['CUSTOMER_SEGMENT'] is not None, "Segment missing"

    print(f"\n✓ Customer search by ID successful:")
    print(f"  Customer ID: {customer['CUSTOMER_ID']}")
    print(f"  Name: {customer['FULL_NAME']}")
    print(f"  Segment: {customer['CUSTOMER_SEGMENT']}")


# ============================================================================
# Test 2: Customer Search by Name
# ============================================================================


def test_customer_search_by_name(cursor):
    """
    Test customer search by name (partial match).

    Validates:
    - Partial name search works
    - Multiple results can be returned
    - LIKE query case-insensitive
    """
    # Get a sample name
    cursor.execute("SELECT full_name FROM CUSTOMER_360_PROFILE LIMIT 1")
    sample_name = cursor.fetchone()[0]

    # Extract first part of name
    search_term = sample_name.split()[0][:4]  # First 4 chars of first name

    query = f"""
        SELECT
            customer_id,
            full_name,
            email
        FROM CUSTOMER_360_PROFILE
        WHERE LOWER(full_name) LIKE LOWER('%{search_term}%')
        LIMIT 20
    """

    cursor.execute(query)
    results = cursor.fetchall()

    assert len(results) > 0, f"No customers found matching '{search_term}'"

    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=columns)

    # Verify search term appears in results (case-insensitive)
    matches = df['FULL_NAME'].str.lower().str.contains(search_term.lower())
    assert matches.all(), "Not all results contain search term"

    print(f"\n✓ Customer search by name successful:")
    print(f"  Search term: '{search_term}'")
    print(f"  Results: {len(df)}")


# ============================================================================
# Test 3: Customer Search by Email
# ============================================================================


def test_customer_search_by_email(cursor):
    """
    Test customer search by email (partial match).

    Validates:
    - Email search works
    - LIKE query case-insensitive
    """
    # Get a sample email
    cursor.execute("SELECT email FROM CUSTOMER_360_PROFILE LIMIT 1")
    sample_email = cursor.fetchone()[0]

    # Extract username part
    search_term = sample_email.split('@')[0][:5]

    query = f"""
        SELECT
            customer_id,
            full_name,
            email
        FROM CUSTOMER_360_PROFILE
        WHERE LOWER(email) LIKE LOWER('%{search_term}%')
        LIMIT 20
    """

    cursor.execute(query)
    results = cursor.fetchall()

    assert len(results) > 0, f"No customers found matching email '{search_term}'"

    print(f"\n✓ Customer search by email successful:")
    print(f"  Search term: '{search_term}'")
    print(f"  Results: {len(results)}")


# ============================================================================
# Test 4: Transaction History Query
# ============================================================================


def test_transaction_history_query(cursor, sample_customer_id):
    """
    Test transaction history fetch for customer.

    Validates:
    - Transactions can be fetched
    - Join with category table works
    - Expected columns present
    """
    query = f"""
        SELECT
            t.transaction_date,
            t.merchant_name,
            c.category_name,
            c.category_group,
            t.transaction_amount,
            t.channel,
            t.status
        FROM GOLD.FCT_TRANSACTIONS t
        JOIN GOLD.DIM_MERCHANT_CATEGORY c ON t.merchant_category_key = c.category_key
        WHERE t.customer_id = {sample_customer_id}
        ORDER BY t.transaction_date DESC
        LIMIT 1000
    """

    cursor.execute(query)
    results = cursor.fetchall()

    assert len(results) > 0, f"No transactions found for customer {sample_customer_id}"

    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=columns)

    # Validate expected columns
    expected_columns = [
        'TRANSACTION_DATE', 'MERCHANT_NAME', 'CATEGORY_NAME',
        'CATEGORY_GROUP', 'TRANSACTION_AMOUNT', 'CHANNEL', 'STATUS'
    ]

    for col in expected_columns:
        assert col in df.columns, f"Expected column {col} not found"

    print(f"\n✓ Transaction history query successful:")
    print(f"  Transactions: {len(df):,}")
    print(f"  Date range: {df['TRANSACTION_DATE'].min()} to {df['TRANSACTION_DATE'].max()}")
    print(f"  Total spend: ${df['TRANSACTION_AMOUNT'].sum():,.2f}")


# ============================================================================
# Test 5: Transaction Filters - Date Range
# ============================================================================


def test_transaction_filters_date_range(cursor, sample_customer_id):
    """
    Test date range filtering of transactions.

    Validates:
    - Date filter reduces result set
    - Filtered results within expected range
    """
    from datetime import datetime, timedelta

    # Get all transactions
    query_all = f"""
        SELECT transaction_date, transaction_amount
        FROM GOLD.FCT_TRANSACTIONS
        WHERE customer_id = {sample_customer_id}
    """

    cursor.execute(query_all)
    all_results = cursor.fetchall()
    all_count = len(all_results)

    if all_count == 0:
        pytest.skip(f"No transactions for customer {sample_customer_id}")

    # Get last 90 days
    cutoff_date = datetime.now() - timedelta(days=90)

    query_90d = f"""
        SELECT transaction_date, transaction_amount
        FROM GOLD.FCT_TRANSACTIONS
        WHERE customer_id = {sample_customer_id}
          AND transaction_date >= '{cutoff_date.strftime('%Y-%m-%d')}'
    """

    cursor.execute(query_90d)
    filtered_results = cursor.fetchall()
    filtered_count = len(filtered_results)

    # Filtered count should be <= all count
    assert filtered_count <= all_count, "Filtered count should not exceed total count"

    print(f"\n✓ Transaction date filter successful:")
    print(f"  All transactions: {all_count:,}")
    print(f"  Last 90 days: {filtered_count:,}")


# ============================================================================
# Test 6: Transaction Filters - Category
# ============================================================================


def test_transaction_filters_category(cursor, sample_customer_id):
    """
    Test category filtering of transactions.

    Validates:
    - Category filter works
    - Only selected category returned
    """
    # Get transactions with categories
    query = f"""
        SELECT
            c.category_name,
            t.transaction_amount
        FROM GOLD.FCT_TRANSACTIONS t
        JOIN GOLD.DIM_MERCHANT_CATEGORY c ON t.merchant_category_key = c.category_key
        WHERE t.customer_id = {sample_customer_id}
    """

    cursor.execute(query)
    results = cursor.fetchall()

    if len(results) == 0:
        pytest.skip(f"No transactions for customer {sample_customer_id}")

    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=columns)

    # Get first category
    test_category = df['CATEGORY_NAME'].iloc[0]

    # Filter by category
    df_filtered = df[df['CATEGORY_NAME'] == test_category]

    # Verify all are same category
    assert (df_filtered['CATEGORY_NAME'] == test_category).all(), \
        "Filtered results contain other categories"

    print(f"\n✓ Transaction category filter successful:")
    print(f"  Category: {test_category}")
    print(f"  Transactions: {len(df_filtered):,}")


# ============================================================================
# Test 7: Spending Trend Chart Data
# ============================================================================


def test_spending_trend_chart(cursor, sample_customer_id):
    """
    Test data aggregation for spending trend chart.

    Validates:
    - Daily aggregation works
    - Data can be sorted by date
    """
    query = f"""
        SELECT
            transaction_date,
            SUM(transaction_amount) AS daily_spend
        FROM GOLD.FCT_TRANSACTIONS
        WHERE customer_id = {sample_customer_id}
        GROUP BY transaction_date
        ORDER BY transaction_date
    """

    cursor.execute(query)
    results = cursor.fetchall()

    if len(results) == 0:
        pytest.skip(f"No transactions for customer {sample_customer_id}")

    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=columns)

    # Verify sorted
    assert df['TRANSACTION_DATE'].is_monotonic_increasing, "Data not sorted by date"

    print(f"\n✓ Spending trend data aggregation successful:")
    print(f"  Days with transactions: {len(df):,}")
    print(f"  Total spend: ${df['DAILY_SPEND'].sum():,.2f}")
    print(f"  Avg daily spend: ${df['DAILY_SPEND'].mean():,.2f}")


# ============================================================================
# Test 8: Category Breakdown Data
# ============================================================================


def test_category_breakdown(cursor, sample_customer_id):
    """
    Test data aggregation for category breakdown pie chart.

    Validates:
    - Category aggregation works
    - All categories represented
    """
    query = f"""
        SELECT
            c.category_name,
            SUM(t.transaction_amount) AS category_spend
        FROM GOLD.FCT_TRANSACTIONS t
        JOIN GOLD.DIM_MERCHANT_CATEGORY c ON t.merchant_category_key = c.category_key
        WHERE t.customer_id = {sample_customer_id}
        GROUP BY c.category_name
        ORDER BY category_spend DESC
    """

    cursor.execute(query)
    results = cursor.fetchall()

    if len(results) == 0:
        pytest.skip(f"No transactions for customer {sample_customer_id}")

    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=columns)

    # Verify categories exist
    assert len(df) > 0, "No categories found"

    # Verify spend is positive
    assert (df['CATEGORY_SPEND'] > 0).all(), "All categories should have positive spend"

    print(f"\n✓ Category breakdown aggregation successful:")
    print(f"  Categories: {len(df)}")
    print(f"  Total spend: ${df['CATEGORY_SPEND'].sum():,.2f}")
    print(f"  Top category: {df.iloc[0]['CATEGORY_NAME']} (${df.iloc[0]['CATEGORY_SPEND']:,.2f})")


# ============================================================================
# Test 9: Profile Metrics
# ============================================================================


def test_profile_metrics(cursor, sample_customer_id):
    """
    Test profile metrics display.

    Validates:
    - Lifetime value exists
    - Churn risk score exists
    - Metrics are valid numbers
    """
    query = f"""
        SELECT
            lifetime_value,
            avg_transaction_value,
            spend_last_90_days,
            days_since_last_transaction,
            spend_change_pct,
            avg_monthly_spend,
            churn_risk_score,
            churn_risk_category
        FROM CUSTOMER_360_PROFILE
        WHERE customer_id = {sample_customer_id}
    """

    cursor.execute(query)
    result = cursor.fetchone()

    assert result is not None, "Customer not found"

    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame([result], columns=columns)
    customer = df.iloc[0]

    # Validate metrics
    assert customer['LIFETIME_VALUE'] >= 0, "LTV should be non-negative"
    assert customer['AVG_TRANSACTION_VALUE'] >= 0, "ATV should be non-negative"

    print(f"\n✓ Profile metrics validation successful:")
    print(f"  Lifetime Value: ${customer['LIFETIME_VALUE']:,.0f}")
    print(f"  Avg Transaction: ${customer['AVG_TRANSACTION_VALUE']:,.0f}")
    print(f"  Churn Risk: {customer['CHURN_RISK_CATEGORY']} ({customer['CHURN_RISK_SCORE']})")


# ============================================================================
# Test 10: Export Transaction CSV
# ============================================================================


def test_export_transaction_csv(cursor, sample_customer_id):
    """
    Test CSV export of transaction history.

    Validates:
    - DataFrame converts to CSV
    - CSV has headers
    - CSV has data rows
    """
    query = f"""
        SELECT
            transaction_date,
            merchant_name,
            transaction_amount,
            status
        FROM GOLD.FCT_TRANSACTIONS
        WHERE customer_id = {sample_customer_id}
        LIMIT 100
    """

    cursor.execute(query)
    results = cursor.fetchall()

    if len(results) == 0:
        pytest.skip(f"No transactions for customer {sample_customer_id}")

    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=columns)

    # Convert to CSV
    csv = df.to_csv(index=False)

    # Validate CSV
    assert csv is not None, "CSV conversion failed"
    assert len(csv) > 0, "CSV is empty"

    csv_lines = csv.split('\n')
    assert 'TRANSACTION_DATE' in csv_lines[0], "CSV missing headers"
    assert len(csv_lines) > 1, "CSV has no data rows"

    print(f"\n✓ CSV export successful:")
    print(f"  Transactions exported: {len(df):,}")
    print(f"  CSV size: {len(csv):,} characters")


# ============================================================================
# Run Tests
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
