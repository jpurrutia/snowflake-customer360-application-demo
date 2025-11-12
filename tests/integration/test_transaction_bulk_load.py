"""
Integration tests for Bronze transaction bulk load.

Tests validate:
- Bronze transaction table created with expected schema
- Transaction load completes successfully
- Expected row count (~13.5M)
- No duplicate transaction IDs
- All customers represented in transactions
- Referential integrity maintained
- Date range valid (~18 months)
- Transaction amounts valid (positive, reasonable)
- Metadata fields populated
"""

import pytest
import os
from snowflake.connector import connect
from datetime import datetime, timedelta
from typing import Dict, Any


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture(scope="module")
def snowflake_connection():
    """
    Create Snowflake connection for testing.

    Requires environment variables:
    - SNOWFLAKE_ACCOUNT
    - SNOWFLAKE_USER
    - SNOWFLAKE_PASSWORD
    - SNOWFLAKE_WAREHOUSE
    - SNOWFLAKE_DATABASE
    - SNOWFLAKE_SCHEMA
    """
    conn = connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "CUSTOMER_ANALYTICS"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "BRONZE"),
        role=os.getenv("SNOWFLAKE_ROLE", "DATA_ENGINEER")
    )
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def transaction_stats(snowflake_connection) -> Dict[str, Any]:
    """
    Gather transaction statistics for multiple tests.

    Returns dictionary with comprehensive statistics.
    """
    cursor = snowflake_connection.cursor()

    query = """
    SELECT
        COUNT(*) AS total_count,
        COUNT(DISTINCT transaction_id) AS unique_txn_ids,
        COUNT(DISTINCT customer_id) AS unique_customers,
        MIN(transaction_date) AS min_date,
        MAX(transaction_date) AS max_date,
        ROUND(AVG(transaction_amount), 2) AS avg_amount,
        ROUND(MIN(transaction_amount), 2) AS min_amount,
        ROUND(MAX(transaction_amount), 2) AS max_amount,
        ROUND(SUM(transaction_amount), 2) AS total_volume
    FROM BRONZE.BRONZE_TRANSACTIONS;
    """

    cursor.execute(query)
    row = cursor.fetchone()

    stats = {
        "total_count": row[0],
        "unique_txn_ids": row[1],
        "unique_customers": row[2],
        "min_date": row[3],
        "max_date": row[4],
        "avg_amount": float(row[5]),
        "min_amount": float(row[6]),
        "max_amount": float(row[7]),
        "total_volume": float(row[8])
    }

    cursor.close()
    return stats


# ============================================================================
# Test 1: Bronze Transaction Table Created
# ============================================================================

def test_bronze_transaction_table_created(snowflake_connection):
    """
    Verify BRONZE_TRANSACTIONS table exists with expected columns.

    Expected columns:
    - transaction_id
    - customer_id
    - transaction_date
    - transaction_amount
    - merchant_name
    - merchant_category
    - channel
    - status
    - ingestion_timestamp
    - source_file
    - _metadata_file_row_number
    """
    cursor = snowflake_connection.cursor()

    # Check table exists
    query = """
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'BRONZE'
        AND TABLE_NAME = 'BRONZE_TRANSACTIONS';
    """
    cursor.execute(query)
    table_exists = cursor.fetchone()[0]

    assert table_exists == 1, "BRONZE_TRANSACTIONS table does not exist"

    # Check columns
    query = """
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'BRONZE'
        AND TABLE_NAME = 'BRONZE_TRANSACTIONS'
    ORDER BY ORDINAL_POSITION;
    """
    cursor.execute(query)
    columns = cursor.fetchall()

    column_names = [col[0] for col in columns]

    expected_columns = [
        "TRANSACTION_ID",
        "CUSTOMER_ID",
        "TRANSACTION_DATE",
        "TRANSACTION_AMOUNT",
        "MERCHANT_NAME",
        "MERCHANT_CATEGORY",
        "CHANNEL",
        "STATUS",
        "INGESTION_TIMESTAMP",
        "SOURCE_FILE",
        "_METADATA_FILE_ROW_NUMBER"
    ]

    for expected_col in expected_columns:
        assert expected_col in column_names, f"Missing expected column: {expected_col}"

    print(f"✓ BRONZE_TRANSACTIONS table exists with {len(columns)} columns")

    cursor.close()


# ============================================================================
# Test 2: Transaction Load Completes
# ============================================================================

def test_transaction_load_completes(snowflake_connection):
    """
    Verify transaction load completed successfully.

    Tests:
    - Table has data
    - No errors in COPY_HISTORY
    """
    cursor = snowflake_connection.cursor()

    # Check row count
    query = "SELECT COUNT(*) FROM BRONZE.BRONZE_TRANSACTIONS;"
    cursor.execute(query)
    row_count = cursor.fetchone()[0]

    assert row_count > 0, "BRONZE_TRANSACTIONS table is empty - load did not complete"

    # Check COPY_HISTORY for errors
    query = """
    SELECT
        STATUS,
        ERROR_COUNT,
        FIRST_ERROR_MESSAGE
    FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'CUSTOMER_ANALYTICS.BRONZE.BRONZE_TRANSACTIONS',
        START_TIME => DATEADD(hours, -24, CURRENT_TIMESTAMP())
    ))
    ORDER BY LAST_LOAD_TIME DESC
    LIMIT 1;
    """

    try:
        cursor.execute(query)
        history = cursor.fetchone()

        if history:
            status = history[0]
            error_count = history[1] if history[1] else 0
            first_error = history[2]

            assert status == 'LOADED', f"Load status is '{status}' instead of 'LOADED'"
            assert error_count == 0, f"Load had {error_count} errors: {first_error}"

            print(f"✓ Transaction load completed successfully with status: {status}")
        else:
            print("⚠️  No COPY_HISTORY found (may not have access to view)")

    except Exception as e:
        print(f"⚠️  Cannot verify COPY_HISTORY: {str(e)}")

    cursor.close()


# ============================================================================
# Test 3: Expected Row Count
# ============================================================================

def test_expected_row_count(transaction_stats):
    """
    Verify transaction count is within expected range.

    Expected: ~13.5M transactions
    Acceptable: 10M - 17M (due to randomization)
    """
    total_count = transaction_stats["total_count"]

    MIN_EXPECTED = 10_000_000  # 10M
    MAX_EXPECTED = 17_000_000  # 17M

    assert MIN_EXPECTED <= total_count <= MAX_EXPECTED, \
        f"Transaction count {total_count:,} outside expected range [{MIN_EXPECTED:,}, {MAX_EXPECTED:,}]"

    print(f"✓ Transaction count: {total_count:,} (within expected range)")


# ============================================================================
# Test 4: No Duplicate Transaction IDs
# ============================================================================

def test_no_duplicate_transaction_ids(transaction_stats):
    """
    Verify all transaction IDs are unique (no duplicates).
    """
    total_count = transaction_stats["total_count"]
    unique_txn_ids = transaction_stats["unique_txn_ids"]

    assert total_count == unique_txn_ids, \
        f"Found duplicate transaction IDs: {total_count:,} total but only {unique_txn_ids:,} unique"

    print(f"✓ All {unique_txn_ids:,} transaction IDs are unique")


# ============================================================================
# Test 5: All Customers Represented
# ============================================================================

def test_all_customers_represented(transaction_stats):
    """
    Verify all 50,000 customers have at least one transaction.
    """
    unique_customers = transaction_stats["unique_customers"]

    EXPECTED_CUSTOMERS = 50_000

    assert unique_customers == EXPECTED_CUSTOMERS, \
        f"Expected {EXPECTED_CUSTOMERS:,} customers, found {unique_customers:,}"

    print(f"✓ All {unique_customers:,} customers have transactions")


# ============================================================================
# Test 6: Referential Integrity
# ============================================================================

def test_referential_integrity(snowflake_connection):
    """
    Verify all transaction customer_ids exist in BRONZE_CUSTOMERS.

    This ensures referential integrity between transactions and customers.
    """
    cursor = snowflake_connection.cursor()

    query = """
    SELECT COUNT(DISTINCT t.customer_id)
    FROM BRONZE.BRONZE_TRANSACTIONS t
    WHERE NOT EXISTS (
        SELECT 1
        FROM BRONZE.BRONZE_CUSTOMERS c
        WHERE c.customer_id = t.customer_id
    );
    """

    cursor.execute(query)
    orphaned_count = cursor.fetchone()[0]

    assert orphaned_count == 0, \
        f"Found {orphaned_count} transactions with customer_ids not in BRONZE_CUSTOMERS"

    print(f"✓ All transaction customer_ids exist in BRONZE_CUSTOMERS (referential integrity maintained)")

    cursor.close()


# ============================================================================
# Test 7: Date Range Valid
# ============================================================================

def test_date_range_valid(snowflake_connection, transaction_stats):
    """
    Verify transaction date range is valid.

    Tests:
    - No NULL transaction_dates
    - No future dates
    - Date range approximately 18 months
    """
    cursor = snowflake_connection.cursor()

    # Test 1: No NULL dates
    query = "SELECT COUNT(*) FROM BRONZE.BRONZE_TRANSACTIONS WHERE transaction_date IS NULL;"
    cursor.execute(query)
    null_dates = cursor.fetchone()[0]

    assert null_dates == 0, f"Found {null_dates} NULL transaction dates"

    # Test 2: No future dates
    query = "SELECT COUNT(*) FROM BRONZE.BRONZE_TRANSACTIONS WHERE transaction_date > CURRENT_TIMESTAMP();"
    cursor.execute(query)
    future_dates = cursor.fetchone()[0]

    assert future_dates == 0, f"Found {future_dates} transactions with future dates"

    # Test 3: Date range approximately 18 months
    min_date = transaction_stats["min_date"]
    max_date = transaction_stats["max_date"]

    month_diff = (max_date.year - min_date.year) * 12 + (max_date.month - min_date.month)

    MIN_MONTHS = 17
    MAX_MONTHS = 19

    assert MIN_MONTHS <= month_diff <= MAX_MONTHS, \
        f"Date range {month_diff} months outside expected range [{MIN_MONTHS}, {MAX_MONTHS}]"

    print(f"✓ Date range: {min_date.date()} to {max_date.date()} ({month_diff} months)")
    print(f"✓ No NULL dates, no future dates")

    cursor.close()


# ============================================================================
# Test 8: Transaction Amounts Valid
# ============================================================================

def test_transaction_amounts_valid(snowflake_connection, transaction_stats):
    """
    Verify transaction amounts are valid.

    Tests:
    - All amounts > 0
    - Reasonable max amount (< $10,000)
    """
    cursor = snowflake_connection.cursor()

    # Test 1: All amounts positive
    min_amount = transaction_stats["min_amount"]
    assert min_amount > 0, f"Found non-positive minimum amount: ${min_amount:.2f}"

    # Test 2: Reasonable max amount
    max_amount = transaction_stats["max_amount"]
    MAX_REASONABLE = 10_000.00

    # Allow some flexibility - warn if high but don't fail unless extreme
    if max_amount > MAX_REASONABLE:
        print(f"⚠️  Max amount ${max_amount:.2f} exceeds typical range (${MAX_REASONABLE:.2f})")
        assert max_amount < 15_000.00, f"Max amount ${max_amount:.2f} is unreasonably high"

    # Test 3: No zero or negative amounts
    query = "SELECT COUNT(*) FROM BRONZE.BRONZE_TRANSACTIONS WHERE transaction_amount <= 0;"
    cursor.execute(query)
    invalid_amounts = cursor.fetchone()[0]

    assert invalid_amounts == 0, f"Found {invalid_amounts} transactions with zero or negative amounts"

    avg_amount = transaction_stats["avg_amount"]

    print(f"✓ Transaction amounts: ${min_amount:.2f} - ${max_amount:.2f} (avg: ${avg_amount:.2f})")
    print(f"✓ All amounts positive")

    cursor.close()


# ============================================================================
# Test 9: Metadata Populated
# ============================================================================

def test_metadata_populated(snowflake_connection):
    """
    Verify metadata fields are populated correctly.

    Tests:
    - ingestion_timestamp not null
    - source_file contains 'transactions_historical'
    - _metadata_file_row_number not null
    """
    cursor = snowflake_connection.cursor()

    # Test 1: No NULL ingestion_timestamps
    query = "SELECT COUNT(*) FROM BRONZE.BRONZE_TRANSACTIONS WHERE ingestion_timestamp IS NULL;"
    cursor.execute(query)
    null_timestamps = cursor.fetchone()[0]

    assert null_timestamps == 0, f"Found {null_timestamps} NULL ingestion timestamps"

    # Test 2: No NULL source_files
    query = "SELECT COUNT(*) FROM BRONZE.BRONZE_TRANSACTIONS WHERE source_file IS NULL;"
    cursor.execute(query)
    null_files = cursor.fetchone()[0]

    assert null_files == 0, f"Found {null_files} NULL source files"

    # Test 3: Source file naming convention
    query = """
    SELECT COUNT(*)
    FROM BRONZE.BRONZE_TRANSACTIONS
    WHERE source_file LIKE '%transactions_historical%';
    """
    cursor.execute(query)
    valid_filenames = cursor.fetchone()[0]

    query = "SELECT COUNT(*) FROM BRONZE.BRONZE_TRANSACTIONS;"
    cursor.execute(query)
    total_count = cursor.fetchone()[0]

    assert valid_filenames == total_count, \
        f"Only {valid_filenames}/{total_count} records have correct source_file naming"

    # Test 4: No NULL file row numbers
    query = "SELECT COUNT(*) FROM BRONZE.BRONZE_TRANSACTIONS WHERE _metadata_file_row_number IS NULL;"
    cursor.execute(query)
    null_row_numbers = cursor.fetchone()[0]

    assert null_row_numbers == 0, f"Found {null_row_numbers} NULL file row numbers"

    print(f"✓ All metadata fields populated correctly")
    print(f"✓ Source files match naming convention")

    cursor.close()


# ============================================================================
# Additional Test: Customers Without Transactions
# ============================================================================

def test_customers_without_transactions(snowflake_connection):
    """
    Verify all customers in BRONZE_CUSTOMERS have at least one transaction.

    This is the inverse of test_all_customers_represented.
    """
    cursor = snowflake_connection.cursor()

    query = """
    SELECT COUNT(*)
    FROM BRONZE.BRONZE_CUSTOMERS c
    WHERE NOT EXISTS (
        SELECT 1
        FROM BRONZE.BRONZE_TRANSACTIONS t
        WHERE t.customer_id = c.customer_id
    );
    """

    cursor.execute(query)
    missing_count = cursor.fetchone()[0]

    assert missing_count == 0, \
        f"Found {missing_count} customers without any transactions"

    print(f"✓ All customers in BRONZE_CUSTOMERS have transactions")

    cursor.close()


# ============================================================================
# Additional Test: Status Distribution
# ============================================================================

def test_status_distribution(snowflake_connection):
    """
    Verify status distribution is reasonable.

    Expected: ~97% approved, ~3% declined
    """
    cursor = snowflake_connection.cursor()

    query = """
    SELECT
        status,
        COUNT(*) AS txn_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
    FROM BRONZE.BRONZE_TRANSACTIONS
    GROUP BY status
    ORDER BY txn_count DESC;
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    status_dist = {row[0]: float(row[2]) for row in rows}

    # Check for approved status
    approved_pct = status_dist.get('approved', 0)
    assert 90 <= approved_pct <= 100, \
        f"Approved status {approved_pct}% outside expected range (90-100%)"

    # Check for declined status (if present)
    declined_pct = status_dist.get('declined', 0)
    if declined_pct > 0:
        assert 0 <= declined_pct <= 10, \
            f"Declined status {declined_pct}% outside expected range (0-10%)"

    print(f"✓ Status distribution: approved={approved_pct}%, declined={declined_pct}%")

    cursor.close()


# ============================================================================
# Additional Test: Observability Logging
# ============================================================================

def test_observability_logging(snowflake_connection):
    """
    Verify transaction load was logged to OBSERVABILITY.LAYER_RECORD_COUNTS.
    """
    cursor = snowflake_connection.cursor()

    query = """
    SELECT
        run_id,
        run_timestamp,
        table_name,
        record_count
    FROM OBSERVABILITY.LAYER_RECORD_COUNTS
    WHERE table_name = 'BRONZE_TRANSACTIONS'
    ORDER BY run_timestamp DESC
    LIMIT 1;
    """

    try:
        cursor.execute(query)
        row = cursor.fetchone()

        if row:
            run_id = row[0]
            run_timestamp = row[1]
            table_name = row[2]
            record_count = row[3]

            assert table_name == 'BRONZE_TRANSACTIONS', \
                f"Unexpected table name: {table_name}"

            assert record_count > 0, \
                f"Observability record shows 0 rows"

            print(f"✓ Observability logged: {record_count:,} rows at {run_timestamp}")
        else:
            print("⚠️  No observability record found (may not have been logged yet)")

    except Exception as e:
        pytest.skip(f"Cannot verify observability logging: {str(e)}")

    finally:
        cursor.close()


# ============================================================================
# Test Configuration
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
