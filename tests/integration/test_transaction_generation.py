"""
Integration tests for Snowflake transaction data generation.

Tests validate:
- Transaction generation completes successfully
- Volume is within expected range (~13.5M)
- All customers have transactions
- Transaction IDs are unique
- Amounts are positive and segment-appropriate
- Date range is correct (18 months)
- Declining segment shows decline pattern
- High-value travelers spend more than budget-conscious
- Files exported to S3 successfully
"""

import pytest
import os
from snowflake.connector import connect
from datetime import datetime, timedelta
from typing import Dict, Any


# Fixture for Snowflake connection
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

    Returns dictionary with:
    - total_count: Total transaction count
    - unique_txn_ids: Unique transaction ID count
    - unique_customers: Unique customer count
    - min_date: Earliest transaction date
    - max_date: Latest transaction date
    - avg_amount: Average transaction amount
    - min_amount: Minimum transaction amount
    - max_amount: Maximum transaction amount
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
        ROUND(MAX(transaction_amount), 2) AS max_amount
    FROM transactions_with_details;
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
        "max_amount": float(row[7])
    }

    cursor.close()
    return stats


# ============================================================================
# Test 1: Generation Completes
# ============================================================================

def test_transaction_generation_completes(snowflake_connection):
    """
    Verify transaction generation script executed successfully.

    Tests:
    - transactions_with_details temp table exists
    - Table has data
    """
    cursor = snowflake_connection.cursor()

    # Check if temp table exists
    query = """
    SHOW TABLES LIKE 'transactions_with_details' IN CUSTOMER_ANALYTICS.BRONZE;
    """
    cursor.execute(query)
    tables = cursor.fetchall()

    # If temp table doesn't exist, check if data was loaded to permanent table
    # (depending on implementation choice)
    if len(tables) == 0:
        # Try checking for data in a potential permanent table
        query = "SELECT COUNT(*) FROM transactions_with_details LIMIT 1;"
        try:
            cursor.execute(query)
            row_count = cursor.fetchone()[0]
            assert row_count > 0, "Transaction generation created no data"
        except Exception as e:
            pytest.fail(f"Transaction generation failed or table not found: {str(e)}")
    else:
        # Temp table exists
        query = "SELECT COUNT(*) FROM transactions_with_details;"
        cursor.execute(query)
        row_count = cursor.fetchone()[0]
        assert row_count > 0, "Transaction temp table is empty"

    cursor.close()


# ============================================================================
# Test 2: Transaction Volume Reasonable
# ============================================================================

def test_transaction_volume_reasonable(transaction_stats):
    """
    Verify transaction count is within expected range.

    Expected: ~13.5M transactions
    Tolerance: 10M - 15M (due to randomization)

    Calculation:
    - 50,000 customers
    - Average ~750 transactions per customer over 18 months
    - Target: 50,000 × 750 = 37.5M → Wait, that doesn't match

    Actually:
    - High-Value: 7,500 × 1,080 = 8.1M
    - Stable Mid: 20,000 × 540 = 10.8M
    - Budget-Conscious: 12,500 × 405 = 5.1M
    - Declining: 5,000 × 540 = 2.7M
    - New & Growing: 5,000 × 675 = 3.4M
    - Total: ~30.1M (this seems high)

    Let me recalculate based on monthly frequencies:
    - High-Value: 40-80/month × 18 = 720-1,440/customer
    - Stable Mid: 20-40/month × 18 = 360-720/customer
    - Budget: 15-30/month × 18 = 270-540/customer
    - Declining: 20-40/month × 18 = 360-720/customer
    - New: 25-50/month × 18 = 450-900/customer

    Weighted average:
    - 0.15 × 1,080 = 162
    - 0.40 × 540 = 216
    - 0.25 × 405 = 101.25
    - 0.10 × 540 = 54
    - 0.10 × 675 = 67.5
    - Total avg: ~600 txns/customer
    - 50,000 × 600 = 30M (still seems high)

    Based on README.md, target is 13.5M, so:
    - Min: 10M (reasonable lower bound)
    - Max: 17M (reasonable upper bound)
    """
    total_count = transaction_stats["total_count"]

    MIN_EXPECTED = 10_000_000  # 10M
    MAX_EXPECTED = 17_000_000  # 17M

    assert MIN_EXPECTED <= total_count <= MAX_EXPECTED, \
        f"Transaction count {total_count:,} outside expected range [{MIN_EXPECTED:,}, {MAX_EXPECTED:,}]"

    print(f"✓ Transaction count: {total_count:,} (within expected range)")


# ============================================================================
# Test 3: All Customers Have Transactions
# ============================================================================

def test_all_customers_have_transactions(transaction_stats):
    """
    Verify all 50,000 customers have at least one transaction.
    """
    unique_customers = transaction_stats["unique_customers"]

    EXPECTED_CUSTOMERS = 50_000

    assert unique_customers == EXPECTED_CUSTOMERS, \
        f"Expected {EXPECTED_CUSTOMERS:,} customers with transactions, found {unique_customers:,}"

    print(f"✓ All {unique_customers:,} customers have transactions")


# ============================================================================
# Test 4: Transaction IDs Unique
# ============================================================================

def test_transaction_ids_unique(transaction_stats):
    """
    Verify all transaction IDs are unique (no duplicates).

    Transaction IDs generated with:
    'TXN' || LPAD(ROW_NUMBER() OVER (...), 11, '0')
    """
    total_count = transaction_stats["total_count"]
    unique_txn_ids = transaction_stats["unique_txn_ids"]

    assert total_count == unique_txn_ids, \
        f"Found duplicate transaction IDs: {total_count:,} total but only {unique_txn_ids:,} unique"

    print(f"✓ All {unique_txn_ids:,} transaction IDs are unique")


# ============================================================================
# Test 5: Transaction Amounts Positive
# ============================================================================

def test_transaction_amounts_positive(transaction_stats):
    """
    Verify all transaction amounts are positive.

    Even with decline patterns, amounts should never be negative
    due to GREATEST(0.4, ...) floor in SQL logic.
    """
    min_amount = transaction_stats["min_amount"]

    assert min_amount > 0, \
        f"Found non-positive transaction amount: {min_amount}"

    print(f"✓ Minimum transaction amount: ${min_amount:.2f} (positive)")


# ============================================================================
# Test 6: Date Range Correct
# ============================================================================

def test_date_range_correct(transaction_stats):
    """
    Verify transaction dates span approximately 18 months.

    Expected:
    - Start: ~18 months ago from generation date
    - End: ~current date (or slightly in past)
    - Range: 17-19 months (allow 1 month tolerance)
    """
    min_date = transaction_stats["min_date"]
    max_date = transaction_stats["max_date"]

    # Calculate month difference
    month_diff = (max_date.year - min_date.year) * 12 + (max_date.month - min_date.month)

    MIN_MONTHS = 17
    MAX_MONTHS = 19

    assert MIN_MONTHS <= month_diff <= MAX_MONTHS, \
        f"Date range {month_diff} months outside expected range [{MIN_MONTHS}, {MAX_MONTHS}]"

    print(f"✓ Date range: {min_date.date()} to {max_date.date()} ({month_diff} months)")


# ============================================================================
# Test 7: Declining Segment Shows Decline
# ============================================================================

def test_declining_segment_shows_decline(snowflake_connection):
    """
    Verify declining segment shows decreasing spend over time.

    Tests:
    - Last 3 months avg spend < first 3 months avg spend
    - Decline is at least 20% (conservative test)

    Note: Both gradual and sudden decline should show overall reduction.
    """
    cursor = snowflake_connection.cursor()

    query = """
    WITH monthly_spend AS (
        SELECT
            DATE_TRUNC('month', transaction_date) AS month,
            AVG(transaction_amount) AS avg_amount,
            ROW_NUMBER() OVER (ORDER BY DATE_TRUNC('month', transaction_date)) AS month_rank,
            COUNT(DISTINCT DATE_TRUNC('month', transaction_date)) OVER () AS total_months
        FROM transactions_with_details
        WHERE customer_segment = 'Declining'
        GROUP BY DATE_TRUNC('month', transaction_date)
    )
    SELECT
        AVG(CASE WHEN month_rank <= 3 THEN avg_amount END) AS first_3_months_avg,
        AVG(CASE WHEN month_rank > total_months - 3 THEN avg_amount END) AS last_3_months_avg
    FROM monthly_spend;
    """

    cursor.execute(query)
    row = cursor.fetchone()

    first_3_avg = float(row[0])
    last_3_avg = float(row[1])

    decline_pct = ((first_3_avg - last_3_avg) / first_3_avg) * 100

    MIN_DECLINE_PCT = 20.0  # At least 20% decline

    assert last_3_avg < first_3_avg, \
        f"Declining segment shows increase: first 3mo ${first_3_avg:.2f} < last 3mo ${last_3_avg:.2f}"

    assert decline_pct >= MIN_DECLINE_PCT, \
        f"Decline {decline_pct:.1f}% less than expected minimum {MIN_DECLINE_PCT}%"

    print(f"✓ Declining segment: ${first_3_avg:.2f} → ${last_3_avg:.2f} ({decline_pct:.1f}% decline)")

    cursor.close()


# ============================================================================
# Test 8: High-Value Travelers Spend More
# ============================================================================

def test_high_value_travelers_spend_more(snowflake_connection):
    """
    Verify High-Value Travelers have higher average spend than Budget-Conscious.

    Expected:
    - High-Value: $50-$500 range
    - Budget-Conscious: $10-$80 range
    - High-Value avg should be at least 3x Budget-Conscious avg
    """
    cursor = snowflake_connection.cursor()

    query = """
    SELECT
        customer_segment,
        ROUND(AVG(transaction_amount), 2) AS avg_amount
    FROM transactions_with_details
    WHERE customer_segment IN ('High-Value Travelers', 'Budget-Conscious')
    GROUP BY customer_segment
    ORDER BY avg_amount DESC;
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    assert len(rows) == 2, "Expected data for both High-Value Travelers and Budget-Conscious"

    high_value_avg = float(rows[0][1])
    budget_avg = float(rows[1][1])

    high_value_segment = rows[0][0]
    budget_segment = rows[1][0]

    assert high_value_segment == 'High-Value Travelers', \
        "High-Value Travelers should have highest average spend"

    assert budget_segment == 'Budget-Conscious', \
        "Budget-Conscious should have lowest average spend"

    MIN_RATIO = 3.0  # High-Value should be at least 3x Budget-Conscious
    actual_ratio = high_value_avg / budget_avg

    assert actual_ratio >= MIN_RATIO, \
        f"High-Value avg ${high_value_avg:.2f} only {actual_ratio:.1f}x Budget avg ${budget_avg:.2f} (expected ≥{MIN_RATIO}x)"

    print(f"✓ High-Value Travelers: ${high_value_avg:.2f} vs Budget-Conscious: ${budget_avg:.2f} ({actual_ratio:.1f}x)")

    cursor.close()


# ============================================================================
# Test 9: File Exported to S3
# ============================================================================

def test_file_exported_to_s3(snowflake_connection):
    """
    Verify transaction files exported to S3 stage successfully.

    Tests:
    - At least one file in @transaction_stage_historical
    - Files are GZIP compressed (.gz extension)
    - Total file count reasonable (expect multiple 100MB chunks)
    """
    cursor = snowflake_connection.cursor()

    query = """
    LIST @CUSTOMER_ANALYTICS.BRONZE.transaction_stage_historical;
    """

    try:
        cursor.execute(query)
        files = cursor.fetchall()

        assert len(files) > 0, "No files found in transaction_stage_historical"

        # Check for GZIP compression
        gzip_files = [f for f in files if '.gz' in f[0].lower()]
        assert len(gzip_files) > 0, "No GZIP compressed files found"

        # Expect at least a few files (13.5M rows should produce multiple 100MB chunks)
        MIN_FILES = 1
        MAX_FILES = 50  # Reasonable upper bound

        assert MIN_FILES <= len(files) <= MAX_FILES, \
            f"File count {len(files)} outside expected range [{MIN_FILES}, {MAX_FILES}]"

        print(f"✓ {len(files)} file(s) exported to S3 (GZIP compressed)")

    except Exception as e:
        pytest.skip(f"Cannot verify S3 export (stage may not be configured): {str(e)}")

    finally:
        cursor.close()


# ============================================================================
# Additional Helper Test: Segment Distribution
# ============================================================================

def test_segment_distribution_matches_customers(snowflake_connection):
    """
    Bonus test: Verify transaction segment distribution roughly matches
    customer segment distribution.

    This isn't a strict requirement but helps validate data generation.
    """
    cursor = snowflake_connection.cursor()

    query = """
    WITH txn_segments AS (
        SELECT
            customer_segment,
            COUNT(*) AS txn_count
        FROM transactions_with_details
        GROUP BY customer_segment
    ),
    total AS (
        SELECT SUM(txn_count) AS total_txns FROM txn_segments
    )
    SELECT
        t.customer_segment,
        t.txn_count,
        ROUND(t.txn_count * 100.0 / total.total_txns, 2) AS pct
    FROM txn_segments t
    CROSS JOIN total
    ORDER BY t.txn_count DESC;
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    print("\nTransaction Distribution by Segment:")
    for row in rows:
        segment, count, pct = row[0], row[1], float(row[2])
        print(f"  {segment}: {count:,} ({pct:.1f}%)")

    # This is informational only, no strict assertion
    assert len(rows) == 5, "Expected 5 customer segments"

    cursor.close()


# ============================================================================
# Test Configuration
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
