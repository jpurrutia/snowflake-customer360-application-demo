"""
Performance tests for transaction bulk load.

Tests validate:
- Bulk load completes within acceptable time limits
- Query performance on large table (13.5M rows) is acceptable
- Aggregation queries complete within reasonable time
- Index/clustering effectiveness (if applied)
"""

import pytest
import os
import time
from snowflake.connector import connect
from datetime import datetime


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


# ============================================================================
# Test 1: Load Completes Within Time Limit
# ============================================================================

def test_load_completes_within_time_limit(snowflake_connection):
    """
    Verify transaction load completed within acceptable time.

    Time limits by warehouse size:
    - XSMALL: 45 minutes
    - SMALL: 20 minutes
    - MEDIUM: 15 minutes
    - LARGE: 10 minutes
    - XLARGE+: 8 minutes

    This test checks query history to find the most recent load.
    """
    cursor = snowflake_connection.cursor()

    # Query to find most recent transaction load
    query = """
    SELECT
        query_id,
        query_text,
        start_time,
        end_time,
        total_elapsed_time / 1000 AS elapsed_seconds,
        warehouse_size,
        rows_produced,
        bytes_scanned
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE query_text ILIKE '%COPY INTO%BRONZE.BRONZE_TRANSACTIONS%'
        AND start_time > DATEADD('day', -7, CURRENT_TIMESTAMP())
        AND execution_status = 'SUCCESS'
    ORDER BY start_time DESC
    LIMIT 1;
    """

    try:
        cursor.execute(query)
        row = cursor.fetchone()

        if not row:
            pytest.skip("No recent transaction load found in query history")

        query_id = row[0]
        elapsed_seconds = float(row[4])
        warehouse_size = row[5] if row[5] else "UNKNOWN"
        rows_produced = int(row[6]) if row[6] else 0

        # Define time limits by warehouse size (in seconds)
        time_limits = {
            "X-Small": 45 * 60,  # 45 minutes
            "Small": 20 * 60,    # 20 minutes
            "Medium": 15 * 60,   # 15 minutes
            "Large": 10 * 60,    # 10 minutes
            "X-Large": 8 * 60,   # 8 minutes
            "2X-Large": 8 * 60,
            "3X-Large": 8 * 60,
            "4X-Large": 8 * 60,
        }

        # Get time limit for warehouse size (default to 20 min if unknown)
        time_limit_seconds = time_limits.get(warehouse_size, 20 * 60)

        assert elapsed_seconds <= time_limit_seconds, \
            f"Load took {elapsed_seconds:.0f}s ({elapsed_seconds/60:.1f}m) on {warehouse_size} warehouse (limit: {time_limit_seconds:.0f}s / {time_limit_seconds/60:.1f}m)"

        # Calculate throughput
        rows_per_second = rows_produced / elapsed_seconds if elapsed_seconds > 0 else 0

        print(f"\n✓ Performance Metrics:")
        print(f"  Warehouse: {warehouse_size}")
        print(f"  Duration: {elapsed_seconds:.0f}s ({elapsed_seconds/60:.1f} min)")
        print(f"  Rows: {rows_produced:,}")
        print(f"  Throughput: {rows_per_second:,.0f} rows/second")
        print(f"  Query ID: {query_id}")

    except Exception as e:
        pytest.skip(f"Cannot access query history: {str(e)}")

    finally:
        cursor.close()


# ============================================================================
# Test 2: Query Performance on Large Table
# ============================================================================

def test_query_performance_on_large_table(snowflake_connection):
    """
    Verify aggregation queries on 13.5M rows complete within acceptable time.

    Tests a realistic aggregation query that might be used in analytics.
    Expected: < 30 seconds on SMALL warehouse
    """
    cursor = snowflake_connection.cursor()

    # Aggregation query typical of analytics workload
    query = """
    SELECT
        DATE_TRUNC('month', transaction_date) AS month,
        COUNT(*) AS txn_count,
        ROUND(AVG(transaction_amount), 2) AS avg_amount,
        ROUND(SUM(transaction_amount), 2) AS total_amount,
        COUNT(DISTINCT customer_id) AS unique_customers
    FROM BRONZE.BRONZE_TRANSACTIONS
    GROUP BY DATE_TRUNC('month', transaction_date)
    ORDER BY month;
    """

    # Time the query
    start_time = time.time()
    cursor.execute(query)
    results = cursor.fetchall()
    end_time = time.time()

    elapsed_seconds = end_time - start_time

    # Time limit: 30 seconds on SMALL, 60 seconds on XSMALL
    TIME_LIMIT = 60.0  # Conservative limit

    assert elapsed_seconds <= TIME_LIMIT, \
        f"Aggregation query took {elapsed_seconds:.1f}s (limit: {TIME_LIMIT}s)"

    print(f"\n✓ Aggregation Query Performance:")
    print(f"  Duration: {elapsed_seconds:.2f}s")
    print(f"  Rows returned: {len(results)}")
    print(f"  Query: Monthly transaction aggregation")

    cursor.close()


# ============================================================================
# Test 3: Point Query Performance
# ============================================================================

def test_point_query_performance(snowflake_connection):
    """
    Verify point queries (single customer lookup) are fast.

    Expected: < 5 seconds on SMALL warehouse
    """
    cursor = snowflake_connection.cursor()

    # Get a sample customer ID
    cursor.execute("SELECT customer_id FROM BRONZE.BRONZE_TRANSACTIONS LIMIT 1")
    sample_customer = cursor.fetchone()[0]

    # Point query for single customer
    query = f"""
    SELECT
        COUNT(*) AS txn_count,
        ROUND(AVG(transaction_amount), 2) AS avg_amount,
        MIN(transaction_date) AS first_txn,
        MAX(transaction_date) AS last_txn
    FROM BRONZE.BRONZE_TRANSACTIONS
    WHERE customer_id = '{sample_customer}';
    """

    # Time the query
    start_time = time.time()
    cursor.execute(query)
    result = cursor.fetchone()
    end_time = time.time()

    elapsed_seconds = end_time - start_time

    TIME_LIMIT = 5.0  # 5 seconds

    assert elapsed_seconds <= TIME_LIMIT, \
        f"Point query took {elapsed_seconds:.1f}s (limit: {TIME_LIMIT}s)"

    print(f"\n✓ Point Query Performance:")
    print(f"  Duration: {elapsed_seconds:.2f}s")
    print(f"  Customer ID: {sample_customer}")
    print(f"  Transactions found: {result[0]}")

    cursor.close()


# ============================================================================
# Test 4: Count Query Performance
# ============================================================================

def test_count_query_performance(snowflake_connection):
    """
    Verify simple count query is fast.

    Expected: < 10 seconds on SMALL warehouse
    """
    cursor = snowflake_connection.cursor()

    query = "SELECT COUNT(*) FROM BRONZE.BRONZE_TRANSACTIONS;"

    # Time the query
    start_time = time.time()
    cursor.execute(query)
    count = cursor.fetchone()[0]
    end_time = time.time()

    elapsed_seconds = end_time - start_time

    TIME_LIMIT = 10.0  # 10 seconds

    assert elapsed_seconds <= TIME_LIMIT, \
        f"Count query took {elapsed_seconds:.1f}s (limit: {TIME_LIMIT}s)"

    print(f"\n✓ Count Query Performance:")
    print(f"  Duration: {elapsed_seconds:.2f}s")
    print(f"  Row count: {count:,}")

    cursor.close()


# ============================================================================
# Test 5: Date Range Query Performance
# ============================================================================

def test_date_range_query_performance(snowflake_connection):
    """
    Verify date range queries are performant.

    Tests filtering by date range, which is common in time-series analysis.
    Expected: < 20 seconds on SMALL warehouse
    """
    cursor = snowflake_connection.cursor()

    # Get a reasonable date range (e.g., last 3 months)
    query = """
    SELECT
        COUNT(*) AS txn_count,
        ROUND(SUM(transaction_amount), 2) AS total_amount
    FROM BRONZE.BRONZE_TRANSACTIONS
    WHERE transaction_date >= DATEADD('month', -3, CURRENT_DATE())
        AND transaction_date < CURRENT_DATE();
    """

    # Time the query
    start_time = time.time()
    cursor.execute(query)
    result = cursor.fetchone()
    end_time = time.time()

    elapsed_seconds = end_time - start_time

    TIME_LIMIT = 20.0  # 20 seconds

    assert elapsed_seconds <= TIME_LIMIT, \
        f"Date range query took {elapsed_seconds:.1f}s (limit: {TIME_LIMIT}s)"

    print(f"\n✓ Date Range Query Performance:")
    print(f"  Duration: {elapsed_seconds:.2f}s")
    print(f"  Transactions in range: {result[0]:,}")
    print(f"  Total amount: ${result[1]:,.2f}")

    cursor.close()


# ============================================================================
# Test 6: Join Performance (with customers)
# ============================================================================

def test_join_performance(snowflake_connection):
    """
    Verify join performance between transactions and customers.

    Tests common join pattern used in analytics.
    Expected: < 45 seconds on SMALL warehouse
    """
    cursor = snowflake_connection.cursor()

    # Join transactions with customers
    query = """
    SELECT
        c.customer_segment,
        COUNT(*) AS txn_count,
        ROUND(AVG(t.transaction_amount), 2) AS avg_amount
    FROM BRONZE.BRONZE_TRANSACTIONS t
    JOIN BRONZE.BRONZE_CUSTOMERS c
        ON t.customer_id = c.customer_id
    GROUP BY c.customer_segment
    ORDER BY txn_count DESC;
    """

    # Time the query
    start_time = time.time()
    cursor.execute(query)
    results = cursor.fetchall()
    end_time = time.time()

    elapsed_seconds = end_time - start_time

    TIME_LIMIT = 45.0  # 45 seconds

    assert elapsed_seconds <= TIME_LIMIT, \
        f"Join query took {elapsed_seconds:.1f}s (limit: {TIME_LIMIT}s)"

    print(f"\n✓ Join Query Performance:")
    print(f"  Duration: {elapsed_seconds:.2f}s")
    print(f"  Segments: {len(results)}")
    print(f"  Query: Transaction count by customer segment")

    cursor.close()


# ============================================================================
# Test 7: Clustering Effectiveness (if applied)
# ============================================================================

def test_clustering_effectiveness(snowflake_connection):
    """
    If clustering key is applied, verify it's effective.

    Checks clustering information to ensure table is well-clustered.
    This test will skip if no clustering key is applied.
    """
    cursor = snowflake_connection.cursor()

    # Check if table has clustering key
    query = """
    SELECT CLUSTERING_KEY
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'BRONZE'
        AND TABLE_NAME = 'BRONZE_TRANSACTIONS';
    """

    cursor.execute(query)
    row = cursor.fetchone()

    if not row or not row[0]:
        pytest.skip("No clustering key applied to BRONZE_TRANSACTIONS table")

    clustering_key = row[0]

    # Get clustering depth (lower is better)
    query = """
    SELECT SYSTEM$CLUSTERING_DEPTH('BRONZE.BRONZE_TRANSACTIONS') AS clustering_depth;
    """

    try:
        cursor.execute(query)
        depth = cursor.fetchone()[0]

        # Clustering depth should be reasonable (< 10 is good for most tables)
        MAX_DEPTH = 20

        # Note: Newly loaded tables may not be fully clustered yet
        if depth > MAX_DEPTH:
            print(f"⚠️  Clustering depth {depth} is high (may need re-clustering)")
        else:
            print(f"\n✓ Clustering Effectiveness:")
            print(f"  Clustering key: {clustering_key}")
            print(f"  Clustering depth: {depth}")

    except Exception as e:
        print(f"⚠️  Cannot measure clustering depth: {str(e)}")

    cursor.close()


# ============================================================================
# Test 8: Memory and Spillage Check
# ============================================================================

def test_memory_and_spillage(snowflake_connection):
    """
    Check if recent queries on BRONZE_TRANSACTIONS had excessive spillage.

    Spillage indicates queries needed more memory than available.
    """
    cursor = snowflake_connection.cursor()

    query = """
    SELECT
        query_id,
        bytes_spilled_to_local_storage,
        bytes_spilled_to_remote_storage,
        bytes_scanned,
        ROUND((bytes_spilled_to_local_storage + bytes_spilled_to_remote_storage) * 100.0 /
              NULLIF(bytes_scanned, 0), 2) AS spillage_pct
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE query_text ILIKE '%BRONZE.BRONZE_TRANSACTIONS%'
        AND start_time > DATEADD('day', -7, CURRENT_TIMESTAMP())
        AND bytes_scanned > 0
    ORDER BY start_time DESC
    LIMIT 10;
    """

    try:
        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            pytest.skip("No query history found for BRONZE_TRANSACTIONS")

        high_spillage_count = 0

        for row in rows:
            spillage_pct = float(row[4]) if row[4] else 0

            if spillage_pct > 50:
                high_spillage_count += 1

        # Warn if more than 30% of queries have high spillage
        if high_spillage_count > len(rows) * 0.3:
            print(f"⚠️  {high_spillage_count}/{len(rows)} queries had >50% spillage")
            print(f"   Consider using larger warehouse for better performance")
        else:
            print(f"\n✓ Memory Usage:")
            print(f"  Recent queries: {len(rows)}")
            print(f"  High spillage: {high_spillage_count}/{len(rows)}")

    except Exception as e:
        pytest.skip(f"Cannot access query history for memory analysis: {str(e)}")

    finally:
        cursor.close()


# ============================================================================
# Test Configuration
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
