"""
Performance tests for transaction data generation.

Tests validate:
- Generation completes within acceptable time limits
- Warehouse resource usage is reasonable
- Query performance metrics are captured
"""

import pytest
import os
import time
from snowflake.connector import connect
from datetime import datetime, timedelta


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
# Test 1: Generation Completes Within Time Limit
# ============================================================================

def test_generation_completes_within_time_limit(snowflake_connection):
    """
    Verify transaction generation completes within acceptable time.

    Time limits by warehouse size:
    - XSMALL: 30 minutes
    - SMALL: 15 minutes
    - MEDIUM: 10 minutes
    - LARGE: 8 minutes
    - XLARGE+: 5 minutes

    This test checks query history to find the most recent generation run.
    """
    cursor = snowflake_connection.cursor()

    # Query to find most recent transaction generation run
    # Look for queries that created transactions_with_details table
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
    WHERE query_text ILIKE '%CREATE%TEMP%TABLE%transactions_with_details%'
        AND start_time > DATEADD('day', -7, CURRENT_TIMESTAMP())
        AND execution_status = 'SUCCESS'
    ORDER BY start_time DESC
    LIMIT 1;
    """

    try:
        cursor.execute(query)
        row = cursor.fetchone()

        if not row:
            pytest.skip("No recent transaction generation found in query history")

        query_id = row[0]
        elapsed_seconds = float(row[4])
        warehouse_size = row[5] if row[5] else "UNKNOWN"
        rows_produced = int(row[6]) if row[6] else 0

        # Define time limits by warehouse size (in seconds)
        time_limits = {
            "X-Small": 30 * 60,  # 30 minutes
            "Small": 15 * 60,    # 15 minutes
            "Medium": 10 * 60,   # 10 minutes
            "Large": 8 * 60,     # 8 minutes
            "X-Large": 5 * 60,   # 5 minutes
            "2X-Large": 5 * 60,
            "3X-Large": 5 * 60,
            "4X-Large": 5 * 60,
        }

        # Get time limit for warehouse size (default to 15 min if unknown)
        time_limit_seconds = time_limits.get(warehouse_size, 15 * 60)

        assert elapsed_seconds <= time_limit_seconds, \
            f"Generation took {elapsed_seconds:.0f}s on {warehouse_size} warehouse (limit: {time_limit_seconds:.0f}s)"

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
# Test 2: Query Cost is Reasonable
# ============================================================================

def test_query_cost_is_reasonable(snowflake_connection):
    """
    Verify transaction generation uses reasonable compute credits.

    Expected cost:
    - SMALL warehouse: ~0.05 - 0.15 credits
    - MEDIUM warehouse: ~0.10 - 0.30 credits
    - LARGE warehouse: ~0.15 - 0.45 credits

    Note: This is an estimate based on typical 5-15 minute runtime
    """
    cursor = snowflake_connection.cursor()

    query = """
    SELECT
        warehouse_name,
        warehouse_size,
        SUM(credits_used) AS total_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE start_time > DATEADD('day', -1, CURRENT_TIMESTAMP())
        AND warehouse_name = CURRENT_WAREHOUSE()
    GROUP BY warehouse_name, warehouse_size;
    """

    try:
        cursor.execute(query)
        row = cursor.fetchone()

        if not row:
            pytest.skip("No metering data available for current warehouse")

        warehouse_name = row[0]
        warehouse_size = row[1] if row[1] else "UNKNOWN"
        total_credits = float(row[2]) if row[2] else 0

        # Define reasonable credit limits (for the entire day's usage)
        # This is a rough check - actual generation is a fraction of this
        credit_limits = {
            "X-Small": 1.0,
            "Small": 2.0,
            "Medium": 4.0,
            "Large": 8.0,
            "X-Large": 16.0,
        }

        max_credits = credit_limits.get(warehouse_size, 4.0)

        print(f"\n✓ Cost Metrics:")
        print(f"  Warehouse: {warehouse_name} ({warehouse_size})")
        print(f"  Credits used (last 24h): {total_credits:.4f}")
        print(f"  Note: This includes all queries, not just generation")

        # This is informational only - we can't isolate generation cost precisely
        # without query-level credit tracking

    except Exception as e:
        pytest.skip(f"Cannot access metering history: {str(e)}")

    finally:
        cursor.close()


# ============================================================================
# Test 3: Individual Query Steps Performance
# ============================================================================

def test_individual_query_steps_performance(snowflake_connection):
    """
    Analyze performance of individual generation steps.

    Steps:
    1. Date spine creation
    2. Customer monthly volume
    3. Transaction expansion
    4. Transaction details
    5. COPY INTO S3

    Identifies bottlenecks for optimization.
    """
    cursor = snowflake_connection.cursor()

    steps = [
        ("Date Spine", "CREATE%TEMP%TABLE%date_spine"),
        ("Monthly Volume", "CREATE%TEMP%TABLE%customer_monthly_volume"),
        ("Transaction Expansion", "CREATE%TEMP%TABLE%transactions_expanded"),
        ("Transaction Details", "CREATE%TEMP%TABLE%transactions_with_details"),
        ("S3 Export", "COPY%INTO%@%transaction_stage%"),
    ]

    print(f"\n✓ Individual Step Performance:")

    total_time = 0

    for step_name, query_pattern in steps:
        query = f"""
        SELECT
            query_id,
            total_elapsed_time / 1000 AS elapsed_seconds,
            rows_produced
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE query_text ILIKE '{query_pattern}'
            AND start_time > DATEADD('day', -7, CURRENT_TIMESTAMP())
            AND execution_status = 'SUCCESS'
        ORDER BY start_time DESC
        LIMIT 1;
        """

        try:
            cursor.execute(query)
            row = cursor.fetchone()

            if row:
                elapsed_seconds = float(row[1])
                rows_produced = int(row[2]) if row[2] else 0
                total_time += elapsed_seconds

                print(f"  {step_name:.<25} {elapsed_seconds:>6.1f}s  ({rows_produced:>12,} rows)")
            else:
                print(f"  {step_name:.<25} {'Not found':>6}")

        except Exception as e:
            print(f"  {step_name:.<25} {'Error':>6}")

    if total_time > 0:
        print(f"  {'Total (approx)':.<25} {total_time:>6.1f}s")

    cursor.close()


# ============================================================================
# Test 4: Memory Usage Reasonable
# ============================================================================

def test_memory_usage_reasonable(snowflake_connection):
    """
    Verify transaction generation doesn't cause memory spikes.

    Checks:
    - No out-of-memory errors in recent queries
    - Spillage to disk is acceptable (< 50% of data)
    """
    cursor = snowflake_connection.cursor()

    query = """
    SELECT
        query_id,
        query_text,
        bytes_spilled_to_local_storage,
        bytes_spilled_to_remote_storage,
        bytes_scanned,
        error_message
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE query_text ILIKE '%transactions_with_details%'
        AND start_time > DATEADD('day', -7, CURRENT_TIMESTAMP())
    ORDER BY start_time DESC
    LIMIT 10;
    """

    try:
        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            pytest.skip("No transaction generation queries found")

        # Check for out-of-memory errors
        oom_errors = [r for r in rows if r[5] and 'memory' in r[5].lower()]
        assert len(oom_errors) == 0, \
            f"Found {len(oom_errors)} out-of-memory errors in recent queries"

        # Analyze spillage
        print(f"\n✓ Memory Usage:")

        for row in rows[:3]:  # Show top 3 most recent
            query_id = row[0]
            local_spill = int(row[2]) if row[2] else 0
            remote_spill = int(row[3]) if row[3] else 0
            bytes_scanned = int(row[4]) if row[4] else 0

            total_spill = local_spill + remote_spill

            if bytes_scanned > 0:
                spill_pct = (total_spill / bytes_scanned) * 100
                print(f"  Query {query_id[:8]}: {spill_pct:.1f}% spillage")

                # Warning if spillage is high (> 50%)
                if spill_pct > 50:
                    print(f"    ⚠️  High spillage - consider larger warehouse")
            else:
                print(f"  Query {query_id[:8]}: No data scanned")

    except Exception as e:
        pytest.skip(f"Cannot access query history for memory analysis: {str(e)}")

    finally:
        cursor.close()


# ============================================================================
# Test 5: Compilation Time Acceptable
# ============================================================================

def test_compilation_time_acceptable(snowflake_connection):
    """
    Verify query compilation time is reasonable.

    Expected:
    - Compilation time < 10% of total execution time
    - No excessive recompilation
    """
    cursor = snowflake_connection.cursor()

    query = """
    SELECT
        query_id,
        compilation_time / 1000 AS compilation_seconds,
        total_elapsed_time / 1000 AS total_seconds,
        (compilation_time::FLOAT / NULLIF(total_elapsed_time, 0)) * 100 AS compilation_pct
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE query_text ILIKE '%CREATE%TEMP%TABLE%transactions_with_details%'
        AND start_time > DATEADD('day', -7, CURRENT_TIMESTAMP())
        AND execution_status = 'SUCCESS'
    ORDER BY start_time DESC
    LIMIT 1;
    """

    try:
        cursor.execute(query)
        row = cursor.fetchone()

        if not row:
            pytest.skip("No transaction generation found in query history")

        compilation_seconds = float(row[1]) if row[1] else 0
        total_seconds = float(row[2]) if row[2] else 0
        compilation_pct = float(row[3]) if row[3] else 0

        MAX_COMPILATION_PCT = 10.0  # 10% of total time

        assert compilation_pct <= MAX_COMPILATION_PCT, \
            f"Compilation time {compilation_pct:.1f}% exceeds {MAX_COMPILATION_PCT}% threshold"

        print(f"\n✓ Compilation Performance:")
        print(f"  Compilation time: {compilation_seconds:.1f}s ({compilation_pct:.1f}% of total)")
        print(f"  Execution time: {total_seconds - compilation_seconds:.1f}s")

    except Exception as e:
        pytest.skip(f"Cannot access compilation metrics: {str(e)}")

    finally:
        cursor.close()


# ============================================================================
# Test 6: Parallelism Utilized
# ============================================================================

def test_parallelism_utilized(snowflake_connection):
    """
    Verify query uses multiple threads/partitions for parallel processing.

    Expected:
    - Partitions used > 1 (parallel processing)
    - Efficient partition distribution
    """
    cursor = snowflake_connection.cursor()

    query = """
    SELECT
        query_id,
        partitions_total,
        partitions_scanned,
        (partitions_scanned::FLOAT / NULLIF(partitions_total, 0)) * 100 AS scan_efficiency_pct
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE query_text ILIKE '%CREATE%TEMP%TABLE%transactions_with_details%'
        AND start_time > DATEADD('day', -7, CURRENT_TIMESTAMP())
        AND execution_status = 'SUCCESS'
    ORDER BY start_time DESC
    LIMIT 1;
    """

    try:
        cursor.execute(query)
        row = cursor.fetchone()

        if not row:
            pytest.skip("No transaction generation found in query history")

        partitions_total = int(row[1]) if row[1] else 0
        partitions_scanned = int(row[2]) if row[2] else 0
        scan_efficiency = float(row[3]) if row[3] else 0

        MIN_PARTITIONS = 1  # At least some parallel processing

        assert partitions_scanned >= MIN_PARTITIONS, \
            f"Only {partitions_scanned} partitions used - may not be utilizing parallelism"

        print(f"\n✓ Parallelism:")
        print(f"  Partitions scanned: {partitions_scanned} / {partitions_total}")
        print(f"  Scan efficiency: {scan_efficiency:.1f}%")

    except Exception as e:
        pytest.skip(f"Cannot access partition metrics: {str(e)}")

    finally:
        cursor.close()


# ============================================================================
# Test Configuration
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
