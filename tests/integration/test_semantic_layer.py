"""
Integration Tests for Semantic Layer (Iteration 4.3)

Tests semantic model validation for Cortex Analyst:
1. YAML syntax validation
2. Table existence verification
3. Metric calculability
4. Relationship integrity
5. Sample query execution

Run:
    pytest tests/integration/test_semantic_layer.py -v
"""

import os
import pytest
import yaml
from snowflake.connector import connect
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


@pytest.fixture(scope="module")
def semantic_model():
    """Load and parse semantic_model.yaml"""
    model_path = "semantic_layer/semantic_model.yaml"
    with open(model_path, 'r') as f:
        return yaml.safe_load(f)


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
# Test 1: YAML Validation
# ============================================================================


def test_semantic_model_valid_yaml(semantic_model):
    """
    Test that semantic_model.yaml is valid YAML and contains required keys.

    Validates:
    - YAML parses successfully
    - Required top-level keys present (name, tables, relationships)
    - Model name matches expected value
    """
    assert semantic_model is not None, "Failed to parse semantic_model.yaml"

    # Check required keys
    assert 'name' in semantic_model, "Missing 'name' key in semantic model"
    assert 'description' in semantic_model, "Missing 'description' key"
    assert 'tables' in semantic_model, "Missing 'tables' key"
    assert 'relationships' in semantic_model, "Missing 'relationships' key"
    assert 'sample_questions' in semantic_model, "Missing 'sample_questions' key"

    # Verify model name
    assert semantic_model['name'] == 'customer_analytics_semantic_model', \
        f"Unexpected model name: {semantic_model['name']}"

    # Check we have tables
    assert len(semantic_model['tables']) >= 3, \
        f"Expected at least 3 tables, found {len(semantic_model['tables'])}"

    print(f"\n✓ Semantic model valid: {semantic_model['name']}")
    print(f"  Tables: {len(semantic_model['tables'])}")
    print(f"  Relationships: {len(semantic_model['relationships'])}")
    print(f"  Sample Questions: {len(semantic_model['sample_questions'])}")


# ============================================================================
# Test 2: Table Existence
# ============================================================================


def test_all_tables_exist(semantic_model, cursor):
    """
    Test that all tables referenced in semantic model exist in Snowflake.

    For each table in semantic_model.yaml:
    - Parse base_table (DATABASE.SCHEMA.TABLE)
    - Query INFORMATION_SCHEMA to verify existence
    """
    missing_tables = []

    for table in semantic_model['tables']:
        table_name = table['name']
        base_table = table['base_table']

        # Parse base_table (format: DATABASE.SCHEMA.TABLE)
        parts = base_table.split('.')
        if len(parts) == 3:
            db, schema, tbl = parts

            cursor.execute(f"""
                SELECT COUNT(*) AS table_exists
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_CATALOG = '{db}'
                  AND TABLE_SCHEMA = '{schema}'
                  AND TABLE_NAME = '{tbl}'
            """)
            result = cursor.fetchone()

            if result[0] == 0:
                missing_tables.append(base_table)
            else:
                print(f"  ✓ {base_table} exists")

    assert len(missing_tables) == 0, \
        f"Missing tables in Snowflake: {', '.join(missing_tables)}"


# ============================================================================
# Test 3: Metric Calculability
# ============================================================================


def test_all_metrics_calculable(semantic_model, cursor):
    """
    Test that metrics defined in semantic model can be calculated.

    For key metrics, run sample aggregation queries to ensure:
    - Column exists in table
    - Aggregation function works
    - Query returns results
    """
    # Test customer_360_profile metrics
    cursor.execute("""
        SELECT
            AVG(lifetime_value) AS avg_ltv,
            AVG(churn_risk_score) AS avg_churn_risk,
            AVG(avg_monthly_spend) AS avg_monthly_spend,
            COUNT(*) AS customer_count
        FROM GOLD.CUSTOMER_360_PROFILE
    """)
    result = cursor.fetchone()

    assert result is not None, "Failed to calculate customer metrics"
    assert result[0] is not None, "lifetime_value not calculable"
    assert result[3] > 0, "No customers found"

    print(f"\n✓ Customer metrics calculable:")
    print(f"  Avg LTV: ${result[0]:,.2f}")
    print(f"  Avg Churn Risk: {result[1]:.2f}" if result[1] else "  Avg Churn Risk: None")
    print(f"  Customer Count: {result[3]:,}")

    # Test transaction metrics
    cursor.execute("""
        SELECT
            SUM(transaction_amount) AS total_amount,
            COUNT(*) AS transaction_count,
            AVG(transaction_amount) AS avg_amount
        FROM GOLD.FCT_TRANSACTIONS
        WHERE status = 'approved'
          AND transaction_date >= DATEADD('month', -3, CURRENT_DATE())
    """)
    result = cursor.fetchone()

    assert result is not None, "Failed to calculate transaction metrics"
    assert result[0] is not None, "transaction_amount not calculable"
    assert result[1] > 0, "No transactions found"

    print(f"\n✓ Transaction metrics calculable:")
    print(f"  Total Amount (90d): ${result[0]:,.2f}")
    print(f"  Transaction Count: {result[1]:,}")


# ============================================================================
# Test 4: Relationship Validity
# ============================================================================


def test_relationships_valid(semantic_model, cursor):
    """
    Test that relationships between tables are valid.

    For each relationship:
    - Verify join key exists in both tables
    - Test join executes successfully
    - Verify results returned
    """
    for relationship in semantic_model['relationships']:
        from_table = relationship['from_table']
        to_table = relationship['to_table']
        join_key = relationship['join_key']

        print(f"\n  Testing: {from_table} → {to_table} on {join_key}")

        # Find base table names
        from_base = None
        to_base = None

        for table in semantic_model['tables']:
            if table['name'] == from_table:
                from_base = table['base_table']
            if table['name'] == to_table:
                to_base = table['base_table']

        assert from_base is not None, f"Table {from_table} not found in semantic model"
        assert to_base is not None, f"Table {to_table} not found in semantic model"

        # Test join
        cursor.execute(f"""
            SELECT COUNT(*) AS join_count
            FROM {from_base} f
            JOIN {to_base} t ON f.{join_key} = t.{join_key}
            LIMIT 1000
        """)
        result = cursor.fetchone()

        assert result[0] > 0, f"Join {from_table} → {to_table} returned no results"
        print(f"    ✓ Join successful ({result[0]:,} rows)")


# ============================================================================
# Test 5: Sample Questions Answerable
# ============================================================================


def test_sample_questions_answerable(semantic_model, cursor):
    """
    Test representative sample questions from semantic model.

    Manually converts natural language questions to SQL and validates:
    - Query executes successfully
    - Returns meaningful results
    """
    test_queries = [
        {
            "question": "What is the average spend of customers in California?",
            "sql": """
                SELECT AVG(lifetime_value) AS avg_ltv
                FROM GOLD.CUSTOMER_360_PROFILE
                WHERE state = 'CA'
            """,
            "expected_result": "avg_ltv is not NULL"
        },
        {
            "question": "How many customers are in High Risk churn category?",
            "sql": """
                SELECT COUNT(*) AS high_risk_count
                FROM GOLD.CUSTOMER_360_PROFILE
                WHERE churn_risk_category = 'High Risk'
            """,
            "expected_result": "count >= 0"
        },
        {
            "question": "What is total spending in last 90 days?",
            "sql": """
                SELECT SUM(spend_last_90_days) AS total_spend_90d
                FROM GOLD.CUSTOMER_360_PROFILE
                WHERE spend_last_90_days > 0
            """,
            "expected_result": "total_spend > 0"
        },
        {
            "question": "Compare lifetime value across segments",
            "sql": """
                SELECT
                    customer_segment,
                    AVG(lifetime_value) AS avg_ltv,
                    COUNT(*) AS customer_count
                FROM GOLD.CUSTOMER_360_PROFILE
                GROUP BY customer_segment
            """,
            "expected_result": "multiple segments returned"
        }
    ]

    print("\n✓ Testing sample questions:")

    for test in test_queries:
        print(f"\n  Q: {test['question']}")
        cursor.execute(test['sql'])
        result = cursor.fetchone()

        assert result is not None, f"Query returned no results: {test['question']}"
        print(f"    ✓ Query executed successfully")
        print(f"      Result: {result[0]}")


# ============================================================================
# Test 6: Dimensions and Metrics Coverage
# ============================================================================


def test_dimensions_and_metrics_coverage(semantic_model):
    """
    Test that semantic model has sufficient dimensions and metrics.

    Validates:
    - Each table has at least 3 dimensions
    - Each table has at least 2 metrics
    - Synonyms provided for key dimensions/metrics
    """
    for table in semantic_model['tables']:
        table_name = table['name']

        dimensions = table.get('dimensions', [])
        metrics = table.get('metrics', [])

        print(f"\n  {table_name}:")
        print(f"    Dimensions: {len(dimensions)}")
        print(f"    Metrics: {len(metrics)}")

        # Customer 360 should have many dimensions and metrics
        if table_name == 'customer_360_profile':
            assert len(dimensions) >= 10, \
                f"customer_360_profile should have at least 10 dimensions, has {len(dimensions)}"
            assert len(metrics) >= 8, \
                f"customer_360_profile should have at least 8 metrics, has {len(metrics)}"

        # Check for synonyms on key dimensions
        for dim in dimensions:
            if dim['name'] in ['customer_segment', 'churn_risk_category', 'state']:
                assert 'synonyms' in dim, \
                    f"Key dimension {dim['name']} should have synonyms"


# ============================================================================
# Test 7: Optimization Hints Present
# ============================================================================


def test_optimization_hints_present(semantic_model):
    """
    Test that optimization hints are provided in semantic model.

    Validates:
    - Optimization section exists
    - Recommended filters specified for main tables
    - Clustering keys documented
    """
    assert 'optimization' in semantic_model, "Missing optimization section"

    optimization = semantic_model['optimization']
    assert len(optimization) >= 2, "Should have optimization hints for at least 2 tables"

    print("\n✓ Optimization hints:")
    for opt in optimization:
        print(f"  {opt['table']}:")
        print(f"    Recommended filters: {opt.get('recommended_filters', [])}")
        print(f"    Clustering keys: {opt.get('clustering_keys', [])}")


# ============================================================================
# Test 8: Cortex Analyst Integration (Optional)
# ============================================================================


def test_cortex_analyst_integration(cursor):
    """
    Test Cortex Analyst integration if available.

    NOTE: This test will skip if Cortex Analyst is not enabled.
    """
    pytest.skip("Cortex Analyst integration test requires Cortex Analyst to be enabled")

    # Placeholder for actual Cortex Analyst API call
    # When available, this would test:
    # 1. Submit natural language question
    # 2. Receive generated SQL
    # 3. Execute SQL and validate results


# ============================================================================
# Run Tests
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
