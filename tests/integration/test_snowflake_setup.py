"""
Integration tests for Snowflake foundation setup.
Tests database objects, schemas, roles, and RBAC configuration.
"""
import os
import pytest
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Snowflake connection parameters
SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
    'database': 'CUSTOMER_ANALYTICS',
}


@pytest.fixture(scope='module')
def snowflake_connection():
    """Create Snowflake connection for testing."""
    if not all([SNOWFLAKE_CONFIG['account'], SNOWFLAKE_CONFIG['user'], SNOWFLAKE_CONFIG['password']]):
        pytest.skip("Snowflake credentials not configured in .env file")

    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    yield conn
    conn.close()


@pytest.fixture(scope='module')
def cursor(snowflake_connection):
    """Create cursor from connection."""
    return snowflake_connection.cursor()


class TestDatabaseAndSchemas:
    """Test database and schema creation."""

    def test_database_exists(self, cursor):
        """Verify CUSTOMER_ANALYTICS database exists."""
        cursor.execute("SHOW DATABASES LIKE 'CUSTOMER_ANALYTICS'")
        databases = cursor.fetchall()
        assert len(databases) > 0, "CUSTOMER_ANALYTICS database not found"

        db_names = [row[1] for row in databases]
        assert 'CUSTOMER_ANALYTICS' in db_names

    def test_bronze_schema_exists(self, cursor):
        """Verify BRONZE schema exists."""
        cursor.execute("USE DATABASE CUSTOMER_ANALYTICS")
        cursor.execute("SHOW SCHEMAS LIKE 'BRONZE'")
        schemas = cursor.fetchall()
        assert len(schemas) > 0, "BRONZE schema not found"

    def test_silver_schema_exists(self, cursor):
        """Verify SILVER schema exists."""
        cursor.execute("USE DATABASE CUSTOMER_ANALYTICS")
        cursor.execute("SHOW SCHEMAS LIKE 'SILVER'")
        schemas = cursor.fetchall()
        assert len(schemas) > 0, "SILVER schema not found"

    def test_gold_schema_exists(self, cursor):
        """Verify GOLD schema exists."""
        cursor.execute("USE DATABASE CUSTOMER_ANALYTICS")
        cursor.execute("SHOW SCHEMAS LIKE 'GOLD'")
        schemas = cursor.fetchall()
        assert len(schemas) > 0, "GOLD schema not found"

    def test_observability_schema_exists(self, cursor):
        """Verify OBSERVABILITY schema exists."""
        cursor.execute("USE DATABASE CUSTOMER_ANALYTICS")
        cursor.execute("SHOW SCHEMAS LIKE 'OBSERVABILITY'")
        schemas = cursor.fetchall()
        assert len(schemas) > 0, "OBSERVABILITY schema not found"

    def test_all_schemas_have_comments(self, cursor):
        """Verify all schemas have descriptive comments."""
        cursor.execute("USE DATABASE CUSTOMER_ANALYTICS")
        cursor.execute("SHOW SCHEMAS")
        schemas = cursor.fetchall()

        for schema in schemas:
            schema_name = schema[1]
            if schema_name in ['BRONZE', 'SILVER', 'GOLD', 'OBSERVABILITY']:
                comment = schema[4] if len(schema) > 4 else None
                assert comment, f"Schema {schema_name} missing comment"


class TestRoles:
    """Test role creation."""

    def test_data_engineer_role_exists(self, cursor):
        """Verify DATA_ENGINEER role exists."""
        cursor.execute("SHOW ROLES LIKE 'DATA_ENGINEER'")
        roles = cursor.fetchall()
        assert len(roles) > 0, "DATA_ENGINEER role not found"

    def test_marketing_manager_role_exists(self, cursor):
        """Verify MARKETING_MANAGER role exists."""
        cursor.execute("SHOW ROLES LIKE 'MARKETING_MANAGER'")
        roles = cursor.fetchall()
        assert len(roles) > 0, "MARKETING_MANAGER role not found"

    def test_data_analyst_role_exists(self, cursor):
        """Verify DATA_ANALYST role exists."""
        cursor.execute("SHOW ROLES LIKE 'DATA_ANALYST'")
        roles = cursor.fetchall()
        assert len(roles) > 0, "DATA_ANALYST role not found"


class TestObservabilityTables:
    """Test observability table creation."""

    def test_pipeline_run_metadata_table_exists(self, cursor):
        """Verify PIPELINE_RUN_METADATA table exists."""
        cursor.execute("USE DATABASE CUSTOMER_ANALYTICS")
        cursor.execute("USE SCHEMA OBSERVABILITY")
        cursor.execute("SHOW TABLES LIKE 'PIPELINE_RUN_METADATA'")
        tables = cursor.fetchall()
        assert len(tables) > 0, "PIPELINE_RUN_METADATA table not found"

    def test_data_quality_metrics_table_exists(self, cursor):
        """Verify DATA_QUALITY_METRICS table exists."""
        cursor.execute("USE DATABASE CUSTOMER_ANALYTICS")
        cursor.execute("USE SCHEMA OBSERVABILITY")
        cursor.execute("SHOW TABLES LIKE 'DATA_QUALITY_METRICS'")
        tables = cursor.fetchall()
        assert len(tables) > 0, "DATA_QUALITY_METRICS table not found"

    def test_layer_record_counts_table_exists(self, cursor):
        """Verify LAYER_RECORD_COUNTS table exists."""
        cursor.execute("USE DATABASE CUSTOMER_ANALYTICS")
        cursor.execute("USE SCHEMA OBSERVABILITY")
        cursor.execute("SHOW TABLES LIKE 'LAYER_RECORD_COUNTS'")
        tables = cursor.fetchall()
        assert len(tables) > 0, "LAYER_RECORD_COUNTS table not found"

    def test_model_execution_log_table_exists(self, cursor):
        """Verify MODEL_EXECUTION_LOG table exists."""
        cursor.execute("USE DATABASE CUSTOMER_ANALYTICS")
        cursor.execute("USE SCHEMA OBSERVABILITY")
        cursor.execute("SHOW TABLES LIKE 'MODEL_EXECUTION_LOG'")
        tables = cursor.fetchall()
        assert len(tables) > 0, "MODEL_EXECUTION_LOG table not found"

    def test_pipeline_metadata_has_test_row(self, cursor):
        """Verify sample test row was inserted."""
        cursor.execute("USE DATABASE CUSTOMER_ANALYTICS")
        cursor.execute("USE SCHEMA OBSERVABILITY")
        cursor.execute("SELECT COUNT(*) FROM PIPELINE_RUN_METADATA WHERE run_id = 'SETUP_TEST_RUN'")
        result = cursor.fetchone()
        assert result[0] >= 1, "Test row not found in PIPELINE_RUN_METADATA"

    def test_observability_views_exist(self, cursor):
        """Verify observability views were created."""
        cursor.execute("USE DATABASE CUSTOMER_ANALYTICS")
        cursor.execute("USE SCHEMA OBSERVABILITY")

        views = [
            'V_LATEST_PIPELINE_RUNS',
            'V_RECENT_DQ_FAILURES',
            'V_RECORD_COUNT_TRENDS'
        ]

        for view_name in views:
            cursor.execute(f"SHOW VIEWS LIKE '{view_name}'")
            result = cursor.fetchall()
            assert len(result) > 0, f"View {view_name} not found"


class TestRBAC:
    """Test role-based access control permissions."""

    def test_data_engineer_has_database_access(self, cursor):
        """Verify DATA_ENGINEER has access to CUSTOMER_ANALYTICS."""
        cursor.execute("SHOW GRANTS TO ROLE DATA_ENGINEER")
        grants = cursor.fetchall()

        # Check for database usage grant
        database_grants = [g for g in grants if 'CUSTOMER_ANALYTICS' in str(g)]
        assert len(database_grants) > 0, "DATA_ENGINEER missing database grants"

    def test_marketing_manager_has_limited_access(self, cursor):
        """Verify MARKETING_MANAGER has only GOLD schema access."""
        cursor.execute("SHOW GRANTS TO ROLE MARKETING_MANAGER")
        grants = cursor.fetchall()

        # Should have GOLD schema grants
        gold_grants = [g for g in grants if 'GOLD' in str(g)]
        assert len(gold_grants) > 0, "MARKETING_MANAGER missing GOLD schema grants"

        # Should NOT have BRONZE schema grants
        bronze_grants = [g for g in grants if 'BRONZE' in str(g) and 'GRANT' in str(g)]
        assert len(bronze_grants) == 0, "MARKETING_MANAGER should not have BRONZE access"

    def test_data_analyst_has_read_access(self, cursor):
        """Verify DATA_ANALYST has SELECT privileges."""
        cursor.execute("SHOW GRANTS TO ROLE DATA_ANALYST")
        grants = cursor.fetchall()

        # Check for SELECT grants
        select_grants = [g for g in grants if 'SELECT' in str(g)]
        assert len(select_grants) > 0, "DATA_ANALYST missing SELECT grants"


class TestWarehouseAccess:
    """Test warehouse access for roles."""

    def test_compute_wh_exists(self, cursor):
        """Verify COMPUTE_WH warehouse exists."""
        cursor.execute("SHOW WAREHOUSES LIKE 'COMPUTE_WH'")
        warehouses = cursor.fetchall()
        assert len(warehouses) > 0, "COMPUTE_WH warehouse not found"

    def test_roles_have_warehouse_usage(self, cursor):
        """Verify all roles have warehouse USAGE."""
        roles = ['DATA_ENGINEER', 'MARKETING_MANAGER', 'DATA_ANALYST']

        for role in roles:
            cursor.execute(f"SHOW GRANTS TO ROLE {role}")
            grants = cursor.fetchall()

            warehouse_grants = [g for g in grants if 'COMPUTE_WH' in str(g) and 'USAGE' in str(g)]
            assert len(warehouse_grants) > 0, f"{role} missing warehouse USAGE grant"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
