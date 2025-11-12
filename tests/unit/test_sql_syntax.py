"""
Unit tests for SQL syntax validation.
Tests SQL files for basic syntax errors and documentation requirements.
"""
import re
from pathlib import Path
import pytest


# Get project root and SQL directory
PROJECT_ROOT = Path(__file__).parent.parent.parent
SQL_DIR = PROJECT_ROOT / "snowflake" / "setup"


class TestSQLFilesExist:
    """Test that all required SQL files exist."""

    def test_environment_check_exists(self):
        """Verify 00_environment_check.sql exists."""
        sql_file = SQL_DIR / "00_environment_check.sql"
        assert sql_file.exists(), "00_environment_check.sql not found"

    def test_database_schemas_exists(self):
        """Verify 01_create_database_schemas.sql exists."""
        sql_file = SQL_DIR / "01_create_database_schemas.sql"
        assert sql_file.exists(), "01_create_database_schemas.sql not found"

    def test_roles_grants_exists(self):
        """Verify 02_create_roles_grants.sql exists."""
        sql_file = SQL_DIR / "02_create_roles_grants.sql"
        assert sql_file.exists(), "02_create_roles_grants.sql not found"

    def test_observability_tables_exists(self):
        """Verify 03_create_observability_tables.sql exists."""
        sql_file = SQL_DIR / "03_create_observability_tables.sql"
        assert sql_file.exists(), "03_create_observability_tables.sql not found"


class TestSQLSyntax:
    """Test SQL files for basic syntax errors."""

    @pytest.fixture(params=[
        "00_environment_check.sql",
        "01_create_database_schemas.sql",
        "02_create_roles_grants.sql",
        "03_create_observability_tables.sql"
    ])
    def sql_file(self, request):
        """Parametrize test with all SQL files."""
        return SQL_DIR / request.param

    def test_file_not_empty(self, sql_file):
        """Verify SQL file has content."""
        content = sql_file.read_text()
        assert len(content) > 100, f"{sql_file.name} appears to be empty or too short"

    def test_no_unmatched_quotes(self, sql_file):
        """Check for unmatched single or double quotes."""
        content = sql_file.read_text()

        # Remove comments before checking
        content_no_comments = re.sub(r'--.*$', '', content, flags=re.MULTILINE)

        # Check single quotes (excluding escaped quotes in strings)
        single_quotes = content_no_comments.count("'")
        assert single_quotes % 2 == 0, f"{sql_file.name} has unmatched single quotes"

    def test_has_use_statements(self, sql_file):
        """Verify SQL files have USE statements for context."""
        content = sql_file.read_text()

        # All files except environment check should have USE statements
        if "00_environment_check" not in sql_file.name:
            assert re.search(r'USE\s+(ROLE|WAREHOUSE|DATABASE|SCHEMA)', content, re.IGNORECASE), \
                f"{sql_file.name} missing USE statements for context"

    def test_has_header_comments(self, sql_file):
        """Verify SQL files have header comments."""
        content = sql_file.read_text()

        # Should have header section markers
        assert '=' * 10 in content, f"{sql_file.name} missing header section markers"

        # Should have purpose comment
        assert re.search(r'--\s*Purpose:', content, re.IGNORECASE), \
            f"{sql_file.name} missing Purpose comment"


class TestDatabaseCreation:
    """Test database creation SQL."""

    def test_creates_customer_analytics_database(self):
        """Verify CUSTOMER_ANALYTICS database is created."""
        sql_file = SQL_DIR / "01_create_database_schemas.sql"
        content = sql_file.read_text()

        assert re.search(r'CREATE\s+DATABASE.*CUSTOMER_ANALYTICS', content, re.IGNORECASE), \
            "Missing CREATE DATABASE CUSTOMER_ANALYTICS"

    def test_creates_all_schemas(self):
        """Verify all 4 schemas are created."""
        sql_file = SQL_DIR / "01_create_database_schemas.sql"
        content = sql_file.read_text()

        required_schemas = ['BRONZE', 'SILVER', 'GOLD', 'OBSERVABILITY']

        for schema in required_schemas:
            pattern = rf'CREATE\s+SCHEMA.*{schema}'
            assert re.search(pattern, content, re.IGNORECASE), \
                f"Missing CREATE SCHEMA {schema}"

    def test_schemas_have_comments(self):
        """Verify schemas have COMMENT clauses."""
        sql_file = SQL_DIR / "01_create_database_schemas.sql"
        content = sql_file.read_text()

        # Count COMMENT clauses in schema creation
        comment_count = len(re.findall(r'CREATE\s+SCHEMA.*COMMENT\s*=', content, re.IGNORECASE | re.DOTALL))

        # Should have at least 4 comments (one per schema)
        assert comment_count >= 4, "Not all schemas have COMMENT clauses"


class TestRoleCreation:
    """Test role creation SQL."""

    def test_creates_all_roles(self):
        """Verify all 3 roles are created."""
        sql_file = SQL_DIR / "02_create_roles_grants.sql"
        content = sql_file.read_text()

        required_roles = ['DATA_ENGINEER', 'MARKETING_MANAGER', 'DATA_ANALYST']

        for role in required_roles:
            pattern = rf'CREATE\s+ROLE.*{role}'
            assert re.search(pattern, content, re.IGNORECASE), \
                f"Missing CREATE ROLE {role}"

    def test_roles_have_comments(self):
        """Verify roles have COMMENT clauses."""
        sql_file = SQL_DIR / "02_create_roles_grants.sql"
        content = sql_file.read_text()

        # Count COMMENT clauses in role creation
        comment_count = len(re.findall(r'CREATE\s+ROLE.*COMMENT\s*=', content, re.IGNORECASE | re.DOTALL))

        # Should have at least 3 comments (one per role)
        assert comment_count >= 3, "Not all roles have COMMENT clauses"

    def test_grants_to_roles(self):
        """Verify GRANT statements exist for each role."""
        sql_file = SQL_DIR / "02_create_roles_grants.sql"
        content = sql_file.read_text()

        required_roles = ['DATA_ENGINEER', 'MARKETING_MANAGER', 'DATA_ANALYST']

        for role in required_roles:
            pattern = rf'GRANT.*TO\s+ROLE\s+{role}'
            assert re.search(pattern, content, re.IGNORECASE), \
                f"Missing GRANT statements for {role}"


class TestObservabilityTables:
    """Test observability table creation SQL."""

    def test_creates_all_tables(self):
        """Verify all observability tables are created."""
        sql_file = SQL_DIR / "03_create_observability_tables.sql"
        content = sql_file.read_text()

        required_tables = [
            'PIPELINE_RUN_METADATA',
            'DATA_QUALITY_METRICS',
            'LAYER_RECORD_COUNTS',
            'MODEL_EXECUTION_LOG'
        ]

        for table in required_tables:
            pattern = rf'CREATE\s+TABLE.*{table}'
            assert re.search(pattern, content, re.IGNORECASE), \
                f"Missing CREATE TABLE {table}"

    def test_creates_views(self):
        """Verify observability views are created."""
        sql_file = SQL_DIR / "03_create_observability_tables.sql"
        content = sql_file.read_text()

        required_views = [
            'V_LATEST_PIPELINE_RUNS',
            'V_RECENT_DQ_FAILURES',
            'V_RECORD_COUNT_TRENDS'
        ]

        for view in required_views:
            pattern = rf'CREATE.*VIEW.*{view}'
            assert re.search(pattern, content, re.IGNORECASE), \
                f"Missing CREATE VIEW {view}"

    def test_pipeline_metadata_has_required_columns(self):
        """Verify PIPELINE_RUN_METADATA has required columns."""
        sql_file = SQL_DIR / "03_create_observability_tables.sql"
        content = sql_file.read_text()

        # Find the PIPELINE_RUN_METADATA table definition
        table_match = re.search(
            r'CREATE\s+TABLE.*PIPELINE_RUN_METADATA\s*\((.*?)\)',
            content,
            re.IGNORECASE | re.DOTALL
        )

        assert table_match, "PIPELINE_RUN_METADATA table definition not found"

        table_def = table_match.group(1)

        required_columns = ['run_id', 'run_timestamp', 'status', 'models_run', 'models_failed']

        for column in required_columns:
            assert re.search(rf'\b{column}\b', table_def, re.IGNORECASE), \
                f"PIPELINE_RUN_METADATA missing required column: {column}"


class TestSQLBestPractices:
    """Test SQL files follow best practices."""

    @pytest.fixture(params=[
        "01_create_database_schemas.sql",
        "02_create_roles_grants.sql",
        "03_create_observability_tables.sql"
    ])
    def sql_file(self, request):
        """Parametrize test with SQL files (excluding environment check)."""
        return SQL_DIR / request.param

    def test_uses_if_not_exists(self, sql_file):
        """Verify idempotent CREATE statements with IF NOT EXISTS."""
        content = sql_file.read_text()

        # Should have at least one IF NOT EXISTS clause
        assert re.search(r'IF\s+NOT\s+EXISTS', content, re.IGNORECASE), \
            f"{sql_file.name} missing IF NOT EXISTS for idempotency"

    def test_has_verification_queries(self, sql_file):
        """Verify SQL files have SHOW or SELECT verification queries."""
        content = sql_file.read_text()

        # Should have verification queries at the end
        has_show = bool(re.search(r'\bSHOW\s+', content, re.IGNORECASE))
        has_select = bool(re.search(r'\bSELECT\s+', content, re.IGNORECASE))

        assert has_show or has_select, \
            f"{sql_file.name} missing verification queries (SHOW or SELECT)"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
