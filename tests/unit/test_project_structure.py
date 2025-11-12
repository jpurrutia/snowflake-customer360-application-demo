"""
Test suite to validate project structure and configuration files.
This test ensures all required directories and files exist and are properly configured.
"""
import os
from pathlib import Path
import pytest


# Get project root (two levels up from this test file)
PROJECT_ROOT = Path(__file__).parent.parent.parent


class TestProjectStructure:
    """Test that all required directories exist."""

    def test_root_directories_exist(self):
        """Verify all required root-level directories exist."""
        required_dirs = [
            "terraform",
            "snowflake",
            "snowflake/setup",
            "dbt_customer_analytics",
            "data_generation",
            "ml",
            "semantic_layer",
            "streamlit",
            "tests",
        ]

        for directory in required_dirs:
            dir_path = PROJECT_ROOT / directory
            assert dir_path.exists(), f"Required directory '{directory}' does not exist"
            assert dir_path.is_dir(), f"'{directory}' exists but is not a directory"

    def test_test_subdirectories_exist(self):
        """Verify all test subdirectories exist."""
        test_dirs = [
            "tests/unit",
            "tests/integration",
            "tests/performance",
            "tests/data_quality",
        ]

        for directory in test_dirs:
            dir_path = PROJECT_ROOT / directory
            assert dir_path.exists(), f"Required test directory '{directory}' does not exist"
            assert dir_path.is_dir(), f"'{directory}' exists but is not a directory"


class TestConfigurationFiles:
    """Test that all required configuration files exist and are valid."""

    def test_readme_exists(self):
        """Verify README.md exists."""
        readme_path = PROJECT_ROOT / "README.md"
        assert readme_path.exists(), "README.md does not exist"
        assert readme_path.is_file(), "README.md exists but is not a file"

        # Verify README has content
        content = readme_path.read_text()
        assert len(content) > 100, "README.md appears to be empty or too short"
        assert "Snowflake" in content, "README.md should mention Snowflake"
        assert "Customer 360" in content, "README.md should mention Customer 360"

    def test_gitignore_exists(self):
        """Verify .gitignore exists and has required patterns."""
        gitignore_path = PROJECT_ROOT / ".gitignore"
        assert gitignore_path.exists(), ".gitignore does not exist"
        assert gitignore_path.is_file(), ".gitignore exists but is not a file"

        # Verify key patterns exist (using flexible matching for wildcards)
        content = gitignore_path.read_text()
        required_patterns = [
            "__pycache__",
            (".pyc", "*.py[cod]"),  # Either exact or glob pattern
            ".env",
            "*.tfstate",
            ".terraform",
            ".vscode",
            ".idea",
            "*.csv",
            "target/",
            "logs/",
        ]

        for pattern in required_patterns:
            if isinstance(pattern, tuple):
                # Check if any of the alternatives exist
                assert any(p in content for p in pattern), \
                    f".gitignore missing required pattern: {pattern[0]} (or equivalent)"
            else:
                assert pattern in content, f".gitignore missing required pattern: {pattern}"

    def test_env_example_exists(self):
        """Verify .env.example exists and has required environment variables."""
        env_example_path = PROJECT_ROOT / ".env.example"
        assert env_example_path.exists(), ".env.example does not exist"
        assert env_example_path.is_file(), ".env.example exists but is not a file"

        # Verify required environment variables
        content = env_example_path.read_text()
        required_vars = [
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PASSWORD",
            "SNOWFLAKE_WAREHOUSE",
            "SNOWFLAKE_DATABASE",
            "SNOWFLAKE_SCHEMA",
            "SNOWFLAKE_ROLE",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_REGION",
            "S3_BUCKET_NAME",
        ]

        for var in required_vars:
            assert var in content, f".env.example missing required variable: {var}"

    def test_requirements_txt_exists_and_valid(self):
        """Verify requirements.txt exists and can be parsed."""
        requirements_path = PROJECT_ROOT / "requirements.txt"
        assert requirements_path.exists(), "requirements.txt does not exist"
        assert requirements_path.is_file(), "requirements.txt exists but is not a file"

        # Verify key dependencies
        content = requirements_path.read_text()
        required_packages = [
            "snowflake-connector-python",
            "dbt-snowflake",
            "faker",
            "pandas",
            "pytest",
            "boto3",
            "python-dotenv",
            "tenacity",
        ]

        for package in required_packages:
            assert package in content, f"requirements.txt missing required package: {package}"

        # Verify file can be parsed (no syntax errors)
        lines = [line.strip() for line in content.split("\n") if line.strip()]
        # Filter out comments
        package_lines = [line for line in lines if not line.startswith("#")]
        assert len(package_lines) > 0, "requirements.txt has no package specifications"

    def test_pyproject_toml_exists(self):
        """Verify pyproject.toml exists for UV package manager."""
        pyproject_path = PROJECT_ROOT / "pyproject.toml"
        assert pyproject_path.exists(), "pyproject.toml does not exist"
        assert pyproject_path.is_file(), "pyproject.toml exists but is not a file"

        # Verify basic structure
        content = pyproject_path.read_text()
        assert "[project]" in content, "pyproject.toml missing [project] section"
        assert "name" in content, "pyproject.toml missing project name"
        assert "dependencies" in content, "pyproject.toml missing dependencies section"

    def test_makefile_exists(self):
        """Verify Makefile exists and has required targets."""
        makefile_path = PROJECT_ROOT / "Makefile"
        assert makefile_path.exists(), "Makefile does not exist"
        assert makefile_path.is_file(), "Makefile exists but is not a file"

        # Verify required targets
        content = makefile_path.read_text()
        required_targets = ["setup", "test", "lint", "clean"]

        for target in required_targets:
            # Check for target definition (target:)
            assert f"{target}:" in content, f"Makefile missing required target: {target}"


class TestPythonPackages:
    """Test that Python packages are properly initialized."""

    def test_init_files_exist(self):
        """Verify __init__.py files exist in Python packages."""
        required_init_files = [
            "data_generation/__init__.py",
            "tests/__init__.py",
            "tests/unit/__init__.py",
            "tests/integration/__init__.py",
        ]

        for init_file in required_init_files:
            init_path = PROJECT_ROOT / init_file
            assert init_path.exists(), f"Required __init__.py missing: {init_file}"
            assert init_path.is_file(), f"'{init_file}' exists but is not a file"


class TestProjectMetadata:
    """Test project metadata and documentation."""

    def test_project_root_is_valid(self):
        """Verify we can correctly identify the project root."""
        assert PROJECT_ROOT.exists(), "Cannot locate project root"
        assert PROJECT_ROOT.is_dir(), "Project root is not a directory"

        # Verify we're in the right place by checking for key files
        assert (PROJECT_ROOT / "README.md").exists(), "Project root missing README.md"
        assert (PROJECT_ROOT / "pyproject.toml").exists(), "Project root missing pyproject.toml"

    def test_no_env_file_committed(self):
        """Verify .env file is not committed (security check)."""
        env_path = PROJECT_ROOT / ".env"
        # It's okay if .env doesn't exist (not created yet)
        # But if it does exist, verify it's in .gitignore
        if env_path.exists():
            gitignore_path = PROJECT_ROOT / ".gitignore"
            gitignore_content = gitignore_path.read_text()
            assert ".env" in gitignore_content, ".env file exists but not in .gitignore!"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
