"""
Unit tests for Terraform variable definitions.
Validates that all required variables are defined with correct types and defaults.
"""
import re
from pathlib import Path
import pytest


# Get project root and terraform directory
PROJECT_ROOT = Path(__file__).parent.parent.parent
TERRAFORM_DIR = PROJECT_ROOT / "terraform"
VARIABLES_FILE = TERRAFORM_DIR / "variables.tf"


class TestTerraformVariables:
    """Test Terraform variables.tf configuration."""

    def test_variables_file_exists(self):
        """Verify variables.tf file exists."""
        assert VARIABLES_FILE.exists(), "terraform/variables.tf does not exist"
        assert VARIABLES_FILE.is_file(), "terraform/variables.tf is not a file"

    def test_variables_file_not_empty(self):
        """Verify variables.tf has content."""
        content = VARIABLES_FILE.read_text()
        assert len(content) > 50, "variables.tf appears to be empty or too short"

    def test_required_variables_defined(self):
        """Verify all required variables are defined."""
        content = VARIABLES_FILE.read_text()

        required_variables = [
            "project_name",
            "environment",
            "aws_region",
            "snowflake_account_id",
            "snowflake_external_id",
        ]

        for var in required_variables:
            # Check for variable block definition
            pattern = rf'variable\s+"{var}"\s*{{'
            assert re.search(pattern, content), \
                f"Required variable '{var}' not defined in variables.tf"

    def test_variable_defaults(self):
        """Verify default values are set for appropriate variables."""
        content = VARIABLES_FILE.read_text()

        # Variables that SHOULD have defaults
        defaults_expected = {
            "project_name": "snowflake-customer-analytics",
            "environment": "demo",
            "aws_region": "us-east-1",
        }

        for var, expected_default in defaults_expected.items():
            # Find the variable block
            var_pattern = rf'variable\s+"{var}"\s*{{([^}}]+)}}'
            match = re.search(var_pattern, content, re.DOTALL)
            assert match, f"Variable '{var}' not found in variables.tf"

            var_block = match.group(1)

            # Check if default is present
            default_pattern = r'default\s*=\s*"([^"]+)"'
            default_match = re.search(default_pattern, var_block)
            assert default_match, f"Variable '{var}' should have a default value"
            assert default_match.group(1) == expected_default, \
                f"Variable '{var}' has incorrect default: {default_match.group(1)}"

    def test_sensitive_variables_no_defaults(self):
        """Verify sensitive variables do NOT have defaults."""
        content = VARIABLES_FILE.read_text()

        # Variables that should NOT have defaults (require user input)
        no_defaults = [
            "snowflake_account_id",
            "snowflake_external_id",
        ]

        for var in no_defaults:
            # Find the variable block
            var_pattern = rf'variable\s+"{var}"\s*{{([^}}]+)}}'
            match = re.search(var_pattern, content, re.DOTALL)
            assert match, f"Variable '{var}' not found in variables.tf"

            var_block = match.group(1)

            # Check that default is NOT present
            default_pattern = r'default\s*='
            assert not re.search(default_pattern, var_block), \
                f"Variable '{var}' should NOT have a default value (requires user input)"

    def test_variable_types_defined(self):
        """Verify variable types are explicitly defined."""
        content = VARIABLES_FILE.read_text()

        required_variables = [
            "project_name",
            "environment",
            "aws_region",
            "snowflake_account_id",
            "snowflake_external_id",
        ]

        for var in required_variables:
            # Find the variable block
            var_pattern = rf'variable\s+"{var}"\s*{{([^}}]+)}}'
            match = re.search(var_pattern, content, re.DOTALL)
            assert match, f"Variable '{var}' not found in variables.tf"

            var_block = match.group(1)

            # Check that type is defined
            type_pattern = r'type\s*=\s*(\w+)'
            type_match = re.search(type_pattern, var_block)
            assert type_match, f"Variable '{var}' should have an explicit type definition"

    def test_variable_descriptions(self):
        """Verify all variables have descriptions."""
        content = VARIABLES_FILE.read_text()

        required_variables = [
            "project_name",
            "environment",
            "aws_region",
            "snowflake_account_id",
            "snowflake_external_id",
        ]

        for var in required_variables:
            # Find the variable block
            var_pattern = rf'variable\s+"{var}"\s*{{([^}}]+)}}'
            match = re.search(var_pattern, content, re.DOTALL)
            assert match, f"Variable '{var}' not found in variables.tf"

            var_block = match.group(1)

            # Check that description is present
            desc_pattern = r'description\s*=\s*"([^"]+)"'
            desc_match = re.search(desc_pattern, var_block)
            assert desc_match, f"Variable '{var}' should have a description"
            assert len(desc_match.group(1)) > 10, \
                f"Variable '{var}' description is too short"

    def test_variable_validations(self):
        """Verify important variables have validation rules."""
        content = VARIABLES_FILE.read_text()

        # Variables that should have validations
        validation_expected = [
            "project_name",      # Should validate format (lowercase, hyphens)
            "environment",       # Should validate allowed values
            "snowflake_account_id",  # Should validate 12-digit format
        ]

        for var in validation_expected:
            # Find the variable block
            var_pattern = rf'variable\s+"{var}"\s*{{([^}}]+)}}'
            match = re.search(var_pattern, content, re.DOTALL)
            assert match, f"Variable '{var}' not found in variables.tf"

            var_block = match.group(1)

            # Check that validation is present
            validation_pattern = r'validation\s*{'
            assert re.search(validation_pattern, var_block), \
                f"Variable '{var}' should have validation rules"


class TestTerraformFilesExist:
    """Test that all required Terraform files exist."""

    def test_main_tf_exists(self):
        """Verify main.tf exists."""
        main_file = TERRAFORM_DIR / "main.tf"
        assert main_file.exists(), "terraform/main.tf does not exist"

    def test_s3_tf_exists(self):
        """Verify s3.tf exists."""
        s3_file = TERRAFORM_DIR / "s3.tf"
        assert s3_file.exists(), "terraform/s3.tf does not exist"

    def test_iam_tf_exists(self):
        """Verify iam.tf exists."""
        iam_file = TERRAFORM_DIR / "iam.tf"
        assert iam_file.exists(), "terraform/iam.tf does not exist"

    def test_outputs_tf_exists(self):
        """Verify outputs.tf exists."""
        outputs_file = TERRAFORM_DIR / "outputs.tf"
        assert outputs_file.exists(), "terraform/outputs.tf does not exist"

    def test_readme_exists(self):
        """Verify terraform/README.md exists."""
        readme_file = TERRAFORM_DIR / "README.md"
        assert readme_file.exists(), "terraform/README.md does not exist"

    def test_tfvars_example_exists(self):
        """Verify terraform.tfvars.example exists."""
        tfvars_example = TERRAFORM_DIR / "terraform.tfvars.example"
        assert tfvars_example.exists(), "terraform/terraform.tfvars.example does not exist"


class TestOutputsConfiguration:
    """Test Terraform outputs.tf configuration."""

    def test_outputs_file_has_required_outputs(self):
        """Verify required outputs are defined."""
        outputs_file = TERRAFORM_DIR / "outputs.tf"
        content = outputs_file.read_text()

        required_outputs = [
            "s3_bucket_name",
            "s3_bucket_arn",
            "iam_role_arn",
            "iam_role_name",
        ]

        for output in required_outputs:
            pattern = rf'output\s+"{output}"\s*{{'
            assert re.search(pattern, content), \
                f"Required output '{output}' not defined in outputs.tf"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
