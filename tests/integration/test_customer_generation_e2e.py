"""
End-to-end integration tests for customer data generation.

Tests the complete workflow from CLI invocation to CSV file creation.
"""

import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path
from click.testing import CliRunner

from data_generation.cli import cli
from data_generation.customer_generator import validate_customer_data


class TestCustomerGenerationE2E:
    """End-to-end tests for customer generation workflow."""

    def test_cli_generates_valid_file(self):
        """Test complete workflow: CLI -> generate -> validate -> save."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "test_customers.csv"

            # Run CLI command
            result = runner.invoke(
                cli,
                [
                    "generate-customers",
                    "--count", "1000",
                    "--output", str(output_file),
                    "--seed", "42",
                ]
            )

            # Check command succeeded
            assert result.exit_code == 0, \
                f"CLI command failed with output:\n{result.output}"

            # Check file was created
            assert output_file.exists(), "Output file was not created"

            # Load CSV
            df = pd.read_csv(output_file)

            # Verify row count
            assert len(df) == 1000, f"Expected 1000 rows, got {len(df)}"

            # Run validation
            validation = validate_customer_data(df)

            # Check validation passes
            assert validation["is_valid"] is True, \
                f"Validation failed with errors: {validation['errors']}"

            # Verify expected columns exist
            expected_columns = [
                "customer_id", "first_name", "last_name", "email", "age",
                "state", "city", "employment_status", "card_type",
                "credit_limit", "account_open_date", "customer_segment", "decline_type"
            ]

            for col in expected_columns:
                assert col in df.columns, f"Missing column: {col}"

    def test_cli_with_custom_output_path(self):
        """Test CLI with nested output directory path."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Use nested path
            output_file = Path(tmpdir) / "data" / "output" / "customers.csv"

            result = runner.invoke(
                cli,
                [
                    "generate-customers",
                    "--count", "500",
                    "--output", str(output_file),
                    "--seed", "123",
                ]
            )

            assert result.exit_code == 0, f"CLI failed: {result.output}"
            assert output_file.exists(), "File not created in nested directory"

            df = pd.read_csv(output_file)
            assert len(df) == 500

    def test_cli_different_seeds_produce_different_data(self):
        """Test that different seeds produce different customer data."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            file1 = Path(tmpdir) / "customers_seed1.csv"
            file2 = Path(tmpdir) / "customers_seed2.csv"

            # Generate with seed 42
            result1 = runner.invoke(
                cli,
                ["generate-customers", "--count", "100", "--output", str(file1), "--seed", "42"]
            )
            assert result1.exit_code == 0

            # Generate with seed 123
            result2 = runner.invoke(
                cli,
                ["generate-customers", "--count", "100", "--output", str(file2), "--seed", "123"]
            )
            assert result2.exit_code == 0

            df1 = pd.read_csv(file1)
            df2 = pd.read_csv(file2)

            # DataFrames should have different data (not identical)
            # Check that at least some emails differ (high probability with different seeds)
            assert not df1["email"].equals(df2["email"]), \
                "Different seeds should produce different data"

    def test_cli_same_seed_produces_identical_data(self):
        """Test that same seed produces identical customer data."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            file1 = Path(tmpdir) / "customers_run1.csv"
            file2 = Path(tmpdir) / "customers_run2.csv"

            # Generate twice with same seed
            for filepath in [file1, file2]:
                result = runner.invoke(
                    cli,
                    ["generate-customers", "--count", "100", "--output", str(filepath), "--seed", "42"]
                )
                assert result.exit_code == 0

            df1 = pd.read_csv(file1)
            df2 = pd.read_csv(file2)

            # DataFrames should be identical
            pd.testing.assert_frame_equal(df1, df2)

    def test_cli_large_customer_count(self):
        """Test CLI can generate large number of customers (10K)."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "customers_10k.csv"

            result = runner.invoke(
                cli,
                ["generate-customers", "--count", "10000", "--output", str(output_file)]
            )

            assert result.exit_code == 0, f"Large generation failed: {result.output}"
            assert output_file.exists()

            df = pd.read_csv(output_file)
            assert len(df) == 10000

            # Quick validation
            validation = validate_customer_data(df)
            assert validation["is_valid"] is True

    def test_cli_output_includes_statistics(self):
        """Test CLI output includes validation statistics."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "customers.csv"

            result = runner.invoke(
                cli,
                ["generate-customers", "--count", "1000", "--output", str(output_file)]
            )

            # Check output contains expected statistics
            assert "Statistics:" in result.output
            assert "Segment Distribution:" in result.output
            assert "Card Type Distribution:" in result.output
            assert "Validation passed" in result.output
            assert "Successfully saved" in result.output

    def test_csv_file_has_correct_structure(self):
        """Test generated CSV has correct column order and data types."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "customers.csv"

            result = runner.invoke(
                cli,
                ["generate-customers", "--count", "100", "--output", str(output_file)]
            )

            assert result.exit_code == 0

            df = pd.read_csv(output_file)

            # Check column order (first few columns)
            assert df.columns[0] == "customer_id"
            assert df.columns[1] == "first_name"
            assert df.columns[2] == "last_name"
            assert df.columns[3] == "email"

            # Check data types can be inferred correctly
            assert df["customer_id"].dtype == object
            assert df["age"].dtype in [int, 'int64']
            assert df["credit_limit"].dtype in [int, 'int64']


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
