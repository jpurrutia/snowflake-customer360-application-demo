"""
Unit tests for customer data generator.

Tests customer generation logic, data validation, and data quality.
"""

import pytest
import pandas as pd
import re
from data_generation.customer_generator import (
    generate_customers,
    validate_customer_data,
)
from data_generation.config import SEGMENTS, MIN_CREDIT_LIMIT, MAX_CREDIT_LIMIT, CREDIT_LIMIT_STEP


class TestCustomerGeneration:
    """Test customer generation functionality."""

    def test_generates_correct_row_count(self):
        """Verify correct number of customers are generated."""
        df = generate_customers(1000, seed=42)
        assert len(df) == 1000

    def test_customer_id_format(self):
        """Verify customer IDs match expected format and are unique."""
        df = generate_customers(100, seed=42)

        # Check format
        customer_id_pattern = re.compile(r'^CUST\d{8}$')
        for customer_id in df["customer_id"]:
            assert customer_id_pattern.match(customer_id), \
                f"Customer ID {customer_id} doesn't match format CUST########"

        # Check uniqueness
        assert df["customer_id"].nunique() == 100, "Customer IDs are not unique"

    def test_customer_id_sequential(self):
        """Verify customer IDs are sequential starting from CUST00000001."""
        df = generate_customers(100, seed=42)

        expected_ids = [f"CUST{str(i).zfill(8)}" for i in range(1, 101)]
        actual_ids = sorted(df["customer_id"].tolist())

        assert actual_ids == expected_ids, "Customer IDs are not sequential"

    def test_segment_distribution(self):
        """Verify customer segment distribution matches target percentages."""
        df = generate_customers(10000, seed=42)

        segment_counts = df["customer_segment"].value_counts()
        total = len(df)

        for segment_name, expected_pct in SEGMENTS.items():
            actual_count = segment_counts.get(segment_name, 0)
            actual_pct = actual_count / total
            diff = abs(actual_pct - expected_pct)

            assert diff <= 0.05, \
                f"Segment '{segment_name}' distribution {actual_pct:.1%} " \
                f"differs from target {expected_pct:.1%} by {diff:.1%}"

    def test_no_null_required_fields(self):
        """Verify required fields have no null values."""
        df = generate_customers(100, seed=42)

        required_fields = ["customer_id", "email", "state", "card_type", "credit_limit"]

        for field in required_fields:
            null_count = df[field].isnull().sum()
            assert null_count == 0, f"Field '{field}' has {null_count} null values"

    def test_credit_limit_ranges(self):
        """Verify credit limits are within valid range and multiples of 1000."""
        df = generate_customers(1000, seed=42)

        # Check minimum
        assert df["credit_limit"].min() >= MIN_CREDIT_LIMIT, \
            f"Found credit limit below minimum {MIN_CREDIT_LIMIT}"

        # Check maximum
        assert df["credit_limit"].max() <= MAX_CREDIT_LIMIT, \
            f"Found credit limit above maximum {MAX_CREDIT_LIMIT}"

        # Check multiples of 1000
        non_multiples = df[df["credit_limit"] % CREDIT_LIMIT_STEP != 0]
        assert len(non_multiples) == 0, \
            f"Found {len(non_multiples)} credit limits not multiples of {CREDIT_LIMIT_STEP}"

    def test_email_format(self):
        """Verify email addresses have valid format."""
        df = generate_customers(100, seed=42)

        for email in df["email"]:
            assert "@" in email, f"Email {email} missing '@'"
            assert "." in email, f"Email {email} missing '.'"

            # More thorough email validation
            email_pattern = re.compile(r'^[^@]+@[^@]+\.[^@]+$')
            assert email_pattern.match(email), f"Email {email} has invalid format"

    def test_reproducibility(self):
        """Verify same seed produces identical results."""
        df1 = generate_customers(100, seed=42)
        df2 = generate_customers(100, seed=42)

        # DataFrames should be identical
        pd.testing.assert_frame_equal(df1, df2)

    def test_decline_type_only_for_declining_segment(self):
        """Verify decline_type is only set for Declining segment customers."""
        df = generate_customers(1000, seed=42)

        # Declining segment customers should have decline_type
        declining = df[df["customer_segment"] == "Declining"]
        if len(declining) > 0:
            assert declining["decline_type"].notnull().all(), \
                "Some Declining customers missing decline_type"
            assert declining["decline_type"].isin(["gradual", "sudden"]).all(), \
                "Invalid decline_type values found"

        # Non-declining customers should have null decline_type
        non_declining = df[df["customer_segment"] != "Declining"]
        if len(non_declining) > 0:
            assert non_declining["decline_type"].isnull().all(), \
                "Non-Declining customers have decline_type set"

    def test_age_range(self):
        """Verify customer ages are within valid range."""
        df = generate_customers(1000, seed=42)

        assert df["age"].min() >= 22, "Found age below minimum 22"
        assert df["age"].max() <= 75, "Found age above maximum 75"

    def test_card_type_values(self):
        """Verify card_type only contains valid values."""
        df = generate_customers(1000, seed=42)

        valid_card_types = ["Standard", "Premium"]
        invalid_cards = df[~df["card_type"].isin(valid_card_types)]

        assert len(invalid_cards) == 0, \
            f"Found {len(invalid_cards)} customers with invalid card_type"

    def test_state_values(self):
        """Verify state contains valid US state abbreviations."""
        df = generate_customers(1000, seed=42)

        from data_generation.config import US_STATES
        invalid_states = df[~df["state"].isin(US_STATES)]

        assert len(invalid_states) == 0, \
            f"Found {len(invalid_states)} customers with invalid state"

    def test_premium_card_distribution(self):
        """Verify Premium cards are primarily for High-Value Travelers."""
        df = generate_customers(5000, seed=42)

        premium_customers = df[df["card_type"] == "Premium"]

        if len(premium_customers) > 0:
            # Most premium customers should be High-Value Travelers
            hvt_premium = premium_customers[
                premium_customers["customer_segment"] == "High-Value Travelers"
            ]
            hvt_percentage = len(hvt_premium) / len(premium_customers)

            # Allow some flexibility, but most should be HVT
            assert hvt_percentage > 0.5, \
                f"Only {hvt_percentage:.1%} of Premium cards belong to High-Value Travelers"


class TestCustomerValidation:
    """Test customer data validation functionality."""

    def test_validation_passes_for_valid_data(self):
        """Verify validation passes for correctly generated data."""
        df = generate_customers(100, seed=42)
        validation = validate_customer_data(df)

        assert validation["is_valid"] is True, \
            f"Validation failed with errors: {validation['errors']}"

    def test_validation_fails_for_duplicate_ids(self):
        """Verify validation detects duplicate customer IDs."""
        df = generate_customers(100, seed=42)

        # Create duplicate by copying first row
        df_with_duplicates = pd.concat([df, df.head(1)], ignore_index=True)

        validation = validate_customer_data(df_with_duplicates)

        assert validation["is_valid"] is False, \
            "Validation should fail for duplicate customer_ids"
        assert any("duplicate" in error.lower() for error in validation["errors"]), \
            "Validation errors should mention duplicates"

    def test_validation_fails_for_null_required_fields(self):
        """Verify validation detects null values in required fields."""
        df = generate_customers(100, seed=42)

        # Set some emails to null
        df.loc[0:4, "email"] = None

        validation = validate_customer_data(df)

        assert validation["is_valid"] is False, \
            "Validation should fail for null required fields"
        assert any("email" in error.lower() for error in validation["errors"]), \
            "Validation errors should mention email field"

    def test_validation_detects_invalid_customer_id_format(self):
        """Verify validation detects invalid customer ID format."""
        df = generate_customers(100, seed=42)

        # Break format of first customer ID
        df.loc[0, "customer_id"] = "INVALID123"

        validation = validate_customer_data(df)

        assert validation["is_valid"] is False, \
            "Validation should fail for invalid customer_id format"

    def test_validation_detects_invalid_credit_limits(self):
        """Verify validation detects credit limits outside valid range."""
        df = generate_customers(100, seed=42)

        # Set invalid credit limit
        df.loc[0, "credit_limit"] = 100000  # Above maximum

        validation = validate_customer_data(df)

        assert validation["is_valid"] is False, \
            "Validation should fail for invalid credit limits"

    def test_validation_includes_statistics(self):
        """Verify validation result includes statistics."""
        df = generate_customers(100, seed=42)
        validation = validate_customer_data(df)

        assert "statistics" in validation, "Validation result missing statistics"

        stats = validation["statistics"]
        assert "total_customers" in stats
        assert "unique_customer_ids" in stats
        assert "segment_distribution" in stats
        assert "card_type_distribution" in stats

        assert stats["total_customers"] == 100
        assert stats["unique_customer_ids"] == 100

    def test_validation_warns_on_segment_distribution_deviation(self):
        """Verify validation warns if segment distribution deviates significantly."""
        # This test is probabilistic - with small samples, distribution might deviate
        # We'll generate a large sample to minimize randomness
        df = generate_customers(10000, seed=42)
        validation = validate_customer_data(df)

        # With 10K customers and seed=42, distribution should be close to target
        # If warnings exist, they should be about minor deviations
        if validation["warnings"]:
            for warning in validation["warnings"]:
                assert "segment" in warning.lower() or "distribution" in warning.lower()

    def test_validation_detects_invalid_email_format(self):
        """Verify validation detects invalid email formats."""
        df = generate_customers(100, seed=42)

        # Set invalid email
        df.loc[0, "email"] = "not-an-email"

        validation = validate_customer_data(df)

        assert validation["is_valid"] is False, \
            "Validation should fail for invalid email format"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
