"""
Customer data generator for synthetic credit card portfolio.

This module generates realistic customer data for a credit card portfolio with
5 distinct customer segments, each with different spending patterns and characteristics.
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
from typing import Dict, Any
import re

from .config import (
    SEGMENTS,
    SEGMENT_SPEND_RANGES,
    CARD_TYPES,
    EMPLOYMENT_STATUSES,
    US_STATES,
    MIN_CREDIT_LIMIT,
    MAX_CREDIT_LIMIT,
    CREDIT_LIMIT_STEP,
    MIN_AGE,
    MAX_AGE,
    ACCOUNT_OPEN_MIN_YEARS_AGO,
    ACCOUNT_OPEN_MAX_YEARS_AGO,
    DECLINE_TYPES,
    GRADUAL_DECLINE_PERCENTAGE,
)


def generate_customers(n: int, seed: int = 42) -> pd.DataFrame:
    """
    Generate synthetic customer data for credit card portfolio.

    Args:
        n: Number of customers to generate
        seed: Random seed for reproducibility

    Returns:
        pd.DataFrame: DataFrame containing customer data with columns:
            - customer_id: Unique identifier (CUST00000001 format)
            - first_name: Customer first name
            - last_name: Customer last name
            - email: Customer email address
            - age: Customer age (22-75)
            - state: US state abbreviation
            - city: City name
            - employment_status: Employment status
            - card_type: Standard or Premium
            - credit_limit: Credit limit ($5K-$50K, multiples of $1K)
            - account_open_date: Date account was opened
            - customer_segment: Customer segment category
            - decline_type: Type of decline (only for Declining segment)

    Example:
        >>> df = generate_customers(1000, seed=42)
        >>> len(df)
        1000
    """
    # Set random seeds for reproducibility
    np.random.seed(seed)
    fake = Faker()
    Faker.seed(seed)

    customers = []

    # Calculate segment counts
    segment_counts = {}
    total_assigned = 0
    segment_list = list(SEGMENTS.keys())

    for i, (segment_name, percentage) in enumerate(SEGMENTS.items()):
        if i == len(SEGMENTS) - 1:
            # Last segment gets remaining customers to ensure exact total
            count = n - total_assigned
        else:
            count = int(n * percentage)
            total_assigned += count
        segment_counts[segment_name] = count

    # Create customer list for each segment
    segment_assignments = []
    for segment_name, count in segment_counts.items():
        segment_assignments.extend([segment_name] * count)

    # Shuffle to avoid clustering by segment
    np.random.shuffle(segment_assignments)

    for i in range(n):
        customer_id = f"CUST{str(i + 1).zfill(8)}"
        segment = segment_assignments[i]

        # Generate basic demographics
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"{first_name.lower()}.{last_name.lower()}@{fake.free_email_domain()}"
        age = np.random.randint(MIN_AGE, MAX_AGE + 1)
        state = np.random.choice(US_STATES)
        city = fake.city()
        employment_status = np.random.choice(EMPLOYMENT_STATUSES)

        # Assign card type
        # Premium cards: 30% of High-Value Travelers, rest get Standard
        if segment == "High-Value Travelers" and np.random.random() < 0.30:
            card_type = "Premium"
        else:
            card_type = "Standard"

        # Generate credit limit (multiples of $1000)
        num_steps = (MAX_CREDIT_LIMIT - MIN_CREDIT_LIMIT) // CREDIT_LIMIT_STEP + 1
        credit_limit = MIN_CREDIT_LIMIT + (np.random.randint(0, num_steps) * CREDIT_LIMIT_STEP)

        # Generate account open date (2-5 years ago)
        days_ago_min = ACCOUNT_OPEN_MAX_YEARS_AGO * 365
        days_ago_max = ACCOUNT_OPEN_MIN_YEARS_AGO * 365
        days_ago = np.random.randint(days_ago_min, days_ago_max + 1)
        account_open_date = (datetime.now() - timedelta(days=days_ago)).date()

        # Assign decline type (only for Declining segment)
        if segment == "Declining":
            decline_type = "gradual" if np.random.random() < GRADUAL_DECLINE_PERCENTAGE else "sudden"
        else:
            decline_type = None

        customers.append({
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "age": age,
            "state": state,
            "city": city,
            "employment_status": employment_status,
            "card_type": card_type,
            "credit_limit": credit_limit,
            "account_open_date": account_open_date,
            "customer_segment": segment,
            "decline_type": decline_type,
        })

    return pd.DataFrame(customers)


def validate_customer_data(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate customer data quality and integrity.

    Args:
        df: DataFrame containing customer data

    Returns:
        Dict containing validation results:
            - is_valid: Overall validation status (bool)
            - errors: List of validation errors
            - warnings: List of validation warnings
            - statistics: Data quality statistics

    Example:
        >>> df = generate_customers(1000)
        >>> result = validate_customer_data(df)
        >>> result['is_valid']
        True
    """
    errors = []
    warnings = []
    statistics = {}

    # Check required fields have no nulls
    required_fields = ["customer_id", "email", "state", "card_type", "credit_limit",
                      "customer_segment", "first_name", "last_name"]

    for field in required_fields:
        null_count = df[field].isnull().sum()
        if null_count > 0:
            errors.append(f"Field '{field}' has {null_count} null values")

    # Check customer_id uniqueness and format
    if df["customer_id"].duplicated().any():
        duplicate_count = df["customer_id"].duplicated().sum()
        errors.append(f"Found {duplicate_count} duplicate customer_ids")

    # Validate customer_id format
    customer_id_pattern = re.compile(r'^CUST\d{8}$')
    invalid_ids = df[~df["customer_id"].str.match(customer_id_pattern)]
    if len(invalid_ids) > 0:
        errors.append(f"Found {len(invalid_ids)} customer_ids with invalid format")

    # Check segment distribution
    segment_distribution = df["customer_segment"].value_counts(normalize=True).to_dict()
    statistics["segment_distribution"] = segment_distribution

    for segment_name, expected_pct in SEGMENTS.items():
        actual_pct = segment_distribution.get(segment_name, 0)
        diff = abs(actual_pct - expected_pct)
        if diff > 0.05:  # More than 5% deviation
            warnings.append(
                f"Segment '{segment_name}' distribution {actual_pct:.1%} "
                f"deviates from target {expected_pct:.1%} by {diff:.1%}"
            )

    # Validate email format (only for non-null emails)
    email_pattern = re.compile(r'^[^@]+@[^@]+\.[^@]+$')
    non_null_emails = df[df["email"].notnull()]
    if len(non_null_emails) > 0:
        invalid_emails = non_null_emails[~non_null_emails["email"].str.match(email_pattern)]
        if len(invalid_emails) > 0:
            errors.append(f"Found {len(invalid_emails)} emails with invalid format")

    # Check credit limits
    invalid_credit_limits = df[
        (df["credit_limit"] < MIN_CREDIT_LIMIT) |
        (df["credit_limit"] > MAX_CREDIT_LIMIT) |
        (df["credit_limit"] % CREDIT_LIMIT_STEP != 0)
    ]
    if len(invalid_credit_limits) > 0:
        errors.append(f"Found {len(invalid_credit_limits)} invalid credit limits")

    # Check decline_type is only set for Declining segment
    declining_customers = df[df["customer_segment"] == "Declining"]
    non_declining_customers = df[df["customer_segment"] != "Declining"]

    if declining_customers["decline_type"].isnull().any():
        null_decline_count = declining_customers["decline_type"].isnull().sum()
        errors.append(f"Found {null_decline_count} Declining customers without decline_type")

    if non_declining_customers["decline_type"].notnull().any():
        invalid_decline_count = non_declining_customers["decline_type"].notnull().sum()
        errors.append(
            f"Found {invalid_decline_count} non-Declining customers with decline_type set"
        )

    # Gather statistics
    statistics["total_customers"] = len(df)
    statistics["unique_customer_ids"] = df["customer_id"].nunique()
    statistics["credit_limit_min"] = df["credit_limit"].min()
    statistics["credit_limit_max"] = df["credit_limit"].max()
    statistics["credit_limit_avg"] = df["credit_limit"].mean()
    statistics["card_type_distribution"] = df["card_type"].value_counts().to_dict()
    statistics["employment_distribution"] = df["employment_status"].value_counts().to_dict()

    return {
        "is_valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "statistics": statistics,
    }


def save_to_csv(df: pd.DataFrame, filepath: str) -> None:
    """
    Save customer DataFrame to CSV file.

    Args:
        df: DataFrame containing customer data
        filepath: Path to output CSV file

    Example:
        >>> df = generate_customers(1000)
        >>> save_to_csv(df, 'customers.csv')
        Saved 1000 customers to customers.csv
    """
    df.to_csv(filepath, index=False)
    print(f"Saved {len(df)} customers to {filepath}")
