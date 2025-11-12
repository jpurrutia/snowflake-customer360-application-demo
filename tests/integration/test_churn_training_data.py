"""
Integration tests for ML churn training data preparation.

Tests verify:
- Churn labels created correctly
- Churn label logic applied properly
- Training features complete
- No null values in critical features
- Class balance reasonable
- Feature ranges valid
- Sufficient training examples

Run with:
    uv run pytest tests/integration/test_churn_training_data.py -v
"""

import pytest
from pathlib import Path
from tests.conftest import snowflake_connection, dbt_project_dir


@pytest.fixture(scope="module")
def churn_data_setup(snowflake_connection):
    """
    Execute ML scripts to create churn labels and training features.
    """
    import subprocess

    cursor = snowflake_connection.cursor()

    # Execute SQL scripts
    scripts_dir = Path("/Users/jpurrutia/projects/snowflake-panel-demo/snowflake/ml")

    # Script 1: Create churn labels
    print("\nExecuting 01_create_churn_labels.sql...")
    with open(scripts_dir / "01_create_churn_labels.sql", 'r') as f:
        sql_script = f.read()
        # Execute script (split by semicolons for multi-statement)
        for statement in sql_script.split(';'):
            statement = statement.strip()
            if statement and not statement.startswith('/*') and not statement.startswith('--'):
                try:
                    cursor.execute(statement)
                except Exception as e:
                    # Some statements might fail (like CREATE INDEX IF NOT EXISTS), continue
                    if "already exists" not in str(e).lower():
                        print(f"Warning: {e}")

    # Script 2: Create training features
    print("\nExecuting 02_create_training_features.sql...")
    with open(scripts_dir / "02_create_training_features.sql", 'r') as f:
        sql_script = f.read()
        for statement in sql_script.split(';'):
            statement = statement.strip()
            if statement and not statement.startswith('/*') and not statement.startswith('--'):
                try:
                    cursor.execute(statement)
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        print(f"Warning: {e}")

    yield snowflake_connection


# ============================================================================
# Test 1: Churn Labels Created
# ============================================================================

def test_churn_labels_created(churn_data_setup):
    """
    Verify CHURN_LABELS table created and has data.
    """
    cursor = churn_data_setup.cursor()

    # Check table exists
    cursor.execute("""
        SELECT table_name
        FROM SNOWFLAKE_DEMO.INFORMATION_SCHEMA.TABLES
        WHERE table_schema = 'GOLD'
          AND table_name = 'CHURN_LABELS'
          AND table_type = 'BASE TABLE'
    """)

    tables = cursor.fetchall()
    assert len(tables) == 1, "CHURN_LABELS table not found"

    # Check has rows
    cursor.execute("""
        SELECT COUNT(*) AS row_count
        FROM GOLD.CHURN_LABELS
    """)

    row_count = cursor.fetchone()[0]

    # Expected: 40K-45K customers with baseline data
    assert row_count >= 30000, f"Expected ≥30K rows, got {row_count:,}"

    print(f"✓ CHURN_LABELS table created with {row_count:,} rows")


# ============================================================================
# Test 2: Churn Label Logic - Inactive Customers
# ============================================================================

def test_churn_label_logic_inactive(churn_data_setup):
    """
    Verify customers with no recent transactions labeled as churned.
    """
    cursor = churn_data_setup.cursor()

    # Query customers with 60+ days since last transaction
    cursor.execute("""
        SELECT
            COUNT(*) AS total_inactive,
            SUM(CASE WHEN churned = TRUE THEN 1 ELSE 0 END) AS labeled_churned
        FROM GOLD.CHURN_LABELS
        WHERE days_since_last_transaction > 60
    """)

    row = cursor.fetchone()
    total_inactive, labeled_churned = row

    # All inactive customers should be labeled as churned
    assert labeled_churned == total_inactive, \
        f"Only {labeled_churned}/{total_inactive} inactive customers labeled as churned"

    print(f"✓ All {total_inactive:,} inactive customers (60+ days) labeled as churned")


# ============================================================================
# Test 3: Churn Label Logic - Declining Customers
# ============================================================================

def test_churn_label_logic_declining(churn_data_setup):
    """
    Verify customers with significant spend decline labeled as churned.
    """
    cursor = churn_data_setup.cursor()

    # Query customers with recent spend < 30% of baseline
    cursor.execute("""
        SELECT
            COUNT(*) AS total_declining,
            SUM(CASE WHEN churned = TRUE THEN 1 ELSE 0 END) AS labeled_churned
        FROM GOLD.CHURN_LABELS
        WHERE recent_avg_spend < (baseline_avg_spend * 0.30)
          AND recent_avg_spend > 0  -- Exclude completely inactive
    """)

    row = cursor.fetchone()
    total_declining, labeled_churned = row

    # All declining customers should be labeled as churned
    assert labeled_churned == total_declining, \
        f"Only {labeled_churned}/{total_declining} declining customers labeled as churned"

    print(f"✓ All {total_declining:,} declining customers (<30% baseline) labeled as churned")


# ============================================================================
# Test 4: Churn Label Logic - Active Customers
# ============================================================================

def test_churn_label_logic_active(churn_data_setup):
    """
    Verify stable customers (spending normally) labeled as active.
    """
    cursor = churn_data_setup.cursor()

    # Query customers who are active (recent transactions, normal spending)
    cursor.execute("""
        SELECT
            COUNT(*) AS total_stable,
            SUM(CASE WHEN churned = FALSE THEN 1 ELSE 0 END) AS labeled_active
        FROM GOLD.CHURN_LABELS
        WHERE days_since_last_transaction <= 60
          AND recent_avg_spend >= (baseline_avg_spend * 0.30)
    """)

    row = cursor.fetchone()
    total_stable, labeled_active = row

    # All stable customers should be labeled as active (not churned)
    assert labeled_active == total_stable, \
        f"Only {labeled_active}/{total_stable} stable customers labeled as active"

    print(f"✓ All {total_stable:,} stable customers labeled as active (not churned)")


# ============================================================================
# Test 5: Training Features Created
# ============================================================================

def test_training_features_created(churn_data_setup):
    """
    Verify ML_TRAINING_DATA table created with all expected columns.
    """
    cursor = churn_data_setup.cursor()

    # Check table exists
    cursor.execute("""
        SELECT table_name
        FROM SNOWFLAKE_DEMO.INFORMATION_SCHEMA.TABLES
        WHERE table_schema = 'GOLD'
          AND table_name = 'ML_TRAINING_DATA'
          AND table_type = 'BASE TABLE'
    """)

    tables = cursor.fetchall()
    assert len(tables) == 1, "ML_TRAINING_DATA table not found"

    # Check expected columns exist
    cursor.execute("""
        SELECT column_name
        FROM SNOWFLAKE_DEMO.INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema = 'GOLD'
          AND table_name = 'ML_TRAINING_DATA'
        ORDER BY column_name
    """)

    columns = {row[0] for row in cursor.fetchall()}

    # Expected critical columns
    expected_columns = {
        'CUSTOMER_ID',
        'AGE',
        'CREDIT_LIMIT',
        'LIFETIME_VALUE',
        'AVG_TRANSACTION_VALUE',
        'DAYS_SINCE_LAST_TRANSACTION',
        'SPEND_CHANGE_PCT',
        'AVG_MONTHLY_SPEND',
        'CREDIT_UTILIZATION_PCT',
        'TENURE_MONTHS',
        'CHURNED',  # Target variable
    }

    missing_columns = expected_columns - columns
    assert len(missing_columns) == 0, f"Missing columns: {missing_columns}"

    print(f"✓ ML_TRAINING_DATA table created with {len(columns)} columns")


# ============================================================================
# Test 6: No Null Features
# ============================================================================

def test_no_null_features(churn_data_setup):
    """
    Verify no null values in critical features.
    """
    cursor = churn_data_setup.cursor()

    # Check for nulls in critical features
    cursor.execute("""
        SELECT
            SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END) AS null_age,
            SUM(CASE WHEN credit_limit IS NULL THEN 1 ELSE 0 END) AS null_credit_limit,
            SUM(CASE WHEN avg_monthly_spend IS NULL THEN 1 ELSE 0 END) AS null_avg_monthly_spend,
            SUM(CASE WHEN tenure_months IS NULL THEN 1 ELSE 0 END) AS null_tenure_months,
            SUM(CASE WHEN lifetime_value IS NULL THEN 1 ELSE 0 END) AS null_lifetime_value
        FROM GOLD.ML_TRAINING_DATA
    """)

    row = cursor.fetchone()
    null_age, null_credit, null_monthly, null_tenure, null_ltv = row

    # All critical features should have 0 nulls
    assert null_age == 0, f"Found {null_age} null age values"
    assert null_credit == 0, f"Found {null_credit} null credit_limit values"
    assert null_monthly == 0, f"Found {null_monthly} null avg_monthly_spend values"
    assert null_tenure == 0, f"Found {null_tenure} null tenure_months values"
    assert null_ltv == 0, f"Found {null_ltv} null lifetime_value values"

    print("✓ No null values found in critical features")


# ============================================================================
# Test 7: Class Balance
# ============================================================================

def test_class_balance(churn_data_setup):
    """
    Verify class distribution is realistic (8-15% churn rate).
    """
    cursor = churn_data_setup.cursor()

    # Get class distribution
    cursor.execute("""
        SELECT
            churned,
            COUNT(*) AS count,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS percentage
        FROM GOLD.ML_TRAINING_DATA
        GROUP BY churned
        ORDER BY churned DESC
    """)

    results = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

    # Churned class (positive class)
    churned_count, churned_pct = results.get(1, (0, 0))

    # Active class (negative class)
    active_count, active_pct = results.get(0, (0, 0))

    # Verify churn rate is realistic (8-15%)
    assert 5 <= churned_pct <= 20, \
        f"Churn rate {churned_pct:.2f}% outside realistic range (5-20%)"

    # Verify we have both classes
    assert churned_count > 0, "No churned customers in training data"
    assert active_count > 0, "No active customers in training data"

    print(f"✓ Class balance: {churned_count:,} churned ({churned_pct:.2f}%), {active_count:,} active ({active_pct:.2f}%)")


# ============================================================================
# Test 8: Feature Ranges
# ============================================================================

def test_feature_ranges(churn_data_setup):
    """
    Verify feature values are within realistic ranges.
    """
    cursor = churn_data_setup.cursor()

    # Check demographic features
    cursor.execute("""
        SELECT
            MIN(age) AS min_age,
            MAX(age) AS max_age,
            MIN(credit_limit) AS min_credit,
            MAX(credit_limit) AS max_credit
        FROM GOLD.ML_TRAINING_DATA
    """)

    row = cursor.fetchone()
    min_age, max_age, min_credit, max_credit = row

    # Validate demographics
    assert 18 <= min_age <= 100, f"min_age {min_age} outside valid range"
    assert 18 <= max_age <= 100, f"max_age {max_age} outside valid range"
    assert 5000 <= min_credit <= 50000, f"min_credit_limit {min_credit} outside valid range"
    assert 5000 <= max_credit <= 50000, f"max_credit_limit {max_credit} outside valid range"

    # Check spending features
    cursor.execute("""
        SELECT
            MIN(avg_monthly_spend) AS min_monthly,
            MAX(avg_monthly_spend) AS max_monthly,
            MIN(credit_utilization_pct) AS min_util,
            MAX(credit_utilization_pct) AS max_util
        FROM GOLD.ML_TRAINING_DATA
    """)

    row = cursor.fetchone()
    min_monthly, max_monthly, min_util, max_util = row

    # Validate spending
    assert min_monthly >= 0, f"min_avg_monthly_spend {min_monthly} is negative"
    assert min_util >= 0, f"min_credit_utilization_pct {min_util} is negative"
    assert max_util <= 200, f"max_credit_utilization_pct {max_util} unrealistically high"

    print(f"✓ Feature ranges valid (age: {min_age}-{max_age}, credit: ${min_credit:,}-${max_credit:,})")


# ============================================================================
# Test 9: Sufficient Training Examples
# ============================================================================

def test_sufficient_training_examples(churn_data_setup):
    """
    Verify we have sufficient training examples (≥1000 minimum).
    """
    cursor = churn_data_setup.cursor()

    # Get total count
    cursor.execute("""
        SELECT COUNT(*) AS total_count
        FROM GOLD.ML_TRAINING_DATA
    """)

    total_count = cursor.fetchone()[0]

    # Minimum threshold
    assert total_count >= 1000, \
        f"Insufficient training examples: {total_count:,} (minimum 1,000)"

    # Recommended threshold
    if total_count >= 40000:
        status = "✓ EXCELLENT"
    elif total_count >= 10000:
        status = "✓ GOOD"
    else:
        status = "⚠ ACCEPTABLE"

    print(f"{status}: {total_count:,} training examples")


# ============================================================================
# Test 10: Sufficient Examples Per Class
# ============================================================================

def test_sufficient_examples_per_class(churn_data_setup):
    """
    Verify both classes have sufficient examples (≥100 minimum per class).
    """
    cursor = churn_data_setup.cursor()

    # Get counts per class
    cursor.execute("""
        SELECT
            SUM(CASE WHEN churned = 1 THEN 1 ELSE 0 END) AS churned_count,
            SUM(CASE WHEN churned = 0 THEN 1 ELSE 0 END) AS active_count
        FROM GOLD.ML_TRAINING_DATA
    """)

    row = cursor.fetchone()
    churned_count, active_count = row

    # Minimum per class: 100
    assert churned_count >= 100, \
        f"Insufficient churned examples: {churned_count:,} (minimum 100)"

    assert active_count >= 100, \
        f"Insufficient active examples: {active_count:,} (minimum 100)"

    # Recommended per class: 1000
    churned_status = "✓" if churned_count >= 1000 else "⚠"
    active_status = "✓" if active_count >= 1000 else "⚠"

    print(f"{churned_status} Churned examples: {churned_count:,}")
    print(f"{active_status} Active examples: {active_count:,}")


# ============================================================================
# Summary
# ============================================================================

def test_summary(churn_data_setup):
    """
    Print summary of training data.
    """
    cursor = churn_data_setup.cursor()

    # Get summary statistics
    cursor.execute("""
        SELECT
            COUNT(*) AS total_examples,
            SUM(churned) AS churned_count,
            ROUND(AVG(churned) * 100, 2) AS churn_rate_pct,
            ROUND(AVG(age), 2) AS avg_age,
            ROUND(AVG(avg_monthly_spend), 2) AS avg_monthly_spend,
            ROUND(AVG(credit_utilization_pct), 2) AS avg_credit_util,
            ROUND(AVG(tenure_months), 2) AS avg_tenure
        FROM GOLD.ML_TRAINING_DATA
    """)

    row = cursor.fetchone()

    print("\n" + "="*80)
    print("ML TRAINING DATA SUMMARY")
    print("="*80)
    print(f"Total examples:        {row[0]:>12,}")
    print(f"Churned examples:      {row[1]:>12,} ({row[2]:>5.2f}%)")
    print(f"Active examples:       {row[0] - row[1]:>12,} ({100 - row[2]:>5.2f}%)")
    print(f"Average age:           {row[3]:>12.2f}")
    print(f"Average monthly spend: ${row[4]:>11,.2f}")
    print(f"Average credit util:   {row[5]:>12.2f}%")
    print(f"Average tenure:        {row[6]:>12.2f} months")
    print("="*80)
