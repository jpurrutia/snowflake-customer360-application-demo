-- ============================================================================
-- Customer Generator Stored Procedure
-- ============================================================================
-- Purpose: Generate synthetic customer data and write to internal stage
-- Output: Parquet file in @customer_data_stage for dbt Bronze ingestion
-- Based on: data_generation/customer_generator.py
-- Usage: CALL BRONZE.GENERATE_CUSTOMERS(50000, 42);
-- ============================================================================

USE ROLE DATA_ENGINEER;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA BRONZE;

-- ============================================================================
-- Create Stored Procedure
-- ============================================================================

CREATE OR REPLACE PROCEDURE GENERATE_CUSTOMERS(
    NUM_CUSTOMERS INT,
    SEED INT DEFAULT 42
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'faker', 'numpy', 'pandas')
HANDLER = 'main'
COMMENT = 'Generate synthetic customer data for Customer 360 Analytics Platform'
AS
$$
def main(session, num_customers, seed):
    """
    Generate synthetic customer data and write to internal stage as Parquet.

    Args:
        session: Snowpark session object
        num_customers: Number of customers to generate
        seed: Random seed for reproducibility

    Returns:
        String summary with stage location and generation statistics
    """
    from faker import Faker
    import numpy as np
    from datetime import datetime, timedelta
    import pandas as pd

    # Configuration constants (from config.py)
    SEGMENTS = {
        "High-Value Travelers": 0.15,
        "Stable Mid-Spenders": 0.40,
        "Budget-Conscious": 0.25,
        "Declining": 0.10,
        "New & Growing": 0.10,
    }

    EMPLOYMENT_STATUSES = ["Employed", "Self-Employed", "Retired", "Unemployed"]

    US_STATES = [
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
        "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
        "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
        "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
        "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC"
    ]

    MIN_CREDIT_LIMIT = 5000
    MAX_CREDIT_LIMIT = 50000
    CREDIT_LIMIT_STEP = 1000
    MIN_AGE = 22
    MAX_AGE = 75
    ACCOUNT_OPEN_MIN_YEARS_AGO = 5
    ACCOUNT_OPEN_MAX_YEARS_AGO = 2
    GRADUAL_DECLINE_PERCENTAGE = 0.70

    # Initialize random generators
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
            count = num_customers - total_assigned
        else:
            count = int(num_customers * percentage)
            total_assigned += count
        segment_counts[segment_name] = count

    # Create customer list for each segment
    segment_assignments = []
    for segment_name, count in segment_counts.items():
        segment_assignments.extend([segment_name] * count)

    # Shuffle to avoid clustering by segment
    np.random.shuffle(segment_assignments)

    # Generate customers
    for i in range(num_customers):
        customer_id = f"CUST{str(i + 1).zfill(8)}"
        segment = segment_assignments[i]

        # Generate basic demographics
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"{first_name.lower()}.{last_name.lower()}@{fake.free_email_domain()}"
        age = int(np.random.randint(MIN_AGE, MAX_AGE + 1))
        state = str(np.random.choice(US_STATES))
        city = fake.city()
        employment_status = str(np.random.choice(EMPLOYMENT_STATUSES))

        # Assign card type (30% of High-Value Travelers get Premium, rest get Standard)
        if segment == "High-Value Travelers" and np.random.random() < 0.30:
            card_type = "Premium"
        else:
            card_type = "Standard"

        # Generate credit limit (multiples of $1000)
        num_steps = (MAX_CREDIT_LIMIT - MIN_CREDIT_LIMIT) // CREDIT_LIMIT_STEP + 1
        credit_limit = int(MIN_CREDIT_LIMIT + (np.random.randint(0, num_steps) * CREDIT_LIMIT_STEP))

        # Generate account open date (2-5 years ago)
        days_ago_min = ACCOUNT_OPEN_MAX_YEARS_AGO * 365
        days_ago_max = ACCOUNT_OPEN_MIN_YEARS_AGO * 365
        days_ago = int(np.random.randint(days_ago_min, days_ago_max + 1))
        account_open_date = (datetime.now() - timedelta(days=days_ago)).date()

        # Assign decline type (only for Declining segment)
        if segment == "Declining":
            decline_type = "gradual" if np.random.random() < GRADUAL_DECLINE_PERCENTAGE else "sudden"
        else:
            decline_type = None

        customers.append({
            "CUSTOMER_ID": customer_id,
            "FIRST_NAME": first_name,
            "LAST_NAME": last_name,
            "EMAIL": email,
            "AGE": age,
            "STATE": state,
            "CITY": city,
            "EMPLOYMENT_STATUS": employment_status,
            "CARD_TYPE": card_type,
            "CREDIT_LIMIT": credit_limit,
            "ACCOUNT_OPEN_DATE": account_open_date,
            "CUSTOMER_SEGMENT": segment,
            "DECLINE_TYPE": decline_type,
        })

    # Create DataFrame
    df = pd.DataFrame(customers)

    # Convert to Snowpark DataFrame
    snowpark_df = session.create_dataframe(df)

    # Generate timestamp for file versioning
    from datetime import datetime
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    stage_path = f"@customer_data_stage/customers_seed{seed}_{timestamp}.parquet"

    # Write to internal stage as Parquet
    snowpark_df.write.mode("overwrite").parquet(stage_path)

    # Gather statistics for return message
    segment_distribution = df["CUSTOMER_SEGMENT"].value_counts().to_dict()
    card_type_distribution = df["CARD_TYPE"].value_counts().to_dict()
    avg_credit_limit = df["CREDIT_LIMIT"].mean()

    # Build summary message
    summary = f"✓ Successfully generated {num_customers} customers to stage\n\n"
    summary += f"Stage Location: {stage_path}\n\n"
    summary += "Segment Distribution:\n"
    for segment, count in sorted(segment_distribution.items()):
        pct = count / num_customers * 100
        summary += f"  {segment}: {count} ({pct:.1f}%)\n"

    summary += f"\nCard Types:\n"
    for card_type, count in sorted(card_type_distribution.items()):
        pct = count / num_customers * 100
        summary += f"  {card_type}: {count} ({pct:.1f}%)\n"

    summary += f"\nAverage Credit Limit: ${avg_credit_limit:,.2f}\n"
    summary += f"Random Seed: {seed}\n"
    summary += f"\nNext Step: Run 'dbt run --select bronze.raw_customers' to load into Bronze layer\n"

    return summary
$$;

-- ============================================================================
-- Grant Permissions
-- ============================================================================

GRANT USAGE ON PROCEDURE GENERATE_CUSTOMERS(INT, INT) TO ROLE DATA_ENGINEER;
GRANT USAGE ON PROCEDURE GENERATE_CUSTOMERS(INT, INT) TO ROLE ACCOUNTADMIN;

-- ============================================================================
-- Usage Examples
-- ============================================================================

-- Step 1: Generate 50,000 customers with default seed (42) to stage
-- CALL BRONZE.GENERATE_CUSTOMERS(50000, 42);

-- Step 2: Load from stage to Bronze table using dbt
-- dbt run --select bronze.raw_customers

-- Generate 1,000 customers for testing with different seed
-- CALL BRONZE.GENERATE_CUSTOMERS(1000, 123);

-- ============================================================================
-- Verification Queries
-- ============================================================================

-- List files in stage
-- LIST @customer_data_stage;

-- Preview data in stage
-- SELECT * FROM @customer_data_stage (FILE_FORMAT => 'PARQUET') LIMIT 10;

-- Check customer count after dbt load
-- SELECT COUNT(*) FROM BRONZE.RAW_CUSTOMERS;

-- Check segment distribution
-- SELECT
--     CUSTOMER_SEGMENT,
--     COUNT(*) AS customer_count,
--     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
-- FROM BRONZE.RAW_CUSTOMERS
-- GROUP BY CUSTOMER_SEGMENT
-- ORDER BY customer_count DESC;

-- Check card type distribution
-- SELECT
--     CARD_TYPE,
--     COUNT(*) AS customer_count,
--     ROUND(AVG(CREDIT_LIMIT), 2) AS avg_credit_limit
-- FROM BRONZE.RAW_CUSTOMERS
-- GROUP BY CARD_TYPE;

-- ============================================================================
-- Display confirmation
-- ============================================================================

SELECT '✓ Stored procedure GENERATE_CUSTOMERS created successfully' AS status;
SELECT 'Usage: CALL BRONZE.GENERATE_CUSTOMERS(50000, 42);' AS example;
