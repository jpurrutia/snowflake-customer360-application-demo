# Customer Data Generator

Synthetic customer data generator for credit card portfolio analytics.

## Overview

This module generates realistic customer data for a credit card portfolio with 5 distinct customer segments. It uses the Faker library to create demographically diverse customers with realistic spending patterns and characteristics.

## Customer Segments

The generator creates customers across 5 segments with different characteristics:

### 1. High-Value Travelers (15%)
- **Characteristics**: Premium customers, frequent travelers
- **Monthly Spend Range**: $2,000 - $8,000
- **Card Type**: 30% Premium, 70% Standard
- **Typical Use Cases**: Business travel, international purchases, high-value transactions

### 2. Stable Mid-Spenders (40%)
- **Characteristics**: Consistent spending, low churn risk
- **Monthly Spend Range**: $800 - $2,500
- **Card Type**: Standard
- **Typical Use Cases**: Regular household expenses, predictable patterns

### 3. Budget-Conscious (25%)
- **Characteristics**: Low spend, high frequency
- **Monthly Spend Range**: $200 - $800
- **Card Type**: Standard
- **Typical Use Cases**: Small daily purchases, grocery shopping

### 4. Declining (10%)
- **Characteristics**: At-risk, decreasing engagement
- **Monthly Spend Range**: $500 - $2,000 (historical, before decline)
- **Decline Types**:
  - **Gradual** (70%): Slow decrease in spending over time
  - **Sudden** (30%): Abrupt drop in activity
- **Card Type**: Standard
- **Risk**: High churn probability

### 5. New & Growing (10%)
- **Characteristics**: Recent customers, increasing spend
- **Monthly Spend Range**: $300 - $1,200
- **Card Type**: Standard
- **Typical Use Cases**: Building credit history, increasing engagement

## Generated Data Schema

Each customer record includes:

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `customer_id` | string | Unique identifier | CUST00000001 |
| `first_name` | string | Customer first name | John |
| `last_name` | string | Customer last name | Smith |
| `email` | string | Email address | john.smith@example.com |
| `age` | int | Customer age (22-75) | 45 |
| `state` | string | US state abbreviation | CA |
| `city` | string | City name | Los Angeles |
| `employment_status` | string | Employment status | Employed |
| `card_type` | string | Standard or Premium | Standard |
| `credit_limit` | int | Credit limit ($5K-$50K) | 25000 |
| `account_open_date` | date | Account opening date | 2021-03-15 |
| `customer_segment` | string | Segment category | Stable Mid-Spenders |
| `decline_type` | string | Decline pattern (Declining only) | gradual |

## Usage

### Command Line Interface

Generate 50,000 customers (default):
```bash
python -m data_generation generate-customers
```

Generate custom number of customers:
```bash
python -m data_generation generate-customers --count 10000
```

Specify output file:
```bash
python -m data_generation generate-customers --output data/my_customers.csv
```

Use custom random seed for reproducibility:
```bash
python -m data_generation generate-customers --seed 123
```

Full example:
```bash
python -m data_generation generate-customers \
  --count 50000 \
  --output data/customers.csv \
  --seed 42
```

### Python API

```python
from data_generation.customer_generator import generate_customers, validate_customer_data, save_to_csv

# Generate customers
df = generate_customers(n=50000, seed=42)

# Validate data quality
validation_result = validate_customer_data(df)

if validation_result['is_valid']:
    print("Validation passed!")
    print(f"Statistics: {validation_result['statistics']}")

    # Save to CSV
    save_to_csv(df, 'customers.csv')
else:
    print("Validation failed:")
    for error in validation_result['errors']:
        print(f"  - {error}")
```

## Data Quality Validation

The generator includes comprehensive validation:

### Required Field Checks
- All required fields (customer_id, email, state, card_type, credit_limit) have no nulls

### Customer ID Validation
- Format: `CUST########` (8 digits)
- Sequential: CUST00000001 through CUST0005000
- Uniqueness: No duplicates

### Segment Distribution
- Matches target percentages within 5% tolerance
- Warns if deviations exceed threshold

### Email Validation
- Valid format: `name@domain.com`
- Contains `@` and `.`

### Credit Limit Validation
- Range: $5,000 - $50,000
- Multiples of $1,000

### Decline Type Validation
- Only set for Declining segment customers
- Values: `gradual` or `sudden`
- Null for all other segments

## Output

The CLI provides detailed statistics:

```
📊 Statistics:
  Total customers: 50000
  Unique IDs: 50000
  Credit limit range: $5,000 - $50,000
  Average credit limit: $27,589.76

  Segment Distribution:
    Stable Mid-Spenders: 40.0%
    Budget-Conscious: 25.0%
    High-Value Travelers: 15.0%
    Declining: 10.0%
    New & Growing: 10.0%

  Card Type Distribution:
    Standard: 47727 (95.5%)
    Premium: 2273 (4.5%)
```

## Configuration

All configuration parameters are defined in `data_generation/config.py`:

```python
# Segment percentages
SEGMENTS = {
    "High-Value Travelers": 0.15,
    "Stable Mid-Spenders": 0.40,
    "Budget-Conscious": 0.25,
    "Declining": 0.10,
    "New & Growing": 0.10,
}

# Credit limits
MIN_CREDIT_LIMIT = 5000
MAX_CREDIT_LIMIT = 50000
CREDIT_LIMIT_STEP = 1000

# Age range
MIN_AGE = 22
MAX_AGE = 75
```

## Testing

Run unit tests:
```bash
pytest tests/unit/test_customer_generator.py -v
```

Run integration tests:
```bash
pytest tests/integration/test_customer_generation_e2e.py -v
```

Run all tests:
```bash
pytest tests/ -v
```

## Reproducibility

The generator uses a random seed for reproducibility:

```bash
# Same seed produces identical data
python -m data_generation generate-customers --count 1000 --seed 42
python -m data_generation generate-customers --count 1000 --seed 42
# Both commands produce identical output
```

## Files Generated

```
data/
├── customers.csv          # Full 50K customer dataset
└── test_customers.csv     # Test dataset (1K customers)
```

## Next Steps

After generating customer data:

1. Upload to S3: Use `scripts/upload_to_s3.py` (Iteration 2.2)
2. Load to Snowflake: Execute `sql/bronze/load_customers.sql` (Iteration 2.3)
3. Generate transactions: Use Snowflake GENERATOR() (Iteration 2.4)

## Troubleshooting

### Memory Issues with Large Datasets
If generating very large datasets (>100K), consider:
- Generating in batches
- Increasing available memory
- Using a machine with more RAM

### Validation Warnings
Segment distribution warnings are normal for small datasets (<1000 customers). With larger datasets, distribution should be within 5% of target.

### Different Results with Same Seed
Ensure you're using the same version of:
- Python
- pandas
- faker
- numpy

Version differences can affect random number generation.
