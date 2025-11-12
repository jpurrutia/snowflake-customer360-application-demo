## Snowflake Transaction Data Generation

Documentation for generating synthetic transaction data at scale using Snowflake's GENERATOR() function.

---

## Overview

This directory contains SQL scripts for generating 13.5 million synthetic transactions directly in Snowflake using the GENERATOR() table function. This approach is significantly faster than Python-based generation for large datasets.

---

## Why Snowflake for Transaction Generation?

### Performance Benefits

1. **Scale**: Generate millions of rows in minutes instead of hours
2. **Parallel Processing**: Snowflake automatically parallelizes across compute nodes
3. **No Data Movement**: Data generated and stored directly in Snowflake
4. **Memory Efficient**: No need to load 13.5M rows into Python memory

### Performance Comparison

| Method | 13.5M Rows | Memory | Complexity |
|--------|------------|--------|------------|
| Python (Faker) | ~2-3 hours | High (GB) | High |
| Snowflake (GENERATOR) | ~5-10 minutes | Low (MB) | Low |

---

## Transaction Volume by Segment

### Monthly Transaction Frequency

| Segment | Monthly Txns | Annual Txns | Total (18mo) |
|---------|--------------|-------------|--------------|
| High-Value Travelers | 40-80 | 480-960 | 720-1,440 |
| Stable Mid-Spenders | 20-40 | 240-480 | 360-720 |
| Budget-Conscious | 15-30 | 180-360 | 270-540 |
| Declining | 20-40 | 240-480 | 360-720 |
| New & Growing | 25-50 | 300-600 | 450-900 |

**Total Expected**: ~13.5M transactions (50K customers × ~750 avg txns)

---

## Spending Patterns by Segment

### High-Value Travelers (15% of customers)
- **Amount Range**: $50 - $500 per transaction
- **Frequency**: 40-80 transactions/month
- **Merchant Categories**: Travel, Dining, Hotels, Airlines
- **Characteristics**: High spend, consistent patterns

### Stable Mid-Spenders (40% of customers)
- **Amount Range**: $30 - $150 per transaction
- **Frequency**: 20-40 transactions/month
- **Merchant Categories**: Retail, Dining, Entertainment, etc.
- **Characteristics**: Consistent spend, low volatility

### Budget-Conscious (25% of customers)
- **Amount Range**: $10 - $80 per transaction
- **Frequency**: 15-30 transactions/month
- **Merchant Categories**: Grocery, Gas, Utilities
- **Characteristics**: Small purchases, high frequency

### Declining (10% of customers)
- **Amount Range**: $30 - $150 (decreasing over time)
- **Frequency**: 20-40 transactions/month (decreasing)
- **Decline Patterns**:
  - **Gradual** (70%): Linear 10% reduction per month after month 12
  - **Sudden** (30%): 60% drop after month 16
- **Characteristics**: Clear downward trajectory

### New & Growing (10% of customers)
- **Amount Range**: $20 - $100 (increasing 5% per month)
- **Frequency**: 25-50 transactions/month
- **Characteristics**: Growth trajectory, building relationship

---

## Decline Patterns Explained

### Gradual Decline (70% of Declining segment)

```
Month 1-12: $100 avg (stable)
Month 13:   $90  (-10%)
Month 14:   $80  (-10%)
Month 15:   $70  (-10%)
Month 16:   $60  (-10%)
Month 17:   $50  (-10%)
Month 18:   $40  (-10%)
```

**Formula**: `amount * GREATEST(0.4, 1 - ((month_num - 12) * 0.1))`

**Minimum**: Bottoms out at 40% of original spend

### Sudden Decline (30% of Declining segment)

```
Month 1-15: $100 avg (stable)
Month 16:   $40  (-60%)
Month 17:   $40  (stable)
Month 18:   $40  (stable)
```

**Formula**: `amount * IFF(month_num < 16, 1.0, 0.4)`

**Trigger**: Sharp 60% drop at month 16

---

## Generation Process

### Part A: Date Spine

Creates 540 daily timestamps (18 months × 30 days):

```sql
CREATE TEMP TABLE date_spine AS
SELECT
    DATEADD('day', SEQ4(), DATEADD('month', -18, CURRENT_DATE())) AS transaction_date,
    DATEDIFF('month', ...) AS month_num
FROM TABLE(GENERATOR(ROWCOUNT => 540));
```

**Output**: 540 rows (one per day)

### Part B: Customer Monthly Volume

Determines how many transactions each customer makes per month:

```sql
CREATE TEMP TABLE customer_monthly_volume AS
SELECT
    customer_id,
    customer_segment,
    transaction_date,
    month_num,
    CASE customer_segment
        WHEN 'High-Value Travelers' THEN UNIFORM(40, 80, RANDOM())
        -- ... other segments
    END AS monthly_transactions
FROM BRONZE_CUSTOMERS
CROSS JOIN (first day of each month);
```

**Output**: 50,000 customers × 18 months = 900,000 rows

### Part C: Transaction Expansion

Expands monthly volumes to individual transactions:

```sql
CREATE TEMP TABLE transactions_expanded AS
SELECT ...
FROM customer_monthly_volume
CROSS JOIN TABLE(GENERATOR(ROWCOUNT => 100))
WHERE seq < monthly_transactions;
```

**Output**: ~13.5M rows (varies based on random monthly volumes)

### Part D: Transaction Details

Adds transaction amounts, merchant info, and segment-specific patterns:

```sql
CREATE TEMP TABLE transactions_with_details AS
SELECT
    'TXN' || LPAD(ROW_NUMBER() OVER (...), 11, '0') AS transaction_id,
    customer_id,
    transaction_date,
    -- Segment-specific amount logic
    CASE customer_segment
        WHEN 'Declining' THEN
            CASE decline_type
                WHEN 'gradual' THEN amount * (1 - (month * 0.1))
                WHEN 'sudden' THEN amount * IFF(month < 16, 1.0, 0.4)
            END
        -- ... other segments
    END AS transaction_amount,
    -- ... merchant, category, channel
FROM transactions_expanded;
```

**Output**: ~13.5M transactions with all details

### Part E: Export to S3

Exports compressed CSV files to S3 for bulk load:

```sql
COPY INTO @transaction_stage_historical/transactions_historical.csv
FROM transactions_with_details
FILE_FORMAT = (TYPE='CSV' COMPRESSION='GZIP')
HEADER = TRUE
MAX_FILE_SIZE = 104857600;  -- 100MB chunks
```

**Output**: Multiple GZIP compressed CSV files in S3

---

## Execution Instructions

### Prerequisites

1. Customer data loaded in `BRONZE.BRONZE_CUSTOMERS` (50,000 rows)
2. Storage integration configured
3. External stage `@transaction_stage_historical` created
4. Warehouse sized appropriately (SMALL or larger recommended)

### Option 1: Manual Execution (Snowflake UI)

```sql
-- Copy and paste contents of generate_transactions.sql into Snowflake worksheet
-- Execute the script
-- Monitor progress in worksheet
```

### Option 2: Using Script (SnowSQL)

```bash
cd snowflake/data_generation
./run_transaction_generation.sh
```

### Option 3: SnowSQL Direct

```bash
snowsql -f snowflake/data_generation/generate_transactions.sql
```

---

## Performance Optimization

### Warehouse Sizing

| Warehouse | Time | Cost | Recommended For |
|-----------|------|------|-----------------|
| XSMALL | ~30 min | Low | Testing only |
| SMALL | ~10-15 min | Medium | Development |
| MEDIUM | ~5-8 min | Higher | Production |
| LARGE | ~3-5 min | Highest | Large-scale prod |

**Recommendation**: Use **SMALL** or **MEDIUM** for this workload

### Tips for Performance

1. **Use Larger Warehouse**: Generation is compute-intensive
2. **Avoid Peak Hours**: Run during off-peak for faster credits
3. **Monitor Progress**: Check `QUERY_HISTORY` for bottlenecks
4. **Temp Tables**: Used automatically for intermediate results

---

## Expected Output

### File Structure in S3

```
s3://bucket/transactions/historical/
├── transactions_historical_0_0_0.csv.gz (100MB)
├── transactions_historical_0_0_1.csv.gz (100MB)
├── transactions_historical_0_0_2.csv.gz (100MB)
└── ... (total ~1-2GB compressed)
```

### Row Counts by Segment

| Segment | Customers | Avg Txns | Total Txns (approx) |
|---------|-----------|----------|---------------------|
| High-Value Travelers | 7,500 | 1,080 | 8.1M |
| Stable Mid-Spenders | 20,000 | 540 | 10.8M |
| Budget-Conscious | 12,500 | 405 | 5.1M |
| Declining | 5,000 | 540 | 2.7M |
| New & Growing | 5,000 | 675 | 3.4M |
| **Total** | **50,000** | **~750** | **~13.5M** |

*(Actual counts vary due to randomization)*

---

## Validation Queries

### Check Total Count

```sql
SELECT COUNT(*) FROM transactions_with_details;
-- Expected: 10M - 15M (target: 13.5M)
```

### Verify All Customers Have Transactions

```sql
SELECT COUNT(DISTINCT customer_id) FROM transactions_with_details;
-- Expected: 50,000
```

### Check Declining Pattern

```sql
SELECT
    MONTH(transaction_date) AS month,
    AVG(transaction_amount) AS avg_amount
FROM transactions_with_details
WHERE customer_segment = 'Declining'
GROUP BY month
ORDER BY month;
-- Expected: Decreasing trend over time
```

### Verify Date Range

```sql
SELECT
    MIN(transaction_date) AS earliest,
    MAX(transaction_date) AS latest,
    DATEDIFF('month', MIN(transaction_date), MAX(transaction_date)) AS months
FROM transactions_with_details;
-- Expected: ~18 months
```

---

## Troubleshooting

### Issue: "Out of Memory"

**Cause**: Warehouse too small for 13.5M rows

**Solution**:
```sql
USE WAREHOUSE LARGE_WH;  -- Use larger warehouse
```

### Issue: "Generation Too Slow"

**Possible Causes**:
- Warehouse too small
- Peak usage time
- Complex CASE statements

**Solutions**:
1. Resize warehouse to MEDIUM or LARGE
2. Run during off-peak hours
3. Simplify logic if needed (remove some randomization)

### Issue: "Files Not Appearing in S3"

**Cause**: Stage not configured correctly

**Solution**:
```sql
-- Verify stage exists
SHOW STAGES LIKE 'transaction_stage_historical';

-- Test stage access
LIST @transaction_stage_historical;

-- Check storage integration
DESC STORAGE INTEGRATION customer360_s3_integration;
```

### Issue: "Random() Produces Same Values"

**Note**: Snowflake RANDOM() is deterministic within a query

**This is expected behavior** - multiple runs will produce different results, but within a single query execution, patterns are reproducible.

---

## Testing the Data

### Segment Distribution

```sql
SELECT
    customer_segment,
    COUNT(*) AS txn_count,
    ROUND(AVG(transaction_amount), 2) AS avg_amount
FROM transactions_with_details
GROUP BY customer_segment
ORDER BY txn_count DESC;
```

**Expected**: High-Value Travelers have highest avg_amount

### Decline Validation

```sql
WITH monthly_spend AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', transaction_date) AS month,
        SUM(transaction_amount) AS monthly_total
    FROM transactions_with_details
    WHERE customer_segment = 'Declining'
    GROUP BY customer_id, month
)
SELECT
    customer_id,
    MAX(monthly_total) AS max_month,
    MIN(monthly_total) AS min_month,
    ROUND((MAX(monthly_total) - MIN(monthly_total)) / MAX(monthly_total) * 100, 2) AS decline_pct
FROM monthly_spend
GROUP BY customer_id
HAVING decline_pct > 30;  -- Should see significant declines
```

---

## Next Steps

After successful transaction generation:

1. ✅ Verify files in S3: `LIST @transaction_stage_historical`
2. ✅ Check file sizes (total ~1-2GB compressed)
3. ✅ Validate transaction counts (~13.5M)
4. ➡️ **Iteration 2.5**: Load transactions into Bronze layer
5. ➡️ **Phase 3**: Build dbt transformations

---

## Additional Resources

- [Snowflake GENERATOR() Function](https://docs.snowflake.com/en/sql-reference/functions/generator.html)
- [Snowflake RANDOM() Function](https://docs.snowflake.com/en/sql-reference/functions/random.html)
- [COPY INTO S3](https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html)
- [Temporary Tables](https://docs.snowflake.com/en/user-guide/tables-temp-transient.html)
