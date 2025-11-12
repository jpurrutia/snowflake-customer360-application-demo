# Star Schema Design - Customer 360 Transactions

**Version**: 1.0
**Date**: 2025-11-11
**Layer**: Gold (Dimensional Model)

---

## Overview

This document describes the **star schema dimensional model** for customer transaction analysis. The model follows Kimball methodology with a central fact table surrounded by dimension tables.

### Purpose

Enable efficient analytical queries for:
- Customer spending patterns by segment
- Transaction trends over time
- Merchant category analysis
- Channel performance
- Customer behavior tracking with historical context (SCD Type 2)

---

## Star Schema ERD

```
                    ┌─────────────────────────┐
                    │    dim_customer         │
                    ├─────────────────────────┤
                    │ PK customer_key         │ ← Surrogate Key (SCD Type 2)
                    │    customer_id          │   Natural Key
                    │    first_name           │
                    │    last_name            │
                    │    email                │
                    │    age                  │
                    │    state                │
                    │    city                 │
                    │    card_type            │ ← SCD Type 2 tracked
                    │    credit_limit         │ ← SCD Type 2 tracked
                    │    customer_segment     │
                    │    employment_status    │
                    │    account_open_date    │
                    │    valid_from           │ ← SCD Type 2 dates
                    │    valid_to             │ ← SCD Type 2 dates
                    │    is_current           │ ← SCD Type 2 flag
                    └────────────┬────────────┘
                                 │
                                 │ 1:N
                                 │
        ┌────────────────────────┼────────────────────────┐
        │                        │                        │
        │                        │                        │
┌───────▼────────┐      ┌────────▼──────────────┐  ┌─────▼──────────────┐
│   dim_date     │      │  fct_transactions     │  │ dim_merchant_cat   │
├────────────────┤      ├───────────────────────┤  ├────────────────────┤
│ PK date_key    │ N:1  │ PK transaction_key    │  │ PK category_key    │
│    date_day    │◄─────┤    transaction_id     │  │    category_name   │
│    year        │      │ FK customer_key       │  │    category_group  │
│    quarter     │      │ FK date_key           │  │    spending_type   │
│    month       │      │ FK merchant_cat_key   ├──►│    discr_flag      │
│    month_name  │      │    transaction_date   │  └────────────────────┘
│    week        │      │    transaction_amount │           1:N
│    day_name    │      │    merchant_name      │
│    is_weekend  │      │    channel            │
│    is_weekday  │      │    status             │
└────────────────┘      │    ingestion_ts       │
       1:N              │    source_file        │
                        └───────────────────────┘

Legend:
PK = Primary Key (unique identifier)
FK = Foreign Key (references dimension)
SCD Type 2 = Slowly Changing Dimension with history tracking
```

---

## Table Details

### Fact Table: `fct_transactions`

**Grain**: One row per transaction
**Row Count**: ~13.5 million
**Materialization**: Incremental (for performance)
**Clustering**: By `transaction_date` (time-series optimization)

**Keys**:
- `transaction_key`: Surrogate key (PK)
- `transaction_id`: Natural key from source
- `customer_key`: FK to `dim_customer` (surrogate key)
- `date_key`: FK to `dim_date` (YYYYMMDD format)
- `merchant_category_key`: FK to `dim_merchant_category`

**Measures** (Additive):
- `transaction_amount`: Dollar amount (SUM, AVG, MIN, MAX)
- Transaction count: `COUNT(*)`

**Degenerate Dimensions** (stored in fact):
- `merchant_name`: Merchant identifier
- `channel`: Online, In-Store, Mobile
- `status`: approved, declined

**Metadata**:
- `ingestion_timestamp`: Bronze load timestamp
- `source_file`: S3 file path for lineage

---

### Dimension: `dim_customer`

**Grain**: One row per customer **version** (SCD Type 2)
**Row Count**: ~50,000 initial + growth from changes
**Materialization**: Table (incremental with change detection)

**SCD Type 2 Strategy**:
- **Tracked Attributes** (history maintained):
  - `card_type`: Standard → Premium upgrades
  - `credit_limit`: Credit limit changes
- **Type 1 Attributes** (overwrite only):
  - Demographics: `first_name`, `last_name`, `email`, `age`, `state`, `city`
  - Account: `employment_status`, `account_open_date`

**Keys**:
- `customer_key`: Surrogate key (PK, unique per version)
- `customer_id`: Natural key (same across versions)

**SCD Type 2 Columns**:
- `valid_from`: Version effective start date
- `valid_to`: Version expiration date (NULL = current)
- `is_current`: TRUE = current version, FALSE = historical

**Attributes**:
- `customer_segment`: High Value, Medium Value, Low Value
- `decline_type`: Low Decline, Medium Decline, High Decline

---

### Dimension: `dim_date`

**Grain**: One row per calendar day
**Row Count**: ~580 days (18 months + 30 day buffer)
**Materialization**: Table
**Coverage**: Transaction date range + buffer for future dates

**Keys**:
- `date_key`: Surrogate key in YYYYMMDD format (e.g., 20240615 = June 15, 2024)
- `date_day`: Actual DATE value

**Attributes**:
- **Year**: `year`, `quarter`, `fiscal_year`
- **Month**: `month`, `month_name`, `year_month`
- **Week**: `week_of_year`, `week_iso`
- **Day**: `day_of_month`, `day_of_week`, `day_name`, `day_of_year`
- **Flags**: `is_weekend`, `is_weekday`, `is_today`, `is_first_day_of_month`

---

### Dimension: `dim_merchant_category`

**Grain**: One row per unique merchant category
**Row Count**: ~10-15 categories
**Materialization**: Table

**Keys**:
- `category_key`: Surrogate key (auto-generated sequential)
- `category_name`: Category identifier (Travel, Dining, Grocery, etc.)

**Attributes**:
- `category_group`: High-level grouping
  - **Leisure**: Travel, Dining, Hotels, Airlines, Entertainment
  - **Necessities**: Grocery, Gas, Utilities, Healthcare
  - **Retail**: Shopping
  - **Other**: Uncategorized
- `spending_type`: Description (e.g., "High discretionary spending")
- `discretionary_flag`: Discretionary, Essential, Other

---

## Query Patterns

### Basic Star Schema Query

```sql
-- Customer segment spending by category and time
SELECT
    c.customer_segment,
    cat.category_group,
    d.year,
    d.month_name,
    COUNT(*) AS txn_count,
    SUM(f.transaction_amount) AS total_spend,
    AVG(f.transaction_amount) AS avg_txn_amount
FROM GOLD.FCT_TRANSACTIONS f
JOIN GOLD.DIM_CUSTOMER c ON f.customer_key = c.customer_key
JOIN GOLD.DIM_MERCHANT_CATEGORY cat ON f.merchant_category_key = cat.category_key
JOIN GOLD.DIM_DATE d ON f.date_key = d.date_key
WHERE c.is_current = TRUE  -- Current customer state
  AND d.year = 2024
GROUP BY 1, 2, 3, 4
ORDER BY total_spend DESC;
```

### Point-in-Time Historical Analysis (SCD Type 2)

```sql
-- Customer spending with historical card type at time of transaction
SELECT
    c.customer_id,
    c.card_type,  -- Card type at time of transaction
    c.credit_limit,
    f.transaction_date,
    f.transaction_amount
FROM GOLD.FCT_TRANSACTIONS f
JOIN GOLD.DIM_CUSTOMER c
  ON f.customer_key = c.customer_key
  -- Point-in-time join: match customer version active during transaction
  AND f.transaction_date BETWEEN c.valid_from AND COALESCE(c.valid_to, '9999-12-31')
WHERE c.customer_id = 'CUST00000001'
ORDER BY f.transaction_date;
```

### Current State Join (Simplified)

```sql
-- Most common pattern: Current customer state
SELECT
    c.customer_segment,
    SUM(f.transaction_amount) AS total_spend
FROM GOLD.FCT_TRANSACTIONS f
JOIN GOLD.DIM_CUSTOMER c
  ON f.customer_key = c.customer_key
  AND c.is_current = TRUE  -- Only current version
GROUP BY c.customer_segment;
```

### Time-Series Analysis

```sql
-- Monthly spending trend with date dimension
SELECT
    d.year_month,
    d.month_name,
    d.is_weekend,
    COUNT(*) AS txn_count,
    SUM(f.transaction_amount) AS total_spend
FROM GOLD.FCT_TRANSACTIONS f
JOIN GOLD.DIM_DATE d ON f.date_key = d.date_key
GROUP BY 1, 2, 3
ORDER BY d.year_month;
```

### Category Analysis

```sql
-- Discretionary vs Essential spending by customer segment
SELECT
    c.customer_segment,
    cat.discretionary_flag,
    cat.category_group,
    COUNT(*) AS txn_count,
    SUM(f.transaction_amount) AS total_spend,
    AVG(f.transaction_amount) AS avg_spend
FROM GOLD.FCT_TRANSACTIONS f
JOIN GOLD.DIM_CUSTOMER c ON f.customer_key = c.customer_key
JOIN GOLD.DIM_MERCHANT_CATEGORY cat ON f.merchant_category_key = cat.category_key
WHERE c.is_current = TRUE
GROUP BY 1, 2, 3
ORDER BY total_spend DESC;
```

### Channel Performance

```sql
-- Channel performance by customer segment (degenerate dimension)
SELECT
    c.customer_segment,
    f.channel,
    f.status,
    COUNT(*) AS txn_count,
    SUM(f.transaction_amount) AS total_spend,
    AVG(f.transaction_amount) AS avg_spend
FROM GOLD.FCT_TRANSACTIONS f
JOIN GOLD.DIM_CUSTOMER c ON f.customer_key = c.customer_key
WHERE c.is_current = TRUE
  AND f.status = 'approved'
GROUP BY 1, 2, 3
ORDER BY total_spend DESC;
```

### Customer Upgrade Analysis (SCD Type 2)

```sql
-- Find customers who upgraded to Premium card
SELECT
    customer_id,
    first_name,
    last_name,
    card_type,
    valid_from AS upgrade_date,
    DATEDIFF('day', account_open_date, valid_from) AS days_to_upgrade
FROM GOLD.DIM_CUSTOMER
WHERE card_type = 'Premium'
  AND valid_from > account_open_date  -- Upgraded after opening
ORDER BY valid_from DESC
LIMIT 100;
```

### Spending Before vs After Upgrade

```sql
-- Compare spending patterns before/after Premium upgrade
WITH upgrades AS (
    SELECT
        customer_id,
        valid_from AS upgrade_date
    FROM GOLD.DIM_CUSTOMER
    WHERE card_type = 'Premium'
      AND valid_from > account_open_date
)
SELECT
    u.customer_id,
    CASE
        WHEN f.transaction_date < u.upgrade_date THEN 'Before Upgrade'
        ELSE 'After Upgrade'
    END AS upgrade_period,
    COUNT(*) AS txn_count,
    SUM(f.transaction_amount) AS total_spend,
    AVG(f.transaction_amount) AS avg_spend
FROM upgrades u
JOIN GOLD.FCT_TRANSACTIONS f
  ON f.customer_key IN (
      SELECT customer_key FROM GOLD.DIM_CUSTOMER WHERE customer_id = u.customer_id
  )
GROUP BY 1, 2
ORDER BY u.customer_id, upgrade_period;
```

---

## Join Best Practices

### ✅ Recommended: Use Surrogate Keys

```sql
-- GOOD: Join on surrogate keys (fast, indexed)
FROM GOLD.FCT_TRANSACTIONS f
JOIN GOLD.DIM_CUSTOMER c ON f.customer_key = c.customer_key
JOIN GOLD.DIM_DATE d ON f.date_key = d.date_key
```

### ⚠️ Avoid: Join on Natural Keys

```sql
-- SLOW: Join on natural keys (no index, slower)
FROM GOLD.FCT_TRANSACTIONS f
JOIN GOLD.DIM_CUSTOMER c ON f.customer_id = c.customer_id  -- Don't do this
```

### ✅ Current vs Historical Joins

```sql
-- For CURRENT customer state (most common):
JOIN GOLD.DIM_CUSTOMER c
  ON f.customer_key = c.customer_key
  AND c.is_current = TRUE

-- For HISTORICAL customer state (point-in-time):
JOIN GOLD.DIM_CUSTOMER c
  ON f.customer_key = c.customer_key
  AND f.transaction_date BETWEEN c.valid_from AND COALESCE(c.valid_to, '9999-12-31')
```

### ✅ Filter Pushdown

```sql
-- GOOD: Filter dimensions before joining (reduces data volume)
WITH high_value_customers AS (
    SELECT customer_key
    FROM GOLD.DIM_CUSTOMER
    WHERE customer_segment = 'High Value'
      AND is_current = TRUE
)
SELECT ...
FROM GOLD.FCT_TRANSACTIONS f
JOIN high_value_customers c ON f.customer_key = c.customer_key;
```

---

## Performance Optimization

### Clustering

**Fact Table**: Clustered on `transaction_date`
- Optimizes time-series queries (date range filters)
- Reduces data scanning for monthly/quarterly reports

```sql
-- Clustering benefits queries like:
WHERE transaction_date BETWEEN '2024-01-01' AND '2024-12-31'
```

### Incremental Loading

**Fact Table**: Incremental materialization
- Only processes new transactions since last run
- Filter: `WHERE ingestion_timestamp > MAX(ingestion_timestamp)`

**Dimension (SCD Type 2)**: Incremental with change detection
- Detects changes in `card_type` or `credit_limit`
- Expires old versions, inserts new versions

### Warehouse Sizing

| Query Type | Recommended Warehouse | Avg Execution Time |
|------------|----------------------|-------------------|
| Simple aggregations (1 dimension) | SMALL | < 2 seconds |
| Star schema (3-4 dimensions) | SMALL | < 5 seconds |
| Complex analytics (5+ dimensions) | MEDIUM | < 10 seconds |
| Full table scans | MEDIUM/LARGE | Varies |

---

## Data Quality & Testing

### Referential Integrity Tests

Defined in `schema.yml`:

```yaml
# fct_transactions.customer_key → dim_customer.customer_key
- relationships:
    to: ref('dim_customer')
    field: customer_key
    config:
      where: "is_current = TRUE"

# fct_transactions.date_key → dim_date.date_key
- relationships:
    to: ref('dim_date')
    field: date_key

# fct_transactions.merchant_category_key → dim_merchant_category.category_key
- relationships:
    to: ref('dim_merchant_category')
    field: category_key
```

### Integration Tests

File: `tests/integration/test_fact_transaction.py`

Tests include:
1. Dimensional model builds successfully
2. Fact table row count (~13.5M)
3. All FK relationships valid
4. No orphan transactions
5. Clustering applied
6. Star schema query performance
7. Incremental loading
8. Star schema integrity

---

## Common Anti-Patterns to Avoid

### ❌ Joining Fact to Fact

```sql
-- AVOID: Joining fact tables directly
FROM GOLD.FCT_TRANSACTIONS f1
JOIN GOLD.FCT_OTHER_FACTS f2 ON f1.customer_key = f2.customer_key  -- Expensive!
```

**Solution**: Join through shared dimensions or aggregate first.

### ❌ Missing `is_current` Filter

```sql
-- WRONG: Joins to ALL customer versions (including historical)
JOIN GOLD.DIM_CUSTOMER c ON f.customer_key = c.customer_key

-- CORRECT: Filter to current version
JOIN GOLD.DIM_CUSTOMER c ON f.customer_key = c.customer_key AND c.is_current = TRUE
```

### ❌ SELECT * from Fact Table

```sql
-- AVOID: Scanning entire 13.5M row fact table
SELECT * FROM GOLD.FCT_TRANSACTIONS  -- Very slow!

-- BETTER: Always filter and aggregate
SELECT
    customer_key,
    SUM(transaction_amount) AS total_spend
FROM GOLD.FCT_TRANSACTIONS
WHERE transaction_date >= '2024-01-01'
GROUP BY customer_key;
```

---

## Maintenance

### Incremental Runs

```bash
# Daily incremental run (recommended)
dbt run --models marts.core

# Processes:
# - New transactions in staging
# - Customer dimension changes (SCD Type 2)
# - Fact table updates
```

### Full Refresh

```bash
# Full refresh (rebuild all tables from scratch)
dbt run --models marts.core --full-refresh

# Use when:
# - Schema changes
# - Historical data corrections
# - Testing/development
```

### Testing

```bash
# Run all dbt tests
dbt test --models marts.core

# Run integration tests
uv run pytest tests/integration/test_fact_transaction.py -v
```

---

## Future Enhancements

Potential additions to star schema:

1. **Additional Dimensions**:
   - `dim_merchant`: Merchant-level attributes (location, type, size)
   - `dim_product`: Product/service details if available
   - `dim_geography`: State/city hierarchy

2. **Additional Facts**:
   - `fct_customer_balance`: Daily balance snapshots (periodic snapshot fact)
   - `fct_customer_events`: Account lifecycle events (factless fact)

3. **Aggregated Facts** (OLAP cubes):
   - `fct_monthly_customer_summary`: Pre-aggregated monthly metrics
   - `fct_daily_category_summary`: Daily category totals

4. **Additional SCD Type 2 Attributes**:
   - Track `customer_segment` changes over time
   - Track `employment_status` changes

---

## References

**dbt Models**:
- `models/marts/core/fct_transactions.sql`
- `models/marts/core/dim_customer.sql`
- `models/marts/core/dim_date.sql`
- `models/marts/core/dim_merchant_category.sql`
- `models/marts/core/schema.yml`

**Documentation**:
- `docs/prompt_3.2_completion_summary.md` (SCD Type 2 design)
- `docs/prompt_3.3_completion_summary.md` (Fact table design)

**Tests**:
- `tests/integration/test_dim_customer.py`
- `tests/integration/test_fact_transaction.py`

---

**End of Star Schema Design Documentation**
