# Snowflake Data Loading

Documentation for loading data from S3 into Snowflake Bronze layer.

---

## Overview

This directory contains SQL scripts for bulk loading data from S3 external stages into Snowflake Bronze layer tables using the `COPY INTO` command.

---

## Load Process

### 1. Customer Bulk Load

**Script**: `load_customers_bulk.sql`

**Purpose**: Load customer data from S3 into `BRONZE.RAW_CUSTOMERS` table

**Prerequisites**:
- Bronze table created (`06_create_bronze_tables.sql`)
- S3 stage configured (`05_create_stages.sql`)
- Customer CSV uploaded to S3

**Process Flow**:
```
S3 Bucket (customers/customers.csv)
    ↓
External Stage (@customer_stage)
    ↓
COPY INTO Command
    ↓
BRONZE.RAW_CUSTOMERS Table
    ↓
Observability Logging
```

---

## COPY INTO Command

### Basic Syntax

```sql
COPY INTO <table_name>
FROM @<stage_name>
FILE_FORMAT = (...)
PATTERN = '...'
ON_ERROR = 'ABORT_STATEMENT' | 'CONTINUE';
```

### Key Parameters

#### FILE_FORMAT Options

| Option | Value | Purpose |
|--------|-------|---------|
| TYPE | 'CSV' | File type |
| SKIP_HEADER | 1 | Skip header row |
| FIELD_OPTIONALLY_ENCLOSED_BY | '"' | Handle quoted fields |
| TRIM_SPACE | TRUE | Remove leading/trailing spaces |
| ERROR_ON_COLUMN_COUNT_MISMATCH | FALSE | Allow flexible column counts |
| NULL_IF | ('NULL', 'null', '') | Treat these as NULL |

#### ON_ERROR Options

**ABORT_STATEMENT** (Default for Bulk Loads):
- Fails immediately on first error
- No data committed if any row fails
- Best for: Bulk historical loads where data quality is known
- Use when: Loading validated, generated data

**CONTINUE**:
- Skips rows with errors
- Loads remaining valid rows
- Best for: Incremental loads with potential bad data
- Use when: Loading external/unknown data sources

**SKIP_FILE**:
- Skips entire file if any error
- Continues to next file
- Best for: Multi-file loads

### PATTERN Matching

```sql
-- Match specific file
PATTERN = '.*customers\.csv'

-- Match all CSV files
PATTERN = '.*\.csv'

-- Match date-partitioned files
PATTERN = '.*2024-11-.*\.csv'
```

### FORCE Option

```sql
-- Default: Skip already-loaded files
FORCE = FALSE

-- Reload files (will create duplicates!)
FORCE = TRUE
```

**Warning**: `FORCE = TRUE` will load files again even if already loaded, creating duplicates.

---

## Error Handling

### Common Errors

#### Error 1: "File not found"

```
Error: No files found matching pattern
```

**Cause**: File not in S3 or pattern doesn't match

**Solution**:
```sql
-- Check what files are in stage
LIST @customer_stage;

-- Verify pattern matches
PATTERN = '.*customers\.csv'  -- Correct
PATTERN = '.*customer\.csv'   -- Wrong (missing 's')
```

#### Error 2: "Number of columns mismatch"

```
Error: Number of columns in file (13) does not match table (16)
```

**Cause**: CSV columns don't match table columns

**Solution**:
```sql
-- Use column mapping in SELECT
COPY INTO table (col1, col2, col3)
FROM (
    SELECT $1, $2, $3
    FROM @stage
)
```

#### Error 3: "Access Denied"

```
Error: Access Denied (service: Amazon S3; Status Code: 403)
```

**Cause**: Storage integration not configured correctly

**Solution**:
1. Verify storage integration exists
2. Check IAM trust policy includes external ID
3. Verify IAM role has S3 permissions

```sql
DESC STORAGE INTEGRATION customer360_s3_integration;
LIST @customer_stage;
```

#### Error 4: "Date format error"

```
Error: Date 'YYYY-MM-DD' is not recognized
```

**Cause**: Date format in CSV doesn't match Snowflake expectation

**Solution**:
```sql
-- Use explicit casting
$11::DATE AS account_open_date

-- Or specify format
TO_DATE($11, 'YYYY-MM-DD')
```

---

## Metadata Columns

Snowflake provides metadata about loaded files:

### METADATA$FILENAME

Path to source file in S3:
```
s3://bucket/customers/customers.csv
```

Usage:
```sql
SELECT METADATA$FILENAME AS source_file
FROM @customer_stage
```

### METADATA$FILE_ROW_NUMBER

Row number in source file (1-indexed):
```
1, 2, 3, ... N
```

Usage:
```sql
SELECT METADATA$FILE_ROW_NUMBER AS row_num
FROM @customer_stage
```

### Why Capture Metadata?

1. **Data Lineage**: Track which file provided each row
2. **Debugging**: Find problematic rows in source files
3. **Incremental Loads**: Identify which files have been processed
4. **Auditing**: Compliance and data governance

---

## Manual Load Instructions

### Step 1: Verify Prerequisites

```sql
USE ROLE DATA_ENGINEER;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA BRONZE;

-- Check stage is accessible
LIST @customer_stage;

-- Verify table exists
SHOW TABLES LIKE 'RAW_CUSTOMERS';
```

### Step 2: Run Load Script

```sql
-- Execute load script
-- Copy contents of load_customers_bulk.sql and run in Snowflake UI
```

### Step 3: Verify Load

```sql
-- Check row count
SELECT COUNT(*) FROM RAW_CUSTOMERS;
-- Expected: 50,000

-- Run full validation
-- Execute verify_customer_load.sql
```

---

## Load History

### View Previous Loads

```sql
-- View load history for table
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'CUSTOMER_ANALYTICS.BRONZE.RAW_CUSTOMERS',
    START_TIME => DATEADD(days, -7, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;
```

### Load Statistics

```sql
-- Files loaded
-- Rows loaded
-- Errors encountered
-- Time taken
```

---

## Observability

Every load logs metrics to `OBSERVABILITY.LAYER_RECORD_COUNTS`:

```sql
SELECT *
FROM CUSTOMER_ANALYTICS.OBSERVABILITY.LAYER_RECORD_COUNTS
WHERE table_name = 'RAW_CUSTOMERS'
ORDER BY run_timestamp DESC;
```

**Tracked Metrics**:
- run_id: Unique identifier for load
- run_timestamp: When load occurred
- record_count: Total rows loaded
- distinct_keys: Unique customer IDs
- null_key_count: Rows with NULL customer_id
- duplicate_key_count: Duplicate customer IDs

---

## Performance Tips

### 1. Warehouse Sizing

```sql
-- For bulk loads, use larger warehouse
USE WAREHOUSE TRANSFORMATION_WH;  -- SMALL or MEDIUM

-- For small incremental loads
USE WAREHOUSE INGESTION_WH;  -- XSMALL
```

### 2. File Size

**Optimal**: 100-250 MB compressed files

**Too Small** (<10 MB): Many small files slow down load
**Too Large** (>1 GB): Single-threaded, slow

### 3. Partitioning

```sql
-- Load partitioned data
COPY INTO table
FROM @stage
PATTERN = '.*/year=2024/month=11/.*\.csv';
```

### 4. Parallel Loading

Snowflake automatically parallelizes:
- Multiple files loaded in parallel
- Large files split across compute nodes

---

## Troubleshooting

### Issue: "Load took too long"

**Possible Causes**:
- Warehouse too small
- Files too large
- Network latency

**Solutions**:
1. Use larger warehouse
2. Split large files into smaller chunks
3. Compress files (GZIP, Snappy)

### Issue: "Duplicate rows after reload"

**Cause**: Using `FORCE = TRUE` or truncating table before reload

**Solution**:
- Use `FORCE = FALSE` (default)
- Snowflake tracks loaded files automatically
- Or use MERGE instead of COPY for idempotent loads

### Issue: "Some rows rejected"

**Check rejected rows**:
```sql
-- View error details
SELECT *
FROM TABLE(VALIDATE(RAW_CUSTOMERS, JOB_ID => '<copy_job_id>'));
```

---

## Next Steps

After successful customer load:

1. ✅ Verify all validations pass (verify_customer_load.sql)
2. ➡️ **Iteration 2.4**: Generate transaction data
3. ➡️ **Iteration 2.5**: Load transactions into Bronze layer
4. ➡️ **Phase 3**: Build dbt Silver layer transformations

---

## Additional Resources

- [Snowflake COPY INTO Documentation](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html)
- [Loading Data from S3](https://docs.snowflake.com/en/user-guide/data-load-s3.html)
- [File Format Options](https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html)
- [Copy History](https://docs.snowflake.com/en/sql-reference/functions/copy_history.html)
