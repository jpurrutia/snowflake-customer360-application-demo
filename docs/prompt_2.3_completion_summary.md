# Prompt 2.3: Bronze Layer - Customer Bulk Load - Completion Summary

**Status**: ✅ **COMPLETE** (SQL Scripts Ready)
**Date**: 2025-11-11

---

## Overview

Created comprehensive SQL scripts for Bronze layer table creation, bulk data loading from S3, and data validation. All scripts are ready for execution in Snowflake once storage integration and stages are configured.

---

## Deliverables

### ✅ SQL Scripts Created

1. **snowflake/setup/06_create_bronze_tables.sql** (90 lines)
   - Creates `BRONZE.BRONZE_CUSTOMERS` table
   - 13 data columns + 3 metadata columns
   - No constraints (raw data as-is)
   - Comprehensive comments explaining Bronze layer purpose
   - Grant permissions to DATA_ANALYST and MARKETING_MANAGER
   - **Status**: ✅ Ready for execution

2. **snowflake/load/load_customers_bulk.sql** (150 lines)
   - COPY INTO command with proper column mapping
   - Uses METADATA$FILENAME and METADATA$FILE_ROW_NUMBER
   - ON_ERROR = 'ABORT_STATEMENT' for fail-fast behavior
   - FORCE = FALSE to prevent duplicate loads
   - Post-load summary queries (total rows, segments, card types)
   - Logs to OBSERVABILITY.LAYER_RECORD_COUNTS
   - Shows COPY_HISTORY for audit trail
   - **Status**: ✅ Ready for execution

3. **snowflake/load/verify_customer_load.sql** (220 lines)
   - 10 comprehensive validation checks:
     1. Row count (expect 50,000)
     2. Null customer IDs (expect 0)
     3. Duplicate customer IDs (expect 0)
     4. Segment distribution (within tolerance)
     5. Date range validation
     6. Email format validation
     7. Credit limit range (5K-50K)
     8. Age range (22-75)
     9. Metadata fields populated
     10. Decline type logic
   - Overall validation summary
   - Sample data preview
   - **Status**: ✅ Ready for execution

4. **snowflake/load/README.md** (350 lines)
   - Complete documentation for loading process
   - COPY INTO command reference
   - File format options explained
   - ON_ERROR strategies (ABORT vs CONTINUE)
   - Common errors and solutions
   - Metadata columns usage
   - Manual load instructions
   - Load history queries
   - Performance tips
   - Troubleshooting guide
   - **Status**: ✅ Complete

---

## Table Schema

### BRONZE.BRONZE_CUSTOMERS

**Data Columns** (13):
| Column | Type | Description |
|--------|------|-------------|
| customer_id | STRING | Unique identifier (CUST00000001) |
| first_name | STRING | Customer first name |
| last_name | STRING | Customer last name |
| email | STRING | Email address |
| age | INT | Customer age (22-75) |
| state | STRING | US state abbreviation |
| city | STRING | City name |
| employment_status | STRING | Employment status |
| card_type | STRING | Standard or Premium |
| credit_limit | NUMBER(10,2) | Credit limit ($5K-$50K) |
| account_open_date | DATE | Account opening date |
| customer_segment | STRING | Customer segment |
| decline_type | STRING | Decline pattern (Declining only) |

**Metadata Columns** (3):
| Column | Type | Description |
|--------|------|-------------|
| ingestion_timestamp | TIMESTAMP | Load timestamp (auto) |
| source_file | STRING | Source S3 file path |
| _metadata_file_row_number | INT | Row number in source file |

**Design Decisions**:
- ❌ No PRIMARY KEY (Bronze accepts duplicates)
- ❌ No NOT NULL constraints (accept data as-is)
- ❌ No FOREIGN KEY constraints (validated in Silver/Gold)
- ✅ Metadata for lineage and debugging

---

## COPY INTO Command Structure

```sql
COPY INTO BRONZE.BRONZE_CUSTOMERS (
    customer_id,
    first_name,
    -- ... all columns ...
    source_file,
    _metadata_file_row_number
)
FROM (
    SELECT
        $1::STRING AS customer_id,
        $2::STRING AS first_name,
        -- ... column mappings ...
        METADATA$FILENAME AS source_file,
        METADATA$FILE_ROW_NUMBER AS _metadata_file_row_number
    FROM @customer_stage
)
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    NULL_IF = ('NULL', 'null', '')
)
PATTERN = '.*customers\.csv'
ON_ERROR = 'ABORT_STATEMENT'
FORCE = FALSE;
```

**Key Features**:
- Explicit column mapping ($1, $2, etc.)
- Metadata capture (filename, row number)
- Fail-fast error handling
- Prevents duplicate loads

---

## Validation Checks

### 1. Row Count Validation
```sql
Expected: 50,000 rows
Actual: COUNT(*) FROM BRONZE_CUSTOMERS
Status: ✓ PASS if exact match
```

### 2. Data Quality Validations

| Check | Criteria | Pass Condition |
|-------|----------|----------------|
| Null IDs | customer_id IS NULL | Count = 0 |
| Duplicates | COUNT(*) GROUP BY customer_id | No IDs with count > 1 |
| Segments | Distribution % | Within ±5% of target |
| Emails | Contains @ and . | 100% valid |
| Credit Limits | $5K-$50K, multiples of $1K | All within range |
| Ages | 22-75 | All within range |
| Dates | 2-5 years ago | All within window |
| Metadata | source_file, ingestion_timestamp | All NOT NULL |
| Decline Type | Only for Declining segment | Correct logic |

### 3. Segment Distribution Validation

| Segment | Target % | Tolerance | Pass Range |
|---------|----------|-----------|------------|
| High-Value Travelers | 15% | ±5% | 10-20% |
| Stable Mid-Spenders | 40% | ±5% | 35-45% |
| Budget-Conscious | 25% | ±5% | 20-30% |
| Declining | 10% | ±5% | 5-15% |
| New & Growing | 10% | ±5% | 5-15% |

---

## Observability Logging

### LAYER_RECORD_COUNTS Table

After each load, metrics are logged:

```sql
INSERT INTO OBSERVABILITY.LAYER_RECORD_COUNTS
SELECT
    'BULK_LOAD_CUSTOMERS_' || TIMESTAMP AS run_id,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'bronze' AS layer,
    'BRONZE' AS schema_name,
    'BRONZE_CUSTOMERS' AS table_name,
    COUNT(*) AS record_count,
    COUNT(DISTINCT customer_id) AS distinct_keys,
    COUNT_IF(customer_id IS NULL) AS null_key_count,
    COUNT(*) - COUNT(DISTINCT customer_id) AS duplicate_key_count
FROM BRONZE_CUSTOMERS;
```

**Tracked Metrics**:
- Total records loaded
- Distinct customer IDs
- Null customer IDs
- Duplicate customer IDs

---

## Load History Tracking

Snowflake automatically tracks COPY INTO operations:

```sql
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'CUSTOMER_ANALYTICS.BRONZE.BRONZE_CUSTOMERS',
    START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;
```

**Captured Information**:
- File name
- Rows loaded
- Rows parsed
- Errors encountered
- Load timestamp
- Warehouse used

---

## Error Handling Strategy

### ON_ERROR Options Comparison

| Option | Behavior | Use Case | Our Choice |
|--------|----------|----------|------------|
| ABORT_STATEMENT | Fail immediately, rollback | Known good data | ✅ Bulk loads |
| CONTINUE | Skip bad rows, load rest | Unknown data quality | Incremental |
| SKIP_FILE | Skip entire file on error | Multi-file loads | Not used |

**Rationale for ABORT_STATEMENT**:
- Customer data is generated and validated
- Data quality is known and high
- Any error indicates a problem (corrupted file, schema mismatch)
- Fail fast to catch issues early

---

## Files Structure

```
snowflake/
├── setup/
│   └── 06_create_bronze_tables.sql       # Table creation
└── load/
    ├── load_customers_bulk.sql           # COPY INTO command
    ├── verify_customer_load.sql          # Validation queries
    └── README.md                          # Documentation
```

---

## Manual Execution Steps

Since Snowflake storage integration and stages need to be created manually:

### Prerequisites (To Be Completed)

1. ✅ S3 bucket created (via Terraform)
2. ✅ IAM role created (via Terraform)
3. ✅ Customer CSV uploaded to S3 (6.04 MB)
4. ⏳ Storage integration created (04_create_storage_integration.sql)
5. ⏳ External stages created (05_create_stages.sql)

### Execution Sequence

```bash
# 1. Create storage integration (manual in Snowflake)
# Run: snowflake/setup/04_create_storage_integration.sql
# Update placeholders: IAM_ROLE_ARN, S3_BUCKET_NAME

# 2. Update Terraform with external ID (manual)
# Get external ID from: DESC STORAGE INTEGRATION customer360_s3_integration;
# Update: terraform/terraform.tfvars
# Re-run: cd terraform && ./deploy.sh

# 3. Create stages (manual in Snowflake)
# Run: snowflake/setup/05_create_stages.sql
# Update placeholder: S3_BUCKET_NAME

# 4. Verify stage access
LIST @CUSTOMER_ANALYTICS.BRONZE.customer_stage;
# Expected: customers/customers.csv appears

# 5. Create Bronze table (manual in Snowflake)
# Run: snowflake/setup/06_create_bronze_tables.sql

# 6. Load data (manual in Snowflake)
# Run: snowflake/load/load_customers_bulk.sql
# Expected: 50,000 rows loaded

# 7. Validate load (manual in Snowflake)
# Run: snowflake/load/verify_customer_load.sql
# Expected: All checks show ✓ PASS
```

---

## Expected Results

### After Table Creation

```sql
DESC TABLE BRONZE_CUSTOMERS;
-- Shows 16 columns (13 data + 3 metadata)

SELECT COUNT(*) FROM BRONZE_CUSTOMERS;
-- Returns: 0 (empty table)
```

### After Bulk Load

```sql
SELECT COUNT(*) FROM BRONZE_CUSTOMERS;
-- Returns: 50,000

SELECT COUNT(DISTINCT customer_id) FROM BRONZE_CUSTOMERS;
-- Returns: 50,000 (no duplicates)

SELECT customer_segment, COUNT(*)
FROM BRONZE_CUSTOMERS
GROUP BY customer_segment;
-- Shows distribution matching target percentages
```

### After Validation

```
All 10 validation checks: ✓ PASS
Overall status: ✓ ALL VALIDATIONS PASSED
```

---

## Documentation Updates

### README.md Updated

Added "Data Loading - Customer Bulk Load" section:
- Expected row count (50,000)
- Link to snowflake/load/README.md
- Troubleshooting for common load errors

---

## Troubleshooting Guide

### Issue 1: "File not found in stage"

**Error**: `No files found matching pattern`

**Cause**: Customer CSV not uploaded or wrong bucket

**Solution**:
```sql
LIST @customer_stage;
-- Verify customers.csv appears

-- Check actual file in S3
aws s3 ls s3://bucket-name/customers/
```

### Issue 2: "Column count mismatch"

**Error**: `Number of columns in file does not match`

**Cause**: CSV has different number of columns than expected

**Solution**:
- Verify CSV has 13 columns
- Check for extra commas or missing columns
- Verify SKIP_HEADER = 1 is working

### Issue 3: "Access Denied"

**Error**: `Access Denied (service: Amazon S3)`

**Cause**: Storage integration trust relationship not complete

**Solution**:
1. Verify storage integration created
2. Check external ID updated in Terraform
3. Re-run terraform apply
4. Wait 2-3 minutes for IAM propagation

### Issue 4: "Validation fails on segment distribution"

**Warning**: Segment % outside tolerance

**Cause**: Random seed variation (unlikely with seed=42)

**Solution**:
- Verify customer CSV generated with correct seed
- Check if table truncated and reloaded correctly
- Regenerate customer data if needed

---

## Performance Metrics

### Expected Load Performance

| Metric | Value |
|--------|-------|
| File Size | 6.04 MB |
| Row Count | 50,000 |
| Warehouse | COMPUTE_WH (SMALL) |
| Expected Duration | 5-15 seconds |
| Rows/Second | ~5,000-10,000 |

**Factors Affecting Performance**:
- Warehouse size (XSMALL vs LARGE)
- Network latency to S3
- File compression
- Number of compute nodes

---

## Next Steps

After successful Bronze layer load:

1. ✅ Verify all validation checks pass
2. ✅ Check observability logging
3. ✅ Review load history
4. ➡️ **Iteration 2.4**: Generate transaction data (13.5M records)
5. ➡️ **Iteration 2.5**: Load transactions into Bronze layer
6. ➡️ **Phase 3**: Build dbt Silver layer transformations

---

## Success Criteria

- [x] Bronze table SQL script created
- [x] Load SQL script created with COPY INTO
- [x] Validation SQL script created (10 checks)
- [x] Observability logging implemented
- [x] Load README documentation complete
- [x] Error handling strategy documented
- [ ] Bronze table created in Snowflake (pending manual execution)
- [ ] Data loaded successfully (pending manual execution)
- [ ] All validations pass (pending manual execution)
- [ ] Observability table updated (pending manual execution)

---

## Completion Status

✅ **All SQL scripts and documentation complete**

**Ready for manual execution in Snowflake** once:
1. Storage integration is created (step 4 in setup guide)
2. External stages are created (step 5 in setup guide)
3. IAM trust policy is updated with external ID

**Status**: Production-ready SQL scripts awaiting Snowflake configuration
