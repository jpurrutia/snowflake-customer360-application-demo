## Snowflake Foundation Setup

## Overview

This directory contains SQL scripts and automation to set up the Snowflake foundation for the Customer 360 Analytics Platform.

### What Gets Created

- **Database**: `CUSTOMER_ANALYTICS`
- **Schemas**: `BRONZE`, `SILVER`, `GOLD`, `OBSERVABILITY` (Medallion Architecture)
- **Roles**: `DATA_ENGINEER`, `MARKETING_MANAGER`, `DATA_ANALYST` (RBAC)
- **Observability Tables**: 4 tables + 3 views for pipeline monitoring

## Prerequisites

- Snowflake account (trial or production)
- ACCOUNTADMIN or SYSADMIN role access
- (Optional) SnowSQL CLI installed

## Setup Methods

### Method 1: Automated Setup (Recommended)

Using the provided shell script:

```bash
# Navigate to project root
cd /path/to/snowflake-panel-demo

# Ensure .env file exists with Snowflake credentials
cp .env.example .env
# Edit .env with your credentials

# Run setup script
./snowflake/run_setup.sh
```

### Method 2: Manual Setup

Execute SQL files in order using Snowflake UI:

1. Log into Snowflake web UI
2. Create a new worksheet
3. Execute each SQL file in order:
   - `setup/00_environment_check.sql`
   - `setup/01_create_database_schemas.sql`
   - `setup/02_create_roles_grants.sql`
   - `setup/03_create_observability_tables.sql`

## SQL Scripts

### 00_environment_check.sql

**Purpose**: Verify Snowflake connectivity and current environment

**Checks**:
- Current account, user, role, warehouse
- Available warehouses
- Current role grants
- ACCOUNTADMIN privilege check

**Usage**:
```sql
-- Run in Snowflake UI or via SnowSQL
!source snowflake/setup/00_environment_check.sql
```

### 01_create_database_schemas.sql

**Purpose**: Create database and schemas for medallion architecture

**Creates**:
- Database: `CUSTOMER_ANALYTICS`
- Schema: `BRONZE` - Raw data landing zone
- Schema: `SILVER` - Cleaned, deduplicated data
- Schema: `GOLD` - Analytics-ready dimensional models
- Schema: `OBSERVABILITY` - Pipeline metadata and DQ metrics

**Requires**: `SYSADMIN` or `ACCOUNTADMIN` role

### 02_create_roles_grants.sql

**Purpose**: Set up role-based access control (RBAC)

**Roles Created**:

| Role | Access | Use Case |
|------|--------|----------|
| `DATA_ENGINEER` | Full access to all schemas | ETL development, data transformations |
| `MARKETING_MANAGER` | Read-only GOLD schema | Business intelligence, reporting |
| `DATA_ANALYST` | Read-only all schemas | Analysis, validation, troubleshooting |

**Requires**: `ACCOUNTADMIN` or `SECURITYADMIN` role

**Role Hierarchy**:
```
ACCOUNTADMIN
  └── SYSADMIN
      ├── DATA_ENGINEER
      ├── MARKETING_MANAGER
      └── DATA_ANALYST
```

### 03_create_observability_tables.sql

**Purpose**: Create tables and views for operational monitoring

**Tables**:

1. **PIPELINE_RUN_METADATA**
   - Tracks all pipeline runs with status and duration
   - Columns: run_id, pipeline_name, status, models_run, models_failed, error_message, etc.

2. **DATA_QUALITY_METRICS**
   - Tracks DQ checks with failure rates
   - Columns: check_id, layer, table_name, check_type, records_checked, records_failed, failure_rate

3. **LAYER_RECORD_COUNTS**
   - Tracks record counts for trend analysis
   - Columns: layer, table_name, record_count, distinct_keys, null_key_count, duplicate_key_count

4. **MODEL_EXECUTION_LOG**
   - Detailed execution log for each transformation
   - Columns: model_name, execution_time_seconds, rows_affected, credits_used, error_message

**Views**:
- `V_LATEST_PIPELINE_RUNS` - Most recent run per pipeline
- `V_RECENT_DQ_FAILURES` - Failed DQ checks (last 7 days)
- `V_RECORD_COUNT_TRENDS` - Daily record count trends

**Requires**: `DATA_ENGINEER` role

## Verification

After setup, verify everything was created correctly:

```sql
-- Use ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Check database
SHOW DATABASES LIKE 'CUSTOMER_ANALYTICS';

-- Check schemas
USE DATABASE CUSTOMER_ANALYTICS;
SHOW SCHEMAS;

-- Check roles
SHOW ROLES LIKE '%ENGINEER%';
SHOW ROLES LIKE '%MANAGER%';
SHOW ROLES LIKE '%ANALYST%';

-- Check role grants
SHOW GRANTS TO ROLE DATA_ENGINEER;
SHOW GRANTS TO ROLE MARKETING_MANAGER;
SHOW GRANTS TO ROLE DATA_ANALYST;

-- Check observability tables
USE SCHEMA OBSERVABILITY;
SHOW TABLES;

-- Verify sample data
SELECT * FROM PIPELINE_RUN_METADATA;
```

## Testing RBAC

### Test MARKETING_MANAGER Role (GOLD Only)

```sql
USE ROLE MARKETING_MANAGER;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CUSTOMER_ANALYTICS;

-- Should work: Access GOLD schema
USE SCHEMA GOLD;
SHOW TABLES;

-- Should fail: Access BRONZE schema
USE SCHEMA BRONZE;  -- Error: Insufficient privileges
```

### Test DATA_ANALYST Role (Read-Only All)

```sql
USE ROLE DATA_ANALYST;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CUSTOMER_ANALYTICS;

-- Should work: Access all schemas
USE SCHEMA BRONZE;
USE SCHEMA SILVER;
USE SCHEMA GOLD;

-- Should fail: Create table
CREATE TABLE GOLD.TEST_TABLE (id INT);  -- Error: Insufficient privileges
```

### Test DATA_ENGINEER Role (Full Access)

```sql
USE ROLE DATA_ENGINEER;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CUSTOMER_ANALYTICS;

-- Should work: Everything
USE SCHEMA GOLD;
CREATE OR REPLACE TABLE TEST_TABLE (id INT);
INSERT INTO TEST_TABLE VALUES (1);
SELECT * FROM TEST_TABLE;
DROP TABLE TEST_TABLE;
```

## Troubleshooting

### Issue: "Role does not exist"

**Cause**: Roles not created or insufficient privileges

**Fix**:
```sql
-- Switch to ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- Re-run role creation script
!source snowflake/setup/02_create_roles_grants.sql
```

### Issue: "Object does not exist"

**Cause**: Schemas or tables not created

**Fix**:
```sql
-- Verify database exists
SHOW DATABASES;

-- If missing, re-run schema creation
USE ROLE SYSADMIN;
!source snowflake/setup/01_create_database_schemas.sql
```

### Issue: "Insufficient privileges"

**Cause**: User not granted custom role

**Fix**:
```sql
USE ROLE ACCOUNTADMIN;

-- Grant role to user
GRANT ROLE DATA_ENGINEER TO USER your_username;
GRANT ROLE MARKETING_MANAGER TO USER your_username;
GRANT ROLE DATA_ANALYST TO USER your_username;
```

## Next Steps

After Snowflake foundation is set up:

1. ✅ **Verify Setup**: Run verification queries above
2. ✅ **Test RBAC**: Switch roles and test permissions
3. ➡️ **Prompt 2.1**: Generate synthetic customer data
4. ➡️ **Prompt 2.2**: Upload data to S3
5. ➡️ **Prompt 2.3**: Load data into BRONZE layer

## Additional Resources

- [Snowflake Medallion Architecture](https://www.snowflake.com/guides/what-medallion-architecture)
- [Snowflake RBAC](https://docs.snowflake.com/en/user-guide/security-access-control-overview.html)
- [SnowSQL Installation](https://docs.snowflake.com/en/user-guide/snowsql-install-config.html)
