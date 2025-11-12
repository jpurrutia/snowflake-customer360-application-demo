# Customer 360 Analytics Platform - Setup Guide

Complete step-by-step guide to set up the Customer 360 Analytics Platform from scratch.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Quick Setup (Automated)](#quick-setup-automated)
3. [Manual Setup (Step-by-Step)](#manual-setup-step-by-step)
4. [Verification](#verification)
5. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Tools

- [x] **Python 3.12+** with UV package manager
- [x] **Terraform** 1.0+
- [x] **AWS CLI** configured with credentials
- [x] **Snowflake Account** (trial or production)
- [x] **Git** (for version control)

### Required Access

- [x] **AWS Account** with permissions to create:
  - S3 buckets
  - IAM roles and policies
- [x] **Snowflake Account** with `ACCOUNTADMIN` role access

### Installation Commands

```bash
# Install UV (Python package manager)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install Terraform (macOS)
brew install terraform

# Install AWS CLI (macOS)
brew install awscli

# Configure AWS credentials
aws configure
```

---

## Quick Setup (Automated)

For a streamlined setup, use the automated script:

```bash
# From project root
./scripts/setup_end_to_end.sh
```

This script will:
1. ✅ Generate 50,000 customer records
2. ✅ Apply Terraform to create S3 bucket and IAM role
3. ⚠️ Prompt you to manually create Snowflake storage integration
4. ✅ Upload customer data to S3

**Note**: Snowflake storage integration requires manual steps (see below).

---

## Manual Setup (Step-by-Step)

### Phase 1: Generate Customer Data

#### Step 1.1: Install Dependencies

```bash
cd /path/to/snowflake-panel-demo
uv sync
```

#### Step 1.2: Generate Customers

```bash
uv run python -m data_generation generate-customers \
    --count 50000 \
    --output data/customers.csv \
    --seed 42
```

**Expected Output**:
```
Generating 50000 customers with seed 42...
✓ Generated 50000 customer records

📊 Statistics:
  Total customers: 50000
  Segment Distribution:
    Stable Mid-Spenders: 40.0%
    Budget-Conscious: 25.0%
    High-Value Travelers: 15.0%
    ...

✓ Validation passed
✓ Successfully saved to data/customers.csv
```

---

### Phase 2: AWS Infrastructure Setup

#### Step 2.1: Get Snowflake Account Information

Run in Snowflake:

```sql
-- Get account locator
SELECT CURRENT_ACCOUNT() AS account_locator;

-- Get organization and region
SELECT CURRENT_ORGANIZATION_NAME() AS organization_name,
       CURRENT_REGION() AS region;
```

**Note the outputs** - you'll need these for Terraform.

#### Step 2.2: Create terraform.tfvars

Create `terraform/terraform.tfvars`:

```hcl
snowflake_account_id  = "976709231746"  # Snowflake's AWS account ID
snowflake_external_id = "PLACEHOLDER"   # Will update after storage integration
aws_region            = "us-east-1"
s3_bucket_name        = "customer360-analytics-data-20250111"  # Use today's date
environment           = "dev"
```

**Snowflake AWS Account ID by Region**:
- Most commercial regions: `976709231746`
- Check Snowflake docs if using GovCloud or China regions

#### Step 2.3: Run Terraform

```bash
cd terraform
./deploy.sh
```

**Review the plan** when prompted, then type `yes` to apply.

**Expected Output**:
```
✅ Deployment Complete!

Outputs:
  iam_role_arn = "arn:aws:iam::YOUR_ACCOUNT:role/snowflake-customer360-s3-access"
  s3_bucket_name = "customer360-analytics-data-20250111"
```

**📝 Important**: Save the `iam_role_arn` - you'll need it for Snowflake.

---

### Phase 3: Snowflake Storage Integration

This is the trickiest part - creating a trust relationship between Snowflake and AWS.

#### Step 3.1: Create Storage Integration

1. Open `snowflake/setup/04_create_storage_integration.sql`

2. Replace placeholders:
   ```sql
   STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_ACCOUNT:role/snowflake-customer360-s3-access'
   STORAGE_ALLOWED_LOCATIONS = (
     's3://customer360-analytics-data-20250111/customers/',
     's3://customer360-analytics-data-20250111/transactions/'
   )
   ```

3. Run the script in Snowflake

4. Get the external ID:
   ```sql
   DESC STORAGE INTEGRATION customer360_s3_integration;
   ```

5. Look for `STORAGE_AWS_EXTERNAL_ID` in the output (example):
   ```
   UC08848_SFCRole=3_1u+jS7RAYdkBmTD6dpptvpYo3FE=
   ```

#### Step 3.2: Update Terraform with External ID

1. Update `terraform/terraform.tfvars`:
   ```hcl
   snowflake_external_id = "UC08848_SFCRole=3_1u+jS7RAYdkBmTD6dpptvpYo3FE="
   ```

2. Re-run Terraform to update IAM trust policy:
   ```bash
   cd terraform
   ./deploy.sh
   ```

This updates the IAM role's trust policy to accept the Snowflake external ID.

#### Step 3.3: Create Snowflake Stages

1. Open `snowflake/setup/05_create_stages.sql`

2. Replace `<S3_BUCKET_NAME>` with your actual bucket name

3. Run the script in Snowflake

4. Test stage access:
   ```sql
   LIST @CUSTOMER_ANALYTICS.BRONZE.customer_stage;
   ```

**Expected**: "No files found" (before upload) or file listing (after upload)

---

### Phase 4: Upload Data to S3

```bash
cd /path/to/snowflake-panel-demo

uv run python -m data_generation upload-customers \
    --file data/customers.csv \
    --bucket customer360-analytics-data-20250111
```

**Expected Output**:
```
Uploading data/customers.csv to S3 bucket: customer360-analytics-data-20250111
  File size: 6.04 MB

Uploading to S3...
Verifying upload...

✓ Upload successful!
  S3 location: s3://customer360-analytics-data-20250111/customers/customers.csv
```

---

## Verification

### 1. Verify S3 Upload

```bash
aws s3 ls s3://customer360-analytics-data-20250111/customers/
```

**Expected**: `customers.csv` listed with ~6MB size

### 2. Verify Snowflake Can Access S3

Run in Snowflake:

```sql
USE ROLE DATA_ENGINEER;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA BRONZE;

-- List files in stage
LIST @customer_stage;
```

**Expected**: `customers/customers.csv` appears in listing

### 3. Preview Data from S3

```sql
-- Preview first 10 rows
SELECT $1 AS customer_id,
       $2 AS first_name,
       $3 AS last_name,
       $4 AS email
FROM @customer_stage/customers.csv
LIMIT 10;
```

**Expected**: Customer data displayed

---

## Troubleshooting

### Issue 1: "Not authorized to perform sts:AssumeRole"

**Cause**: IAM trust policy not updated with Snowflake external ID

**Solution**:
1. Run `DESC STORAGE INTEGRATION customer360_s3_integration;` in Snowflake
2. Copy the `STORAGE_AWS_EXTERNAL_ID` value
3. Update `terraform/terraform.tfvars` with this value
4. Re-run `cd terraform && ./deploy.sh`
5. Wait 2-3 minutes for IAM changes to propagate
6. Try `LIST @customer_stage;` again

### Issue 2: "Access Denied" when listing S3 stage

**Possible Causes**:
- IAM role missing S3 permissions
- Bucket name mismatch
- Storage integration not properly configured

**Solutions**:

**Check IAM Role Permissions**:
```bash
aws iam get-role-policy \
  --role-name snowflake-customer360-s3-access \
  --policy-name snowflake-s3-access-policy
```

Should show:
- `s3:GetObject` on bucket
- `s3:ListBucket` on bucket

**Verify Bucket Name**:
```sql
DESC STORAGE INTEGRATION customer360_s3_integration;
```

Check that `STORAGE_ALLOWED_LOCATIONS` matches your actual bucket.

### Issue 3: "Storage Integration does not exist"

**Cause**: Storage integration not created or wrong database/schema context

**Solution**:
```sql
USE ROLE ACCOUNTADMIN;
SHOW STORAGE INTEGRATIONS LIKE 'customer360_s3_integration';
```

If not found, run `04_create_storage_integration.sql` again.

### Issue 4: terraform.tfvars not found

**Cause**: Configuration file not created

**Solution**:
```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your values
```

### Issue 5: AWS Credentials Not Found

**Cause**: AWS CLI not configured

**Solution**:
```bash
aws configure
# Enter: Access Key ID, Secret Access Key, Region, Output format
```

Or set environment variables:
```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=us-east-1
```

### Issue 6: Upload Fails with "NoCredentialsError"

**Cause**: AWS credentials not accessible to Python

**Solution**:
```bash
# Verify AWS credentials work
aws sts get-caller-identity

# If works, credentials are valid but not accessible to boto3
# Try setting explicit credentials:
export AWS_PROFILE=default
```

---

## Understanding the Trust Relationship

The Snowflake → S3 connection works through this flow:

```
┌─────────────────────┐
│ Snowflake IAM User  │  (arn:aws:iam::976709231746:user/abc123)
│ (Managed by         │
│  Snowflake)         │
└──────────┬──────────┘
           │ Assumes role using External ID
           ▼
┌─────────────────────┐
│ Your IAM Role       │  (arn:aws:iam::YOUR_ACCOUNT:role/snowflake-s3-access)
│ (Created by         │
│  Terraform)         │
└──────────┬──────────┘
           │ Has permissions to
           ▼
┌─────────────────────┐
│ Your S3 Bucket      │  (s3://customer360-analytics-data-20250111)
│                     │
└─────────────────────┘
```

**Key Points**:
1. Snowflake never gets direct access to your AWS credentials
2. External ID acts as a shared secret
3. IAM role limits what Snowflake can access (only specified buckets)
4. You maintain full control via IAM policies

---

## Next Steps

After successful setup:

1. ✅ **Verify**: Run all verification queries above
2. ➡️ **Iteration 2.3**: Load customers into Bronze layer
3. ➡️ **Iteration 2.4**: Generate transaction data
4. ➡️ **Iteration 2.5**: Load transactions into Bronze layer
5. ➡️ **Phase 3**: Build dbt transformations (Silver/Gold layers)

---

## Additional Resources

- [Snowflake Storage Integration Docs](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration.html)
- [Terraform AWS Provider Docs](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
- [AWS IAM Trust Policies](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user.html)
- [Snowflake External Stages](https://docs.snowflake.com/en/user-guide/data-load-s3.html)
