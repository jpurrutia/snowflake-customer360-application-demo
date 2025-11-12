# AWS Infrastructure for Snowflake Customer 360 Analytics

## Overview

This Terraform configuration provisions the AWS infrastructure required for Snowflake data ingestion:

- **S3 Bucket**: Data lake storage for customer and transaction data
- **IAM Role**: Grants Snowflake read access to S3 bucket
- **IAM Policy**: Defines S3 permissions (ListBucket, GetObject, GetObjectVersion)
- **Folder Structure**: Organized paths for customers/ and transactions/

## Infrastructure Created

### S3 Bucket
- **Name**: `snowflake-customer-analytics-data-{environment}`
- **Features**:
  - Versioning enabled
  - Server-side encryption (SSE-S3)
  - Public access blocked
  - Lifecycle policy: transition to Glacier after 90 days
- **Folder Structure**:
  - `customers/` - Customer data files
  - `transactions/historical/` - Bulk transaction loads
  - `transactions/streaming/` - Real-time transaction ingestion

### IAM Role
- **Name**: `snowflake-customer-analytics-snowflake-s3-access-{environment}`
- **Trust Policy**: Allows Snowflake AWS account to assume role
- **Permissions**: Read-only access to S3 bucket
- **External ID**: Required for enhanced security

## Prerequisites

1. **AWS Account** with permissions to create:
   - S3 buckets
   - IAM roles and policies

2. **AWS CLI** configured with credentials:
   ```bash
   aws configure
   ```

3. **Terraform** installed (v1.5.0+):
   ```bash
   terraform version
   ```

4. **Snowflake Account** (required for storage integration)

## Step 1: Obtain Snowflake Account Information

Before applying Terraform, you need Snowflake's AWS account ID and external ID.

### Option A: Create Storage Integration First (Recommended)

1. In Snowflake, create a temporary storage integration:
   ```sql
   USE ROLE ACCOUNTADMIN;

   CREATE OR REPLACE STORAGE INTEGRATION temp_s3_integration
     TYPE = EXTERNAL_STAGE
     STORAGE_PROVIDER = 'S3'
     ENABLED = TRUE
     STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_AWS_ACCOUNT:role/temp-role'
     STORAGE_ALLOWED_LOCATIONS = ('s3://temp-bucket/');
   ```

2. Retrieve Snowflake's AWS account ID and external ID:
   ```sql
   DESC STORAGE INTEGRATION temp_s3_integration;
   ```

3. Look for these fields in the output:
   - `STORAGE_AWS_IAM_USER_ARN` - Extract the 12-digit account ID
   - `STORAGE_AWS_EXTERNAL_ID` - Copy this value

### Option B: Use Snowflake Documentation Values

Refer to Snowflake documentation for the AWS account ID used in your region.

## Step 2: Configure Terraform Variables

1. Copy the example variables file:
   ```bash
   cd terraform
   cp terraform.tfvars.example terraform.tfvars
   ```

2. Edit `terraform.tfvars` with your values:
   ```hcl
   project_name          = "snowflake-customer-analytics"
   environment           = "demo"
   aws_region            = "us-east-1"
   snowflake_account_id  = "123456789012"  # From Step 1
   snowflake_external_id = "ABC12345_SFCRole=1_XXXX"  # From Step 1
   ```

3. **Important**: `terraform.tfvars` is git-ignored and should NOT be committed

## Step 3: Initialize Terraform

```bash
cd terraform
terraform init
```

This will:
- Download required providers (AWS, null)
- Initialize the local backend
- Prepare the working directory

## Step 4: Plan Infrastructure Changes

```bash
terraform plan
```

Review the plan output to see what resources will be created:
- 1 S3 bucket
- 4 S3 bucket configurations (versioning, encryption, lifecycle, public access block)
- 4 S3 objects (folder markers)
- 1 IAM role
- 1 IAM policy
- 1 IAM policy attachment

**Expected Resource Count**: ~12 resources

## Step 5: Apply Infrastructure

```bash
terraform apply
```

Type `yes` when prompted to confirm.

**Expected Duration**: 30-60 seconds

## Step 6: Capture Outputs

After successful apply, capture the output values:

```bash
terraform output
```

You'll need these values for Snowflake configuration:
- `iam_role_arn` - Use in CREATE STORAGE INTEGRATION
- `s3_bucket_name` - Use in STORAGE_ALLOWED_LOCATIONS
- `folder_structure` - S3 paths for data organization

### Example Output:
```
iam_role_arn = "arn:aws:iam::YOUR_AWS_ACCOUNT:role/snowflake-customer-analytics-snowflake-s3-access-demo"
s3_bucket_name = "snowflake-customer-analytics-data-demo"
folder_structure = {
  customers = "snowflake-customer-analytics-data-demo/customers/"
  transactions = "snowflake-customer-analytics-data-demo/transactions/"
  transactions_historical = "snowflake-customer-analytics-data-demo/transactions/historical/"
  transactions_streaming = "snowflake-customer-analytics-data-demo/transactions/streaming/"
}
```

## Step 7: Update Snowflake Storage Integration

Now update the Snowflake storage integration with the actual IAM role ARN:

```sql
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION s3_customer_analytics_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<iam_role_arn from terraform output>'
  STORAGE_ALLOWED_LOCATIONS = (
    's3://<s3_bucket_name>/customers/',
    's3://<s3_bucket_name>/transactions/'
  );

-- Verify integration
DESC STORAGE INTEGRATION s3_customer_analytics_integration;

-- Grant usage to data engineer role
GRANT USAGE ON INTEGRATION s3_customer_analytics_integration TO ROLE DATA_ENGINEER;
```

## Step 8: Update IAM Trust Policy (Important!)

After creating the storage integration, Snowflake provides a new IAM user ARN and external ID. You need to update the IAM role trust policy:

1. Get the new values from Snowflake:
   ```sql
   DESC STORAGE INTEGRATION s3_customer_analytics_integration;
   ```

2. Update `terraform.tfvars` with the new `snowflake_account_id` and `snowflake_external_id`

3. Re-apply Terraform:
   ```bash
   terraform apply
   ```

This updates the IAM role's trust policy with the correct Snowflake credentials.

## Step 9: Test S3 Access from Snowflake

```sql
-- Create external stage
CREATE OR REPLACE STAGE customer_analytics_stage
  STORAGE_INTEGRATION = s3_customer_analytics_integration
  URL = 's3://<s3_bucket_name>/customers/'
  FILE_FORMAT = (TYPE = CSV);

-- List files (should be empty initially)
LIST @customer_analytics_stage;
```

## Viewing Infrastructure

### AWS Console
- **S3 Bucket**: AWS Console → S3 → `snowflake-customer-analytics-data-demo`
- **IAM Role**: AWS Console → IAM → Roles → `snowflake-customer-analytics-snowflake-s3-access-demo`

### Terraform State
```bash
terraform show
terraform state list
```

## Updating Infrastructure

To modify infrastructure:

1. Edit Terraform files (*.tf)
2. Run `terraform plan` to preview changes
3. Run `terraform apply` to apply changes

## Destroying Infrastructure

⚠️ **Warning**: This will delete all resources and data in the S3 bucket

```bash
terraform destroy
```

Type `yes` when prompted to confirm.

## Cost Considerations

- **S3 Storage**: ~$0.023 per GB/month (first 50 TB)
- **S3 Requests**: Minimal (data ingestion only)
- **IAM Role**: No cost
- **Glacier Storage**: ~$0.004 per GB/month (after 90 days)

**Estimated Monthly Cost**: < $5 for demo workload (< 10 GB data)

## Troubleshooting

### Issue: Snowflake cannot access S3

**Symptoms**: `AWS_ROLE_NOT_FOUND` or `EXTERNAL_STAGE_ACCESS_DENIED` errors

**Solutions**:
1. Verify IAM role ARN matches Terraform output
2. Verify Snowflake account ID and external ID are correct in `terraform.tfvars`
3. Check IAM role trust policy includes Snowflake AWS account
4. Ensure storage integration is ENABLED

### Issue: Terraform apply fails

**Symptoms**: Resource creation errors

**Solutions**:
1. Verify AWS credentials: `aws sts get-caller-identity`
2. Check AWS permissions (S3, IAM)
3. Verify S3 bucket name is globally unique
4. Check Terraform version: `terraform version`

### Issue: S3 bucket name already exists

**Symptoms**: `BucketAlreadyExists` error

**Solutions**:
1. Modify `project_name` or `environment` variable in `terraform.tfvars`
2. Choose a unique bucket name

## Next Steps

After infrastructure is provisioned and tested:

1. **Prompt 1.3**: Set up Snowflake foundation (databases, schemas, roles)
2. **Prompt 2.1**: Generate customer data
3. **Prompt 2.2**: Upload data to S3
4. **Prompt 2.3**: Load data into Snowflake Bronze layer

## Future Enhancements

- **SNS/SQS**: Add event notifications for Snowpipe (after storage integration created)
- **S3 Backend**: Migrate to remote state storage
- **Multi-Environment**: Create dev/staging/prod workspaces
- **KMS Encryption**: Use customer-managed keys instead of SSE-S3
- **VPC Endpoint**: Private S3 access without internet gateway
