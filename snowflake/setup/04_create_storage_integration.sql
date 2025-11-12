-- ============================================================================
-- Create Snowflake Storage Integration for S3 Access
-- ============================================================================
-- Purpose: Set up secure access from Snowflake to AWS S3 using IAM role
-- Requires: ACCOUNTADMIN role, IAM role ARN from Terraform outputs
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- BEFORE RUNNING THIS SCRIPT
-- ============================================================================
-- 1. Run Terraform to create S3 bucket and IAM role:
--    cd terraform && ./deploy.sh
-- 2. Note the IAM role ARN from Terraform outputs
-- 3. Replace <IAM_ROLE_ARN> below with the actual ARN
-- 4. Replace <S3_BUCKET_NAME> with your bucket name

-- ============================================================================
-- Create Storage Integration
-- ============================================================================

CREATE OR REPLACE STORAGE INTEGRATION customer360_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<IAM_ROLE_ARN>'  -- Replace with output from Terraform
  STORAGE_ALLOWED_LOCATIONS = (
    's3://<S3_BUCKET_NAME>/customers/',
    's3://<S3_BUCKET_NAME>/transactions/'
  )
  COMMENT = 'Storage integration for Customer 360 Analytics data lake in S3';

-- Example (DO NOT USE THESE VALUES):
-- STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::339712742264:role/snowflake-customer360-s3-access'
-- STORAGE_ALLOWED_LOCATIONS = ('s3://customer360-analytics-data-20250111/customers/', ...)

-- ============================================================================
-- Retrieve Storage Integration Details
-- ============================================================================

-- Run this to get the values you need to update Terraform
DESC STORAGE INTEGRATION customer360_s3_integration;

-- Look for these two values in the output:
-- 1. STORAGE_AWS_IAM_USER_ARN
--    Example: arn:aws:iam::976709231746:user/abc123-a
--    This is the Snowflake IAM user that will assume your role
--
-- 2. STORAGE_AWS_EXTERNAL_ID
--    Example: UC08848_SFCRole=3_1u+jS7RAYdkBmTD6dpptvpYo3FE=
--    This is the external ID for the trust relationship

-- ============================================================================
-- IMPORTANT: Update Terraform Variables
-- ============================================================================

-- Copy the STORAGE_AWS_EXTERNAL_ID value from above
-- Update your terraform/terraform.tfvars file:
--
-- snowflake_external_id = "UC08848_SFCRole=3_1u+jS7RAYdkBmTD6dpptvpYo3FE="
--
-- Then re-run Terraform to update the IAM trust policy:
-- cd terraform && ./deploy.sh

-- ============================================================================
-- Verify Storage Integration
-- ============================================================================

-- Check that storage integration was created
SHOW STORAGE INTEGRATIONS LIKE 'customer360_s3_integration';

-- Grant usage to DATA_ENGINEER role
GRANT USAGE ON INTEGRATION customer360_s3_integration TO ROLE DATA_ENGINEER;

-- Verify grants
SHOW GRANTS ON INTEGRATION customer360_s3_integration;

-- ============================================================================
-- Understanding the Trust Relationship
-- ============================================================================

-- The storage integration creates a trust relationship between:
-- 1. Your AWS IAM role (created by Terraform)
-- 2. Snowflake's AWS IAM user (shown in STORAGE_AWS_IAM_USER_ARN)
--
-- The flow is:
-- Snowflake IAM User (976709231746:user/abc123)
--   ↓ assumes role using external ID
-- Your IAM Role (your-account:role/snowflake-customer360-s3-access)
--   ↓ has permissions to
-- Your S3 Bucket (s3://customer360-analytics-data-YYYYMMDD)
--
-- The external ID acts as a secret to prevent unauthorized role assumption

-- ============================================================================
-- Troubleshooting
-- ============================================================================

-- If you get "Not authorized to perform sts:AssumeRole":
-- 1. Verify you updated terraform.tfvars with the correct external ID
-- 2. Re-run terraform apply to update the trust policy
-- 3. Wait a few minutes for IAM changes to propagate
-- 4. Try running LIST @stage_name again

-- If you get "Access Denied" when listing S3:
-- 1. Verify the IAM role has s3:GetObject and s3:ListBucket permissions
-- 2. Verify the bucket name in STORAGE_ALLOWED_LOCATIONS matches your actual bucket
-- 3. Check that the IAM policy is attached to the role

-- Display confirmation
SELECT '✓ Storage Integration Created' AS status;
SELECT 'Next: Update terraform.tfvars with external ID and re-run Terraform' AS next_step;
