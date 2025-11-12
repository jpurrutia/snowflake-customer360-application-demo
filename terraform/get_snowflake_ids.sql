-- ============================================================================
-- Get Snowflake Account Identifiers for Terraform
-- ============================================================================
-- Purpose: Retrieve Snowflake account information needed for Terraform configuration
-- Usage: Run these queries in Snowflake and use outputs to populate terraform.tfvars
-- ============================================================================

-- IMPORTANT: Run these queries BEFORE creating the storage integration
-- You need these values to set up the initial IAM trust policy in AWS

-- ============================================================================
-- Query 1: Get Snowflake Account Locator and Organization
-- ============================================================================

SELECT
    CURRENT_ACCOUNT() AS account_locator,
    CURRENT_ORGANIZATION_NAME() AS organization_name,
    CURRENT_REGION() AS region;

-- Expected output (example):
-- ACCOUNT_LOCATOR: BJVVFJJ
-- ORGANIZATION_NAME: UC08848
-- REGION: AWS_US_EAST_1

-- ============================================================================
-- Query 2: Get AWS Account ID for Snowflake
-- ============================================================================

-- This query helps you find the AWS account ID that Snowflake uses
-- You'll need this for the terraform.tfvars file

-- For AWS regions, Snowflake uses these account IDs:
-- us-east-1 (N. Virginia): 976709231746
-- us-east-2 (Ohio): 976709231746
-- us-west-2 (Oregon): 976709231746
-- eu-west-1 (Ireland): 976709231746
-- eu-central-1 (Frankfurt): 976709231746
-- ap-southeast-1 (Singapore): 976709231746
-- ap-southeast-2 (Sydney): 976709231746

-- For your reference, the Snowflake AWS account ID for most commercial regions is: 976709231746

-- ============================================================================
-- How to Use These Values in terraform.tfvars
-- ============================================================================

-- Create a file terraform/terraform.tfvars with:
--
-- snowflake_account_id  = "976709231746"  # Snowflake's AWS account ID
-- snowflake_external_id = "PLACEHOLDER"   # Will be updated after storage integration
-- aws_region            = "us-east-1"
-- s3_bucket_name        = "customer360-analytics-data-20250111"  # Use today's date
-- environment           = "dev"

-- ============================================================================
-- Next Steps
-- ============================================================================

-- After populating terraform.tfvars:
-- 1. Run: cd terraform && ./deploy.sh
-- 2. Note the IAM role ARN from Terraform outputs
-- 3. Create storage integration in Snowflake (04_create_storage_integration.sql)
-- 4. Get external ID from: DESC STORAGE INTEGRATION customer360_s3_integration;
-- 5. Update terraform.tfvars with the external ID
-- 6. Re-run: ./deploy.sh (to update IAM trust policy)

-- ============================================================================
-- Troubleshooting
-- ============================================================================

-- If you're unsure about the Snowflake AWS account ID:
-- 1. Create a temporary storage integration
-- 2. Run DESC STORAGE INTEGRATION on it
-- 3. Look for STORAGE_AWS_IAM_USER_ARN in the output
-- 4. Extract the 12-digit AWS account ID from that ARN
