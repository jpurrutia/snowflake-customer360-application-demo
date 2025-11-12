#!/bin/bash
# ============================================================================
# Terraform Deployment Script
# ============================================================================
# Purpose: Apply Terraform configuration to create AWS infrastructure
# Usage: ./terraform/deploy.sh
# ============================================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}Terraform Deployment${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""

# ============================================================================
# Step 1: Check Prerequisites
# ============================================================================

echo -e "${BLUE}Step 1: Checking prerequisites...${NC}"

# Check if terraform is installed
if ! command -v terraform &> /dev/null; then
    echo -e "${RED}✗ Terraform not found${NC}"
    echo "Please install Terraform: https://www.terraform.io/downloads"
    exit 1
fi
echo -e "${GREEN}✓ Terraform installed: $(terraform version -json | grep terraform_version)${NC}"

# Check if terraform.tfvars exists
if [ ! -f "$SCRIPT_DIR/terraform.tfvars" ]; then
    echo -e "${RED}✗ terraform.tfvars not found${NC}"
    echo ""
    echo "Please create terraform.tfvars with required variables:"
    echo "  snowflake_account_id  = \"your-snowflake-aws-account-id\""
    echo "  snowflake_external_id = \"your-external-id-from-storage-integration\""
    echo "  aws_region            = \"us-east-1\""
    echo "  s3_bucket_name        = \"customer360-analytics-data-YYYYMMDD\""
    echo "  environment           = \"dev\""
    echo ""
    echo "See terraform/terraform.tfvars.example for a template"
    exit 1
fi
echo -e "${GREEN}✓ terraform.tfvars exists${NC}"

# Check AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    echo -e "${RED}✗ AWS credentials not configured or invalid${NC}"
    echo "Please configure AWS credentials using 'aws configure' or set environment variables"
    exit 1
fi
echo -e "${GREEN}✓ AWS credentials configured${NC}"

echo ""

# ============================================================================
# Step 2: Terraform Init
# ============================================================================

echo -e "${BLUE}Step 2: Initializing Terraform...${NC}"
cd "$SCRIPT_DIR"

if terraform init; then
    echo -e "${GREEN}✓ Terraform initialized${NC}"
else
    echo -e "${RED}✗ Terraform init failed${NC}"
    exit 1
fi

echo ""

# ============================================================================
# Step 3: Terraform Validate
# ============================================================================

echo -e "${BLUE}Step 3: Validating configuration...${NC}"

if terraform validate; then
    echo -e "${GREEN}✓ Configuration is valid${NC}"
else
    echo -e "${RED}✗ Configuration validation failed${NC}"
    exit 1
fi

echo ""

# ============================================================================
# Step 4: Terraform Plan
# ============================================================================

echo -e "${BLUE}Step 4: Creating execution plan...${NC}"

if terraform plan -out=tfplan; then
    echo -e "${GREEN}✓ Execution plan created${NC}"
else
    echo -e "${RED}✗ Plan creation failed${NC}"
    exit 1
fi

echo ""

# ============================================================================
# Step 5: Review and Confirm
# ============================================================================

echo -e "${YELLOW}=========================================${NC}"
echo -e "${YELLOW}REVIEW REQUIRED${NC}"
echo -e "${YELLOW}=========================================${NC}"
echo ""
echo "Please review the plan above."
echo ""
read -p "Do you want to apply this plan? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo -e "${YELLOW}Deployment cancelled${NC}"
    rm -f tfplan
    exit 0
fi

echo ""

# ============================================================================
# Step 6: Terraform Apply
# ============================================================================

echo -e "${BLUE}Step 6: Applying configuration...${NC}"

if terraform apply tfplan; then
    echo -e "${GREEN}✓ Configuration applied successfully${NC}"
else
    echo -e "${RED}✗ Apply failed${NC}"
    rm -f tfplan
    exit 1
fi

# Clean up plan file
rm -f tfplan

echo ""

# ============================================================================
# Step 7: Save Outputs
# ============================================================================

echo -e "${BLUE}Step 7: Saving outputs...${NC}"

if terraform output -json > outputs.json; then
    echo -e "${GREEN}✓ Outputs saved to outputs.json${NC}"
else
    echo -e "${RED}✗ Failed to save outputs${NC}"
    exit 1
fi

echo ""

# ============================================================================
# Summary
# ============================================================================

echo -e "${BLUE}=========================================${NC}"
echo -e "${GREEN}✅ Deployment Complete!${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""
echo -e "${BLUE}Outputs:${NC}"
terraform output

echo ""
echo -e "${BLUE}Next Steps:${NC}"
echo "1. Note the IAM role ARN from the output above"
echo "2. Create Snowflake storage integration using:"
echo "   snowflake/setup/04_create_storage_integration.sql"
echo "3. Update terraform.tfvars with the external ID from Snowflake"
echo "4. Re-run this script to update IAM trust policy"
echo "5. Create Snowflake stages using:"
echo "   snowflake/setup/05_create_stages.sql"
echo ""

exit 0
