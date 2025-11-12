#!/bin/bash
# ============================================================================
# End-to-End Setup Script for Customer 360 Analytics Platform
# ============================================================================
# Purpose: Orchestrate complete setup from data generation to S3 upload
# Usage: ./scripts/setup_end_to_end.sh
# ============================================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}Customer 360 Analytics Platform Setup${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""

# ============================================================================
# Step 1: Generate Customer Data
# ============================================================================

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Step 1: Generating Customer Data${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

cd "$PROJECT_ROOT"

if [ -f "data/customers.csv" ]; then
    echo -e "${YELLOW}⚠️  customers.csv already exists${NC}"
    read -p "Do you want to regenerate? (yes/no): " regenerate
    if [ "$regenerate" == "yes" ]; then
        echo "Generating 50,000 customers..."
        uv run python -m data_generation generate-customers \
            --count 50000 \
            --output data/customers.csv \
            --seed 42
    else
        echo -e "${GREEN}✓ Using existing customers.csv${NC}"
    fi
else
    echo "Generating 50,000 customers..."
    uv run python -m data_generation generate-customers \
        --count 50000 \
        --output data/customers.csv \
        --seed 42
fi

echo ""

# ============================================================================
# Step 2: Apply Terraform
# ============================================================================

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Step 2: Applying Terraform Configuration${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

if [ ! -f "$PROJECT_ROOT/terraform/terraform.tfvars" ]; then
    echo -e "${RED}✗ terraform.tfvars not found${NC}"
    echo ""
    echo "Please create terraform/terraform.tfvars with:"
    echo "  snowflake_account_id  = \"976709231746\""
    echo "  snowflake_external_id = \"PLACEHOLDER\""
    echo "  aws_region            = \"us-east-1\""
    echo "  s3_bucket_name        = \"customer360-analytics-data-YYYYMMDD\""
    echo "  environment           = \"dev\""
    echo ""
    exit 1
fi

read -p "Run Terraform deployment? (yes/no): " run_terraform
if [ "$run_terraform" == "yes" ]; then
    cd "$PROJECT_ROOT/terraform"
    ./deploy.sh

    # Extract bucket name from outputs
    S3_BUCKET=$(terraform output -raw s3_bucket_name 2>/dev/null || echo "")
    IAM_ROLE_ARN=$(terraform output -raw iam_role_arn 2>/dev/null || echo "")

    echo ""
    echo -e "${GREEN}Terraform outputs saved${NC}"
    echo "  S3 Bucket: $S3_BUCKET"
    echo "  IAM Role ARN: $IAM_ROLE_ARN"
else
    echo -e "${YELLOW}Skipping Terraform deployment${NC}"
    # Try to get bucket from existing outputs
    cd "$PROJECT_ROOT/terraform"
    S3_BUCKET=$(terraform output -raw s3_bucket_name 2>/dev/null || echo "")
fi

echo ""

# ============================================================================
# Step 3: Create Snowflake Storage Integration
# ============================================================================

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Step 3: Snowflake Storage Integration${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

echo -e "${YELLOW}MANUAL STEPS REQUIRED:${NC}"
echo ""
echo "1. Open Snowflake and run:"
echo "   snowflake/setup/04_create_storage_integration.sql"
echo ""
echo "2. Replace placeholders with:"
echo "   <IAM_ROLE_ARN> = $IAM_ROLE_ARN"
echo "   <S3_BUCKET_NAME> = $S3_BUCKET"
echo ""
echo "3. Run DESC STORAGE INTEGRATION customer360_s3_integration;"
echo ""
echo "4. Copy the STORAGE_AWS_EXTERNAL_ID value"
echo ""
echo "5. Update terraform/terraform.tfvars with the external ID"
echo ""
echo "6. Re-run: cd terraform && ./deploy.sh"
echo ""
echo "7. Then run: snowflake/setup/05_create_stages.sql"
echo "   (Replace <S3_BUCKET_NAME> with: $S3_BUCKET)"
echo ""

read -p "Press Enter when you've completed Snowflake setup..."

echo ""

# ============================================================================
# Step 4: Upload Customer Data to S3
# ============================================================================

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Step 4: Uploading Customer Data to S3${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

cd "$PROJECT_ROOT"

if [ -z "$S3_BUCKET" ]; then
    read -p "Enter S3 bucket name: " S3_BUCKET
fi

echo "Uploading customers.csv to s3://$S3_BUCKET/customers/"

uv run python -m data_generation upload-customers \
    --file data/customers.csv \
    --bucket "$S3_BUCKET"

echo ""

# ============================================================================
# Completion
# ============================================================================

echo -e "${BLUE}=========================================${NC}"
echo -e "${GREEN}✅ Setup Complete!${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""
echo -e "${GREEN}Completed steps:${NC}"
echo "  ✓ Generated 50,000 customers"
echo "  ✓ Applied Terraform (S3 + IAM)"
echo "  ✓ Created Snowflake storage integration and stages"
echo "  ✓ Uploaded customer data to S3"
echo ""
echo -e "${BLUE}Verify in Snowflake:${NC}"
echo "  LIST @CUSTOMER_ANALYTICS.BRONZE.customer_stage;"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo "  1. Load customers into Bronze layer (Iteration 2.3)"
echo "  2. Generate and load transactions (Iterations 2.4-2.5)"
echo "  3. Build dbt transformations (Phase 3)"
echo ""

exit 0
