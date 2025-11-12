#!/bin/bash
# Integration test for Terraform configuration
# Validates Terraform syntax and runs plan with dummy variables (no apply)

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TERRAFORM_DIR="$PROJECT_ROOT/terraform"

echo "========================================="
echo "Terraform Configuration Integration Test"
echo "========================================="
echo ""

# Change to terraform directory
cd "$TERRAFORM_DIR"

echo "📁 Working directory: $(pwd)"
echo ""

# Test 1: Check if Terraform is installed
echo "🔍 Test 1: Checking Terraform installation..."
if ! command -v terraform &> /dev/null; then
    echo -e "${RED}❌ FAILED: Terraform is not installed${NC}"
    echo "   Install from: https://www.terraform.io/downloads"
    exit 1
fi

TERRAFORM_VERSION=$(terraform version -json | grep -o '"terraform_version":"[^"]*' | cut -d'"' -f4)
echo -e "${GREEN}✅ PASSED: Terraform installed (version: $TERRAFORM_VERSION)${NC}"
echo ""

# Test 2: Validate Terraform configuration files exist
echo "🔍 Test 2: Checking required Terraform files..."
REQUIRED_FILES=("variables.tf" "main.tf" "s3.tf" "iam.tf" "outputs.tf" "README.md" "terraform.tfvars.example")
MISSING_FILES=()

for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        MISSING_FILES+=("$file")
    fi
done

if [ ${#MISSING_FILES[@]} -gt 0 ]; then
    echo -e "${RED}❌ FAILED: Missing required files:${NC}"
    for file in "${MISSING_FILES[@]}"; do
        echo "   - $file"
    done
    exit 1
fi

echo -e "${GREEN}✅ PASSED: All required Terraform files exist${NC}"
echo ""

# Test 3: Initialize Terraform
echo "🔍 Test 3: Initializing Terraform..."
if terraform init -backend=false > /dev/null 2>&1; then
    echo -e "${GREEN}✅ PASSED: Terraform initialization successful${NC}"
else
    echo -e "${RED}❌ FAILED: Terraform initialization failed${NC}"
    terraform init -backend=false
    exit 1
fi
echo ""

# Test 4: Validate Terraform configuration
echo "🔍 Test 4: Validating Terraform configuration syntax..."
if terraform validate > /dev/null 2>&1; then
    echo -e "${GREEN}✅ PASSED: Terraform configuration is valid${NC}"
else
    echo -e "${RED}❌ FAILED: Terraform configuration validation failed${NC}"
    terraform validate
    exit 1
fi
echo ""

# Test 5: Format check
echo "🔍 Test 5: Checking Terraform formatting..."
if terraform fmt -check -recursive > /dev/null 2>&1; then
    echo -e "${GREEN}✅ PASSED: Terraform files are properly formatted${NC}"
else
    echo -e "${YELLOW}⚠️  WARNING: Some Terraform files need formatting${NC}"
    echo "   Run: terraform fmt -recursive"
    # Don't fail the test, just warn
fi
echo ""

# Test 6: Create dummy tfvars for plan test
echo "🔍 Test 6: Running Terraform plan with dummy variables..."

# Create temporary tfvars file with dummy values
TEMP_TFVARS=$(mktemp)
cat > "$TEMP_TFVARS" << EOF
project_name          = "test-project"
environment           = "demo"
aws_region            = "us-east-1"
snowflake_account_id  = "123456789012"
snowflake_external_id = "TEST_EXTERNAL_ID_12345"
EOF

echo "   Using dummy variables for testing (no resources will be created)"

# Run terraform plan (should succeed even without AWS credentials since we're not applying)
if terraform plan -var-file="$TEMP_TFVARS" -out=/dev/null > /dev/null 2>&1; then
    echo -e "${GREEN}✅ PASSED: Terraform plan executes successfully${NC}"
    SUCCESS=true
else
    # Plan might fail due to missing AWS credentials, which is acceptable for this test
    # Check if it's a credentials issue or a configuration issue
    PLAN_OUTPUT=$(terraform plan -var-file="$TEMP_TFVARS" 2>&1 || true)

    if echo "$PLAN_OUTPUT" | grep -q "NoCredentialProviders\|InvalidClientTokenId\|InvalidAccessKeyId\|no valid credential sources"; then
        echo -e "${YELLOW}⚠️  WARNING: AWS credentials not configured (expected for CI/CD)${NC}"
        echo "   Terraform configuration syntax is valid"
        echo "   Note: Actual infrastructure deployment requires AWS credentials"
        SUCCESS=true
    else
        echo -e "${RED}❌ FAILED: Terraform plan failed with configuration errors${NC}"
        echo "$PLAN_OUTPUT"
        SUCCESS=false
    fi
fi

# Clean up temp file
rm -f "$TEMP_TFVARS"

if [ "$SUCCESS" = false ]; then
    exit 1
fi
echo ""

# Test 7: Check for common issues
echo "🔍 Test 7: Checking for common configuration issues..."
ISSUES_FOUND=false

# Check if hardcoded values exist in main configuration files
if grep -r "hardcoded" --include="*.tf" . > /dev/null 2>&1; then
    echo -e "${YELLOW}⚠️  WARNING: Found hardcoded values in Terraform files${NC}"
    ISSUES_FOUND=true
fi

# Check if sensitive data might be in files
if grep -rE "(password|secret|key)" --include="*.tf" . | grep -v "variable\|description\|aws_secret\|bucket_key" > /dev/null 2>&1; then
    echo -e "${YELLOW}⚠️  WARNING: Potential sensitive data found in Terraform files${NC}"
    echo "   Review: $(grep -rE "(password|secret|key)" --include="*.tf" . | grep -v "variable\|description\|aws_secret\|bucket_key")"
    ISSUES_FOUND=true
fi

# Check if terraform.tfvars exists (should be git-ignored)
if [ -f "terraform.tfvars" ]; then
    echo -e "${YELLOW}⚠️  WARNING: terraform.tfvars file exists${NC}"
    echo "   Ensure this file is in .gitignore and not committed"
    ISSUES_FOUND=true
fi

if [ "$ISSUES_FOUND" = false ]; then
    echo -e "${GREEN}✅ PASSED: No common issues detected${NC}"
fi
echo ""

# Test 8: Verify outputs are defined
echo "🔍 Test 8: Verifying Terraform outputs..."
REQUIRED_OUTPUTS=("s3_bucket_name" "s3_bucket_arn" "iam_role_arn" "iam_role_name")
MISSING_OUTPUTS=()

for output in "${REQUIRED_OUTPUTS[@]}"; do
    if ! grep -q "output \"$output\"" outputs.tf; then
        MISSING_OUTPUTS+=("$output")
    fi
done

if [ ${#MISSING_OUTPUTS[@]} -gt 0 ]; then
    echo -e "${RED}❌ FAILED: Missing required outputs:${NC}"
    for output in "${MISSING_OUTPUTS[@]}"; do
        echo "   - $output"
    done
    exit 1
fi

echo -e "${GREEN}✅ PASSED: All required outputs are defined${NC}"
echo ""

# Summary
echo "========================================="
echo "✅ All Terraform integration tests passed!"
echo "========================================="
echo ""
echo "Next steps:"
echo "1. Configure AWS credentials: aws configure"
echo "2. Create terraform.tfvars with actual values"
echo "3. Run: terraform plan"
echo "4. Run: terraform apply"
echo ""

exit 0
