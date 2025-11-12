#!/bin/bash
# ============================================================================
# Master Bronze Layer Data Load Script
# ============================================================================
# Purpose: Orchestrate complete Bronze layer table creation and data loading
# Usage: ./scripts/load_all_bronze.sh
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
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Bronze Layer Data Load${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# ============================================================================
# Prerequisites Check
# ============================================================================

echo -e "${BLUE}Checking prerequisites...${NC}"

# Check if SnowSQL is installed
if ! command -v snowsql &> /dev/null; then
    echo -e "${RED}✗ SnowSQL not found${NC}"
    echo ""
    echo "SnowSQL is required to run this script."
    echo "Please install SnowSQL: https://docs.snowflake.com/en/user-guide/snowsql-install-config.html"
    echo ""
    echo "Alternative: Run SQL scripts manually in Snowflake UI:"
    echo "  1. snowflake/setup/06_create_bronze_tables.sql"
    echo "  2. snowflake/setup/07_create_bronze_transaction_table.sql"
    echo "  3. snowflake/load/load_customers_bulk.sql"
    echo "  4. snowflake/load/load_transactions_bulk.sql"
    exit 1
fi
echo -e "${GREEN}✓ SnowSQL installed${NC}"

# Check if SQL files exist
REQUIRED_FILES=(
    "snowflake/setup/06_create_bronze_tables.sql"
    "snowflake/setup/07_create_bronze_transaction_table.sql"
    "snowflake/load/load_customers_bulk.sql"
    "snowflake/load/verify_customer_load.sql"
    "snowflake/load/load_transactions_bulk.sql"
    "snowflake/load/verify_transaction_load.sql"
)

for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$PROJECT_ROOT/$file" ]; then
        echo -e "${RED}✗ Required file not found: $file${NC}"
        exit 1
    fi
done
echo -e "${GREEN}✓ All SQL files found${NC}"

echo ""

# ============================================================================
# Display Load Plan
# ============================================================================

echo -e "${BLUE}Load Plan:${NC}"
echo "  Step 1: Create Bronze customer table"
echo "  Step 2: Create Bronze transaction table"
echo "  Step 3: Load customer data from S3 (50,000 rows)"
echo "  Step 4: Validate customer load"
echo "  Step 5: Load transaction data from S3 (~13.5M rows)"
echo "  Step 6: Validate transaction load"
echo ""

echo -e "${YELLOW}Note: This will use Snowflake compute credits${NC}"
echo -e "${YELLOW}Recommended warehouse: SMALL or MEDIUM${NC}"
echo -e "${YELLOW}Estimated duration: 15-30 minutes total${NC}"
echo ""

read -p "Do you want to proceed? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
    echo -e "${YELLOW}Load cancelled${NC}"
    exit 0
fi

echo ""

# ============================================================================
# Track overall timing
# ============================================================================

OVERALL_START_TIME=$(date +%s)

# ============================================================================
# Step 1: Create Bronze Customer Table
# ============================================================================

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Step 1: Creating Bronze Customer Table${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

STEP_START=$(date +%s)

if snowsql -f "$PROJECT_ROOT/snowflake/setup/06_create_bronze_tables.sql"; then
    STEP_END=$(date +%s)
    STEP_DURATION=$((STEP_END - STEP_START))
    echo ""
    echo -e "${GREEN}✓ Bronze customer table created (${STEP_DURATION}s)${NC}"
else
    echo ""
    echo -e "${RED}✗ Failed to create Bronze customer table${NC}"
    exit 1
fi

echo ""

# ============================================================================
# Step 2: Create Bronze Transaction Table
# ============================================================================

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Step 2: Creating Bronze Transaction Table${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

STEP_START=$(date +%s)

if snowsql -f "$PROJECT_ROOT/snowflake/setup/07_create_bronze_transaction_table.sql"; then
    STEP_END=$(date +%s)
    STEP_DURATION=$((STEP_END - STEP_START))
    echo ""
    echo -e "${GREEN}✓ Bronze transaction table created (${STEP_DURATION}s)${NC}"
else
    echo ""
    echo -e "${RED}✗ Failed to create Bronze transaction table${NC}"
    exit 1
fi

echo ""

# ============================================================================
# Step 3: Load Customer Data
# ============================================================================

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Step 3: Loading Customer Data${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo "Loading 50,000 customers from S3..."
echo ""

STEP_START=$(date +%s)

if snowsql -f "$PROJECT_ROOT/snowflake/load/load_customers_bulk.sql"; then
    STEP_END=$(date +%s)
    STEP_DURATION=$((STEP_END - STEP_START))
    echo ""
    echo -e "${GREEN}✓ Customer data loaded (${STEP_DURATION}s)${NC}"
else
    echo ""
    echo -e "${RED}✗ Failed to load customer data${NC}"
    exit 1
fi

echo ""

# ============================================================================
# Step 4: Validate Customer Load
# ============================================================================

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Step 4: Validating Customer Load${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

STEP_START=$(date +%s)

if snowsql -f "$PROJECT_ROOT/snowflake/load/verify_customer_load.sql"; then
    STEP_END=$(date +%s)
    STEP_DURATION=$((STEP_END - STEP_START))
    echo ""
    echo -e "${GREEN}✓ Customer validation complete (${STEP_DURATION}s)${NC}"
    echo -e "${YELLOW}Review validation results above for any warnings${NC}"
else
    echo ""
    echo -e "${RED}✗ Customer validation failed${NC}"
    exit 1
fi

echo ""

# ============================================================================
# Step 5: Load Transaction Data
# ============================================================================

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Step 5: Loading Transaction Data${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo "Loading ~13.5M transactions from S3..."
echo "This may take 10-20 minutes depending on warehouse size..."
echo ""

STEP_START=$(date +%s)

if snowsql -f "$PROJECT_ROOT/snowflake/load/load_transactions_bulk.sql"; then
    STEP_END=$(date +%s)
    STEP_DURATION=$((STEP_END - STEP_START))
    MINUTES=$((STEP_DURATION / 60))
    SECONDS=$((STEP_DURATION % 60))
    echo ""
    echo -e "${GREEN}✓ Transaction data loaded (${MINUTES}m ${SECONDS}s)${NC}"
else
    echo ""
    echo -e "${RED}✗ Failed to load transaction data${NC}"
    exit 1
fi

echo ""

# ============================================================================
# Step 6: Validate Transaction Load
# ============================================================================

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Step 6: Validating Transaction Load${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

STEP_START=$(date +%s)

if snowsql -f "$PROJECT_ROOT/snowflake/load/verify_transaction_load.sql"; then
    STEP_END=$(date +%s)
    STEP_DURATION=$((STEP_END - STEP_START))
    echo ""
    echo -e "${GREEN}✓ Transaction validation complete (${STEP_DURATION}s)${NC}"
    echo -e "${YELLOW}Review validation results above for any warnings${NC}"
else
    echo ""
    echo -e "${RED}✗ Transaction validation failed${NC}"
    exit 1
fi

echo ""

# ============================================================================
# Overall Summary
# ============================================================================

OVERALL_END_TIME=$(date +%s)
OVERALL_DURATION=$((OVERALL_END_TIME - OVERALL_START_TIME))
OVERALL_MINUTES=$((OVERALL_DURATION / 60))
OVERALL_SECONDS=$((OVERALL_DURATION % 60))

echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}✅ Bronze Layer Load Complete${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo "Summary:"
echo "  - Bronze customer table: Created ✓"
echo "  - Bronze transaction table: Created ✓"
echo "  - Customer data: 50,000 rows loaded ✓"
echo "  - Transaction data: ~13.5M rows loaded ✓"
echo "  - All validations: Complete ✓"
echo ""
echo "Total Duration: ${OVERALL_MINUTES}m ${OVERALL_SECONDS}s"
echo ""
echo "Next Steps:"
echo "  1. Review validation results above for any warnings"
echo "  2. Query Bronze tables to verify data:"
echo "     SELECT COUNT(*) FROM BRONZE.BRONZE_CUSTOMERS;"
echo "     SELECT COUNT(*) FROM BRONZE.BRONZE_TRANSACTIONS;"
echo "  3. Proceed to Phase 3: dbt Silver layer transformations"
echo ""

exit 0
