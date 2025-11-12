#!/bin/bash
# ============================================================================
# Run Transaction Generation Script
# ============================================================================
# Purpose: Execute Snowflake SQL script to generate 13.5M transactions
# Usage: ./snowflake/data_generation/run_transaction_generation.sh
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
SQL_FILE="$SCRIPT_DIR/generate_transactions.sql"

echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}Transaction Data Generation${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""

# ============================================================================
# Prerequisites Check
# ============================================================================

echo -e "${BLUE}Checking prerequisites...${NC}"

# Check if SQL file exists
if [ ! -f "$SQL_FILE" ]; then
    echo -e "${RED}✗ SQL file not found: $SQL_FILE${NC}"
    exit 1
fi
echo -e "${GREEN}✓ SQL file found${NC}"

# Check if SnowSQL is installed
if ! command -v snowsql &> /dev/null; then
    echo -e "${YELLOW}⚠️  SnowSQL not found${NC}"
    echo ""
    echo "SnowSQL is not installed. You have two options:"
    echo ""
    echo "Option 1: Install SnowSQL and configure credentials"
    echo "  https://docs.snowflake.com/en/user-guide/snowsql-install-config.html"
    echo ""
    echo "Option 2: Run SQL script manually in Snowflake UI"
    echo "  1. Copy contents of: $SQL_FILE"
    echo "  2. Paste into Snowflake worksheet"
    echo "  3. Execute the script"
    echo ""
    exit 1
fi
echo -e "${GREEN}✓ SnowSQL installed${NC}"

echo ""

# ============================================================================
# Display Information
# ============================================================================

echo -e "${BLUE}Generation Details:${NC}"
echo "  Target Rows: ~13.5 million transactions"
echo "  Time Period: 18 months"
echo "  Customers: 50,000"
echo "  Output: S3 stage (compressed CSV files)"
echo ""

echo -e "${YELLOW}Note: This will use Snowflake compute credits${NC}"
echo -e "${YELLOW}Recommended warehouse: SMALL or MEDIUM${NC}"
echo -e "${YELLOW}Estimated duration: 5-15 minutes${NC}"
echo ""

read -p "Do you want to proceed? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
    echo -e "${YELLOW}Generation cancelled${NC}"
    exit 0
fi

echo ""

# ============================================================================
# Execute SQL Script
# ============================================================================

echo -e "${BLUE}Starting transaction generation...${NC}"
echo "Start time: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

START_TIME=$(date +%s)

# Execute SQL file with SnowSQL
if snowsql -f "$SQL_FILE"; then
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    MINUTES=$((DURATION / 60))
    SECONDS=$((DURATION % 60))

    echo ""
    echo -e "${GREEN}✓ Generation completed successfully${NC}"
    echo "End time: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "Duration: ${MINUTES}m ${SECONDS}s"
else
    echo ""
    echo -e "${RED}✗ Generation failed${NC}"
    echo "Check Snowflake query history for error details"
    exit 1
fi

echo ""

# ============================================================================
# Summary
# ============================================================================

echo -e "${BLUE}=========================================${NC}"
echo -e "${GREEN}✅ Transaction Generation Complete${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""
echo "Summary:"
echo "  - Transactions generated in Snowflake temp tables"
echo "  - Files exported to S3 stage: @transaction_stage_historical"
echo "  - Files are GZIP compressed CSV format"
echo "  - Each file is up to 100MB"
echo ""
echo "Verify generation:"
echo "  1. Check file count in S3:"
echo "     LIST @CUSTOMER_ANALYTICS.BRONZE.transaction_stage_historical;"
echo ""
echo "  2. Check row count (if still in temp table):"
echo "     SELECT COUNT(*) FROM transactions_with_details;"
echo ""
echo "Next steps:"
echo "  1. Verify transaction data quality"
echo "  2. Proceed to Iteration 2.5: Load transactions into Bronze layer"
echo ""

exit 0
