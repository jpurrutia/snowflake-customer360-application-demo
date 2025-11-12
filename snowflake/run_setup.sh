#!/bin/bash
# ============================================================================
# Snowflake Foundation Setup Script
# ============================================================================
# Purpose: Execute all Snowflake setup SQL scripts in order
# Usage: ./snowflake/run_setup.sh
# Requires: SnowSQL installed and configured, or manual SQL execution
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

echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}Snowflake Foundation Setup${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""

# ============================================================================
# Check for SnowSQL
# ============================================================================

if ! command -v snowsql &> /dev/null; then
    echo -e "${YELLOW}⚠️  SnowSQL not found${NC}"
    echo ""
    echo "SnowSQL is not installed or not in PATH."
    echo "You have two options:"
    echo ""
    echo "Option 1: Install SnowSQL"
    echo "  https://docs.snowflake.com/en/user-guide/snowsql-install-config.html"
    echo ""
    echo "Option 2: Run SQL scripts manually in Snowflake UI"
    echo "  1. Log into Snowflake web UI"
    echo "  2. Execute each SQL file in order:"
    echo "     - snowflake/setup/00_environment_check.sql"
    echo "     - snowflake/setup/01_create_database_schemas.sql"
    echo "     - snowflake/setup/02_create_roles_grants.sql"
    echo "     - snowflake/setup/03_create_observability_tables.sql"
    echo ""
    exit 1
fi

# ============================================================================
# Load Environment Variables
# ============================================================================

if [ -f "$PROJECT_ROOT/.env" ]; then
    echo -e "${GREEN}📄 Loading environment variables from .env${NC}"
    export $(cat "$PROJECT_ROOT/.env" | grep -v '^#' | xargs)
else
    echo -e "${YELLOW}⚠️  .env file not found${NC}"
    echo "Please create .env file with Snowflake credentials:"
    echo "  SNOWFLAKE_ACCOUNT=your-account"
    echo "  SNOWFLAKE_USER=your-username"
    echo "  SNOWFLAKE_PASSWORD=your-password"
    echo "  SNOWFLAKE_WAREHOUSE=COMPUTE_WH"
    echo "  SNOWFLAKE_DATABASE=CUSTOMER_ANALYTICS"
    echo "  SNOWFLAKE_ROLE=SYSADMIN"
    echo ""
    exit 1
fi

# Verify required variables
if [ -z "$SNOWFLAKE_ACCOUNT" ] || [ -z "$SNOWFLAKE_USER" ] || [ -z "$SNOWFLAKE_PASSWORD" ]; then
    echo -e "${RED}❌ Missing required Snowflake credentials in .env${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Environment variables loaded${NC}"
echo "  Account: $SNOWFLAKE_ACCOUNT"
echo "  User: $SNOWFLAKE_USER"
echo ""

# ============================================================================
# Execute SQL Scripts
# ============================================================================

SQL_FILES=(
    "00_environment_check.sql"
    "01_create_database_schemas.sql"
    "02_create_roles_grants.sql"
    "03_create_observability_tables.sql"
)

SETUP_DIR="$SCRIPT_DIR/setup"

for sql_file in "${SQL_FILES[@]}"; do
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}Executing: $sql_file${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    if [ ! -f "$SETUP_DIR/$sql_file" ]; then
        echo -e "${RED}❌ File not found: $SETUP_DIR/$sql_file${NC}"
        exit 1
    fi

    # Execute SQL file with SnowSQL
    if snowsql \
        -a "$SNOWFLAKE_ACCOUNT" \
        -u "$SNOWFLAKE_USER" \
        -f "$SETUP_DIR/$sql_file" \
        --authenticator externalbrowser \
        -o exit_on_error=true \
        -o friendly=false \
        -o output_format=psql; then
        echo -e "${GREEN}✓ Successfully executed: $sql_file${NC}"
        echo ""
    else
        echo -e "${RED}❌ Failed to execute: $sql_file${NC}"
        echo -e "${RED}Setup aborted${NC}"
        exit 1
    fi
done

# ============================================================================
# Summary
# ============================================================================

echo -e "${BLUE}=========================================${NC}"
echo -e "${GREEN}✅ Snowflake Foundation Setup Complete!${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""
echo "Created:"
echo "  ✓ Database: CUSTOMER_ANALYTICS"
echo "  ✓ Schemas: BRONZE, SILVER, GOLD, OBSERVABILITY"
echo "  ✓ Roles: DATA_ENGINEER, MARKETING_MANAGER, DATA_ANALYST"
echo "  ✓ Observability Tables: 4 tables + 3 views"
echo ""
echo "Next steps:"
echo "  1. Verify setup: Run queries in Snowflake UI"
echo "  2. Test RBAC: Switch to different roles and test permissions"
echo "  3. Proceed to Prompt 2.1: Data Generation"
echo ""

exit 0
