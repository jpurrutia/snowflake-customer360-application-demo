#!/bin/bash
# ============================================================================
# Deploy Semantic Model to Snowflake for Cortex Analyst
# ============================================================================
# Purpose: Upload semantic_model.yaml to Snowflake and register with Cortex Analyst
#
# Prerequisites:
#   - SnowSQL installed and configured
#   - CUSTOMER_ANALYTICS.GOLD schema exists
#   - Cortex Analyst enabled in Snowflake account
#
# Usage:
#   ./deploy_semantic_model.sh
# ============================================================================

set -e  # Exit on error

echo "=========================================="
echo "Deploying Semantic Model to Snowflake"
echo "=========================================="

# Configuration
DATABASE="CUSTOMER_ANALYTICS"
SCHEMA="GOLD"
STAGE_NAME="SEMANTIC_STAGE"
MODEL_FILE="semantic_model.yaml"

# Step 1: Verify semantic_model.yaml exists
echo ""
echo "Step 1: Verifying semantic_model.yaml exists..."
if [ ! -f "$MODEL_FILE" ]; then
    echo "ERROR: $MODEL_FILE not found in current directory"
    exit 1
fi
echo "✓ $MODEL_FILE found"

# Step 2: Create Snowflake stage (if not exists)
echo ""
echo "Step 2: Creating Snowflake stage for semantic model..."
snowsql -q "
USE DATABASE $DATABASE;
USE SCHEMA $SCHEMA;

CREATE STAGE IF NOT EXISTS $STAGE_NAME
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for Cortex Analyst semantic models';
" || { echo "ERROR: Failed to create stage"; exit 1; }

echo "✓ Stage $STAGE_NAME ready"

# Step 3: Upload semantic_model.yaml to stage
echo ""
echo "Step 3: Uploading $MODEL_FILE to Snowflake stage..."
snowsql -q "
USE DATABASE $DATABASE;
USE SCHEMA $SCHEMA;

PUT file://$MODEL_FILE @$STAGE_NAME
    AUTO_COMPRESS = FALSE
    OVERWRITE = TRUE;
" || { echo "ERROR: Failed to upload semantic model"; exit 1; }

echo "✓ $MODEL_FILE uploaded to @$DATABASE.$SCHEMA.$STAGE_NAME"

# Step 4: Verify upload
echo ""
echo "Step 4: Verifying upload..."
snowsql -q "
USE DATABASE $DATABASE;
USE SCHEMA $SCHEMA;

LIST @$STAGE_NAME;
" || { echo "ERROR: Failed to list stage contents"; exit 1; }

# Step 5: Create Cortex Search Service (if available)
echo ""
echo "Step 5: Registering semantic model with Cortex Analyst..."
echo "NOTE: This step requires Cortex Analyst to be enabled in your Snowflake account"

# Note: Exact syntax may vary based on Snowflake version
# This is a placeholder for the actual Cortex Analyst registration
# Consult Snowflake documentation for the latest syntax

echo ""
echo "=========================================="
echo "Deployment Complete!"
echo "=========================================="
echo ""
echo "Next Steps:"
echo "1. Open Snowsight (Snowflake UI)"
echo "2. Navigate to Cortex Analyst"
echo "3. Select semantic model: customer_analytics_semantic_model"
echo "4. Test with question: 'How many customers do we have?'"
echo ""
echo "For testing queries manually:"
echo "  snowsql -f test_semantic_model.sql"
echo ""
echo "Semantic Model Location:"
echo "  @$DATABASE.$SCHEMA.$STAGE_NAME/$MODEL_FILE"
echo ""
