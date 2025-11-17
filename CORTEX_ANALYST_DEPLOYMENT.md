# Cortex Analyst Deployment Guide

## Overview

This guide walks you through deploying Snowflake Cortex Analyst integration for the AI Assistant tab in the Customer 360 Analytics Platform.

**What is Cortex Analyst?**
- LLM-powered natural language to SQL feature from Snowflake
- Uses semantic models to understand your data schema
- Generates SQL queries from natural language questions
- Supports multi-turn conversations with context

**Current Implementation:**
- Real Cortex Analyst integration with automatic fallback to mock implementation
- Semantic model defines 3 tables (customer_profile, transactions, merchant_categories)
- 8 verified queries for common business questions
- Support for conversation history and context

---

## Prerequisites

1. **Snowflake Account Requirements:**
   - Cortex Analyst must be available in your Snowflake account
   - Account must be in a supported region (US, EU)
   - ACCOUNTADMIN role or sufficient privileges

2. **Required Roles:**
   - `ACCOUNTADMIN` - for initial setup
   - `DATA_ANALYST` - for running Streamlit app (already created)

3. **Existing Infrastructure:**
   - Customer 360 Analytics Platform deployed
   - Data in CUSTOMER_ANALYTICS.GOLD schema
   - Streamlit app deployed

---

## Deployment Steps

### Step 1: Setup Cortex Analyst Infrastructure

Run the infrastructure setup script to create the database, schema, and stage for semantic models:

```bash
snowsql -c default -f snowflake/setup/cortex_analyst_setup.sql
```

**What this does:**
- Creates `SEMANTIC_MODELS` database
- Creates `SEMANTIC_MODELS.DEFINITIONS` schema
- Creates `SEMANTIC_STAGE` with directory enabled
- Creates `CORTEX_ANALYST_ROLE` with necessary privileges
- Grants permissions to `DATA_ANALYST` role
- Validates setup

**Expected Output:**
```
Database SEMANTIC_MODELS created
Schema DEFINITIONS created
Stage SEMANTIC_STAGE created
Grants completed
Status: Cortex Analyst infrastructure setup complete!
```

---

### Step 2: Deploy Semantic Model

Upload the semantic model YAML file to the Snowflake stage:

#### Option A: Automated Deployment (Recommended)

From the project root directory:

```bash
cd /Users/jpurrutia/projects/snowflake-panel-demo
snowsql -c default -f snowflake/setup/deploy_semantic_model.sql
```

#### Option B: Manual Deployment

```bash
snowsql -c default

USE ROLE ACCOUNTADMIN;
USE DATABASE SEMANTIC_MODELS;
USE SCHEMA DEFINITIONS;

-- Update path to match your local directory
PUT file:///Users/jpurrutia/projects/snowflake-panel-demo/semantic_models/customer_analytics.yaml
    @SEMANTIC_STAGE
    AUTO_COMPRESS=FALSE
    OVERWRITE=TRUE;

-- Verify upload
LIST @SEMANTIC_STAGE;
```

**Expected Output:**
```
customer_analytics.yaml | 45678 | ...
Semantic model deployed successfully!
```

---

### Step 3: Redeploy Streamlit App

The updated `ai_assistant.py` is already in your codebase. Redeploy the Streamlit app via CI/CD:

```bash
# Commit and push changes
git add streamlit/tabs/ai_assistant.py
git commit -m "Integrate Cortex Analyst for AI Assistant"
git push origin main

# GitHub Actions will automatically redeploy the Streamlit app
```

Or manually redeploy:

```bash
cd streamlit
snow streamlit deploy --replace
```

**What changed in ai_assistant.py:**
- Real Cortex Analyst integration using `SNOWFLAKE.CORTEX.COMPLETE()`
- Conversation history support for multi-turn conversations
- Automatic fallback to mock implementation if Cortex unavailable
- Display AI interpretation alongside generated SQL
- Updated UI to reflect Cortex Analyst status

---

### Step 4: Verify Deployment

#### 4.1 Check Semantic Model Upload

```sql
USE ROLE ACCOUNTADMIN;
USE DATABASE SEMANTIC_MODELS;

-- List files in stage
LIST @DEFINITIONS.SEMANTIC_STAGE;

-- Preview semantic model contents
SELECT $1 FROM @DEFINITIONS.SEMANTIC_STAGE/customer_analytics.yaml LIMIT 10;
```

#### 4.2 Test Cortex Analyst Directly

```sql
USE ROLE DATA_ANALYST;
USE DATABASE CUSTOMER_ANALYTICS;
USE WAREHOUSE COMPUTE_WH;

-- Test simple query
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'analyst',
    '[{"role": "user", "content": [{"type": "text", "text": "How many customers are in each segment?"}]}]',
    {
        'semantic_model_file': '@SEMANTIC_MODELS.DEFINITIONS.SEMANTIC_STAGE/customer_analytics.yaml'
    }
) AS response;
```

**Expected Response:**
```json
{
  "sql": "SELECT customer_segment, COUNT(*) AS customer_count FROM CUSTOMER_ANALYTICS.GOLD.CUSTOMER_360_PROFILE GROUP BY customer_segment",
  "interpretation": "This query counts the number of customers in each behavioral segment."
}
```

#### 4.3 Test in Streamlit App

1. Open Streamlit app: https://[your-snowflake-account].snowflakecomputing.com/streamlit/CUSTOMER_360_APP
2. Navigate to "AI Assistant" tab
3. Try a suggested question: "Which customers are at highest risk of churning?"
4. Verify:
   - ✅ No warning about mock implementation
   - ✅ SQL is generated
   - ✅ Results are displayed
   - ✅ AI interpretation shown (if available)

---

## Semantic Model Overview

The semantic model defines the business terminology and relationships that Cortex Analyst uses to generate SQL.

### Tables Defined

1. **customer_profile** → `CUSTOMER_ANALYTICS.GOLD.CUSTOMER_360_PROFILE`
   - Demographics, segments, metrics
   - 40+ dimensions and measures
   - Includes churn risk, LTV, spending patterns

2. **transactions** → `CUSTOMER_ANALYTICS.GOLD.FCT_TRANSACTIONS`
   - Transaction fact table
   - Measures: transaction_amount, transaction_count
   - Time dimension: transaction_date

3. **merchant_categories** → `CUSTOMER_ANALYTICS.GOLD.DIM_MERCHANT_CATEGORY`
   - Category classifications
   - Dimensions: category_name, category_group, spending_type

### Verified Queries

The semantic model includes 8 pre-verified queries for common questions:

1. Top customers by lifetime value
2. High churn risk customers
3. Customer segment summary
4. Transaction volume by month
5. Spending by category
6. Declining customers analysis
7. Premium vs standard card performance
8. Customer distribution by state

These queries serve as examples for Cortex Analyst and improve response accuracy.

---

## Troubleshooting

### Issue: "Cortex Analyst not available" warning

**Cause:** Cortex Analyst feature not enabled in your Snowflake account

**Solution:**
- Contact Snowflake support to enable Cortex Analyst
- Or continue using mock implementation (automatically activated)

**Check:**
```sql
-- Verify Cortex functions are available
SHOW FUNCTIONS LIKE '%CORTEX%' IN ACCOUNT;
```

---

### Issue: "Semantic model not found" error

**Cause:** Semantic model YAML not uploaded to stage

**Solution:**
```sql
-- Check if file exists in stage
LIST @SEMANTIC_MODELS.DEFINITIONS.SEMANTIC_STAGE;

-- If missing, re-run deployment
PUT file:///path/to/semantic_models/customer_analytics.yaml @SEMANTIC_STAGE;
```

---

### Issue: "Permission denied" errors

**Cause:** DATA_ANALYST role lacks CORTEX_USER privilege

**Solution:**
```sql
USE ROLE ACCOUNTADMIN;

-- Grant Cortex privilege to DATA_ANALYST
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE DATA_ANALYST;

-- Verify grants
SHOW GRANTS TO ROLE DATA_ANALYST;
```

---

### Issue: Generated SQL returns no results

**Cause:** Semantic model may not accurately reflect actual schema

**Solution:**
```sql
-- Test table access
SELECT COUNT(*) FROM CUSTOMER_ANALYTICS.GOLD.CUSTOMER_360_PROFILE;
SELECT COUNT(*) FROM CUSTOMER_ANALYTICS.GOLD.FCT_TRANSACTIONS;

-- Compare semantic model table definitions with actual schema
DESCRIBE TABLE CUSTOMER_ANALYTICS.GOLD.CUSTOMER_360_PROFILE;
```

Update semantic model if schema has changed, then redeploy.

---

### Issue: Cortex Analyst generates incorrect SQL

**Cause:** Ambiguous question or semantic model needs refinement

**Solution:**
1. Rephrase question to be more specific
2. Use terms defined in semantic model (see verified queries for examples)
3. Add synonyms to semantic model for better understanding
4. Add more verified queries for common patterns

---

## Monitoring and Maintenance

### Monitor Cortex Analyst Usage

```sql
-- View usage history (requires ACCOUNTADMIN)
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_ANALYST_USAGE_HISTORY
WHERE QUERY_TEXT LIKE '%customer_analytics%'
ORDER BY START_TIME DESC
LIMIT 100;
```

### Update Semantic Model

When you update the semantic model:

1. Edit `semantic_models/customer_analytics.yaml`
2. Test locally if possible
3. Redeploy to Snowflake:
   ```bash
   snowsql -c default -f snowflake/setup/deploy_semantic_model.sql
   ```
4. No need to redeploy Streamlit app (semantic model is referenced, not embedded)

### Best Practices

1. **Add Verified Queries:** When users ask good questions that work well, add them as verified queries
2. **Monitor Performance:** Track which questions work well and which need refinement
3. **Update Synonyms:** Add business terminology as synonyms when users struggle with specific terms
4. **Version Control:** Keep semantic model in Git and version it
5. **Test Changes:** Always test semantic model changes before deploying to production

---

## Fallback Behavior

The AI Assistant is designed to gracefully handle Cortex Analyst unavailability:

1. **Primary Mode:** Tries to use real Cortex Analyst
2. **Automatic Fallback:** If Cortex unavailable, switches to mock implementation
3. **User Notification:** Shows warning when using mock
4. **Limited Functionality:** Mock supports only pre-defined questions

**Mock Implementation Supports:**
- "Which customers are at highest risk of churning?"
- "How many customers are in each segment?"
- "Compare lifetime value across segments"
- "What is the total spending in the last 90 days?"
- "Which Premium cardholders are at medium or high risk?"

All other questions will return: "Question not recognized by mock. Try a suggested question or wait for Cortex Analyst integration."

---

## Files Reference

| File | Purpose | When to Use |
|------|---------|-------------|
| `snowflake/setup/cortex_analyst_setup.sql` | Infrastructure setup | One-time initial setup |
| `snowflake/setup/deploy_semantic_model.sql` | Upload semantic model | Initial deployment + updates |
| `semantic_models/customer_analytics.yaml` | Semantic model definition | Edit to refine AI understanding |
| `streamlit/tabs/ai_assistant.py` | Streamlit UI + integration | Already deployed via CI/CD |

---

## Support and Documentation

**Snowflake Documentation:**
- [Cortex Analyst Overview](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst)
- [Semantic Models](https://docs.snowflake.com/en/user-guide/views-semantic/overview)
- [CORTEX.COMPLETE Function](https://docs.snowflake.com/en/sql-reference/functions/complete-snowflake-cortex)

**Project Documentation:**
- `ML_PIPELINE_GUIDE.md` - ML churn prediction pipeline
- `data_flow.md` - Overall data architecture

---

## Next Steps

After successful deployment:

1. ✅ Test with suggested questions
2. ✅ Try custom questions in your domain
3. ✅ Monitor usage and refine semantic model
4. ✅ Add more verified queries for common patterns
5. ✅ Train users on how to ask effective questions
6. ✅ Set up monitoring dashboards for Cortex usage

Congratulations! Your AI Assistant is now powered by Snowflake Cortex Analyst.
