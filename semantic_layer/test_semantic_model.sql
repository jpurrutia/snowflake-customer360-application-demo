-- ============================================================================
-- Test Semantic Model with Sample Queries
-- ============================================================================
-- Purpose: Validate semantic model by testing representative queries
--
-- These queries simulate natural language questions that Cortex Analyst
-- would convert to SQL. Running these ensures:
-- 1. All referenced tables and columns exist
-- 2. Metrics are calculable
-- 3. Relationships (joins) work correctly
-- 4. Queries return meaningful results
--
-- Usage:
--   Run in Snowflake SQL Worksheet or SnowSQL
--   Each section tests a different aspect of the semantic model
-- ============================================================================

USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA GOLD;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================================
-- Test 1: Customer Profile Queries
-- ============================================================================

-- Question: "What is the average spend of customers in California?"
SELECT
    state,
    AVG(lifetime_value) AS avg_lifetime_value,
    AVG(avg_monthly_spend) AS avg_monthly_spend,
    COUNT(*) AS customer_count
FROM CUSTOMER_360_PROFILE
WHERE state = 'CA'
GROUP BY state;

-- Question: "Show me customers in Texas spending over $5K per month"
SELECT
    customer_id,
    full_name,
    state,
    avg_monthly_spend,
    lifetime_value,
    customer_segment
FROM CUSTOMER_360_PROFILE
WHERE state = 'TX'
  AND avg_monthly_spend > 5000
ORDER BY avg_monthly_spend DESC
LIMIT 20;

-- Question: "How many High-Value Travelers are there?"
SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    AVG(lifetime_value) AS avg_ltv,
    AVG(avg_monthly_spend) AS avg_monthly_spend
FROM CUSTOMER_360_PROFILE
WHERE customer_segment = 'High-Value Travelers'
GROUP BY customer_segment;

-- Question: "Which customers have a lifetime value over $100,000?"
SELECT
    customer_id,
    full_name,
    state,
    customer_segment,
    lifetime_value,
    total_transactions,
    avg_transaction_value
FROM CUSTOMER_360_PROFILE
WHERE lifetime_value > 100000
ORDER BY lifetime_value DESC
LIMIT 10;

-- ============================================================================
-- Test 2: Churn Risk Queries
-- ============================================================================

-- Question: "Which customers are at highest risk of churning?"
SELECT
    customer_id,
    full_name,
    customer_segment,
    churn_risk_score,
    churn_risk_category,
    lifetime_value,
    days_since_last_transaction,
    spend_change_pct
FROM CUSTOMER_360_PROFILE
WHERE churn_risk_category = 'High Risk'
ORDER BY churn_risk_score DESC, lifetime_value DESC
LIMIT 10;

-- Question: "How many customers are in the High Risk churn category?"
SELECT
    churn_risk_category,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total,
    AVG(churn_risk_score) AS avg_risk_score
FROM CUSTOMER_360_PROFILE
WHERE churn_risk_category IS NOT NULL
GROUP BY churn_risk_category
ORDER BY customer_count DESC;

-- Question: "What is the average churn risk score for the Declining segment?"
SELECT
    customer_segment,
    AVG(churn_risk_score) AS avg_churn_risk,
    COUNT(*) AS customer_count,
    SUM(CASE WHEN churn_risk_category = 'High Risk' THEN 1 ELSE 0 END) AS high_risk_count
FROM CUSTOMER_360_PROFILE
WHERE customer_segment = 'Declining'
GROUP BY customer_segment;

-- Question: "Show me customers who haven't transacted in over 60 days"
SELECT
    customer_id,
    full_name,
    customer_segment,
    days_since_last_transaction,
    spend_last_90_days,
    churn_risk_score,
    recency_status
FROM CUSTOMER_360_PROFILE
WHERE days_since_last_transaction > 60
ORDER BY lifetime_value DESC
LIMIT 20;

-- ============================================================================
-- Test 3: Spending Trend Queries
-- ============================================================================

-- Question: "Show me spending trends in the travel category over the last 6 months"
SELECT
    DATE_TRUNC('month', f.transaction_date) AS month,
    SUM(f.transaction_amount) AS total_travel_spend,
    COUNT(f.transaction_key) AS transaction_count,
    AVG(f.transaction_amount) AS avg_transaction_value
FROM FCT_TRANSACTIONS f
JOIN DIM_MERCHANT_CATEGORY cat
    ON f.merchant_category_key = cat.category_key
WHERE cat.category_name IN ('Travel', 'Airlines', 'Hotels')
  AND f.transaction_date >= DATEADD('month', -6, CURRENT_DATE())
  AND f.status = 'approved'
GROUP BY DATE_TRUNC('month', f.transaction_date)
ORDER BY month DESC;

-- Question: "Which merchant categories are most popular among Premium cardholders?"
SELECT
    cat.category_name,
    COUNT(f.transaction_key) AS transaction_count,
    SUM(f.transaction_amount) AS total_spend,
    AVG(f.transaction_amount) AS avg_transaction_value
FROM FCT_TRANSACTIONS f
JOIN DIM_MERCHANT_CATEGORY cat
    ON f.merchant_category_key = cat.category_key
JOIN CUSTOMER_360_PROFILE cp
    ON f.customer_id = cp.customer_id
WHERE cp.card_type = 'Premium'
  AND f.status = 'approved'
  AND f.transaction_date >= DATEADD('month', -6, CURRENT_DATE())
GROUP BY cat.category_name
ORDER BY total_spend DESC
LIMIT 10;

-- Question: "Which customers increased their spending last quarter?"
SELECT
    customer_id,
    full_name,
    customer_segment,
    spend_last_90_days,
    spend_prior_90_days,
    spend_change_pct,
    churn_risk_category
FROM CUSTOMER_360_PROFILE
WHERE spend_change_pct > 0
ORDER BY spend_change_pct DESC
LIMIT 20;

-- ============================================================================
-- Test 4: Segmentation Queries
-- ============================================================================

-- Question: "Compare lifetime value across customer segments"
SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    AVG(lifetime_value) AS avg_ltv,
    MIN(lifetime_value) AS min_ltv,
    MAX(lifetime_value) AS max_ltv,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lifetime_value) AS median_ltv,
    AVG(avg_monthly_spend) AS avg_monthly_spend
FROM CUSTOMER_360_PROFILE
GROUP BY customer_segment
ORDER BY avg_ltv DESC;

-- Question: "How many customers in each segment have Premium cards?"
SELECT
    customer_segment,
    card_type,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY customer_segment), 2) AS pct_within_segment
FROM CUSTOMER_360_PROFILE
GROUP BY customer_segment, card_type
ORDER BY customer_segment, card_type;

-- Question: "Which segments have the highest churn risk?"
SELECT
    customer_segment,
    AVG(churn_risk_score) AS avg_churn_risk,
    COUNT(*) AS customer_count,
    SUM(CASE WHEN churn_risk_category = 'High Risk' THEN 1 ELSE 0 END) AS high_risk_count,
    ROUND(SUM(CASE WHEN churn_risk_category = 'High Risk' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS high_risk_pct
FROM CUSTOMER_360_PROFILE
WHERE churn_risk_score IS NOT NULL
GROUP BY customer_segment
ORDER BY avg_churn_risk DESC;

-- ============================================================================
-- Test 5: Time-Based Queries
-- ============================================================================

-- Question: "What was total spending in the last 90 days?"
SELECT
    SUM(spend_last_90_days) AS total_spend_last_90_days,
    COUNT(*) AS active_customers,
    AVG(spend_last_90_days) AS avg_spend_per_customer
FROM CUSTOMER_360_PROFILE
WHERE spend_last_90_days > 0;

-- Question: "Show monthly transaction volume trends"
SELECT
    DATE_TRUNC('month', transaction_date) AS month,
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(transaction_amount) AS total_spend,
    AVG(transaction_amount) AS avg_transaction_value
FROM FCT_TRANSACTIONS
WHERE status = 'approved'
  AND transaction_date >= DATEADD('month', -12, CURRENT_DATE())
GROUP BY DATE_TRUNC('month', transaction_date)
ORDER BY month DESC;

-- ============================================================================
-- Test 6: Campaign Targeting Queries
-- ============================================================================

-- Question: "Show me customers eligible for retention campaigns"
SELECT
    customer_id,
    full_name,
    customer_segment,
    churn_risk_category,
    churn_risk_score,
    lifetime_value,
    eligible_for_retention_campaign
FROM CUSTOMER_360_PROFILE
WHERE eligible_for_retention_campaign = TRUE
ORDER BY churn_risk_score DESC, lifetime_value DESC
LIMIT 50;

-- Question: "Which Premium cardholders are at risk of churning?"
SELECT
    customer_id,
    full_name,
    state,
    card_type,
    churn_risk_score,
    lifetime_value,
    days_since_last_transaction,
    spend_change_pct
FROM CUSTOMER_360_PROFILE
WHERE card_type = 'Premium'
  AND churn_risk_category IN ('Medium Risk', 'High Risk')
ORDER BY churn_risk_score DESC
LIMIT 25;

-- ============================================================================
-- Test 7: Geographic Queries
-- ============================================================================

-- Question: "What is the average lifetime value by state?"
SELECT
    state,
    COUNT(*) AS customer_count,
    AVG(lifetime_value) AS avg_ltv,
    AVG(avg_monthly_spend) AS avg_monthly_spend,
    AVG(churn_risk_score) AS avg_churn_risk
FROM CUSTOMER_360_PROFILE
GROUP BY state
HAVING COUNT(*) >= 100  -- Only states with 100+ customers
ORDER BY avg_ltv DESC
LIMIT 20;

-- Question: "Which states have the highest churn risk?"
SELECT
    state,
    COUNT(*) AS customer_count,
    AVG(churn_risk_score) AS avg_churn_risk,
    SUM(CASE WHEN churn_risk_category = 'High Risk' THEN 1 ELSE 0 END) AS high_risk_count,
    ROUND(SUM(CASE WHEN churn_risk_category = 'High Risk' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS high_risk_pct
FROM CUSTOMER_360_PROFILE
WHERE churn_risk_score IS NOT NULL
GROUP BY state
HAVING COUNT(*) >= 50  -- Only states with 50+ customers
ORDER BY avg_churn_risk DESC
LIMIT 15;

-- ============================================================================
-- Test 8: Advanced Analytical Queries
-- ============================================================================

-- Question: "Compare spending patterns between High Risk and Low Risk customers"
SELECT
    churn_risk_category,
    COUNT(*) AS customer_count,
    AVG(lifetime_value) AS avg_ltv,
    AVG(avg_monthly_spend) AS avg_monthly_spend,
    AVG(avg_transaction_value) AS avg_transaction_value,
    AVG(days_since_last_transaction) AS avg_days_since_last_txn,
    AVG(spend_change_pct) AS avg_spend_change_pct,
    AVG(travel_spend_pct) AS avg_travel_pct,
    AVG(necessities_spend_pct) AS avg_necessities_pct
FROM CUSTOMER_360_PROFILE
WHERE churn_risk_category IN ('Low Risk', 'High Risk')
GROUP BY churn_risk_category
ORDER BY churn_risk_category;

-- Question: "Show me the distribution of churn risk scores"
SELECT
    CASE
        WHEN churn_risk_score < 10 THEN '0-9'
        WHEN churn_risk_score < 20 THEN '10-19'
        WHEN churn_risk_score < 30 THEN '20-29'
        WHEN churn_risk_score < 40 THEN '30-39'
        WHEN churn_risk_score < 50 THEN '40-49'
        WHEN churn_risk_score < 60 THEN '50-59'
        WHEN churn_risk_score < 70 THEN '60-69'
        WHEN churn_risk_score < 80 THEN '70-79'
        WHEN churn_risk_score < 90 THEN '80-89'
        ELSE '90-100'
    END AS risk_score_bucket,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM CUSTOMER_360_PROFILE
WHERE churn_risk_score IS NOT NULL
GROUP BY risk_score_bucket
ORDER BY risk_score_bucket;

-- ============================================================================
-- Validation Summary
-- ============================================================================

-- Summary: Count of customers by segment and churn risk
SELECT
    'SUMMARY: Customer Distribution' AS report_type,
    customer_segment,
    churn_risk_category,
    COUNT(*) AS customer_count
FROM CUSTOMER_360_PROFILE
GROUP BY customer_segment, churn_risk_category
ORDER BY customer_segment, churn_risk_category;

-- Summary: Overall metrics
SELECT
    'OVERALL METRICS' AS report_type,
    COUNT(DISTINCT customer_id) AS total_customers,
    AVG(lifetime_value) AS avg_ltv,
    AVG(churn_risk_score) AS avg_churn_risk,
    SUM(spend_last_90_days) AS total_spend_90d
FROM CUSTOMER_360_PROFILE;

-- ============================================================================
-- End of Test Queries
-- ============================================================================
