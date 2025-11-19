-- ============================================================================
-- Create Monthly Spending Aggregated View
-- ============================================================================
-- Purpose: Pre-aggregate spending by customer and month for time-series analysis
-- Use case: Month-over-month trends, seasonal patterns, growth analysis
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA GOLD;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================================
-- Create Monthly Spending View
-- ============================================================================

CREATE OR REPLACE VIEW MONTHLY_CUSTOMER_SPENDING AS
SELECT
  cp.customer_key,
  cp.customer_id,
  cp.full_name,
  cp.customer_segment,
  cp.card_type,
  cp.state,
  cp.city,
  DATE_TRUNC('MONTH', t.transaction_date) AS month,
  SUM(t.transaction_amount) AS total_spend,
  COUNT(t.transaction_id) AS transaction_count,
  AVG(t.transaction_amount) AS avg_transaction_value,
  MIN(t.transaction_date) AS first_transaction_date,
  MAX(t.transaction_date) AS last_transaction_date
FROM FCT_TRANSACTIONS t
JOIN CUSTOMER_360_PROFILE cp
  ON t.customer_key = cp.customer_key
WHERE t.status = 'approved'
GROUP BY
  cp.customer_key,
  cp.customer_id,
  cp.full_name,
  cp.customer_segment,
  cp.card_type,
  cp.state,
  cp.city,
  DATE_TRUNC('MONTH', t.transaction_date);

-- ============================================================================
-- Verify View Creation
-- ============================================================================

-- Check row count
SELECT COUNT(*) AS total_monthly_records FROM MONTHLY_CUSTOMER_SPENDING;

-- Preview data
SELECT * FROM MONTHLY_CUSTOMER_SPENDING
ORDER BY month DESC, total_spend DESC
LIMIT 10;

-- Verify date range coverage
SELECT
  MIN(month) AS earliest_month,
  MAX(month) AS latest_month,
  COUNT(DISTINCT month) AS total_months,
  COUNT(DISTINCT customer_key) AS total_customers
FROM MONTHLY_CUSTOMER_SPENDING;

SELECT 'Monthly spending view created successfully!' AS status;
