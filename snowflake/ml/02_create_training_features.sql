/*
============================================================================
ML: Create Training Features
============================================================================
Purpose: Generate feature table for churn prediction ML model

Features:
- Demographics: age, state, card_type, credit_limit, employment_status
- Spending behavior: LTV, ATV, transaction counts, recency
- Trends: spend_change_pct, category preferences
- Derived: credit_utilization, tenure_months, avg_spend_per_transaction

Target Variable: churned (from CHURN_LABELS table)

Prerequisites:
- GOLD.CHURN_LABELS must exist (run 01_create_churn_labels.sql first)
- GOLD.CUSTOMER_360_PROFILE must exist
- GOLD.CUSTOMER_SEGMENTS must exist

Output: GOLD.ML_TRAINING_DATA table

Usage:
  snowflake-sql -f snowflake/ml/02_create_training_features.sql

Validation:
  SELECT COUNT(*), AVG(churned::INT) FROM GOLD.ML_TRAINING_DATA;
============================================================================
*/

-- Drop existing table if re-running
DROP TABLE IF EXISTS GOLD.ML_TRAINING_DATA;

-- Create feature table for ML model
CREATE TABLE GOLD.ML_TRAINING_DATA AS
SELECT
    cp.customer_id,

    -- ========================================================================
    -- DEMOGRAPHIC FEATURES
    -- ========================================================================

    cp.age,
    cp.state,
    cp.city,

    -- Encode card_type as binary (0 = Standard, 1 = Premium)
    CASE WHEN cp.card_type = 'Premium' THEN 1 ELSE 0 END AS card_type_premium,

    cp.credit_limit,

    -- Encode employment_status (will be one-hot encoded in model)
    cp.employment_status,

    -- ========================================================================
    -- SPENDING BEHAVIOR FEATURES
    -- ========================================================================

    -- Lifetime metrics
    cp.lifetime_value,
    cp.avg_transaction_value,
    cp.total_transactions,
    cp.customer_age_days,

    -- Recent activity
    cp.days_since_last_transaction,
    cp.spend_last_90_days,
    cp.spend_prior_90_days,
    seg.spend_change_pct,
    seg.avg_monthly_spend,

    -- Transaction value variability
    cp.transaction_value_stddev,
    cp.median_transaction_value,

    -- Encode spending_consistency (0 = Consistent, 1 = Moderate, 2 = Variable)
    CASE cp.spending_consistency
        WHEN 'Consistent' THEN 0
        WHEN 'Moderate' THEN 1
        WHEN 'Variable' THEN 2
        ELSE 0
    END AS spending_consistency_encoded,

    -- ========================================================================
    -- CATEGORY PREFERENCE FEATURES
    -- ========================================================================

    COALESCE(seg.travel_spend_pct, 0) AS travel_spend_pct,
    COALESCE(seg.necessities_spend_pct, 0) AS necessities_spend_pct,

    -- Encode spending_profile (0 = Balanced, 1 = Travel-Focused, 2 = Necessity-Focused)
    CASE cp.spending_profile
        WHEN 'Travel-Focused' THEN 1
        WHEN 'Necessity-Focused' THEN 2
        ELSE 0
    END AS spending_profile_encoded,

    -- ========================================================================
    -- SEGMENT FEATURES
    -- ========================================================================

    -- Encode customer_segment as one-hot (binary flags)
    CASE WHEN seg.customer_segment = 'High-Value Travelers' THEN 1 ELSE 0 END AS segment_high_value_travelers,
    CASE WHEN seg.customer_segment = 'Declining' THEN 1 ELSE 0 END AS segment_declining,
    CASE WHEN seg.customer_segment = 'New & Growing' THEN 1 ELSE 0 END AS segment_new_growing,
    CASE WHEN seg.customer_segment = 'Budget-Conscious' THEN 1 ELSE 0 END AS segment_budget_conscious,
    CASE WHEN seg.customer_segment = 'Stable Mid-Spenders' THEN 1 ELSE 0 END AS segment_stable,

    seg.tenure_months,

    -- ========================================================================
    -- DERIVED FEATURES (Engineered)
    -- ========================================================================

    -- Average spend per transaction (alternative to ATV)
    CASE
        WHEN cp.total_transactions > 0
        THEN cp.lifetime_value / cp.total_transactions
        ELSE 0
    END AS avg_spend_per_transaction,

    -- Credit utilization (monthly spend / credit limit)
    CASE
        WHEN cp.credit_limit > 0
        THEN (seg.avg_monthly_spend / cp.credit_limit) * 100
        ELSE 0
    END AS credit_utilization_pct,

    -- Transaction frequency (transactions per day)
    CASE
        WHEN cp.customer_age_days > 0
        THEN cp.total_transactions / cp.customer_age_days
        ELSE 0
    END AS transactions_per_day,

    -- Spending velocity (dollars per day)
    CASE
        WHEN cp.customer_age_days > 0
        THEN cp.lifetime_value / cp.customer_age_days
        ELSE 0
    END AS spend_per_day,

    -- Recency score (0 = very recent, 1 = very old)
    CASE
        WHEN cp.days_since_last_transaction <= 30 THEN 0
        WHEN cp.days_since_last_transaction <= 60 THEN 1
        WHEN cp.days_since_last_transaction <= 90 THEN 2
        ELSE 3
    END AS recency_score,

    -- Encode recency_status (0 = Active, 1 = Recent, 2 = At Risk, 3 = Inactive)
    CASE cp.recency_status
        WHEN 'Active (30 days)' THEN 0
        WHEN 'Recent (60 days)' THEN 1
        WHEN 'At Risk (90 days)' THEN 2
        WHEN 'Inactive (90+ days)' THEN 3
        ELSE 3
    END AS recency_status_encoded,

    -- Spend momentum (last 90 days vs prior 90 days)
    CASE
        WHEN cp.spend_prior_90_days > 0
        THEN cp.spend_last_90_days / cp.spend_prior_90_days
        ELSE 0
    END AS spend_momentum,

    -- ========================================================================
    -- TARGET VARIABLE
    -- ========================================================================

    -- Convert boolean to integer (0 = not churned, 1 = churned)
    labels.churned::INT AS churned,

    -- Include churn reason for analysis (not used in model)
    labels.churn_reason,

    -- Include label metrics for analysis
    labels.baseline_avg_spend,
    labels.recent_avg_spend,
    labels.days_since_last_transaction AS label_days_since_last_txn,

    -- ========================================================================
    -- METADATA
    -- ========================================================================

    CURRENT_TIMESTAMP() AS feature_created_at

FROM GOLD.CUSTOMER_360_PROFILE cp

-- Join segmentation for additional features
JOIN GOLD.CUSTOMER_SEGMENTS seg
    ON cp.customer_id = seg.customer_id

-- Join churn labels (target variable)
JOIN GOLD.CHURN_LABELS labels
    ON cp.customer_id = labels.customer_id

WHERE
    -- Filter: Only include customers with baseline data
    labels.baseline_avg_spend IS NOT NULL
    AND labels.baseline_avg_spend > 0

    -- Filter: Remove customers with insufficient data
    AND cp.total_transactions >= 5  -- At least 5 transactions for reliable features

    -- Filter: Remove outliers (optional, uncomment if needed)
    -- AND cp.lifetime_value < 500000  -- Remove extreme high spenders
    -- AND cp.avg_monthly_spend < 50000  -- Remove unrealistic spenders
;

-- Create index for performance
CREATE INDEX IF NOT EXISTS idx_ml_training_customer_id
    ON GOLD.ML_TRAINING_DATA(customer_id);

CREATE INDEX IF NOT EXISTS idx_ml_training_churned
    ON GOLD.ML_TRAINING_DATA(churned);

-- Validation: Display summary statistics
SELECT
    '=== TRAINING DATA SUMMARY ===' AS summary;

SELECT
    'Total training examples' AS metric,
    COUNT(*) AS value
FROM GOLD.ML_TRAINING_DATA;

SELECT
    'Class distribution' AS metric,
    CASE WHEN churned = 1 THEN 'Churned' ELSE 'Active' END AS class,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM GOLD.ML_TRAINING_DATA
GROUP BY churned
ORDER BY churned DESC;

SELECT
    'Feature statistics' AS metric,
    ROUND(AVG(age), 2) AS avg_age,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    ROUND(AVG(avg_monthly_spend), 2) AS avg_monthly_spend,
    ROUND(AVG(credit_utilization_pct), 2) AS avg_credit_util_pct,
    ROUND(AVG(tenure_months), 2) AS avg_tenure_months
FROM GOLD.ML_TRAINING_DATA;

SELECT
    'Null check' AS metric,
    SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END) AS null_age,
    SUM(CASE WHEN credit_limit IS NULL THEN 1 ELSE 0 END) AS null_credit_limit,
    SUM(CASE WHEN avg_monthly_spend IS NULL THEN 1 ELSE 0 END) AS null_avg_monthly_spend,
    SUM(CASE WHEN tenure_months IS NULL THEN 1 ELSE 0 END) AS null_tenure_months
FROM GOLD.ML_TRAINING_DATA;

SELECT
    'Feature ranges' AS metric,
    MIN(age) AS min_age,
    MAX(age) AS max_age,
    MIN(credit_utilization_pct) AS min_credit_util,
    MAX(credit_utilization_pct) AS max_credit_util,
    MIN(tenure_months) AS min_tenure,
    MAX(tenure_months) AS max_tenure
FROM GOLD.ML_TRAINING_DATA;

-- Success message
SELECT '✓ Training features created successfully' AS status;
SELECT CONCAT(COUNT(*), ' training examples with ', SUM(churned), ' churned customers (', ROUND(AVG(churned) * 100, 2), '% churn rate)') AS summary
FROM GOLD.ML_TRAINING_DATA;
