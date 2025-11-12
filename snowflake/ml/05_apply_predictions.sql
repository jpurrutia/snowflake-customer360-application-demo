-- ============================================================================
-- Apply Churn Predictions to All Active Customers
-- ============================================================================
-- Purpose: Score all customers with churn risk using trained CHURN_MODEL
--
-- Output: GOLD.CHURN_PREDICTIONS table with:
--   - customer_id
--   - predicted_churn (BOOLEAN)
--   - churn_risk_score (0-100)
--   - prediction_date
--
-- Usage:
--   Run after model training and validation
--   Results will be joined to customer_360_profile
--
-- Prerequisites:
--   - CHURN_MODEL exists and validated (03 and 04 scripts)
--   - GOLD.CUSTOMER_360_PROFILE exists (dbt model)
--   - GOLD.CUSTOMER_SEGMENTS exists (dbt model)
-- ============================================================================

-- Apply model predictions to all active customers
CREATE OR REPLACE TABLE GOLD.CHURN_PREDICTIONS AS
WITH customer_features AS (
    SELECT
        cp.customer_id,

        -- Demographics (from customer_360_profile)
        cp.age,
        cp.state,
        cp.card_type,
        cp.credit_limit,
        cp.employment_status,

        -- Spending behavior (from customer_360_profile and metrics)
        cp.lifetime_value,
        cp.avg_transaction_value,
        cp.total_transactions,
        cp.days_since_last_transaction,
        seg.spend_change_pct,
        seg.travel_spend_pct,
        seg.necessities_spend_pct,
        seg.avg_monthly_spend,
        seg.spend_last_90_days,
        cp.account_open_date,

        -- Derived features (same calculations as training data)
        CASE
            WHEN cp.total_transactions > 0
            THEN cp.lifetime_value / cp.total_transactions
            ELSE 0
        END AS avg_spend_per_transaction,

        CASE
            WHEN cp.credit_limit > 0
            THEN (seg.spend_last_90_days / 3) / cp.credit_limit * 100
            ELSE 0
        END AS credit_utilization_pct,

        DATEDIFF('month', cp.account_open_date, CURRENT_DATE()) AS tenure_months,

        -- Segment flags (one-hot encoding)
        CASE WHEN seg.customer_segment = 'High-Value Travelers' THEN 1 ELSE 0 END AS segment_high_value_travelers,
        CASE WHEN seg.customer_segment = 'Declining' THEN 1 ELSE 0 END AS segment_declining,
        CASE WHEN seg.customer_segment = 'New & Growing' THEN 1 ELSE 0 END AS segment_new_growing,
        CASE WHEN seg.customer_segment = 'Budget-Conscious' THEN 1 ELSE 0 END AS segment_budget_conscious,
        CASE WHEN seg.customer_segment = 'Stable Mid-Spenders' THEN 1 ELSE 0 END AS segment_stable,

        -- Additional derived features
        CASE
            WHEN cp.customer_age_days > 0
            THEN cp.total_transactions / cp.customer_age_days
            ELSE 0
        END AS transactions_per_day,

        CASE
            WHEN cp.customer_age_days > 0
            THEN cp.lifetime_value / cp.customer_age_days
            ELSE 0
        END AS spend_per_day,

        -- Spending consistency encoding
        CASE
            WHEN cp.spending_consistency = 'Consistent' THEN 0
            WHEN cp.spending_consistency = 'Moderate' THEN 1
            WHEN cp.spending_consistency = 'Variable' THEN 2
            ELSE 0
        END AS spending_consistency_encoded,

        -- Recency status encoding
        CASE
            WHEN cp.recency_status = 'Active (30 days)' THEN 0
            WHEN cp.recency_status = 'Recent (60 days)' THEN 1
            WHEN cp.recency_status = 'At Risk (90 days)' THEN 2
            WHEN cp.recency_status = 'Inactive (90+ days)' THEN 3
            ELSE 0
        END AS recency_status_encoded,

        -- Spending profile encoding
        CASE
            WHEN cp.spending_profile = 'Balanced' THEN 0
            WHEN cp.spending_profile = 'Travel-Focused' THEN 1
            WHEN cp.spending_profile = 'Necessity-Focused' THEN 2
            ELSE 0
        END AS spending_profile_encoded,

        -- Card type encoding
        CASE WHEN cp.card_type = 'Premium' THEN 1 ELSE 0 END AS card_type_premium,

        -- Transaction value stats
        cp.transaction_value_stddev,
        cp.median_transaction_value,

        -- Spend momentum
        CASE
            WHEN seg.spend_prior_90_days > 0
            THEN seg.spend_last_90_days / seg.spend_prior_90_days
            ELSE 0
        END AS spend_momentum

    FROM GOLD.CUSTOMER_360_PROFILE cp
    JOIN GOLD.CUSTOMER_SEGMENTS seg
        ON cp.customer_id = seg.customer_id

    -- Only score active customers with sufficient history
    WHERE cp.total_transactions >= 5
)

SELECT
    customer_id,

    -- Apply Cortex ML model to predict churn
    CHURN_MODEL!PREDICT(
        OBJECT_CONSTRUCT(
            -- Demographics
            'age', age,
            'state', state,
            'card_type_premium', card_type_premium,
            'credit_limit', credit_limit,
            'employment_status', employment_status,

            -- Spending behavior
            'lifetime_value', lifetime_value,
            'avg_transaction_value', avg_transaction_value,
            'total_transactions', total_transactions,
            'days_since_last_transaction', days_since_last_transaction,
            'spend_change_pct', spend_change_pct,
            'travel_spend_pct', travel_spend_pct,
            'necessities_spend_pct', necessities_spend_pct,
            'avg_monthly_spend', avg_monthly_spend,

            -- Derived features
            'avg_spend_per_transaction', avg_spend_per_transaction,
            'credit_utilization_pct', credit_utilization_pct,
            'tenure_months', tenure_months,

            -- Segment flags
            'segment_high_value_travelers', segment_high_value_travelers,
            'segment_declining', segment_declining,
            'segment_new_growing', segment_new_growing,
            'segment_budget_conscious', segment_budget_conscious,
            'segment_stable', segment_stable,

            -- Additional features
            'transactions_per_day', transactions_per_day,
            'spend_per_day', spend_per_day,
            'spending_consistency_encoded', spending_consistency_encoded,
            'recency_status_encoded', recency_status_encoded,
            'spending_profile_encoded', spending_profile_encoded,
            'transaction_value_stddev', transaction_value_stddev,
            'median_transaction_value', median_transaction_value,
            'spend_momentum', spend_momentum
        )
    ) AS prediction_result,

    -- Extract prediction and probability
    prediction_result['churned']::BOOLEAN AS predicted_churn,
    prediction_result['probability']::FLOAT * 100 AS churn_risk_score,

    -- Metadata
    CURRENT_DATE() AS prediction_date

FROM customer_features;

-- ============================================================================
-- Validation Queries
-- ============================================================================

-- Check row count
SELECT COUNT(*) AS total_predictions FROM GOLD.CHURN_PREDICTIONS;
-- Expected: ~45K-50K (all customers with ≥5 transactions)

-- Check score distribution
SELECT
    COUNT(*) AS total_customers,
    AVG(churn_risk_score) AS avg_risk_score,
    MIN(churn_risk_score) AS min_score,
    MAX(churn_risk_score) AS max_score,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY churn_risk_score) AS median_score
FROM GOLD.CHURN_PREDICTIONS;

-- Check risk categories
SELECT
    CASE
        WHEN churn_risk_score >= 70 THEN 'High Risk'
        WHEN churn_risk_score >= 40 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS risk_category,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM GOLD.CHURN_PREDICTIONS
GROUP BY risk_category
ORDER BY customer_count DESC;

-- ============================================================================
-- Expected Output:
-- ============================================================================
-- GOLD.CHURN_PREDICTIONS table created
-- ~45-50K customers scored
-- Churn risk scores range from 0-100
-- Risk distribution:
--   - Low Risk (0-39): 70-80%
--   - Medium Risk (40-69): 15-25%
--   - High Risk (70-100): 5-10%
-- ============================================================================
