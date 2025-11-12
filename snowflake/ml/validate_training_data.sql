/*
============================================================================
ML: Validate Training Data
============================================================================
Purpose: Comprehensive validation checks for ML training data

Checks:
1. Row count (sufficient examples)
2. Class balance (churn rate 8-15%)
3. Null features (critical features complete)
4. Feature ranges (realistic values)
5. Data quality issues

Prerequisites:
- GOLD.ML_TRAINING_DATA must exist (run 02_create_training_features.sql first)

Usage:
  snowflake-sql -f snowflake/ml/validate_training_data.sql

Expected Results:
- 40K-50K training examples
- 8-15% churn rate (class imbalance)
- 0 null values in critical features
- Realistic feature ranges
============================================================================
*/

SELECT '============================================================================' AS divider;
SELECT 'ML TRAINING DATA VALIDATION REPORT' AS report_title;
SELECT '============================================================================' AS divider;

-- ============================================================================
-- Check 1: Row Count
-- ============================================================================

SELECT
    'Check 1: Row Count' AS check_name,
    COUNT(*) AS total_rows,
    CASE
        WHEN COUNT(*) >= 40000 THEN '✓ PASS: Sufficient training examples'
        WHEN COUNT(*) >= 1000 THEN '⚠ WARNING: Low training examples (recommend 40K+)'
        ELSE '✗ FAIL: Insufficient training examples'
    END AS status
FROM GOLD.ML_TRAINING_DATA;

-- ============================================================================
-- Check 2: Class Distribution (Balance)
-- ============================================================================

SELECT
    'Check 2: Class Distribution' AS check_name,
    CASE WHEN churned = 1 THEN 'Churned (Positive Class)' ELSE 'Active (Negative Class)' END AS class,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    CASE
        WHEN churned = 1 AND COUNT(*) * 100.0 / SUM(COUNT(*) OVER ()) BETWEEN 8 AND 15
        THEN '✓ PASS: Realistic churn rate'
        WHEN churned = 1 AND COUNT(*) * 100.0 / SUM(COUNT(*) OVER ()) < 8
        THEN '⚠ WARNING: Low churn rate (<8%)'
        WHEN churned = 1 AND COUNT(*) * 100.0 / SUM(COUNT(*) OVER ()) > 15
        THEN '⚠ WARNING: High churn rate (>15%)'
        ELSE ''
    END AS status
FROM GOLD.ML_TRAINING_DATA
GROUP BY churned
ORDER BY churned DESC;

-- ============================================================================
-- Check 3: Null Features (Critical Fields)
-- ============================================================================

SELECT
    'Check 3: Null Features' AS check_name,
    SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END) AS null_age,
    SUM(CASE WHEN credit_limit IS NULL THEN 1 ELSE 0 END) AS null_credit_limit,
    SUM(CASE WHEN avg_monthly_spend IS NULL THEN 1 ELSE 0 END) AS null_avg_monthly_spend,
    SUM(CASE WHEN tenure_months IS NULL THEN 1 ELSE 0 END) AS null_tenure_months,
    SUM(CASE WHEN lifetime_value IS NULL THEN 1 ELSE 0 END) AS null_lifetime_value,
    SUM(CASE WHEN days_since_last_transaction IS NULL THEN 1 ELSE 0 END) AS null_days_since_last_txn,
    CASE
        WHEN SUM(CASE WHEN age IS NULL OR credit_limit IS NULL OR avg_monthly_spend IS NULL OR tenure_months IS NULL THEN 1 ELSE 0 END) = 0
        THEN '✓ PASS: No null values in critical features'
        ELSE '✗ FAIL: Null values found in critical features'
    END AS status
FROM GOLD.ML_TRAINING_DATA;

-- ============================================================================
-- Check 4: Feature Ranges (Validation)
-- ============================================================================

SELECT
    'Check 4: Feature Ranges - Demographics' AS check_name,
    MIN(age) AS min_age,
    MAX(age) AS max_age,
    ROUND(AVG(age), 2) AS avg_age,
    MIN(credit_limit) AS min_credit_limit,
    MAX(credit_limit) AS max_credit_limit,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    CASE
        WHEN MIN(age) >= 18 AND MAX(age) <= 100 AND MIN(credit_limit) >= 5000 AND MAX(credit_limit) <= 50000
        THEN '✓ PASS: Realistic demographic ranges'
        ELSE '⚠ WARNING: Check demographic ranges'
    END AS status
FROM GOLD.ML_TRAINING_DATA;

SELECT
    'Check 4: Feature Ranges - Spending' AS check_name,
    ROUND(MIN(avg_monthly_spend), 2) AS min_avg_monthly_spend,
    ROUND(MAX(avg_monthly_spend), 2) AS max_avg_monthly_spend,
    ROUND(AVG(avg_monthly_spend), 2) AS avg_avg_monthly_spend,
    ROUND(MIN(credit_utilization_pct), 2) AS min_credit_util_pct,
    ROUND(MAX(credit_utilization_pct), 2) AS max_credit_util_pct,
    ROUND(AVG(credit_utilization_pct), 2) AS avg_credit_util_pct,
    CASE
        WHEN MIN(avg_monthly_spend) >= 0 AND MIN(credit_utilization_pct) >= 0 AND MAX(credit_utilization_pct) <= 150
        THEN '✓ PASS: Realistic spending ranges'
        ELSE '⚠ WARNING: Check spending ranges'
    END AS status
FROM GOLD.ML_TRAINING_DATA;

SELECT
    'Check 4: Feature Ranges - Activity' AS check_name,
    MIN(tenure_months) AS min_tenure_months,
    MAX(tenure_months) AS max_tenure_months,
    ROUND(AVG(tenure_months), 2) AS avg_tenure_months,
    MIN(total_transactions) AS min_total_transactions,
    MAX(total_transactions) AS max_total_transactions,
    ROUND(AVG(total_transactions), 2) AS avg_total_transactions,
    CASE
        WHEN MIN(tenure_months) >= 0 AND MIN(total_transactions) >= 5
        THEN '✓ PASS: Realistic activity ranges'
        ELSE '⚠ WARNING: Check activity ranges'
    END AS status
FROM GOLD.ML_TRAINING_DATA;

-- ============================================================================
-- Check 5: Feature Completeness (Non-Null Rates)
-- ============================================================================

WITH feature_completeness AS (
    SELECT
        'age' AS feature_name,
        COUNT(*) - SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END) AS non_null_count,
        COUNT(*) AS total_count,
        ROUND((COUNT(*) - SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END)) * 100.0 / COUNT(*), 2) AS completeness_pct
    FROM GOLD.ML_TRAINING_DATA

    UNION ALL

    SELECT
        'credit_limit',
        COUNT(*) - SUM(CASE WHEN credit_limit IS NULL THEN 1 ELSE 0 END),
        COUNT(*),
        ROUND((COUNT(*) - SUM(CASE WHEN credit_limit IS NULL THEN 1 ELSE 0 END)) * 100.0 / COUNT(*), 2)
    FROM GOLD.ML_TRAINING_DATA

    UNION ALL

    SELECT
        'avg_monthly_spend',
        COUNT(*) - SUM(CASE WHEN avg_monthly_spend IS NULL THEN 1 ELSE 0 END),
        COUNT(*),
        ROUND((COUNT(*) - SUM(CASE WHEN avg_monthly_spend IS NULL THEN 1 ELSE 0 END)) * 100.0 / COUNT(*), 2)
    FROM GOLD.ML_TRAINING_DATA

    UNION ALL

    SELECT
        'tenure_months',
        COUNT(*) - SUM(CASE WHEN tenure_months IS NULL THEN 1 ELSE 0 END),
        COUNT(*),
        ROUND((COUNT(*) - SUM(CASE WHEN tenure_months IS NULL THEN 1 ELSE 0 END)) * 100.0 / COUNT(*), 2)
    FROM GOLD.ML_TRAINING_DATA

    UNION ALL

    SELECT
        'travel_spend_pct',
        COUNT(*) - SUM(CASE WHEN travel_spend_pct IS NULL THEN 1 ELSE 0 END),
        COUNT(*),
        ROUND((COUNT(*) - SUM(CASE WHEN travel_spend_pct IS NULL THEN 1 ELSE 0 END)) * 100.0 / COUNT(*), 2)
    FROM GOLD.ML_TRAINING_DATA

    UNION ALL

    SELECT
        'spend_change_pct',
        COUNT(*) - SUM(CASE WHEN spend_change_pct IS NULL THEN 1 ELSE 0 END),
        COUNT(*),
        ROUND((COUNT(*) - SUM(CASE WHEN spend_change_pct IS NULL THEN 1 ELSE 0 END)) * 100.0 / COUNT(*), 2)
    FROM GOLD.ML_TRAINING_DATA
)
SELECT
    'Check 5: Feature Completeness' AS check_name,
    feature_name,
    non_null_count,
    total_count,
    completeness_pct AS completeness_percentage,
    CASE
        WHEN completeness_pct = 100 THEN '✓ COMPLETE'
        WHEN completeness_pct >= 95 THEN '⚠ MOSTLY COMPLETE'
        ELSE '✗ INCOMPLETE'
    END AS status
FROM feature_completeness
ORDER BY completeness_pct DESC;

-- ============================================================================
-- Check 6: Churned vs Active Feature Comparison
-- ============================================================================

SELECT
    'Check 6: Feature Comparison (Churned vs Active)' AS check_name,
    CASE WHEN churned = 1 THEN 'Churned' ELSE 'Active' END AS customer_type,
    ROUND(AVG(age), 2) AS avg_age,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    ROUND(AVG(avg_monthly_spend), 2) AS avg_monthly_spend,
    ROUND(AVG(lifetime_value), 2) AS avg_lifetime_value,
    ROUND(AVG(tenure_months), 2) AS avg_tenure_months,
    ROUND(AVG(days_since_last_transaction), 2) AS avg_days_since_last_txn,
    ROUND(AVG(credit_utilization_pct), 2) AS avg_credit_util_pct
FROM GOLD.ML_TRAINING_DATA
GROUP BY churned
ORDER BY churned DESC;

-- ============================================================================
-- Check 7: Segment Distribution in Training Data
-- ============================================================================

WITH segment_distribution AS (
    SELECT
        CASE
            WHEN segment_high_value_travelers = 1 THEN 'High-Value Travelers'
            WHEN segment_declining = 1 THEN 'Declining'
            WHEN segment_new_growing = 1 THEN 'New & Growing'
            WHEN segment_budget_conscious = 1 THEN 'Budget-Conscious'
            WHEN segment_stable = 1 THEN 'Stable Mid-Spenders'
            ELSE 'Unknown'
        END AS segment,
        churned
    FROM GOLD.ML_TRAINING_DATA
)
SELECT
    'Check 7: Segment Distribution' AS check_name,
    segment,
    COUNT(*) AS total_count,
    SUM(CASE WHEN churned = 1 THEN 1 ELSE 0 END) AS churned_count,
    ROUND(SUM(CASE WHEN churned = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS churn_rate_pct
FROM segment_distribution
GROUP BY segment
ORDER BY churn_rate_pct DESC;

-- ============================================================================
-- Check 8: Sufficient Training Examples per Class
-- ============================================================================

SELECT
    'Check 8: Minimum Examples per Class' AS check_name,
    SUM(CASE WHEN churned = 1 THEN 1 ELSE 0 END) AS churned_examples,
    SUM(CASE WHEN churned = 0 THEN 1 ELSE 0 END) AS active_examples,
    CASE
        WHEN SUM(CASE WHEN churned = 1 THEN 1 ELSE 0 END) >= 1000 AND SUM(CASE WHEN churned = 0 THEN 1 ELSE 0 END) >= 1000
        THEN '✓ PASS: Sufficient examples per class'
        WHEN SUM(CASE WHEN churned = 1 THEN 1 ELSE 0 END) >= 100 AND SUM(CASE WHEN churned = 0 THEN 1 ELSE 0 END) >= 100
        THEN '⚠ WARNING: Low examples per class (recommend 1K+ per class)'
        ELSE '✗ FAIL: Insufficient examples per class'
    END AS status
FROM GOLD.ML_TRAINING_DATA;

-- ============================================================================
-- Summary
-- ============================================================================

SELECT '============================================================================' AS divider;
SELECT 'VALIDATION COMPLETE' AS summary_title;
SELECT '============================================================================' AS divider;

SELECT
    CONCAT(COUNT(*), ' total training examples') AS summary_1
FROM GOLD.ML_TRAINING_DATA;

SELECT
    CONCAT(SUM(churned), ' churned customers (', ROUND(AVG(churned) * 100, 2), '% churn rate)') AS summary_2
FROM GOLD.ML_TRAINING_DATA;

SELECT
    CONCAT(SUM(CASE WHEN churned = 0 THEN 1 ELSE 0 END), ' active customers (', ROUND((1 - AVG(churned)) * 100, 2), '%)') AS summary_3
FROM GOLD.ML_TRAINING_DATA;

SELECT '✓ Review all validation checks above before training model' AS next_steps;
