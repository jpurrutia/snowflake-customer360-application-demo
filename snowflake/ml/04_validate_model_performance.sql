-- ============================================================================
-- Validate Churn Model Performance Against Minimum Thresholds
-- ============================================================================
-- Purpose: Ensure trained model meets business requirements before deployment
--
-- Minimum Thresholds:
--   - F1 Score >= 0.50 (balance of precision/recall)
--   - Precision >= 0.60 (minimize false positives)
--   - Recall >= 0.40 (catch enough churners)
--
-- Usage:
--   Run after 03_train_churn_model.sql
--   Result: PASS or FAIL with specific reason
--
-- Prerequisites:
--   - CHURN_MODEL exists (trained in 03_train_churn_model.sql)
-- ============================================================================

-- Validate model performance meets minimum thresholds
WITH model_metrics AS (
    SELECT * FROM TABLE(CHURN_MODEL!SHOW_EVALUATION_METRICS())
)

SELECT
    CASE
        WHEN (SELECT F1_SCORE FROM model_metrics) < 0.50
        THEN 'FAIL: F1 score too low'
        WHEN (SELECT PRECISION FROM model_metrics) < 0.60
        THEN 'FAIL: Precision too low'
        WHEN (SELECT RECALL FROM model_metrics) < 0.40
        THEN 'FAIL: Recall too low'
        ELSE 'PASS: Model performance acceptable'
    END AS validation_result,
    (SELECT F1_SCORE FROM model_metrics) AS f1_score,
    (SELECT PRECISION FROM model_metrics) AS precision,
    (SELECT RECALL FROM model_metrics) AS recall;

-- ============================================================================
-- Interpretation Guide:
-- ============================================================================
-- F1 Score: Harmonic mean of precision and recall
--   - 0.50-0.60: Acceptable for initial deployment
--   - 0.60-0.70: Good performance
--   - 0.70+: Excellent performance
--
-- Precision: Of customers predicted to churn, how many actually churn?
--   - 0.60 = 60% of predicted churners are true churners
--   - Higher = fewer wasted retention offers (lower cost)
--
-- Recall: Of customers who actually churn, how many did we predict?
--   - 0.40 = catch 40% of actual churners
--   - Higher = fewer missed churners (more revenue saved)
--
-- Business Trade-off:
--   - Low precision = wasted retention offers (low cost per false positive)
--   - Low recall = lost customers (high cost per false negative)
--   - Priority: Maximize recall while maintaining acceptable precision
-- ============================================================================
