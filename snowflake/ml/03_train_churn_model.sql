-- ============================================================================
-- Train Snowflake Cortex ML Classification Model for Churn Prediction
-- ============================================================================
-- Purpose: Train binary classification model to predict customer churn
--
-- Model Type: SNOWFLAKE.ML.CLASSIFICATION
-- Target Variable: churned (BOOLEAN)
-- Training Data: GOLD.ML_TRAINING_DATA (prepared in 02_create_training_features.sql)
-- Evaluation Metric: F1 Score (balances precision and recall)
--
-- Usage:
--   Run this script in Snowflake to train the churn prediction model.
--   Model will be created in the current schema/database.
--
-- Prerequisites:
--   - GOLD.ML_TRAINING_DATA table exists and is validated
--   - VALIDATE_TRAINING_DATA() stored procedure exists
--   - Sufficient compute warehouse size for model training
--
-- Post-Training:
--   - Review evaluation metrics to assess model quality
--   - Check feature importance to understand drivers of churn
--   - Proceed to 05_apply_predictions.sql to score all customers
-- ============================================================================

-- Set context (ACCOUNTADMIN required for ML operations)
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA GOLD;

-- Step 1: Pre-training validation (optional - commented out if stored procedure not available)
-- Ensure training data meets quality thresholds before expensive training
-- CALL VALIDATE_TRAINING_DATA();

-- Step 2: Drop existing model if retraining
-- This allows for iterative model improvement
DROP SNOWFLAKE.ML.CLASSIFICATION IF EXISTS CHURN_MODEL;

-- Step 3: Train the ML classification model
-- Snowflake will automatically:
--   - Split data into train/validation sets
--   - Handle feature encoding (categorical -> numeric)
--   - Tune hyperparameters
--   - Evaluate model performance
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION CHURN_MODEL(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'GOLD.ML_TRAINING_DATA'),
    TARGET_COLNAME => 'churned',
    CONFIG_OBJECT => {'evaluate': TRUE}
);

-- Step 4: Display model evaluation metrics
-- Metrics include: F1, Precision, Recall per class
CALL CHURN_MODEL!SHOW_EVALUATION_METRICS();

-- Step 5: Show global evaluation metrics
-- Shows macro/weighted averages across all classes
CALL CHURN_MODEL!SHOW_GLOBAL_EVALUATION_METRICS();

-- Step 6: Show feature importance
-- Reveals which features most strongly predict churn
CALL CHURN_MODEL!SHOW_FEATURE_IMPORTANCE();

-- ============================================================================
-- Expected Output:
-- ============================================================================
-- CHURN_MODEL created successfully
-- F1 Score: ~0.50-0.70 (acceptable for business use)
-- Precision: ~0.60+ (60%+ of predicted churners actually churn)
-- Recall: ~0.40+ (catch 40%+ of actual churners)
--
-- Top Features (expected):
--   1. days_since_last_transaction (strong predictor)
--   2. spend_change_pct (declining spend = churn risk)
--   3. lifetime_value (low LTV = higher churn)
--   4. avg_monthly_spend (low spend = disengagement)
--   5. tenure_months (new customers churn more)
-- ============================================================================
