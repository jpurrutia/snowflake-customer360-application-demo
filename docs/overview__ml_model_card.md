# ML Model Card: Customer Churn Prediction

**Model Name**: Churn Prediction Classifier
**Version**: 2.0 (Model Trained & Deployed)
**Date**: 2025-11-12
**Status**: Deployed to Production (Iteration 4.2 Complete)

---

## Model Overview

### Problem Statement

Predict which credit card customers will churn (stop using their card) within the next 60-90 days to enable proactive retention campaigns.

**Business Impact**:
- Early identification of at-risk customers
- Targeted retention offers to high-value churners
- Reduced customer acquisition costs
- Improved customer lifetime value

### Model Type

**Binary Classification** (Supervised Learning)
- **Positive Class (1)**: Churned (inactive or declining)
- **Negative Class (0)**: Active (engaged customers)

### Target Variable

**`churned`** (Boolean → Integer encoding)

**Churn Definition**: Customer is churned if **either**:
1. **Inactivity Churn**: No transactions for 60+ days
2. **Decline Churn**: Recent spending < 30% of baseline

---

## Target Variable Definition

### Baseline Period

**First 12 months** of customer history
- **Metric**: Average monthly spend
- **Requirement**: At least 6 months of data
- **Exclusion**: Last 6 months (reserved for validation)

### Recent Period

**Last 3 months** of customer history
- **Metric**: Average monthly spend
- **Comparison**: Against baseline

### Churn Rules

```sql
CASE
    -- Rule 1: Inactivity
    WHEN last_transaction_date IS NULL
         OR days_since_last_transaction > 60
    THEN TRUE  -- Churned

    -- Rule 2: Significant Decline
    WHEN recent_avg_spend < (baseline_avg_spend * 0.30)
    THEN TRUE  -- Churned

    -- Active
    ELSE FALSE
END AS churned
```

### Rationale

| Criterion | Value | Justification |
|-----------|-------|---------------|
| Inactivity threshold | 60 days | Industry standard for credit card dormancy |
| Decline threshold | 30% of baseline | Significant reduction indicating disengagement |
| Baseline period | 12 months | Captures normal spending patterns |
| Recent period | 3 months | Recent enough to detect current behavior |

---

## Features (35+)

### Feature Categories

| Category | Count | Examples |
|----------|-------|----------|
| **Demographics** | 5 | age, state, card_type, credit_limit, employment_status |
| **Spending Behavior** | 15 | lifetime_value, avg_transaction_value, recency, frequency, trends |
| **Category Preferences** | 3 | travel_spend_pct, necessities_spend_pct, spending_profile |
| **Segments** | 6 | segment_high_value_travelers, segment_declining, etc. (one-hot) |
| **Derived Features** | 5+ | credit_utilization_pct, spend_momentum, transactions_per_day |

**Total**: 35+ features

### Feature List with Descriptions

| Feature | Type | Range | Description |
|---------|------|-------|-------------|
| **age** | Numeric | 18-100 | Customer age in years |
| **state** | Categorical | 50 states | Customer state (2-letter code) |
| **card_type_premium** | Binary | 0-1 | 1 if Premium card, 0 if Standard |
| **credit_limit** | Numeric | 5K-50K | Credit limit in dollars |
| **employment_status** | Categorical | 5 types | Full-Time, Part-Time, Self-Employed, etc. |
| **lifetime_value** | Numeric | 0+ | Total spending all-time |
| **avg_transaction_value** | Numeric | 0+ | Mean transaction amount |
| **total_transactions** | Numeric | 5+ | Total transaction count (filtered ≥5) |
| **customer_age_days** | Numeric | 0+ | Days between first/last transaction |
| **days_since_last_transaction** | Numeric | 0+ | Days since last activity (recency) |
| **spend_last_90_days** | Numeric | 0+ | Spending in last 90 days |
| **spend_prior_90_days** | Numeric | 0+ | Spending in prior 90 days (days 91-180) |
| **spend_change_pct** | Numeric | -100 to ∞ | Percentage change in spending |
| **avg_monthly_spend** | Numeric | 0+ | Average monthly spending |
| **transaction_value_stddev** | Numeric | 0+ | Standard deviation of transaction amounts |
| **median_transaction_value** | Numeric | 0+ | Median transaction amount |
| **spending_consistency_encoded** | Categorical | 0-2 | 0=Consistent, 1=Moderate, 2=Variable |
| **travel_spend_pct** | Numeric | 0-100 | % of spending on travel categories |
| **necessities_spend_pct** | Numeric | 0-100 | % of spending on necessities |
| **spending_profile_encoded** | Categorical | 0-2 | 0=Balanced, 1=Travel, 2=Necessity |
| **segment_high_value_travelers** | Binary | 0-1 | Customer segment flag (one-hot) |
| **segment_declining** | Binary | 0-1 | Customer segment flag (one-hot) |
| **segment_new_growing** | Binary | 0-1 | Customer segment flag (one-hot) |
| **segment_budget_conscious** | Binary | 0-1 | Customer segment flag (one-hot) |
| **segment_stable** | Binary | 0-1 | Customer segment flag (one-hot) |
| **tenure_months** | Numeric | 0+ | Months since first transaction |
| **avg_spend_per_transaction** | Numeric | 0+ | lifetime_value / total_transactions |
| **credit_utilization_pct** | Numeric | 0-200 | (monthly_spend / credit_limit) × 100 |
| **transactions_per_day** | Numeric | 0+ | total_transactions / customer_age_days |
| **spend_per_day** | Numeric | 0+ | lifetime_value / customer_age_days |
| **recency_score** | Categorical | 0-3 | 0=Active, 1=Recent, 2=At Risk, 3=Inactive |
| **recency_status_encoded** | Categorical | 0-3 | Encoded recency status |
| **spend_momentum** | Numeric | 0+ | spend_last_90_days / spend_prior_90_days |

### Expected Feature Importance (Hypothesis)

| Rank | Feature | Expected Importance | Rationale |
|------|---------|-------------------|-----------|
| 1 | days_since_last_transaction | Very High | Direct indicator of inactivity |
| 2 | spend_change_pct | Very High | Captures decline behavior |
| 3 | credit_utilization_pct | High | Financial health indicator |
| 4 | segment_declining | High | Pre-identified churn risk |
| 5 | spend_momentum | High | Trend acceleration/deceleration |
| 6 | avg_monthly_spend | Medium | Baseline engagement |
| 7 | tenure_months | Medium | Loyalty indicator |
| 8 | transactions_per_day | Medium | Frequency of engagement |

---

## Training Data

### Dataset Summary

| Metric | Value |
|--------|-------|
| **Table** | `GOLD.ML_TRAINING_DATA` |
| **Total Examples** | ~40-45K |
| **Positive Class (Churned)** | ~4-6K (8-15%) |
| **Negative Class (Active)** | ~35-40K (85-92%) |
| **Features** | 35+ |
| **Nulls in Critical Features** | 0 |

### Class Distribution

**Expected**: Imbalanced (realistic churn rate)
- **Churned**: 8-15% (positive class)
- **Active**: 85-92% (negative class)

**Handling Imbalance** (in model training):
- Class weights (penalize minority class errors more)
- SMOTE (Synthetic Minority Over-sampling)
- Evaluation metrics: F1-Score, ROC-AUC (not just accuracy)

### Data Filters

```sql
WHERE
    -- Only customers with baseline data
    baseline_avg_spend IS NOT NULL AND baseline_avg_spend > 0

    -- Minimum transaction history for reliable features
    AND total_transactions >= 5
```

### Data Quality

- ✓ No null values in critical features
- ✓ Realistic feature ranges validated
- ✓ Sufficient examples per class (≥1K each)
- ✓ Consistent encoding for categorical features

---

## Model Evaluation (Completed in Iteration 4.2)

### Performance Metrics

| Metric | Target | Expected Range | Rationale |
|--------|--------|----------------|-----------|
| **F1-Score** | ≥0.50 | 0.50-0.70 | Balance precision and recall |
| **Precision** | ≥0.60 | 0.60-0.75 | Minimize false positives (wasted retention offers) |
| **Recall** | ≥0.40 | 0.40-0.65 | Maximize true positives (catch churners) |
| **ROC-AUC** | ≥0.70 | 0.70-0.85 | Overall model discrimination |

**Note**: Actual performance metrics are available via:
```sql
SELECT * FROM TABLE(CHURN_MODEL!SHOW_EVALUATION_METRICS());
```

### Confusion Matrix (Expected)

|                | Predicted: Active | Predicted: Churned |
|----------------|-------------------|-------------------|
| **Actual: Active** | TN (high) | FP (low) |
| **Actual: Churned** | FN (medium) | TP (high) |

**Business Trade-off**:
- **False Positives (FP)**: Waste retention offer on active customer (low cost)
- **False Negatives (FN)**: Miss churner, lose customer (high cost)
- **Priority**: Minimize FN (maximize Recall)

---

## Model Deployment (Completed in Iteration 4.2)

### Deployment Architecture

```
Training Data (GOLD.ML_TRAINING_DATA)
    ↓
Cortex ML Training (CHURN_MODEL)
    ↓
Batch Predictions (GOLD.CHURN_PREDICTIONS)
    ↓
Customer 360 Integration (GOLD.CUSTOMER_360_PROFILE)
    ↓
Dashboard/Application Consumption
```

### Scoring Pipeline

1. **Batch Scoring** (snowflake/ml/05_apply_predictions.sql):
   - Runs daily/weekly to score all active customers
   - Generates predictions for ~45-50K customers
   - Stores results in GOLD.CHURN_PREDICTIONS table

   ```sql
   -- Apply predictions to all customers
   CREATE OR REPLACE TABLE GOLD.CHURN_PREDICTIONS AS
   SELECT
       customer_id,
       CHURN_MODEL!PREDICT(...) AS prediction_result,
       prediction_result['churned']::BOOLEAN AS predicted_churn,
       prediction_result['probability']::FLOAT * 100 AS churn_risk_score,
       CURRENT_DATE() AS prediction_date
   FROM customer_features;
   ```

2. **Customer 360 Integration** (dbt model):
   - Automatically joined via dbt customer_360_profile model
   - Adds churn_risk_score and churn_risk_category columns
   - Updates retention campaign eligibility flags

   ```sql
   -- In customer_360_profile.sql
   LEFT JOIN {{ source('gold', 'churn_predictions') }} pred
       ON c.customer_id = pred.customer_id
   ```

3. **Campaign Targeting**:
   ```sql
   SELECT customer_id, full_name, email, churn_risk_score, lifetime_value
   FROM GOLD.CUSTOMER_360_PROFILE
   WHERE churn_risk_category = 'High Risk'
     AND lifetime_value > 10000  -- Target high-value customers
   ORDER BY churn_risk_score DESC
   LIMIT 1000;
   ```

### Churn Risk Categories

| Category | Score Range | Expected % | Action |
|----------|-------------|-----------|--------|
| **Low Risk** | 0-39 | 70-80% | Normal marketing |
| **Medium Risk** | 40-69 | 15-25% | Engagement campaigns |
| **High Risk** | 70-100 | 5-10% | Retention offers |

### Retraining Workflow

**Automated Retraining** (via stored procedures):

```sql
-- Full monthly retraining
CALL RETRAIN_CHURN_MODEL();
-- Returns: 'SUCCESS: Model retrained with F1 score X.XX'

-- Daily prediction refresh (no retraining)
CALL REFRESH_CHURN_PREDICTIONS();
-- Returns: 'SUCCESS: Predictions refreshed for X customers'
```

**Scheduled Tasks** (recommended):
```sql
-- Monthly retraining (1st of month at 2 AM)
CREATE TASK MONTHLY_MODEL_RETRAIN
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 2 1 * * America/Los_Angeles'
AS
    CALL RETRAIN_CHURN_MODEL();

-- Daily prediction refresh (3 AM daily)
CREATE TASK DAILY_PREDICTION_REFRESH
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 3 * * * America/Los_Angeles'
AS
    CALL REFRESH_CHURN_PREDICTIONS();
```

---

## Ethical Considerations

### Bias Mitigation

**Potential Biases**:
- **Geographic**: Certain states may have different spending patterns
- **Demographic**: Age groups may have different churn rates
- **Product**: Premium vs Standard card holders

**Mitigation**:
- Monitor fairness metrics across subgroups (state, age, card_type)
- Ensure model performance is equitable
- Avoid discriminatory features (race, gender not included)

### Privacy

- **PII Handling**: Email and names not used as features
- **Data Retention**: Training data stored securely in GOLD schema
- **Access Control**: Role-based access to ML models and predictions

---

## Model Limitations

1. **Temporal Drift**: Customer behavior may change over time (requires retraining)
2. **Cold Start**: New customers (<12 months) excluded from training
3. **External Factors**: Economic conditions, seasonality not captured
4. **Interpretability**: Complex ML models may be black-box (use SHAP values)

---

## Model Maintenance

### Monitoring

- **Performance Drift**: Track ROC-AUC monthly
- **Feature Drift**: Monitor feature distributions
- **Prediction Distribution**: Track churn rate over time

### Retraining Schedule

- **Quarterly**: Retrain with new data (rolling 18-month window)
- **Trigger-Based**: If ROC-AUC drops below 0.70

---

## References

**Database Objects**:
- `CHURN_MODEL` - Snowflake Cortex ML classification model
- `GOLD.CHURN_LABELS` - Labeled training data
- `GOLD.ML_TRAINING_DATA` - Feature table
- `GOLD.CHURN_PREDICTIONS` - Batch predictions table
- `RETRAIN_CHURN_MODEL()` - Retraining stored procedure
- `REFRESH_CHURN_PREDICTIONS()` - Prediction refresh stored procedure

**SQL Scripts**:
- `snowflake/ml/01_create_churn_labels.sql` - Generate churn labels
- `snowflake/ml/02_create_training_features.sql` - Engineer features
- `snowflake/ml/03_train_churn_model.sql` - Train Cortex ML model
- `snowflake/ml/04_validate_model_performance.sql` - Validate metrics
- `snowflake/ml/05_apply_predictions.sql` - Generate predictions
- `snowflake/ml/validate_training_data.sql` - Data quality validation
- `snowflake/ml/stored_procedures.sql` - Retraining automation

**dbt Models**:
- `dbt_customer_analytics/models/marts/customer_analytics/customer_360_profile.sql` - Integrates predictions

**Documentation**:
- `snowflake/ml/README.md` - ML pipeline guide
- `docs/ml_model_card.md` (this file) - Comprehensive model card
- `docs/prompt_4.1_completion_summary.md` - Training data iteration
- `docs/prompt_4.2_completion_summary.md` - Model training iteration

**Tests**:
- `tests/integration/test_churn_training_data.py` - Training data tests
- `tests/integration/test_churn_model.py` - Model and prediction tests

---

**Model Card Version**: 2.0 (Model Trained & Deployed)
**Status**: Production Ready
**Last Updated**: 2025-11-12
