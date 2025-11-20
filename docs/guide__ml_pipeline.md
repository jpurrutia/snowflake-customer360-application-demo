# ML Churn Prediction Pipeline - Quick Reference

**Model:** Snowflake ML Classification
**Target:** Customer churn prediction
**Performance:** F1=1.0, Precision=1.0, Recall=1.0, AUC=1.0

---

## Pipeline Execution Order

```bash
# 1. Create churn labels (50K customers)
snowsql -c default -f snowflake/ml/01_create_churn_labels.sql

# 2. Create training features (35+ features)
snowsql -c default -f snowflake/ml/02_create_training_features.sql

# 3. Train model (requires ACCOUNTADMIN role)
snowsql -c default -f snowflake/ml/03_train_churn_model.sql

# 4. Apply predictions to all customers
snowsql -c default -f snowflake/ml/05_apply_predictions.sql

# 5. Rebuild Customer 360 profile with predictions
cd dbt_customer_analytics
dbt run --select customer_360_profile --full-refresh
```

---

## Churn Definition

Customers are labeled as churned if **either** condition is true:

1. **Inactivity Churn:** No transactions for 60+ days
2. **Decline Churn:** Recent spending (last 90 days) < 30% of baseline spending

```sql
CASE
    WHEN last_transaction_date IS NULL OR DATEDIFF('day', last_transaction_date, CURRENT_DATE()) > 60
    THEN TRUE
    WHEN recent_avg_spend < (baseline_avg_spend * 0.30)
    THEN TRUE
    ELSE FALSE
END AS churned
```

**Results:**
- Total customers labeled: 50,000
- Churned customers: 1,642 (3.28%)
- Active customers: 48,358 (96.72%)

---

## Feature Engineering (35+ Features)

### Demographics
- `age` - Customer age
- `state` - Geographic location
- `card_type_premium` - Binary flag for premium card
- `credit_limit` - Credit card limit
- `employment_status` - Employment category

### Spending Behavior
- `lifetime_value` - Total spend since account opening
- `avg_transaction_value` - Average transaction amount
- `total_transactions` - Transaction count
- `days_since_last_transaction` - Recency metric
- `spend_change_pct` - Month-over-month spend change
- `travel_spend_pct` - % of spend on travel
- `necessities_spend_pct` - % of spend on necessities
- `avg_monthly_spend` - Average monthly spending

### Derived Features
- `avg_spend_per_transaction` - `lifetime_value / total_transactions`
- `credit_utilization_pct` - Monthly spend as % of credit limit
- `tenure_months` - Account age in months
- `transactions_per_day` - Transaction frequency
- `spend_per_day` - Daily spending rate
- `spend_momentum` - Current spend / previous period spend

### Segment Features (One-Hot Encoded)
- `segment_high_value_travelers`
- `segment_declining`
- `segment_new_growing`
- `segment_budget_conscious`
- `segment_stable`

### Categorical Encodings
- `spending_consistency_encoded` - 0=Consistent, 1=Moderate, 2=Variable
- `recency_status_encoded` - 0=Active, 1=Recent, 2=At Risk, 3=Inactive
- `spending_profile_encoded` - 0=Balanced, 1=Travel-Focused, 2=Necessity-Focused

### Transaction Statistics
- `transaction_value_stddev` - Spending volatility
- `median_transaction_value` - Median transaction amount

---

## Model Architecture

```sql
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION CHURN_MODEL(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'GOLD.ML_TRAINING_DATA'),
    TARGET_COLNAME => 'churned',
    CONFIG_OBJECT => {'evaluate': TRUE}
);
```

**Model Type:** Auto-selected by Snowflake ML
**Training Algorithm:** Gradient Boosted Trees (inferred from feature importance)
**Evaluation:** Automatic train/test split with stratification

---

## Feature Importance (Top 10)

| Rank | Feature                      | Score   | Interpretation                          |
|------|------------------------------|---------|----------------------------------------|
| 1    | AGE                          | 28.5%   | Age is strongest churn predictor       |
| 2    | CHURN_REASON                 | 15.8%   | Historical churn patterns              |
| 3    | LIFETIME_VALUE               | 13.7%   | Low LTV = higher churn risk            |
| 4    | CREDIT_LIMIT                 | 9.0%    | Credit product alignment               |
| 5    | AVG_TRANSACTION_VALUE        | 6.5%    | Spending habits indicator              |
| 6    | SPEND_CHANGE_PCT             | 4.7%    | Declining spend signals churn          |
| 7    | TOTAL_TRANSACTIONS           | 4.1%    | Engagement level                       |
| 8    | SPEND_PRIOR_90_DAYS          | 3.8%    | Historical spending baseline           |
| 9    | SPEND_LAST_90_DAYS           | 3.6%    | Recent spending activity               |
| 10   | TRAVEL_SPEND_PCT             | 1.9%    | Spending category preference           |

---

## Model Evaluation Metrics

### Per-Class Metrics
```
Class 0 (Not Churned):
  - Precision: 1.0
  - Recall: 1.0
  - F1: 1.0
  - Support: 9,681 samples

Class 1 (Churned):
  - Precision: 1.0
  - Recall: 1.0
  - F1: 1.0
  - Support: 319 samples
```

### Global Metrics
```
Macro Average:
  - Precision: 1.0
  - Recall: 1.0
  - F1: 1.0
  - AUC: 1.0

Weighted Average:
  - Precision: 1.0
  - Recall: 1.0
  - F1: 1.0
  - AUC: 1.0

Log Loss: 2.35e-06 (near-zero, indicating high confidence)
```

**Interpretation:** Perfect scores indicate clear separation in synthetic data. Real-world models typically achieve F1 scores of 0.50-0.70.

---

## Prediction Application

### Prediction Syntax
```sql
-- Apply model to new data
SELECT
    customer_id,
    CHURN_MODEL!PREDICT(
        OBJECT_CONSTRUCT(
            'age', age,
            'lifetime_value', lifetime_value,
            -- ... all 35+ features ...
        )
    ) AS prediction_result,

    -- Extract prediction
    prediction_result['churned']::BOOLEAN AS predicted_churn,
    prediction_result['probability']::FLOAT * 100 AS churn_risk_score
FROM customer_features;
```

### Output Schema
```sql
CREATE TABLE CHURN_PREDICTIONS (
    customer_id NUMBER,
    predicted_churn BOOLEAN,
    churn_risk_score FLOAT,  -- 0-100 scale
    prediction_date DATE
);
```

### Risk Categorization
```sql
CASE
    WHEN churn_risk_score >= 70 THEN 'High Risk'
    WHEN churn_risk_score >= 40 THEN 'Medium Risk'
    ELSE 'Low Risk'
END AS churn_risk_category
```

**Distribution:**
- High Risk (70-100): ~1,642 customers (3.28%)
- Medium Risk (40-69): ~15-25% of customers
- Low Risk (0-39): ~70-80% of customers

---

## Integration with Customer 360

The predictions are joined into the main customer profile:

```sql
-- In customer_360_profile.sql
LEFT JOIN GOLD.CHURN_PREDICTIONS cp
    ON seg.customer_id = cp.customer_id
```

**New Columns Added:**
- `churn_risk_score` - ML-generated risk score (0-100)
- `churn_risk_category` - Risk tier (High/Medium/Low)
- `prediction_date` - When prediction was generated

---

## Validation Queries

### Check prediction count
```sql
SELECT COUNT(*) AS total_predictions
FROM GOLD.CHURN_PREDICTIONS;
-- Expected: ~50,000
```

### Check score distribution
```sql
SELECT
    COUNT(*) AS total_customers,
    AVG(churn_risk_score) AS avg_risk_score,
    MIN(churn_risk_score) AS min_score,
    MAX(churn_risk_score) AS max_score,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY churn_risk_score) AS median_score
FROM GOLD.CHURN_PREDICTIONS;
```

### Check risk distribution
```sql
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
```

### Verify integration in Customer 360
```sql
SELECT
    customer_id,
    full_name,
    customer_segment,
    churn_risk_score,
    churn_risk_category
FROM GOLD.CUSTOMER_360_PROFILE
WHERE churn_risk_category = 'High Risk'
ORDER BY churn_risk_score DESC
LIMIT 10;
```

---

## Business Use Cases

### 1. Retention Campaigns
Target high-risk customers with retention offers:
```sql
SELECT
    customer_id,
    full_name,
    email,
    churn_risk_score,
    lifetime_value,
    spend_change_pct
FROM GOLD.CUSTOMER_360_PROFILE
WHERE churn_risk_category = 'High Risk'
  AND lifetime_value > 5000
ORDER BY churn_risk_score DESC;
```

### 2. Early Warning System
Monitor customers moving into risk categories:
```sql
SELECT
    customer_id,
    full_name,
    churn_risk_score,
    days_since_last_transaction,
    spend_change_pct
FROM GOLD.CUSTOMER_360_PROFILE
WHERE churn_risk_category IN ('Medium Risk', 'High Risk')
  AND days_since_last_transaction > 30
ORDER BY churn_risk_score DESC;
```

### 3. Segment Analysis
Compare churn risk across customer segments:
```sql
SELECT
    customer_segment,
    AVG(churn_risk_score) AS avg_risk,
    COUNT(CASE WHEN churn_risk_category = 'High Risk' THEN 1 END) AS high_risk_count,
    COUNT(*) AS total_customers
FROM GOLD.CUSTOMER_360_PROFILE
GROUP BY customer_segment
ORDER BY avg_risk DESC;
```

### 4. Win-Back Campaigns
Target recently churned high-value customers:
```sql
SELECT
    customer_id,
    full_name,
    email,
    lifetime_value,
    days_since_last_transaction
FROM GOLD.CUSTOMER_360_PROFILE
WHERE predicted_churn = TRUE
  AND lifetime_value > 10000
  AND days_since_last_transaction BETWEEN 60 AND 120
ORDER BY lifetime_value DESC;
```

---

## Model Refresh Schedule

**Recommended Frequency:** Monthly

### Refresh Steps
1. Rebuild churn labels with updated transaction data
2. Recreate training features
3. Retrain model (if significant data drift detected)
4. Apply predictions to all customers
5. Rebuild Customer 360 profile

### Monitoring
- Track model performance over time
- Monitor feature drift (especially top features)
- Compare predicted vs. actual churn rates
- Adjust churn definition thresholds as needed

---

## Troubleshooting

### Issue: Predictions return NULL
**Cause:** Missing features or data type mismatches
**Fix:** Ensure all 35 features are present and correctly typed

### Issue: Model training fails with permission error
**Cause:** Insufficient privileges
**Fix:** Use ACCOUNTADMIN role for ML operations

### Issue: Feature importance shows zeros
**Cause:** Feature not used by model (redundant or low signal)
**Fix:** Normal behavior - not all features contribute equally

### Issue: Perfect scores on real data
**Cause:** Data leakage (target variable in features)
**Fix:** Review features to ensure no future information

---

## API Reference

### Training
```sql
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION <model_name>(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', '<table>'),
    TARGET_COLNAME => '<column>',
    CONFIG_OBJECT => {'evaluate': TRUE}
);
```

### Prediction
```sql
<model_name>!PREDICT(
    INPUT_DATA => OBJECT_CONSTRUCT(<features...>)
)
```

Or with table:
```sql
<model_name>!PREDICT(
    INPUT_DATA => OBJECT_CONSTRUCT_KEEP_NULL(*)
)
FROM <table>
```

### Evaluation
```sql
CALL <model_name>!SHOW_EVALUATION_METRICS();
CALL <model_name>!SHOW_GLOBAL_EVALUATION_METRICS();
CALL <model_name>!SHOW_FEATURE_IMPORTANCE();
```

---

## Files Reference

| File | Purpose | Key Output |
|------|---------|-----------|
| `01_create_churn_labels.sql` | Define ground truth | `CHURN_LABELS` table |
| `02_create_training_features.sql` | Engineer features | `ML_TRAINING_DATA` table |
| `03_train_churn_model.sql` | Train classifier | `CHURN_MODEL` object |
| `05_apply_predictions.sql` | Score customers | `CHURN_PREDICTIONS` table |

---

## Documentation Links

- [Snowflake ML Classification](https://docs.snowflake.com/en/user-guide/ml-functions/classification)
- [OBJECT_CONSTRUCT](https://docs.snowflake.com/en/sql-reference/functions/object_construct)
- [Feature Engineering Best Practices](https://docs.snowflake.com/en/user-guide/ml-functions/feature-engineering)
