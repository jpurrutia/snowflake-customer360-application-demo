# Machine Learning: Churn Training Data Preparation

**Version**: 1.0
**Date**: 2025-11-11
**Purpose**: Prepare labeled training data for churn prediction ML model

---

## Overview

This directory contains SQL scripts to create labeled training data for a customer churn prediction model. The dataset will be used with Snowflake Cortex ML (Prompt 4.2) to train a classification model.

---

## Churn Definition

**Churn = TRUE** if either condition is met:

1. **Inactivity Churn**: No transactions for 60+ days
2. **Decline Churn**: Recent spending < 30% of baseline

**Baseline**: Average monthly spend during first 12 months
**Recent**: Average monthly spend in last 3 months

**Rationale**:
- **60 days**: Industry standard for credit card inactivity
- **30% threshold**: Significant decline indicating disengagement
- **Dual criteria**: Captures both complete inactivity and gradual decline

---

## Files

| File | Purpose | Output |
|------|---------|--------|
| `01_create_churn_labels.sql` | Generate churn labels based on behavior | `GOLD.CHURN_LABELS` |
| `02_create_training_features.sql` | Create feature table for ML | `GOLD.ML_TRAINING_DATA` |
| `validate_training_data.sql` | Comprehensive validation checks | Validation report |
| `README.md` | This file | Documentation |

---

## Execution Order

```bash
cd snowflake/ml

# Step 1: Create churn labels
snowflake-sql -f 01_create_churn_labels.sql

# Step 2: Create training features
snowflake-sql -f 02_create_training_features.sql

# Step 3: Validate training data
snowflake-sql -f validate_training_data.sql
```

**Expected Duration**: 2-3 minutes total (SMALL warehouse)

---

## 1. Churn Labels (`01_create_churn_labels.sql`)

### Process

1. **Calculate Baseline** (first 12 months):
   - Average monthly spend
   - Requires at least 6 months of data
   - Excludes last 6 months (reserved for validation)

2. **Analyze Recent Behavior** (last 3 months):
   - Last transaction date
   - Average monthly spend
   - Transaction count

3. **Apply Churn Rules**:
   ```sql
   CASE
       WHEN last_transaction_date IS NULL
            OR days_since_last_transaction > 60
       THEN TRUE  -- Inactivity churn

       WHEN recent_avg_spend < (baseline_avg_spend * 0.30)
       THEN TRUE  -- Decline churn

       ELSE FALSE  -- Active customer
   END AS churned
   ```

### Output Schema: `GOLD.CHURN_LABELS`

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | STRING | Customer identifier |
| `baseline_months` | INT | Months of baseline data |
| `baseline_avg_spend` | NUMBER | Average monthly spend (baseline) |
| `recent_avg_spend` | NUMBER | Average monthly spend (recent) |
| `days_since_last_transaction` | INT | Recency metric |
| `spend_change_pct` | NUMBER | Percentage change |
| `churned` | BOOLEAN | Target variable (TRUE/FALSE) |
| `churn_reason` | STRING | Reason (for analysis) |

### Expected Output

- **Rows**: ~40K-45K customers (those with baseline data)
- **Churn Rate**: 8-15% (class imbalance)
- **Churn Reasons**:
  - Inactive (60+ days): ~5-7%
  - Significant decline: ~3-8%
  - Active: ~85-92%

---

## 2. Training Features (`02_create_training_features.sql`)

### Feature Categories

#### Demographic Features (5)
- `age`: Customer age (18-100)
- `state`: Customer state (2-letter code)
- `card_type_premium`: Binary (0=Standard, 1=Premium)
- `credit_limit`: Credit limit ($5K-$50K)
- `employment_status`: Employment type

#### Spending Behavior Features (15)
- `lifetime_value`: Total spending
- `avg_transaction_value`: Mean transaction amount
- `total_transactions`: Transaction count
- `customer_age_days`: Days between first/last transaction
- `days_since_last_transaction`: Recency
- `spend_last_90_days`: Recent period spending
- `spend_prior_90_days`: Prior period spending
- `spend_change_pct`: Trend metric
- `avg_monthly_spend`: Monthly average
- `transaction_value_stddev`: Spending variability
- `median_transaction_value`: 50th percentile
- `spending_consistency_encoded`: Categorical (0-2)
- `credit_utilization_pct`: Monthly spend / credit limit
- `transactions_per_day`: Frequency metric
- `spend_per_day`: Velocity metric

#### Category Preference Features (3)
- `travel_spend_pct`: % on travel categories
- `necessities_spend_pct`: % on necessities
- `spending_profile_encoded`: Categorical (0-2)

#### Segment Features (6)
- `segment_high_value_travelers`: Binary flag
- `segment_declining`: Binary flag
- `segment_new_growing`: Binary flag
- `segment_budget_conscious`: Binary flag
- `segment_stable`: Binary flag
- `tenure_months`: Months since first transaction

#### Derived Features (5)
- `avg_spend_per_transaction`: LTV / total_transactions
- `recency_score`: Categorical (0-3)
- `recency_status_encoded`: Categorical (0-3)
- `spend_momentum`: Ratio of recent to prior spending

#### Target Variable (1)
- `churned`: Binary (0=Active, 1=Churned)

**Total Features**: 35+ (plus target variable)

### Feature Engineering Rationale

| Feature | Purpose | ML Value |
|---------|---------|----------|
| `credit_utilization_pct` | Spending relative to limit | High predictive power for churn |
| `spend_momentum` | Recent trend vs baseline | Captures acceleration/deceleration |
| `transactions_per_day` | Engagement frequency | High frequency = low churn risk |
| `recency_score` | Categorical recency | Non-linear relationship with churn |
| Segment one-hot encoding | Behavioral patterns | Captures segment-specific churn patterns |

### Output Schema: `GOLD.ML_TRAINING_DATA`

**35+ feature columns** + `churned` (target) + metadata

**Rows**: ~40K-45K
**Class Balance**: 8-15% churned (imbalanced, realistic)

---

## 3. Validation (`validate_training_data.sql`)

### Validation Checks

| Check | Criterion | Expected |
|-------|-----------|----------|
| 1. Row Count | Total examples | ≥40K (✓), ≥1K (⚠) |
| 2. Class Distribution | Churn rate | 8-15% (✓) |
| 3. Null Features | Null count in critical features | 0 (✓) |
| 4. Feature Ranges | Min/max realistic | Age 18-100, Credit 5K-50K (✓) |
| 5. Feature Completeness | Non-null % | 100% for critical features (✓) |
| 6. Feature Comparison | Churned vs Active differences | Significant differences (✓) |
| 7. Segment Distribution | Churn by segment | Declining segment highest (✓) |
| 8. Examples per Class | Minimum per class | ≥1K each (✓) |

### Expected Results

```
✓ PASS: Sufficient training examples (40K-45K)
✓ PASS: Realistic churn rate (8-15%)
✓ PASS: No null values in critical features
✓ PASS: Realistic feature ranges
✓ PASS: Sufficient examples per class (1K+ each)
```

---

## Feature Importance (Expected)

Based on churn prediction domain knowledge:

| Rank | Feature | Expected Importance | Reason |
|------|---------|-------------------|--------|
| 1 | `days_since_last_transaction` | Very High | Direct indicator of inactivity |
| 2 | `spend_change_pct` | Very High | Captures decline behavior |
| 3 | `credit_utilization_pct` | High | Financial health indicator |
| 4 | `segment_declining` | High | Pre-identified churn risk |
| 5 | `spend_momentum` | High | Trend acceleration/deceleration |
| 6 | `avg_monthly_spend` | Medium | Baseline engagement |
| 7 | `tenure_months` | Medium | Loyalty indicator |
| 8 | `transactions_per_day` | Medium | Frequency of engagement |
| 9 | `travel_spend_pct` | Low-Medium | Category preference |
| 10 | `age` | Low | Demographic factor |

---

## Data Quality

### Filters Applied

```sql
WHERE
    -- Only customers with baseline data
    baseline_avg_spend IS NOT NULL
    AND baseline_avg_spend > 0

    -- Minimum transaction history
    AND total_transactions >= 5

    -- Optional outlier removal (commented out)
    -- AND lifetime_value < 500000
    -- AND avg_monthly_spend < 50000
```

### Expected Data Quality

- **No nulls** in critical features (age, credit_limit, avg_monthly_spend, tenure_months)
- **Realistic ranges** for all numeric features
- **Consistent encoding** for categorical features
- **No duplicate** customer_ids

---

## Training/Validation Split Approach

### Temporal Split (Time-Based)

**Training Period**: Months 1-15 (baseline + some recent data)
**Validation Period**: Months 16-18 (held-out recent data)

**Rationale**:
- Simulates real-world deployment (predict future from past)
- Avoids data leakage
- Tests model generalization to new time periods

**Implementation** (in Prompt 4.2):
```sql
-- Training set: Labeled customers from months 1-15
-- Validation set: Labeled customers from months 16-18
-- Test set: Current customers (unlabeled) for inference
```

---

## Next Steps (Prompt 4.2)

After training data preparation:

1. **Train Model** with Snowflake Cortex ML:
   ```sql
   CREATE SNOWFLAKE.ML.CLASSIFICATION churn_model(
       INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'GOLD.ML_TRAINING_DATA'),
       TARGET_COLNAME => 'churned',
       CONFIG_OBJECT => {'on_error': 'skip'}
   );
   ```

2. **Evaluate Model**:
   - Accuracy, Precision, Recall, F1-Score
   - ROC-AUC curve
   - Confusion matrix
   - Feature importance

3. **Generate Predictions**:
   - Score current customers
   - Populate `churn_risk_score` in `GOLD.CUSTOMER_360_PROFILE`
   - Categorize: Low, Medium, High, Very High

4. **Deploy Model**:
   - Scheduled inference (daily/weekly)
   - Real-time scoring API
   - Integration with marketing automation

---

## References

**SQL Scripts**:
- `snowflake/ml/01_create_churn_labels.sql`
- `snowflake/ml/02_create_training_features.sql`
- `snowflake/ml/validate_training_data.sql`

**Documentation**:
- `snowflake/ml/README.md` (this file)
- `docs/ml_model_card.md` (Prompt 4.1 deliverable)
- `docs/prompt_4.1_completion_summary.md`

**Tests**:
- `tests/integration/test_churn_training_data.py`

---

**End of ML README**
