# Semantic Layer for Snowflake Cortex Analyst

**Version**: 1.0
**Date**: 2025-11-12
**Status**: Ready for Deployment

---

## Overview

The semantic layer enables natural language querying of our Customer 360 Analytics Platform using Snowflake Cortex Analyst. Business users can ask questions in plain English without writing SQL.

**Example Questions**:
- "Which customers are at highest risk of churning?"
- "What is the average spend in California?"
- "Show me Premium cardholders with declining spend"

---

## Architecture

```
User Natural Language Question
    ↓
Cortex Analyst (semantic_model.yaml)
    ↓
Generated SQL Query
    ↓
Snowflake Execution
    ↓
Results returned to User
```

---

## Semantic Model Contents

### Tables (4)

1. **customer_360_profile** - Main customer table
   - 50K customers with demographics, segments, churn risk
   - 30+ dimensions (state, segment, churn_risk_category, etc.)
   - 15+ metrics (lifetime_value, churn_risk_score, spend_last_90_days, etc.)

2. **fct_transactions** - Transaction details
   - 13.5M transactions over 18 months
   - Dimensions: transaction_date, merchant_name, channel
   - Metrics: transaction_amount, transaction_count

3. **dim_merchant_category** - Category classification
   - 11 categories (Travel, Dining, Grocery, etc.)
   - Grouped into Leisure, Necessities, Other

4. **customer_segments** - Behavioral segmentation
   - 5 segments with rolling 90-day metrics
   - Metrics: tenure_months

### Relationships

- Transactions → Customers (many-to-one)
- Transactions → Categories (many-to-one)
- Customers → Segments (one-to-one)

### Metrics (30+)

**Customer Metrics**:
- `lifetime_value` - Total all-time spend
- `avg_transaction_value` - Average per transaction
- `spend_last_90_days` - Recent period spend
- `spend_change_pct` - Trend indicator
- `churn_risk_score` - ML prediction (0-100)

**Transaction Metrics**:
- `transaction_amount` - Dollar amount
- `transaction_count` - Number of transactions

---

## Sample Questions by Use Case

### Churn Prediction
- "Which customers are at highest risk of churning?"
- "Show me High-Value Travelers with high churn risk"
- "What is the average churn risk score by segment?"

### Customer Segmentation
- "Compare lifetime value across segments"
- "How many customers in each segment?"
- "Which segments have Premium cards?"

### Spending Trends
- "Show spending trends in travel over last 6 months"
- "Which customers increased spending?"
- "What is monthly transaction volume?"

### Geographic Analysis
- "What is average lifetime value by state?"
- "Which states have highest churn risk?"

### Campaign Targeting
- "Show me customers eligible for retention campaigns"
- "Which Premium cardholders are at risk?"

---

## Deployment

### Prerequisites
- Snowflake account with Cortex Analyst enabled
- CUSTOMER_ANALYTICS database with GOLD schema
- SnowSQL or Snowflake SQL Worksheet access

### Steps

1. **Upload semantic model**:
   ```bash
   cd semantic_layer
   ./deploy_semantic_model.sh
   ```

2. **Test queries**:
   ```bash
   snowsql -f test_semantic_model.sql
   ```

3. **Verify in Cortex Analyst**:
   - Open Cortex Analyst in Snowsight
   - Select `customer_analytics_semantic_model`
   - Ask: "How many customers do we have?"
   - Verify SQL generation and results

---

## Testing

### SQL Tests
```bash
snowsql -f test_semantic_model.sql
```

### Python Integration Tests
```bash
pytest tests/integration/test_semantic_layer.py -v
```

---

## Files

| File | Purpose | Lines |
|------|---------|-------|
| `semantic_model.yaml` | Main semantic model definition | 550+ |
| `test_semantic_model.sql` | SQL validation queries | 400+ |
| `deploy_semantic_model.sh` | Deployment script | 30+ |
| `README.md` | This documentation | 200+ |

---

## Maintenance

### Adding New Metrics
1. Add metric to `semantic_model.yaml` under appropriate table
2. Test with SQL query in `test_semantic_model.sql`
3. Redeploy: `./deploy_semantic_model.sh`

### Adding New Dimensions
1. Add dimension to table in `semantic_model.yaml`
2. Include synonyms for natural language matching
3. Test and redeploy

---

## Troubleshooting

**Issue**: "Table not found"
- **Solution**: Verify table exists in GOLD schema

**Issue**: "Column not found"
- **Solution**: Check column name matches semantic_model.yaml

**Issue**: Cortex Analyst not generating SQL
- **Solution**: Rephrase question using synonyms from semantic model

---

## References

- [Snowflake Cortex Analyst Docs](https://docs.snowflake.com/en/user-guide/cortex-analyst)
- [Semantic Model Specification](https://docs.snowflake.com/en/user-guide/cortex-analyst-semantic-model)
- Project: `docs/prompt_4.3_completion_summary.md`
