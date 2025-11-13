# EDA & Telemetry Tracking for Transaction Generation

This directory contains SQL scripts for performing Exploratory Data Analysis (EDA) and tracking telemetry before and after running the transaction data generator.

## Overview

The transaction generator creates ~13.5M transactions spanning 18 months of historical data for 50,000 customers across 5 customer segments. These scripts help you:

1. **Capture baseline metrics** before generation
2. **Explore current data** to understand the starting state
3. **Validate generated data** with comprehensive quality checks
4. **Compare before/after** to measure the delta
5. **Track telemetry** for ongoing monitoring

## Scripts Execution Order

Run these scripts in the following sequence:

### Phase 1: Pre-Generation (Before running transaction generator)

#### 1. `01_baseline_metrics.sql`
**Purpose**: Capture current state metrics for comparison

**What it does**:
- Creates `metrics_baseline` table
- Captures snapshot of customer count, transaction count, date ranges
- Saves timestamp for before/after comparison

**Run command**:
```bash
snowsql -c default -f 01_baseline_metrics.sql
```

**Expected output**:
- Baseline metrics table created
- Current state captured (likely 0 transactions if not generated yet)

**Duration**: < 1 minute

---

#### 2. `02_pre_generation_eda.sql`
**Purpose**: Comprehensive exploration of existing data

**What it does**:
- **Customer exploration**: Segment distribution, card types, age/credit analysis
- **Transaction exploration**: Date ranges, amounts, channels, categories (if any exist)
- **Data quality checks**: NULL values, duplicates, invalid data
- **Join analysis**: Customer-transaction relationships

**Run command**:
```bash
snowsql -c default -f 02_pre_generation_eda.sql > pre_generation_eda_report.txt
```

**Expected insights**:
- 50,000 customers across 5 segments
- Segment distribution: Stable Mid-Spenders (40%), Budget-Conscious (25%), High-Value Travelers (15%), Declining (10%), New & Growing (10%)
- Card types: Standard vs Premium distribution
- Likely 0 transactions if generator hasn't been run

**Duration**: 1-2 minutes

---

### Phase 2: Run Transaction Generator

#### 3. Run the transaction generator
**Location**: `../data_generation/generate_transactions.sql`

**Run command**:
```bash
cd ../data_generation
snowsql -c default -f generate_transactions.sql
```

**Expected**:
- Generates 10M-17M transactions
- Creates 18 months of historical data
- Takes 5-15 minutes depending on warehouse size

**Monitor progress**:
```sql
SELECT COUNT(*) FROM CUSTOMER_ANALYTICS.BRONZE.BRONZE_TRANSACTIONS;
```

---

### Phase 3: Post-Generation (After generator completes)

#### 4. `03_post_generation_validation.sql`
**Purpose**: Comprehensive validation of generated data

**What it does**:
- **12 validation checks** covering:
  - Row count (10M-17M expected)
  - Unique transaction IDs
  - NULL value checks (8 fields)
  - Customer representation (all 50K customers)
  - Referential integrity
  - Date range (17-19 months expected)
  - Transaction amounts (positive, reasonable)
  - Status distribution (~97% approved, ~3% declined)
  - Channel distribution
  - Merchant category distribution
  - Segment-specific patterns
  - Monthly transaction trend

**Run command**:
```bash
snowsql -c default -f 03_post_generation_validation.sql > validation_report.txt
```

**Expected results**:
- ✓ PASS on all 12 validation checks
- Transaction count: 10M-17M
- Date range: ~18 months
- All customers have transactions
- No orphan transactions or referential integrity issues

**Duration**: 3-5 minutes

---

#### 5. `04_delta_analysis.sql`
**Purpose**: Compare before/after metrics and analyze generated data

**What it does**:
- Captures post-generation snapshot
- Compares before/after metrics (customer count, transaction count, amounts)
- Segment-level transaction analysis
- Monthly transaction growth trends
- Declining segment pattern validation
- Top 10 customers by spend
- Channel and category distribution
- Transaction status analysis
- Data quality summary

**Run command**:
```bash
snowsql -c default -f 04_delta_analysis.sql > delta_analysis_report.txt
```

**Expected insights**:
- Transaction count delta: +10M-17M (from 0 to final count)
- Segment distribution matches expected patterns:
  - High-Value Travelers: Highest avg amount ($50-500)
  - Stable Mid-Spenders: Medium avg amount ($30-150)
  - Budget-Conscious: Lower avg amount ($10-80)
  - Declining: Shows gradual/sudden decline patterns
  - New & Growing: Shows growth trend
- Monthly trends show realistic patterns

**Duration**: 5-10 minutes

---

#### 6. `05_telemetry_tracking.sql`
**Purpose**: Set up ongoing monitoring and telemetry

**What it does**:
- Creates 3 telemetry tables:
  - `generation_telemetry`: Tracks generation execution metrics
  - `data_quality_telemetry`: Tracks quality check results
  - `segment_telemetry`: Tracks segment performance over time
- Populates initial telemetry data
- Creates 2 monitoring views:
  - `v_data_quality_dashboard`: Quality check summary
  - `v_segment_performance`: Segment performance trends
- Queries warehouse performance metrics from ACCOUNT_USAGE

**Run command**:
```bash
snowsql -c default -f 05_telemetry_tracking.sql
```

**Expected tables created**:
- `BRONZE.generation_telemetry`
- `BRONZE.data_quality_telemetry`
- `BRONZE.segment_telemetry`
- `BRONZE.v_data_quality_dashboard` (view)
- `BRONZE.v_segment_performance` (view)

**Duration**: 2-3 minutes

---

## Quick Start

### Full Workflow Commands

```bash
# Navigate to EDA directory
cd /Users/jpurrutia/projects/snowflake-panel-demo/snowflake/eda

# Phase 1: Pre-Generation
snowsql -c default -f 01_baseline_metrics.sql
snowsql -c default -f 02_pre_generation_eda.sql > pre_generation_report.txt

# Phase 2: Generate Transactions
cd ../data_generation
snowsql -c default -f generate_transactions.sql

# Phase 3: Post-Generation
cd ../eda
snowsql -c default -f 03_post_generation_validation.sql > validation_report.txt
snowsql -c default -f 04_delta_analysis.sql > delta_report.txt
snowsql -c default -f 05_telemetry_tracking.sql
```

### Review Reports

```bash
# View reports
cat pre_generation_report.txt
cat validation_report.txt
cat delta_report.txt
```

---

## Tables Created

### Baseline Tables
- `BRONZE.metrics_baseline` - Before/after snapshots

### Telemetry Tables
- `BRONZE.generation_telemetry` - Generation execution metrics
- `BRONZE.data_quality_telemetry` - Quality check results
- `BRONZE.segment_telemetry` - Segment performance snapshots

### Monitoring Views
- `BRONZE.v_data_quality_dashboard` - Quality check summary by category
- `BRONZE.v_segment_performance` - Segment performance with % of total spend

---

## Monitoring Queries

After running the telemetry script, use these queries for ongoing monitoring:

### Daily Data Quality Check
```sql
SELECT * FROM BRONZE.v_data_quality_dashboard
ORDER BY check_timestamp DESC LIMIT 1;
```

### Segment Performance Trend (Last 7 Days)
```sql
SELECT * FROM BRONZE.v_segment_performance
WHERE snapshot_timestamp >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY snapshot_timestamp DESC, total_spend DESC;
```

### Failed Quality Checks
```sql
SELECT * FROM BRONZE.data_quality_telemetry
WHERE check_status = 'FAIL'
ORDER BY check_timestamp DESC;
```

### Transaction Volume Trend
```sql
SELECT
    DATE(transaction_date) AS txn_date,
    COUNT(*) AS txn_count,
    ROUND(AVG(transaction_amount), 2) AS avg_amount
FROM BRONZE.bronze_transactions
GROUP BY txn_date
ORDER BY txn_date;
```

### Customer Activity Rate (Last Month)
```sql
SELECT
    COUNT(DISTINCT customer_id) * 100.0 / (SELECT COUNT(*) FROM BRONZE.bronze_customers) AS active_pct
FROM BRONZE.bronze_transactions
WHERE transaction_date >= DATEADD(month, -1, CURRENT_DATE());
```

---

## Expected Data Characteristics

### Transaction Volume
- **Total transactions**: 10M-17M (13.5M average)
- **Months of data**: 17-19 months (18 months target)
- **Customers with transactions**: 50,000 (100%)

### Segment Distribution
| Segment | % of Customers | Avg Txns/Month | Avg Amount | Characteristics |
|---------|----------------|----------------|------------|-----------------|
| High-Value Travelers | 15% | 40-80 | $50-500 | Travel-heavy, premium cards |
| Stable Mid-Spenders | 40% | 20-40 | $30-150 | Consistent patterns, mixed categories |
| Budget-Conscious | 25% | 15-30 | $10-80 | Frequent small purchases, grocery/gas |
| Declining | 10% | Variable | Variable | Gradual (70%) or sudden (30%) decline |
| New & Growing | 10% | 25-50 | $20-100 | 5% monthly growth pattern |

### Transaction Characteristics
- **Status distribution**: ~97% approved, ~3% declined
- **Channels**: Mix of Online, In-Store, Mobile
- **Categories**: Travel, Dining, Retail, Grocery, Gas, Entertainment, Health, Services, Online Shopping, Utilities
- **Amounts**: $10-500 range (varies by segment)

### Data Quality Standards
- ✓ No NULL values in required fields
- ✓ All transaction IDs unique
- ✓ All amounts positive and < $10,000
- ✓ No future dates
- ✓ 100% referential integrity (all customer_ids exist)
- ✓ All 50K customers have transactions

---

## Troubleshooting

### Issue: "Table 'metrics_baseline' does not exist"
**Solution**: Run `01_baseline_metrics.sql` first

### Issue: "No data in bronze_transactions"
**Solution**: Run the transaction generator: `../data_generation/generate_transactions.sql`

### Issue: Transaction count is outside 10M-17M range
**Solution**: Check the GENERATOR() row count in `generate_transactions.sql`. The randomization can cause variance.

### Issue: Some quality checks show FAIL
**Solution**: Review the specific failed checks in `data_quality_telemetry` table and investigate the root cause

### Issue: Declining segment not showing decline pattern
**Solution**: Check monthly trends in delta analysis. Gradual decline starts at month 12, sudden decline at month 16.

---

## Next Steps After EDA

1. **Review all validation reports** - Ensure data quality meets standards
2. **Run dbt transformations** - Build Gold layer tables
   ```bash
   cd ../../dbt_customer_analytics
   dbt run --full-refresh
   dbt test
   ```
3. **Train ML model** - Run churn prediction model
   ```bash
   snowsql -f ../snowflake/ml/01_create_churn_labels.sql
   snowsql -f ../snowflake/ml/03_train_churn_model.sql
   ```
4. **Launch Streamlit dashboard** - Test the application
   ```bash
   cd ../../streamlit
   streamlit run app.py
   ```

---

## Related Documentation

- **Transaction Generator**: `../data_generation/README.md`
- **Customer Generator**: `../../data_generation/README.md`
- **Onboarding Guide**: `../../docs/ONBOARDING_GUIDE.md`
- **Data Loading**: `../load/README.md`

---

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review validation reports for specific error details
3. Check Snowflake query history for execution errors
4. Review the onboarding guide for complete workflow

---

**Last Updated**: 2025-01-13
