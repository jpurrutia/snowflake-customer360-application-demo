# Customer 360 Analytics Streamlit Application

**Version**: 1.0
**Date**: 2025-11-12
**Status**: Phase 5 Complete (All 4 Tabs Implemented)

---

## Overview

Interactive Streamlit dashboard for the Customer 360 Analytics Platform. Enables business users to explore customer segments, analyze individual customer profiles, ask natural language questions, and simulate marketing campaign performance.

**Target Users**:
- Marketing managers (segment targeting, campaign ROI)
- Customer service reps (individual customer lookup)
- Data analysts (natural language queries)
- Business stakeholders (executive insights)

---

## Features

### ✅ Implemented (Iteration 5.1)

#### 📊 Segment Explorer Tab
- **Filter customers** by segment, state, churn risk, LTV, card type
- **Summary metrics**: customer count, total LTV, average LTV, average churn risk
- **Visualizations**:
  - Customer segment distribution (pie chart)
  - Churn risk distribution (bar chart)
  - Total LTV by segment (bar chart)
- **Customer list** with sortable, searchable data table
- **CSV export** for marketing campaigns

#### 🔍 Customer 360 Deep Dive Tab
- **Customer search** by ID, name, or email (partial match)
- **Profile header** with demographics, segment, churn risk alerts
- **Key metrics**: LTV, avg transaction, spend (90d), days since last transaction
- **Spending trends**: 90-day spend change, avg monthly spend
- **Transaction history** (up to 1,000 recent transactions)
- **Transaction filters**: date range, category, status
- **Visualizations**:
  - Daily spending trend (line chart)
  - Category breakdown (pie chart)
- **Transaction summary**: total transactions, total spend, avg transaction, approval rate
- **CSV export** for transaction history

#### 🤖 AI Assistant Tab
- **Natural language queries** in plain English
- **5 question categories**: Churn Analysis, Customer Segmentation, Spending Trends, Geographic Analysis, Campaign Targeting
- **20+ suggested questions** across all use cases
- **Clickable question buttons** for easy exploration
- **Generated SQL display** (collapsible expander)
- **Results table** with automatic formatting
- **Summary metrics** for small result sets
- **Query history** (last 5 queries with SQL and results)
- **CSV export** for all query results
- **Mock implementation** (ready for Cortex Analyst integration when enabled)
- **Help section** with tips for asking effective questions

#### 📈 Campaign Performance Simulator Tab
- **Target audience selection** with multi-select filters (segment, churn risk, card type)
- **Advanced filters**: Min lifetime value, min churn risk score
- **Campaign parameters**: Incentive per customer ($), expected retention rate (%), campaign cost per customer ($)
- **ROI metrics**: Target customers, total cost, expected retained customers, ROI percentage
- **Cost breakdown**: Pie chart showing incentive vs campaign operations costs
- **Expected value metrics**: Retained customer value, cost per retained customer, net benefit
- **Sensitivity analysis**: Interactive line chart showing ROI vs retention rate (10-80%)
- **Breakeven calculation**: Auto-calculate minimum retention rate for positive ROI
- **Target customer list**: Top 10 highest risk customers with full list export
- **Campaign recommendations**: Actionable messaging, timing, and success metrics
- **CSV export**: Download full target customer list with timestamp

**Use Cases**:
- "Model ROI for $50 statement credit retention campaign targeting Declining customers"
- "Find breakeven retention rate for high-risk Premium cardholders"
- "Compare campaign costs across different target audiences"
- "Export target list for retention campaign deployment"

---

## Local Development Setup

### Prerequisites

- Python 3.10+
- Snowflake account with CUSTOMER_ANALYTICS database
- UV package manager (or pip)

### Installation

1. **Navigate to streamlit directory**:
   ```bash
   cd streamlit
   ```

2. **Install dependencies**:
   ```bash
   # Using pip
   pip install -r requirements.txt

   # Or using UV (recommended)
   uv pip install -r requirements.txt
   ```

3. **Configure Snowflake credentials**:
   ```bash
   cp .env.example .env
   # Edit .env with your Snowflake credentials
   ```

   Edit `.env`:
   ```
   SNOWFLAKE_ACCOUNT=abc12345.us-east-1
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_PASSWORD=your_password
   ```

4. **Run the application**:
   ```bash
   streamlit run app.py
   ```

5. **Access the app**:
   - Open browser to: http://localhost:8501
   - App will connect to Snowflake CUSTOMER_ANALYTICS.GOLD schema

---

## Deployment to Snowflake (Streamlit in Snowflake)

### Option 1: Via Snowsight UI

1. Log into Snowsight
2. Navigate to **Streamlit** in the left sidebar
3. Click **+ Streamlit App**
4. Upload `app.py` and files from `tabs/` directory
5. Set warehouse: `COMPUTE_WH`
6. Set role: `DATA_ANALYST`
7. Click **Create**

### Option 2: Via SQL

```sql
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA GOLD;

CREATE STREAMLIT customer_360_app
    ROOT_LOCATION = '@STREAMLIT_STAGE'
    MAIN_FILE = 'app.py'
    QUERY_WAREHOUSE = 'COMPUTE_WH';
```

---

## Tab Descriptions

### 1. Segment Explorer (Current)

**Purpose**: Identify and export customer segments for targeted marketing campaigns

**Features**:
- Multi-select filters for segment, state, churn risk
- Advanced filters for LTV and card type
- Real-time summary metrics
- Interactive visualizations
- Exportable customer lists (CSV)

**Use Cases**:
- "Find all High-Value Travelers in California with high churn risk"
- "Export Premium cardholders with declining spend for retention campaign"
- "Analyze segment distribution across states"

### 2. Customer 360 Deep Dive (✅ Complete)

**Purpose**: Individual customer profile and transaction analysis for customer service reps and account managers

**Features**:
- **Search methods**: Customer ID, name (partial), email (partial)
- **Profile header**: Name, email, location, segment, card type, credit limit
- **Churn risk alerts**: Color-coded (High/Medium/Low) with risk score
- **Key metrics**: LTV, avg transaction, 90-day spend, days since last transaction
- **Trend metrics**: MoM spend change, avg monthly spend
- **Transaction history**: Last 1,000 transactions with merchant, category, amount
- **Filters**: Date range (30d/90d/6mo/all), category, status
- **Visualizations**: Daily spending line chart, category pie chart
- **Transaction summary**: Count, total spend, avg transaction, approval rate
- **CSV export**: Download full transaction history

**Use Cases**:
- "Look up customer John Smith to check their recent activity"
- "Investigate why customer#12345 has high churn risk"
- "Review transaction history for fraud investigation"
- "Export customer's transactions for dispute resolution"

### 3. AI Assistant (✅ Complete)

**Purpose**: Natural language analytics for business users without SQL knowledge

**Features**:
- **Natural language input**: Ask questions in plain English
- **5 question categories**: Churn Analysis, Customer Segmentation, Spending Trends, Geographic Analysis, Campaign Targeting
- **20+ suggested questions**: Pre-built questions covering common use cases
- **Clickable buttons**: One-click to populate question from suggestions
- **Generated SQL display**: View the SQL generated from your question (collapsible)
- **Results table**: Formatted results with up to 10,000 rows
- **Summary metrics**: Auto-display for small result sets (cards with $ formatting)
- **Query history**: Last 5 queries with timestamps, questions, SQL, and results
- **CSV export**: Download any query results
- **Mock implementation**: Functional with 5+ query patterns (ready for Cortex Analyst)
- **Error handling**: Helpful troubleshooting tips for unrecognized questions
- **Help section**: Tips for asking effective questions

**Use Cases**:
- "Which customers are at highest risk of churning?" (executive dashboard)
- "Compare lifetime value across segments" (business analysis)
- "Show me Premium cardholders at medium or high risk" (targeted campaign)
- "What is the total spending in the last 90 days?" (performance tracking)

### 4. Campaign Performance Simulator (✅ Complete)

**Purpose**: Marketing ROI calculator for retention campaigns

**Features**:
- **Target audience selection**: Multi-select filters for segment, churn risk, card type
- **Advanced filters**: Min LTV ($), min churn risk score
- **Campaign parameters**: Incentive amount ($), expected retention rate (%), campaign cost ($)
- **ROI calculation**: Total cost, expected retained customers, expected value, net benefit, ROI %
- **Cost breakdown**: Pie chart (incentives vs operations)
- **Sensitivity analysis**: Line chart showing ROI across retention rates (10-80%)
- **Breakeven calculation**: Find minimum retention rate for positive ROI
- **Target list display**: Top 10 highest risk customers
- **Campaign recommendations**: Messaging, timing, success metrics
- **CSV export**: Full target customer list with timestamp

**Use Cases**:
- "Model a $50 retention campaign for Declining customers in California"
- "Find breakeven point for Premium cardholders at high risk"
- "Compare ROI across different incentive amounts"
- "Export target list for CRM upload"

---

## Project Structure

```
streamlit/
├── app.py                      # Main application entry point
├── tabs/                       # Tab modules
│   ├── __init__.py
│   ├── segment_explorer.py     # Segment Explorer tab (✅ implemented)
│   ├── customer_360.py         # Customer 360 Deep Dive tab (✅ implemented)
│   ├── ai_assistant.py         # AI Assistant tab (✅ implemented)
│   └── campaign_simulator.py   # Campaign Simulator tab (✅ implemented)
├── requirements.txt            # Python dependencies
├── .env.example                # Environment variable template
└── README.md                   # This file
```

---

## Configuration

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier | `abc12345.us-east-1` |
| `SNOWFLAKE_USER` | Snowflake username | `analyst_user` |
| `SNOWFLAKE_PASSWORD` | Snowflake password | `your_password` |

### Snowflake Resources

- **Database**: `CUSTOMER_ANALYTICS`
- **Schema**: `GOLD`
- **Warehouse**: `COMPUTE_WH`
- **Role**: `DATA_ANALYST`

### Query Limits

- **Timeout**: 60 seconds per query
- **Row limit**: 10,000 rows per query
- **Result caching**: Enabled via `@st.cache_resource`

---

## Troubleshooting

### Connection Issues

**Error**: "Failed to connect to Snowflake"

**Solutions**:
1. Verify `.env` credentials are correct
2. Check Snowflake account identifier format: `account.region`
3. Ensure `DATA_ANALYST` role has access to `CUSTOMER_ANALYTICS` database
4. Test connection via SnowSQL: `snowsql -a <account> -u <user>`

### Query Timeout

**Error**: "Query timed out. Try filtering to a smaller dataset."

**Solutions**:
1. Use more restrictive filters (fewer segments, specific states)
2. Increase warehouse size in Snowflake
3. Reduce `LIMIT` in query (currently 5,000 customers)

### No Data Returned

**Error**: "No customers match the selected filters."

**Solutions**:
1. Verify `CUSTOMER_360_PROFILE` table is populated
2. Check segment names match exactly (case-sensitive)
3. Try removing filters to see all data

---

## Performance Optimization

### Caching

- **Connection caching**: Snowflake connection cached via `@st.cache_resource`
- **Session state**: Filtered results stored in `st.session_state['filtered_customers']`
- **Query optimization**: Pre-aggregated metrics in `CUSTOMER_360_PROFILE` table

### Best Practices

1. **Use filters**: Always apply segment or state filters to reduce data volume
2. **Export large datasets**: For >5,000 customers, export CSV and analyze externally
3. **Warehouse sizing**: Use `COMPUTE_WH` (small) for dashboards, scale up for heavy queries

---

## Testing

Run integration tests:

```bash
# From project root
pytest tests/integration/test_streamlit_segment_explorer.py -v
```

Tests cover:
- Snowflake connection
- Query execution
- Filter logic
- CSV export
- Error handling

---

## Completion Status

✅ **Phase 5 Complete** - All 4 tabs implemented:

1. ✅ **Iteration 5.1**: Segment Explorer tab
2. ✅ **Iteration 5.2**: Customer 360 Deep Dive tab
3. ✅ **Iteration 5.3**: AI Assistant tab with Cortex Analyst (mock)
4. ✅ **Iteration 5.4**: Campaign Performance Simulator

**Next Steps for Production**:
- Deploy to Streamlit in Snowflake
- Enable production Cortex Analyst (replace mock implementation)
- Configure warehouse auto-scaling
- Set up monitoring and alerting

---

## References

- [Streamlit Documentation](https://docs.streamlit.io/)
- [Snowflake Connector for Python](https://docs.snowflake.com/en/user-guide/python-connector)
- [Streamlit in Snowflake](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)
- Project: `IMPLEMENTATION_PROMPTS.md` - Iteration 5.1
