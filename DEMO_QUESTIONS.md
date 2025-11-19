# AI Assistant Demo Questions Guide

This guide contains **tested, bulletproof questions** for demonstrating the Cortex Analyst AI Assistant.

---

## ✅ Recommended Demo Questions (Guaranteed to Work)

### **1. Customer Segmentation Analysis**

```
How many customers are in each segment?
```
**What it shows:** Basic aggregation, segment understanding
**Expected result:** Table/chart with customer counts by segment

```
Compare lifetime value across segments
```
**What it shows:** Comparative analysis, pre-aggregated metrics
**Expected result:** Bar chart showing avg LTV by segment

---

### **2. Churn & Risk Analysis**

```
Which customers are at highest risk of churning?
```
**What it shows:** Filtering, sorting, business logic
**Expected result:** Table of high-risk customers with scores

```
What is the average churn risk score by segment?
```
**What it shows:** Aggregation across dimensions
**Expected result:** Avg risk score per segment

```
Which states have the highest churn risk?
```
**What it shows:** Geographic analysis, auto-choropleth map
**Expected result:** US map with color-coded states

---

### **3. Spending Analysis** ⭐

```
What is the total spending in the last 90 days by customer segment?
```
**What it shows:** Pre-aggregated metrics (uses `spend_last_90_days` field)
**Expected result:** Fast query using customer_profile table, not transactions

```
Which customers increased spending the most?
```
**What it shows:** Spending trend analysis
**Expected result:** Top customers by spend_change_pct

```
What does spending for Stable Mid-Spenders look like month over month for the last 3 months?
```
**What it shows:** Time-series analysis using monthly_spending table
**Expected result:** Monthly trend data with line chart option

---

### **4. Geographic Analysis** 🗺️

```
What is the average lifetime value by state?
```
**What it shows:** Geographic + aggregation
**Expected result:** Option for choropleth map visualization

```
Which states have the most Premium cardholders?
```
**What it shows:** Filtering + grouping
**Expected result:** State-level premium card distribution

---

### **5. Campaign Targeting**

```
Show me customers eligible for retention campaigns
```
**What it shows:** Natural language to business logic
**Expected result:** Customers with high churn risk or declining spend

```
Which Premium cardholders are at medium or high risk?
```
**What it shows:** Multi-condition filtering
**Expected result:** Targeted list for premium retention

```
Find customers with declining spend in the last 90 days
```
**What it shows:** Negative trend identification
**Expected result:** Customers with negative spend_change_pct

---

## ⚠️ Questions to AVOID (May Generate Slow Queries)

### **Avoid Custom Date Ranges Not Pre-Aggregated**

❌ "What is spending in the last 37 days?"
- Will query transactions table with date filter
- Use: "What is spending in the last 90 days?" instead

❌ "Show me transactions from January to March 2024"
- Transaction-level query
- Better for detailed analysis tab, not AI assistant

### **Avoid Overly Vague Questions**

❌ "Show me customers"
- Too broad, no business value
- Add criteria: segment, risk, location

### **Note: Month-over-Month Questions Now Supported! ✅**

✅ "What does spending look like month over month?" - **NOW WORKS!**
- Uses pre-aggregated `monthly_spending` table
- Fast and reliable for time-series analysis
- Best with segment filter for cleaner results

---

## 🎯 Demo Flow Recommendation

### **Opening (Show Ease of Use)**
1. "How many customers are in each segment?"
2. "Compare lifetime value across segments"

### **Business Value (Show Insights)**
3. "Which states have the highest churn risk?" → Switch to map view
4. "What is the total spending in the last 90 days by customer segment?"

### **Advanced (Show Flexibility)**
5. "Show me Premium cardholders in California with high churn risk"
6. "Which customers increased spending the most?"

### **Visualization Showcase**
7. Toggle between Table and Chart views
8. Show choropleth map for geographic questions
9. Demonstrate follow-up suggestions (if Cortex Analyst returns them)

---

## 🔧 Troubleshooting

### If a question generates slow SQL:
1. Check the generated SQL (click "View Generated SQL")
2. Look for `FROM fct_transactions` with date filters
3. If found, rephrase to use "last 90 days" or other pre-aggregated periods

### If "No results found":
1. Check if the generated SQL has syntax errors
2. Try one of the guaranteed questions above
3. May indicate semantic model needs redeployment

### If charts don't appear:
1. Verify data has both categorical and numeric columns
2. Check browser console for JavaScript errors
3. Try refreshing the Streamlit app

---

## 📊 Expected Performance

**Fast Queries (< 2 seconds):**
- Questions using customer_profile table
- Pre-aggregated metrics (spend_last_90_days, lifetime_value)
- Simple aggregations

**Slower Queries (5-10 seconds):**
- Transaction-level analysis
- Custom date range filtering
- Complex multi-table joins

---

## 💡 Pro Tips for Demo Success

1. **Start simple, build complexity** - Show basic questions first
2. **Highlight the visualization toggle** - Shows AI + viz integration
3. **Use geographic questions** - Choropleth maps are impressive
4. **Emphasize "no SQL required"** - Marketing managers can self-serve
5. **Show follow-up suggestions** - Conversational data exploration

---

*Last updated: 2025-11-19*
*Semantic model version: customer_analytics.yaml*
