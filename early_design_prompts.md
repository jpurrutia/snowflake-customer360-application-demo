Ask me one question at a time so we can develop a thorough, step-by-step spec for this idea. Each question should build on my previous answers, and our end goal is to have a detailed specification I can hand off to a developer. Let’s do this iteratively and dig into every relevant detail. Remember, only one question at a time.

<IDEA>

This is a Snowflake Developer and Solutions Architecture Project. Focused on customer needs and snowflake-native ecosystem

You are a Snowflake Data Platform Architecture and Developer Expert and you are helping me develop a demo that helps customer see how a data platform such as snowflake can provide a business value. We will work together to drill down into specific areas of the data platform (Data Engineering, Analytics, AI, Applications & Collaboration). I will prompt you with questions in a chat flow.
value. We will work together to drill down into specific areas of the data 



<BUSINESS CONTEXT>
Customer Business Scenario: 

Acquisition Integration Use Case:
Your company has acquired a fintech startup with a large credit card customer portfolio. You need to quickly integrate their transaction dataset into your Snowflake platform to:

Identify high-value customer segments for targeted marketing campaigns
Prevent customer churn during the transition period (post-acquisition risk)
Optimize marketing spend by understanding customer behavior
Enable self-service analytics for business users with AI

The Challenge
You have inherited:

50,000 credit cardholders with diverse spending patterns
13.5M transactions over 18 months
20 merchant categories with varying engagement
Unknown customer segments that need to be identified and analyzed

Key Business Questions:

Who are our most valuable customers?
Which customers are at risk of churning post-acquisition?
What spending patterns drive customer retention?
How can we personalize marketing campaigns?
Which customers should we target for premium card upgrades?
</BUSINESS CONTEXT>

<SOLUTION CONTEXT>
A Customer 360 analytics platform that provides:
For Marketing Managers:

Customer Segmentation Tool: Filter and export customer lists based on spend, geography, and behavior
Churn Detection: Identify declining customers (30%+ spend drop) and at-risk segments
Campaign Targeting: Export segments for email campaigns and retention programs

For Data Analysts:

Customer 360 View: Complete profile with 18-month transaction history
Behavioral Analytics: Month-over-month trends, category preferences, channel usage
Deep-Dive Analysis: Transaction-level detail for investigation

For Business Users (Non-Technical):

AI-Powered Agent: Ask questions in plain English (e.g., "Show me high-value customers in California")
Natural Language Queries: No SQL knowledge required
Instant Insights: Get answers in seconds with visualizations

Customer Segments Identified
Through the analytics, we classify customers into 5 segments:

High-Value Travelers (15%) - $5K-$12K/month, heavy travel spending
Stable Mid-Spenders (40%) - $2K-$4K/month, consistent behavior
Budget-Conscious (25%) - $500-$1.5K/month, grocery/gas focus
Declining (10%) - 40% spend reduction, high churn risk
New & Growing (10%) - Recent customers with 50% growth

Real-World Impact
Marketing Use Cases:

Target "Declining" segment with retention offers → Prevent $X million in lost revenue
Identify "High-Value Travelers" without premium cards → Upsell opportunity
Find "Budget-Conscious" customers overspending → Credit counseling outreach
Detect "Multi-Channel Users" → Optimize digital/physical touchpoints

Business Value:

Reduce churn by identifying at-risk customers early
Increase revenue by targeting premium card upgrades
Optimize marketing ROI with precise segmentation
Enable data democracy with AI-powered self-service

Technical Demonstration
This use case showcases Snowflake's modern data stack:

✅ Data Engineering (Snowpipe, dbt transformations, SCD Type 2)
✅ Analytics (Star schema, aggregate marts, complex metrics)
✅ Applications (Streamlit dashboards)
✅ AI/ML (Cortex Analyst for natural language queries)

</SOLUTION CONTEXT>

<ARCHITECTURAL CONTEXT>
 <DIAGRAM>
    <images/reference_architecture.png>
 </DIAGRAM>
</ARCHITECTURAL CONTEXT>

<REQUIREMENTS>
Leverage the four pillars of Snowflake's Data Cloud to build this solution:

Data Engineering: Ingest, transform, and orchestrate data movement and quality.

Analytics: Produce insights via SQL and/or Python; optionally power a BI view or metrics layer.

Applications: Implement an application or programmable interface (e.g., Streamlit in Snowflake, Snowpark, UDFs, services) that operationalizes analytics.

(Optional)Collaboration: Share and consume data, models, or apps across accounts or teams (e.g., direct shares, listings/Marketplace, data clean rooms).

Components including but not limited to:
* Data Modeling
* Batch Pipeline (snowflake)
* Streaming Pipeline (snowpipe)
* Credit card data created via synthetic data generator
* s3 storage
* medallion architecture
* data quality
* logging and observability
* incremental load patterns (scd type 2)?
* catalog (horizon)
* some incorporation of metadata
* established semantics
* RBAC

Environment and Tooling:
* Snowflake Trial Account
* Visual Studio Code - with Snowflake extensions
* SnowSQL CLI
* Sample data (TPC-H SF1)
* Text editor SQL IDE
* dbt for sql model orchestration
* streamlit
* uv python environment if it makes sense within snowflake
</REQUIREMENTS>


</IDEA>
