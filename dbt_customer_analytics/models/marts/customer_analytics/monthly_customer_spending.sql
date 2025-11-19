{{
    config(
        materialized='incremental',
        unique_key=['customer_key', 'month'],
        incremental_strategy='merge',
        schema='gold',
        tags=['gold', 'mart', 'time_series', 'monthly_aggregation']
    )
}}

{#
============================================================================
Gold Layer: Monthly Customer Spending
============================================================================
Purpose: Pre-aggregated monthly spending metrics for time-series analysis

Grain: One row per customer per month
Row Count: ~900,000 rows (50K customers × 18 months)

Business Purpose:
- Enable fast month-over-month trend analysis
- Support Cortex Analyst time-series queries
- Avoid expensive transaction-level aggregations
- Power monthly/seasonal dashboards

Contains:
- Customer identifying information
- Monthly spending totals
- Transaction counts and averages
- Customer segment (for grouped analysis)

Performance:
- Incremental: Only processes new/changed months
- Indexed by customer_key + month
- Optimized for Cortex Analyst semantic model

Related Tables:
- Source: fct_transactions (Silver layer)
- Joins: dim_customer (Silver layer)
- Consumed by: Cortex Analyst semantic model
============================================================================
#}

with base_transactions as (
    select
        transaction_id,
        customer_key,
        transaction_date,
        transaction_amount,
        status
    from {{ ref('fct_transactions') }}
    where status = 'approved'

    {% if is_incremental() %}
        -- Only process transactions from months that might have changed
        -- Look back 2 months to catch any late-arriving transactions
        and date_trunc('month', transaction_date) >= (
            select dateadd('month', -2, max(month))
            from {{ this }}
        )
    {% endif %}
),

customer_info as (
    select
        customer_key,
        customer_id,
        full_name,
        customer_segment,
        card_type,
        state,
        city
    from {{ ref('customer_360_profile') }}
),

monthly_aggregation as (
    select
        t.customer_key,
        date_trunc('month', t.transaction_date) as month,
        sum(t.transaction_amount) as total_spend,
        count(t.transaction_id) as transaction_count,
        avg(t.transaction_amount) as avg_transaction_value,
        min(t.transaction_date) as first_transaction_date,
        max(t.transaction_date) as last_transaction_date
    from base_transactions t
    group by
        t.customer_key,
        date_trunc('month', t.transaction_date)
)

select
    m.customer_key,
    c.customer_id,
    c.full_name,
    c.customer_segment,
    c.card_type,
    c.state,
    c.city,
    m.month,
    m.total_spend,
    m.transaction_count,
    m.avg_transaction_value,
    m.first_transaction_date,
    m.last_transaction_date,

    -- Metadata for tracking
    current_timestamp() as dbt_updated_at

from monthly_aggregation m
left join customer_info c
    on m.customer_key = c.customer_key
