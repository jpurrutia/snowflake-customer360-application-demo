import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def render(execute_query):
    """
    Executive Dashboard for Marketing Managers

    High-level KPIs and insights for strategic decision-making:
    - Customer base overview
    - Segment distribution and health
    - Revenue metrics
    - Churn risk indicators
    - Campaign opportunities
    """

    # ========== KEY METRICS ROW ==========
    st.markdown("### 📊 Key Performance Indicators")

    # Get overall stats
    kpi_query = """
    SELECT
        COUNT(DISTINCT customer_id) as total_customers,
        ROUND(AVG(lifetime_value), 0) as avg_ltv,
        ROUND(SUM(lifetime_value) / 1000000, 2) as total_revenue_millions,
        ROUND(AVG(avg_transaction_value), 2) as avg_transaction_value
    FROM GOLD.CUSTOMER_360_PROFILE
    """

    df_kpis = execute_query(kpi_query)

    if not df_kpis.empty:
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Total Customers",
                f"{df_kpis['TOTAL_CUSTOMERS'].iloc[0]:,}",
                help="Active customer base"
            )

        with col2:
            st.metric(
                "Avg Lifetime Value",
                f"${df_kpis['AVG_LTV'].iloc[0]:,.0f}",
                help="Average LTV per customer"
            )

        with col3:
            st.metric(
                "Total Revenue",
                f"${df_kpis['TOTAL_REVENUE_MILLIONS'].iloc[0]:.1f}M",
                help="Total customer lifetime value"
            )

        with col4:
            st.metric(
                "Avg Transaction",
                f"${df_kpis['AVG_TRANSACTION_VALUE'].iloc[0]:,.2f}",
                help="Average transaction size"
            )

    st.markdown("---")

    # ========== SEGMENT OVERVIEW ==========
    col_left, col_right = st.columns([1, 1])

    with col_left:
        st.markdown("### 👥 Customer Segment Distribution")

        segment_query = """
        SELECT
            customer_segment,
            COUNT(*) as customer_count,
            ROUND(AVG(lifetime_value), 0) as avg_ltv,
            ROUND(AVG(spend_last_90_days), 0) as avg_90day_spend
        FROM GOLD.CUSTOMER_SEGMENTS
        GROUP BY customer_segment
        ORDER BY customer_count DESC
        """

        df_segments = execute_query(segment_query)

        if not df_segments.empty:
            # Pie chart
            fig_pie = px.pie(
                df_segments,
                values='CUSTOMER_COUNT',
                names='CUSTOMER_SEGMENT',
                color='CUSTOMER_SEGMENT',
                color_discrete_map={
                    'High-Value Travelers': '#29B5E8',
                    'Stable Mid-Spenders': '#1A73E8',
                    'Budget-Conscious': '#7254A3',
                    'Declining': '#FF9F36',
                    'New & Growing': '#34A853'
                },
                hole=0.4
            )

            fig_pie.update_traces(
                textposition='inside',
                textinfo='percent+label',
                hovertemplate='<b>%{label}</b><br>Count: %{value:,}<br>Percent: %{percent}<extra></extra>'
            )

            fig_pie.update_layout(
                height=400,
                showlegend=True,
                legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="left", x=1.1)
            )

            st.plotly_chart(fig_pie, use_container_width=True)

    with col_right:
        st.markdown("### 💰 Segment Value Analysis")

        if not df_segments.empty:
            # Bar chart showing LTV by segment
            fig_bar = go.Figure()

            fig_bar.add_trace(go.Bar(
                x=df_segments['CUSTOMER_SEGMENT'],
                y=df_segments['AVG_LTV'],
                name='Avg LTV',
                marker_color='#29B5E8',
                text=df_segments['AVG_LTV'].apply(lambda x: f'${x:,.0f}'),
                textposition='outside'
            ))

            fig_bar.update_layout(
                height=400,
                xaxis_title="Customer Segment",
                yaxis_title="Average Lifetime Value ($)",
                showlegend=False,
                yaxis=dict(tickformat='$,.0f')
            )

            st.plotly_chart(fig_bar, use_container_width=True)

    st.markdown("---")

    # ========== CHURN RISK & OPPORTUNITIES ==========
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### ⚠️ Churn Risk - Declining Segment")

        churn_query = """
        SELECT
            COUNT(*) as at_risk_count,
            ROUND(SUM(lifetime_value), 0) as at_risk_revenue,
            ROUND(AVG(spend_change_pct), 1) as avg_decline_pct
        FROM GOLD.CUSTOMER_SEGMENTS
        WHERE customer_segment = 'Declining'
        """

        df_churn = execute_query(churn_query)

        if not df_churn.empty:
            st.metric(
                "At-Risk Customers",
                f"{df_churn['AT_RISK_COUNT'].iloc[0]:,}",
                delta=None,
                help="Customers in Declining segment"
            )

            st.metric(
                "Revenue at Risk",
                f"${df_churn['AT_RISK_REVENUE'].iloc[0]:,.0f}",
                delta=None,
                help="Total LTV of declining customers"
            )

            st.metric(
                "Avg Spend Decline",
                f"{df_churn['AVG_DECLINE_PCT'].iloc[0]:.1f}%",
                delta=None,
                help="Average 90-day spend change"
            )

            st.warning("💡 **Recommended Action:** Launch retention campaign targeting declining customers")

    with col2:
        st.markdown("### 🎯 Growth Opportunities")

        growth_query = """
        SELECT
            COUNT(*) as new_growing_count,
            ROUND(SUM(lifetime_value), 0) as new_revenue,
            ROUND(AVG(spend_change_pct), 1) as avg_growth_pct
        FROM GOLD.CUSTOMER_SEGMENTS
        WHERE customer_segment = 'New & Growing'
        """

        df_growth = execute_query(growth_query)

        if not df_growth.empty:
            st.metric(
                "New & Growing",
                f"{df_growth['NEW_GROWING_COUNT'].iloc[0]:,}",
                delta=None,
                help="Recent customers with positive trend"
            )

            st.metric(
                "Growth Revenue",
                f"${df_growth['NEW_REVENUE'].iloc[0]:,.0f}",
                delta=None,
                help="Total LTV of new & growing customers"
            )

            st.metric(
                "Avg Spend Growth",
                f"+{df_growth['AVG_GROWTH_PCT'].iloc[0]:.1f}%",
                delta=None,
                help="Average 90-day spend increase"
            )

            st.success("💡 **Recommended Action:** Nurture with onboarding campaigns to maximize growth")

    st.markdown("---")

    # ========== SEGMENT DETAILS TABLE ==========
    st.markdown("### 📋 Segment Performance Summary")

    detailed_query = """
    SELECT
        customer_segment as "Segment",
        COUNT(*) as "Customer Count",
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as "% of Base",
        ROUND(AVG(lifetime_value), 0) as "Avg LTV",
        ROUND(AVG(avg_transaction_value), 2) as "Avg Transaction",
        ROUND(AVG(spend_last_90_days), 0) as "Avg 90-Day Spend",
        ROUND(AVG(spend_change_pct), 1) as "90-Day Trend %",
        ROUND(AVG(tenure_months), 0) as "Avg Tenure (Months)"
    FROM GOLD.CUSTOMER_SEGMENTS
    GROUP BY customer_segment
    ORDER BY COUNT(*) DESC
    """

    df_details = execute_query(detailed_query)

    if not df_details.empty:
        # Style the dataframe
        styled_df = df_details.style.format({
            'Customer Count': '{:,}',
            '% of Base': '{:.1f}%',
            'Avg LTV': '${:,.0f}',
            'Avg Transaction': '${:,.2f}',
            'Avg 90-Day Spend': '${:,.0f}',
            '90-Day Trend %': '{:+.1f}%',
            'Avg Tenure (Months)': '{:.0f}'
        }).background_gradient(
            subset=['Avg LTV'],
            cmap='Blues'
        ).background_gradient(
            subset=['90-Day Trend %'],
            cmap='RdYlGn',
            vmin=-50,
            vmax=50
        )

        st.dataframe(styled_df, use_container_width=True, height=250)

    # ========== QUICK INSIGHTS ==========
    st.markdown("---")
    st.markdown("### 💡 Key Insights & Recommendations")

    insights_col1, insights_col2, insights_col3 = st.columns(3)

    with insights_col1:
        st.info("""
        **🎯 High-Value Focus**

        High-Value Travelers represent your premium segment.

        → Focus retention efforts here for maximum ROI
        """)

    with insights_col2:
        st.warning("""
        **⚠️ Churn Prevention**

        Declining customers need immediate attention.

        → Launch targeted retention campaigns
        """)

    with insights_col3:
        st.success("""
        **📈 Growth Acceleration**

        New & Growing customers show strong momentum.

        → Nurture with personalized onboarding
        """)
