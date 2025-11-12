import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime


def calculate_campaign_roi(
    target_customers: pd.DataFrame,
    incentive_per_customer: float,
    expected_retention_rate: float,
    campaign_cost_per_customer: float
) -> dict:
    """
    Calculate ROI for retention campaign.

    Args:
        target_customers: DataFrame of customers to target
        incentive_per_customer: $ incentive offered (e.g., $50 statement credit)
        expected_retention_rate: % of customers expected to be retained (0-100)
        campaign_cost_per_customer: $ cost to run campaign per customer

    Returns:
        dict with ROI metrics
    """
    num_customers = len(target_customers)
    total_ltv = target_customers['LIFETIME_VALUE'].sum()
    avg_ltv = target_customers['LIFETIME_VALUE'].mean()

    # Costs
    total_incentive_cost = num_customers * incentive_per_customer
    total_campaign_cost = num_customers * campaign_cost_per_customer
    total_cost = total_incentive_cost + total_campaign_cost

    # Expected retention
    expected_retained_customers = int(num_customers * (expected_retention_rate / 100))

    # Assume retained customers continue spending at current rate
    # Use avg_monthly_spend * 12 months as proxy for annual value
    expected_retained_value = expected_retained_customers * avg_ltv * 0.20  # 20% of LTV as annual value

    # ROI calculation
    net_benefit = expected_retained_value - total_cost
    roi_pct = (net_benefit / total_cost * 100) if total_cost > 0 else 0

    return {
        'num_customers': num_customers,
        'total_cost': total_cost,
        'incentive_cost': total_incentive_cost,
        'campaign_cost': total_campaign_cost,
        'expected_retained_customers': expected_retained_customers,
        'expected_retained_value': expected_retained_value,
        'net_benefit': net_benefit,
        'roi_pct': roi_pct,
        'cost_per_retained_customer': total_cost / expected_retained_customers if expected_retained_customers > 0 else 0
    }


def render(execute_query, conn):
    """
    Render Campaign Performance Simulator tab.

    Features:
    - Target audience selector (segment, churn risk)
    - Campaign parameter inputs (incentive, retention rate, costs)
    - ROI calculation and visualization
    - Scenario comparison
    - Export campaign target list
    """
    st.title("📈 Campaign Performance Simulator")
    st.markdown("Model retention campaign ROI and target audiences")

    # ========== TARGET AUDIENCE ==========

    st.subheader("🎯 Define Target Audience")

    col1, col2, col3 = st.columns(3)

    with col1:
        # Segment filter
        segment_options = st.multiselect(
            "Customer Segments",
            ["High-Value Travelers", "Declining", "New & Growing", "Budget-Conscious", "Stable Mid-Spenders"],
            default=["Declining"]
        )

    with col2:
        # Churn risk filter
        churn_risk_options = st.multiselect(
            "Churn Risk Levels",
            ["High Risk", "Medium Risk", "Low Risk"],
            default=["High Risk", "Medium Risk"]
        )

    with col3:
        # Card type filter
        card_type_options = st.multiselect(
            "Card Types",
            ["Standard", "Premium"],
            default=["Standard", "Premium"]
        )

    # Advanced filters
    with st.expander("🔧 Advanced Filters"):
        col1, col2 = st.columns(2)

        with col1:
            min_ltv = st.number_input(
                "Min Lifetime Value ($)",
                min_value=0,
                value=5000,
                step=1000
            )

        with col2:
            min_churn_score = st.number_input(
                "Min Churn Risk Score",
                min_value=0,
                max_value=100,
                value=40,
                step=5
            )

    # Build query
    if st.button("🔍 Find Target Audience", type="primary"):
        where_clauses = []

        if segment_options:
            segments_str = "', '".join(segment_options)
            where_clauses.append(f"customer_segment IN ('{segments_str}')")

        if churn_risk_options:
            risk_str = "', '".join(churn_risk_options)
            where_clauses.append(f"churn_risk_category IN ('{risk_str}')")

        if card_type_options:
            card_str = "', '".join(card_type_options)
            where_clauses.append(f"card_type IN ('{card_str}')")

        where_clauses.append(f"lifetime_value >= {min_ltv}")
        where_clauses.append(f"churn_risk_score >= {min_churn_score}")

        where_clause = " AND ".join(where_clauses)

        query = f"""
            SELECT
                customer_id,
                full_name,
                email,
                customer_segment,
                churn_risk_category,
                churn_risk_score,
                card_type,
                lifetime_value,
                avg_monthly_spend,
                spend_last_90_days,
                state
            FROM GOLD.CUSTOMER_360_PROFILE
            WHERE {where_clause}
            ORDER BY churn_risk_score DESC, lifetime_value DESC
        """

        with st.spinner("Finding target audience..."):
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            cursor.close()

            if results:
                df_targets = pd.DataFrame(results, columns=columns)
                st.session_state['target_customers'] = df_targets
                st.success(f"✅ Found {len(df_targets):,} customers matching criteria")
            else:
                st.warning("No customers match the selected criteria")
                return

    # ========== CAMPAIGN PARAMETERS ==========

    if 'target_customers' not in st.session_state:
        st.info("👆 Define target audience to begin campaign simulation")
        return

    df_targets = st.session_state['target_customers']

    st.markdown("---")
    st.subheader("💰 Campaign Parameters")

    col1, col2, col3 = st.columns(3)

    with col1:
        incentive = st.number_input(
            "Incentive per Customer ($)",
            min_value=0,
            max_value=500,
            value=50,
            step=10,
            help="Statement credit or reward offered to retain customer"
        )

    with col2:
        retention_rate = st.slider(
            "Expected Retention Rate (%)",
            min_value=0,
            max_value=100,
            value=30,
            step=5,
            help="% of targeted customers expected to be retained"
        )

    with col3:
        campaign_cost = st.number_input(
            "Campaign Cost per Customer ($)",
            min_value=0,
            max_value=100,
            value=5,
            step=1,
            help="Email, SMS, and operational costs per customer"
        )

    # ========== ROI CALCULATION ==========

    st.markdown("---")
    st.subheader("📊 Campaign ROI Analysis")

    roi_results = calculate_campaign_roi(
        df_targets,
        incentive,
        retention_rate,
        campaign_cost
    )

    # Display key metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Target Customers", f"{roi_results['num_customers']:,}")

    with col2:
        st.metric("Total Cost", f"${roi_results['total_cost']:,.0f}")

    with col3:
        st.metric("Expected Retained", f"{roi_results['expected_retained_customers']:,}")

    with col4:
        roi_color = "normal" if roi_results['roi_pct'] >= 0 else "inverse"
        st.metric(
            "ROI",
            f"{roi_results['roi_pct']:.1f}%",
            delta=f"${roi_results['net_benefit']:,.0f}",
            delta_color=roi_color
        )

    # Detailed breakdown
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**💸 Cost Breakdown**")
        cost_data = pd.DataFrame({
            'Category': ['Incentives', 'Campaign Operations'],
            'Cost': [roi_results['incentive_cost'], roi_results['campaign_cost']]
        })

        fig_cost = px.pie(
            cost_data,
            values='Cost',
            names='Category',
            title='Campaign Cost Breakdown'
        )
        st.plotly_chart(fig_cost, use_container_width=True)

    with col2:
        st.markdown("**📈 Expected Value**")
        st.metric("Retained Customer Value", f"${roi_results['expected_retained_value']:,.0f}")
        st.metric("Cost per Retained Customer", f"${roi_results['cost_per_retained_customer']:,.0f}")
        st.metric("Net Benefit", f"${roi_results['net_benefit']:,.0f}")

    # ========== SENSITIVITY ANALYSIS ==========

    st.markdown("---")
    st.subheader("🔬 Sensitivity Analysis")

    st.markdown("See how ROI changes with different retention rates:")

    # Calculate ROI for range of retention rates
    retention_range = range(10, 81, 10)
    sensitivity_results = []

    for rate in retention_range:
        result = calculate_campaign_roi(df_targets, incentive, rate, campaign_cost)
        sensitivity_results.append({
            'Retention Rate (%)': rate,
            'ROI (%)': result['roi_pct'],
            'Net Benefit ($)': result['net_benefit']
        })

    df_sensitivity = pd.DataFrame(sensitivity_results)

    fig_sensitivity = go.Figure()

    fig_sensitivity.add_trace(go.Scatter(
        x=df_sensitivity['Retention Rate (%)'],
        y=df_sensitivity['ROI (%)'],
        mode='lines+markers',
        name='ROI',
        line=dict(color='blue', width=3),
        marker=dict(size=8)
    ))

    # Add zero line
    fig_sensitivity.add_hline(y=0, line_dash="dash", line_color="gray")

    fig_sensitivity.update_layout(
        title='ROI vs Retention Rate',
        xaxis_title='Retention Rate (%)',
        yaxis_title='ROI (%)',
        hovermode='x'
    )

    st.plotly_chart(fig_sensitivity, use_container_width=True)

    # Breakeven analysis
    breakeven_rate = None
    for rate in range(1, 101):
        result = calculate_campaign_roi(df_targets, incentive, rate, campaign_cost)
        if result['roi_pct'] >= 0:
            breakeven_rate = rate
            break

    if breakeven_rate:
        st.info(f"💡 **Breakeven Point:** Campaign breaks even at {breakeven_rate}% retention rate")
    else:
        st.warning("⚠️ Campaign does not break even at any retention rate up to 100%")

    # ========== TARGET LIST ==========

    st.markdown("---")
    st.subheader("📋 Target Customer List")

    # Top customers by churn risk
    st.markdown(f"**Top 10 Highest Risk Customers (of {len(df_targets):,} total)**")

    display_df = df_targets.head(10).copy()
    display_df['LIFETIME_VALUE'] = display_df['LIFETIME_VALUE'].apply(lambda x: f"${x:,.0f}")
    display_df['AVG_MONTHLY_SPEND'] = display_df['AVG_MONTHLY_SPEND'].apply(lambda x: f"${x:,.0f}")

    st.dataframe(display_df, use_container_width=True)

    # Export full list
    st.download_button(
        label="📥 Download Full Target List (CSV)",
        data=df_targets.to_csv(index=False),
        file_name=f"campaign_targets_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        type="primary"
    )

    # ========== RECOMMENDATIONS ==========

    st.markdown("---")

    with st.expander("💡 Campaign Recommendations"):
        st.markdown(f"""
        **Based on your target audience ({roi_results['num_customers']:,} customers):**

        ✅ **Recommended Actions:**
        - Current ROI: **{roi_results['roi_pct']:.1f}%**
        - Target retention breakeven: **{breakeven_rate}%**
        - Focus on customers with churn risk score > 60
        - Personalize incentives based on customer segment

        📧 **Campaign Messaging:**
        - Emphasize benefits of staying (rewards, benefits)
        - Highlight exclusive offers for loyal customers
        - Create urgency with limited-time offers

        ⏰ **Timing:**
        - Deploy within 7 days for high-risk customers
        - Follow up after 2 weeks
        - Monitor spend changes in next 30 days

        📊 **Success Metrics:**
        - Track retention rate (target: {retention_rate}%+)
        - Monitor spend increase among retained customers
        - Calculate actual ROI vs projected
        """)
