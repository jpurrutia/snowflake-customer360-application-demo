import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
from .utils import format_dataframe_columns

def render(execute_query):
    """Render Segment Explorer tab"""

    st.header("🎯 Customer Segment Explorer")
    st.markdown("Identify and export customer segments for targeted marketing campaigns")

    # ========== FILTERS ==========
    st.subheader("Filters")

    col1, col2, col3 = st.columns(3)

    with col1:
        # Segment filter
        segments = st.multiselect(
            "Customer Segment",
            ["High-Value Travelers", "Stable Mid-Spenders", "Budget-Conscious", "Declining", "New & Growing"],
            default=["High-Value Travelers", "Declining"]
        )

    with col2:
        # State filter - cached to avoid hitting query limit
        @st.cache_data(ttl=3600)
        def get_states():
            states_query = "SELECT DISTINCT state FROM CUSTOMER_360_PROFILE ORDER BY state"
            return execute_query(states_query)

        states_df = get_states()

        if not states_df.empty:
            all_states = ["All"] + states_df['STATE'].tolist()
            selected_states = st.multiselect("State", all_states, default=["All"])
        else:
            selected_states = ["All"]

    with col3:
        # Churn risk filter
        churn_risk = st.selectbox(
            "Churn Risk",
            ["All", "High Risk", "Medium Risk", "Low Risk"]
        )

    # Additional filters (expandable)
    with st.expander("Advanced Filters"):
        col1, col2 = st.columns(2)

        with col1:
            min_ltv = st.number_input("Min Lifetime Value ($)", min_value=0, value=0, step=1000)

        with col2:
            card_type = st.selectbox("Card Type", ["All", "Standard", "Premium"])

    # ========== BUILD QUERY ==========
    st.markdown("---")

    if st.button("Apply Filters", type="primary"):
        # Build WHERE clause
        where_clauses = []

        if segments:
            segments_str = "', '".join(segments)
            where_clauses.append(f"customer_segment IN ('{segments_str}')")

        if "All" not in selected_states:
            states_str = "', '".join(selected_states)
            where_clauses.append(f"state IN ('{states_str}')")

        if churn_risk != "All":
            where_clauses.append(f"churn_risk_category = '{churn_risk}'")

        if min_ltv > 0:
            where_clauses.append(f"lifetime_value >= {min_ltv}")

        if card_type != "All":
            where_clauses.append(f"card_type = '{card_type}'")

        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

        # Execute query
        query = f"""
        SELECT
            customer_id,
            full_name,
            email,
            state,
            city,
            customer_segment,
            card_type,
            lifetime_value,
            avg_transaction_value,
            churn_risk_category,
            churn_risk_score,
            days_since_last_transaction
        FROM CUSTOMER_360_PROFILE
        WHERE {where_clause}
        ORDER BY lifetime_value DESC
        LIMIT 5000
        """

        with st.spinner("Loading customer data..."):
            df = execute_query(query)

        if not df.empty:
            # Store in session state
            st.session_state['filtered_customers'] = df
        else:
            st.warning("No customers match the selected filters.")

    # ========== DISPLAY RESULTS ==========

    if 'filtered_customers' in st.session_state:
        df = st.session_state['filtered_customers']

        # Summary metrics
        st.subheader("📈 Summary Metrics")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Customers", f"{len(df):,}")

        with col2:
            st.metric("Total LTV", f"${df['LIFETIME_VALUE'].sum():,.0f}")

        with col3:
            st.metric("Avg LTV", f"${df['LIFETIME_VALUE'].mean():,.0f}")

        with col4:
            avg_risk = df['CHURN_RISK_SCORE'].mean()
            st.metric("Avg Churn Risk", f"{avg_risk:.1f}%")

        # Visualizations
        st.subheader("📊 Segment Analysis")

        col1, col2 = st.columns(2)

        with col1:
            # Segment distribution pie chart
            segment_counts = df['CUSTOMER_SEGMENT'].value_counts().reset_index()
            segment_counts.columns = ['Segment', 'Count']

            fig_pie = px.pie(
                segment_counts,
                values='Count',
                names='Segment',
                title='Customer Segment Distribution'
            )
            st.plotly_chart(fig_pie, use_container_width=True)

        with col2:
            # Churn risk distribution
            risk_counts = df['CHURN_RISK_CATEGORY'].value_counts().reset_index()
            risk_counts.columns = ['Risk Level', 'Count']

            fig_bar = px.bar(
                risk_counts,
                x='Risk Level',
                y='Count',
                title='Churn Risk Distribution',
                color='Risk Level',
                color_discrete_map={'Low Risk': 'green', 'Medium Risk': 'orange', 'High Risk': 'red'}
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        # LTV by segment
        segment_ltv = df.groupby('CUSTOMER_SEGMENT')['LIFETIME_VALUE'].agg(['mean', 'sum']).reset_index()

        fig_ltv = px.bar(
            segment_ltv,
            x='CUSTOMER_SEGMENT',
            y='sum',
            title='Total LTV by Segment',
            labels={'sum': 'Total LTV', 'CUSTOMER_SEGMENT': 'Segment'},
            text_auto='.2s'
        )
        st.plotly_chart(fig_ltv, use_container_width=True)

        # Customer data table
        st.subheader("👥 Customer List")

        # Format columns for display
        display_df = df.copy()
        display_df['LIFETIME_VALUE'] = display_df['LIFETIME_VALUE'].apply(lambda x: f"${x:,.0f}")
        display_df['AVG_TRANSACTION_VALUE'] = display_df['AVG_TRANSACTION_VALUE'].apply(lambda x: f"${x:,.0f}")
        display_df['CHURN_RISK_SCORE'] = display_df['CHURN_RISK_SCORE'].apply(lambda x: f"{x:.1f}%")

        # Apply human-readable column names
        display_df = format_dataframe_columns(display_df)

        st.dataframe(
            display_df,
            use_container_width=True,
            height=400
        )

        # Export functionality
        st.subheader("📥 Export Segment")

        # Export with human-readable column names
        csv_df = format_dataframe_columns(df.copy())
        csv = csv_df.to_csv(index=False)

        st.download_button(
            label="Download as CSV",
            data=csv,
            file_name=f"customer_segment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            type="primary"
        )

        st.info("💡 **Coming soon:** Direct export to Salesforce, HubSpot, and Google Ads")
