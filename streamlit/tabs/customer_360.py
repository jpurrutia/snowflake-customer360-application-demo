import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from utils import format_dataframe_columns


def render(execute_query, conn):
    """
    Render Customer 360 Deep Dive tab.

    Features:
    - Customer search (by ID, name, email)
    - Profile summary with key metrics
    - Spending trends over time
    - Transaction history table with filters
    - Category breakdown pie chart
    - Alerts for churn risk and unusual activity
    """
    st.title("🔍 Customer 360 Deep Dive")
    st.markdown("Detailed customer profile and transaction analysis")

    # ========== CUSTOMER SEARCH ==========
    st.subheader("🔎 Find Customer")

    search_method = st.radio(
        "Search by:",
        ["Customer ID", "Name", "Email"],
        horizontal=True
    )

    if search_method == "Customer ID":
        customer_id = st.number_input(
            "Enter Customer ID",
            min_value=1,
            max_value=100000,
            value=1,
            step=1
        )

        query = f"""
            SELECT *
            FROM GOLD.CUSTOMER_360_PROFILE
            WHERE customer_id = {customer_id}
        """

    elif search_method == "Name":
        name_search = st.text_input("Enter customer name (partial match)")

        if not name_search:
            st.info("Enter a name to search")
            return

        query = f"""
            SELECT *
            FROM GOLD.CUSTOMER_360_PROFILE
            WHERE LOWER(full_name) LIKE LOWER('%{name_search}%')
            LIMIT 20
        """

    else:  # Email
        email_search = st.text_input("Enter email (partial match)")

        if not email_search:
            st.info("Enter an email to search")
            return

        query = f"""
            SELECT *
            FROM GOLD.CUSTOMER_360_PROFILE
            WHERE LOWER(email) LIKE LOWER('%{email_search}%')
            LIMIT 20
        """

    # Execute search
    if st.button("Search", type="primary"):
        with st.spinner("Searching..."):
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            cursor.close()

            if results:
                df_results = pd.DataFrame(results, columns=columns)
                st.session_state['search_results'] = df_results
            else:
                st.warning("No customers found")
                return

    # Display search results (if multiple)
    if 'search_results' in st.session_state:
        df_results = st.session_state['search_results']

        if len(df_results) > 1:
            st.subheader(f"Found {len(df_results)} customers")

            # Let user select
            selected_idx = st.selectbox(
                "Select customer:",
                range(len(df_results)),
                format_func=lambda i: f"{df_results.iloc[i]['FULL_NAME']} ({df_results.iloc[i]['EMAIL']})"
            )

            customer = df_results.iloc[selected_idx]
        else:
            customer = df_results.iloc[0]

        # Store selected customer
        st.session_state['selected_customer'] = customer

    # ========== CUSTOMER PROFILE ==========

    if 'selected_customer' not in st.session_state:
        st.info("👆 Search for a customer to view their profile")
        return

    customer = st.session_state['selected_customer']
    customer_id = customer['CUSTOMER_ID']

    st.markdown("---")
    st.subheader("👤 Customer Profile")

    # Profile header
    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:
        st.markdown(f"### {customer['FULL_NAME']}")
        st.markdown(f"**Email:** {customer['EMAIL']}")
        st.markdown(f"**Location:** {customer['CITY']}, {customer['STATE']}")
        st.markdown(f"**Segment:** {customer['CUSTOMER_SEGMENT']}")

    with col2:
        st.metric("Card Type", customer['CARD_TYPE'])
        st.metric("Credit Limit", f"${customer['CREDIT_LIMIT']:,.0f}")

    with col3:
        # Churn risk alert
        risk_score = customer['CHURN_RISK_SCORE'] if pd.notna(customer['CHURN_RISK_SCORE']) else 0
        risk_category = customer['CHURN_RISK_CATEGORY'] if pd.notna(customer['CHURN_RISK_CATEGORY']) else 'Unknown'

        if risk_category == 'High Risk':
            st.error(f"⚠️ High Churn Risk\n{risk_score:.1f}%")
        elif risk_category == 'Medium Risk':
            st.warning(f"⚡ Medium Churn Risk\n{risk_score:.1f}%")
        else:
            st.success(f"✅ Low Churn Risk\n{risk_score:.1f}%")

    # ========== KEY METRICS ==========

    st.subheader("📊 Key Metrics")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Lifetime Value", f"${customer['LIFETIME_VALUE']:,.0f}")

    with col2:
        st.metric("Avg Transaction", f"${customer['AVG_TRANSACTION_VALUE']:,.0f}")

    with col3:
        spend_90d = customer['SPEND_LAST_90_DAYS'] if pd.notna(customer['SPEND_LAST_90_DAYS']) else 0
        st.metric("Spend (90d)", f"${spend_90d:,.0f}")

    with col4:
        days_since_last = customer['DAYS_SINCE_LAST_TRANSACTION']
        st.metric("Days Since Last Txn", f"{days_since_last}")

    # Spending trend
    col1, col2 = st.columns(2)

    with col1:
        spend_change = customer['SPEND_CHANGE_PCT'] if pd.notna(customer['SPEND_CHANGE_PCT']) else 0
        delta_color = "normal" if spend_change >= 0 else "inverse"
        st.metric(
            "Spend Change (MoM)",
            f"{spend_change:+.1f}%",
            delta=f"{spend_change:+.1f}%",
            delta_color=delta_color
        )

    with col2:
        avg_monthly = customer['AVG_MONTHLY_SPEND'] if pd.notna(customer['AVG_MONTHLY_SPEND']) else 0
        st.metric("Avg Monthly Spend", f"${avg_monthly:,.0f}")

    # ========== TRANSACTION HISTORY ==========

    st.markdown("---")
    st.subheader("💳 Transaction History")

    # Fetch transactions
    txn_query = f"""
        SELECT
            t.transaction_date,
            t.merchant_name,
            c.category_name,
            c.category_group,
            t.transaction_amount,
            t.channel,
            t.status
        FROM GOLD.FCT_TRANSACTIONS t
        JOIN GOLD.DIM_CUSTOMER d ON t.customer_key = d.customer_key
        JOIN GOLD.DIM_MERCHANT_CATEGORY c ON t.merchant_category_key = c.category_key
        WHERE d.customer_id = '{customer_id}' AND d.is_current = TRUE
        ORDER BY t.transaction_date DESC
        LIMIT 1000
    """

    cursor = conn.cursor()
    cursor.execute(txn_query)
    txn_results = cursor.fetchall()
    txn_columns = [desc[0] for desc in cursor.description]
    cursor.close()

    if not txn_results:
        st.warning("No transactions found for this customer")
        return

    df_txns = pd.DataFrame(txn_results, columns=txn_columns)

    # Transaction filters
    col1, col2, col3 = st.columns(3)

    with col1:
        # Date range filter
        date_range = st.selectbox(
            "Time Period",
            ["Last 30 days", "Last 90 days", "Last 6 months", "All time"]
        )

        if date_range == "Last 30 days":
            cutoff_date = datetime.now() - timedelta(days=30)
        elif date_range == "Last 90 days":
            cutoff_date = datetime.now() - timedelta(days=90)
        elif date_range == "Last 6 months":
            cutoff_date = datetime.now() - timedelta(days=180)
        else:
            cutoff_date = datetime.min

        df_txns_filtered = df_txns[df_txns['TRANSACTION_DATE'] >= cutoff_date]

    with col2:
        # Category filter
        categories = ["All"] + sorted(df_txns['CATEGORY_NAME'].unique().tolist())
        selected_category = st.selectbox("Category", categories)

        if selected_category != "All":
            df_txns_filtered = df_txns_filtered[df_txns_filtered['CATEGORY_NAME'] == selected_category]

    with col3:
        # Status filter
        statuses = ["All"] + sorted(df_txns['STATUS'].unique().tolist())
        selected_status = st.selectbox("Status", statuses)

        if selected_status != "All":
            df_txns_filtered = df_txns_filtered[df_txns_filtered['STATUS'] == selected_status]

    # ========== VISUALIZATIONS ==========

    st.subheader("📈 Spending Trends")

    col1, col2 = st.columns(2)

    with col1:
        # Daily spending over time
        df_daily = df_txns_filtered.groupby('TRANSACTION_DATE')['TRANSACTION_AMOUNT'].sum().reset_index()
        df_daily = df_daily.sort_values('TRANSACTION_DATE')

        fig_trend = px.line(
            df_daily,
            x='TRANSACTION_DATE',
            y='TRANSACTION_AMOUNT',
            title='Daily Spending Over Time',
            labels={'TRANSACTION_AMOUNT': 'Amount ($)', 'TRANSACTION_DATE': 'Date'}
        )
        fig_trend.update_traces(line_color='#1f77b4', line_width=2)
        st.plotly_chart(fig_trend, use_container_width=True)

    with col2:
        # Category breakdown pie chart
        df_category = df_txns_filtered.groupby('CATEGORY_NAME')['TRANSACTION_AMOUNT'].sum().reset_index()
        df_category = df_category.sort_values('TRANSACTION_AMOUNT', ascending=False)

        fig_category = px.pie(
            df_category,
            values='TRANSACTION_AMOUNT',
            names='CATEGORY_NAME',
            title='Spending by Category'
        )
        st.plotly_chart(fig_category, use_container_width=True)

    # ========== TRANSACTION TABLE ==========

    st.subheader("📋 Transaction Details")

    # Summary stats
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Transactions", f"{len(df_txns_filtered):,}")

    with col2:
        st.metric("Total Spend", f"${df_txns_filtered['TRANSACTION_AMOUNT'].sum():,.2f}")

    with col3:
        st.metric("Avg Transaction", f"${df_txns_filtered['TRANSACTION_AMOUNT'].mean():,.2f}")

    with col4:
        approved_pct = (df_txns_filtered['STATUS'] == 'approved').sum() / len(df_txns_filtered) * 100
        st.metric("Approval Rate", f"{approved_pct:.1f}%")

    # Transaction table
    display_df = df_txns_filtered.copy()
    display_df['TRANSACTION_DATE'] = pd.to_datetime(display_df['TRANSACTION_DATE']).dt.strftime('%Y-%m-%d')
    display_df['TRANSACTION_AMOUNT'] = display_df['TRANSACTION_AMOUNT'].apply(lambda x: f"${x:,.2f}")

    # Apply human-readable column names
    display_df = format_dataframe_columns(display_df)

    st.dataframe(
        display_df,
        use_container_width=True,
        height=400
    )

    # Export with human-readable column names
    csv_df = format_dataframe_columns(df_txns_filtered.copy())
    st.download_button(
        label="📥 Download Transaction History (CSV)",
        data=csv_df.to_csv(index=False),
        file_name=f"customer_{customer_id}_transactions_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )
