import streamlit as st
import pandas as pd
from datetime import datetime
import json
import _snowflake
from snowflake.snowpark.context import get_active_session
from .utils import format_dataframe_columns, format_column_name
import plotly.express as px


# Suggested questions organized by use case
SUGGESTED_QUESTIONS = {
    "Churn Analysis": [
        "Which customers are at highest risk of churning?",
        "What is the average churn risk score by segment?",
        "Show me High-Value Travelers with high churn risk",
        "Which states have the highest churn risk?",
    ],
    "Customer Segmentation": [
        "How many customers are in each segment?",
        "Compare lifetime value across segments",
        "Which segments have Premium cards?",
        "Show me Declining segment customers in California",
    ],
    "Spending Trends": [
        "What is the total spending in the last 90 days?",
        "Show spending trends in travel over last 6 months",
        "Which customers increased spending the most?",
        "What is the average transaction value by card type?",
    ],
    "Geographic Analysis": [
        "What is the average lifetime value by state?",
        "Which states have the most Premium cardholders?",
        "Show me customer distribution across states",
        "Compare spending between California and Texas",
    ],
    "Campaign Targeting": [
        "Show me customers eligible for retention campaigns",
        "Which Premium cardholders are at medium or high risk?",
        "Find customers with declining spend in the last 90 days",
        "Show high-value customers with low recent activity",
    ]
}


def suggest_chart_type(df: pd.DataFrame) -> list:
    """
    Suggest appropriate chart types based on DataFrame structure.

    Args:
        df: DataFrame to analyze

    Returns:
        List of suggested chart types
    """
    if df is None or df.empty or len(df) == 0:
        return []

    # Analyze column types
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
    date_cols = df.select_dtypes(include=['datetime', 'datetime64']).columns.tolist()

    suggestions = []

    # Single row or single column - table view only
    if len(df) == 1 or len(df.columns) == 1:
        return []

    # Detect geographic columns
    geo_keywords = ['state', 'country', 'city', 'region', 'zip', 'postal', 'county', 'province']
    geo_cols = [col for col in categorical_cols
                if any(keyword in col.lower() for keyword in geo_keywords)]

    # Geographic data - prioritize map visualizations
    if geo_cols and numeric_cols:
        # Check if it's US state data for choropleth
        if any('state' in col.lower() for col in geo_cols):
            suggestions.append('choropleth_usa')
        suggestions.append('bar')  # Bar chart is also good for geo data
        suggestions.append('pie')

    # Time series data
    elif date_cols and numeric_cols:
        suggestions.extend(['line', 'area'])

    # Categorical + Numeric (most common for business analytics)
    elif categorical_cols and numeric_cols and len(df) > 1:
        suggestions.extend(['bar', 'pie'])
        if len(df) <= 20:  # Only for smaller datasets
            suggestions.append('scatter')

    # Multiple numeric columns
    if len(numeric_cols) >= 2:
        if 'scatter' not in suggestions:
            suggestions.append('scatter')
        if 'line' not in suggestions:
            suggestions.append('line')

    # Single numeric column - distribution
    if len(numeric_cols) == 1 and len(df) > 10:
        suggestions.append('histogram')

    # Hierarchical data (2+ categorical columns)
    if len(categorical_cols) >= 2 and numeric_cols:
        if 'sunburst' not in suggestions:
            suggestions.append('sunburst')

    # Remove duplicates while preserving order
    seen = set()
    return [x for x in suggestions if not (x in seen or seen.add(x))]


def render_chart(df: pd.DataFrame, chart_type: str):
    """
    Render a chart using Plotly based on the chart type and DataFrame structure.

    Args:
        df: DataFrame to visualize
        chart_type: Type of chart to render (bar, line, pie, scatter, etc.)
    """
    if df is None or df.empty:
        st.warning("No data available to visualize")
        return

    # Analyze columns
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
    date_cols = df.select_dtypes(include=['datetime', 'datetime64']).columns.tolist()

    try:
        if chart_type == 'choropleth_usa':
            # US State choropleth map
            geo_keywords = ['state']
            state_col = None
            for col in df.columns:
                if any(keyword in col.lower() for keyword in geo_keywords):
                    state_col = col
                    break

            if state_col and numeric_cols:
                value_col = numeric_cols[0]

                # Standardize state names to abbreviations for plotting
                # This handles both full names and abbreviations
                state_abbrev_map = {
                    'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR', 'california': 'CA',
                    'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE', 'florida': 'FL', 'georgia': 'GA',
                    'hawaii': 'HI', 'idaho': 'ID', 'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA',
                    'kansas': 'KS', 'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
                    'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
                    'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV', 'new hampshire': 'NH',
                    'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY', 'north carolina': 'NC',
                    'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK', 'oregon': 'OR', 'pennsylvania': 'PA',
                    'rhode island': 'RI', 'south carolina': 'SC', 'south dakota': 'SD', 'tennessee': 'TN',
                    'texas': 'TX', 'utah': 'UT', 'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA',
                    'west virginia': 'WV', 'wisconsin': 'WI', 'wyoming': 'WY'
                }

                # Create a copy and normalize state names
                plot_df = df.copy()
                plot_df[state_col] = plot_df[state_col].apply(
                    lambda x: state_abbrev_map.get(str(x).lower(), str(x).upper())
                )

                fig = px.choropleth(
                    plot_df,
                    locations=state_col,
                    locationmode='USA-states',
                    color=value_col,
                    scope='usa',
                    title=f'{format_column_name(value_col)} by State',
                    color_continuous_scale='RdYlGn_r',  # Red (bad) to Green (good)
                    labels={value_col: format_column_name(value_col)}
                )
                fig.update_layout(
                    geo=dict(bgcolor='rgba(0,0,0,0)'),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='white')
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("Choropleth map requires a state column and at least one numeric column")

        elif chart_type == 'bar':
            # Use first categorical/date column as x, first numeric as y
            x_col = categorical_cols[0] if categorical_cols else (date_cols[0] if date_cols else df.columns[0])
            y_col = numeric_cols[0] if numeric_cols else df.columns[1]
            fig = px.bar(df, x=x_col, y=y_col, title=f"{y_col} by {x_col}")
            st.plotly_chart(fig, use_container_width=True)

        elif chart_type == 'line':
            x_col = date_cols[0] if date_cols else (categorical_cols[0] if categorical_cols else df.columns[0])
            y_col = numeric_cols[0] if numeric_cols else df.columns[1]
            fig = px.line(df, x=x_col, y=y_col, title=f"{y_col} over {x_col}")
            st.plotly_chart(fig, use_container_width=True)

        elif chart_type == 'area':
            x_col = date_cols[0] if date_cols else (categorical_cols[0] if categorical_cols else df.columns[0])
            y_col = numeric_cols[0] if numeric_cols else df.columns[1]
            fig = px.area(df, x=x_col, y=y_col, title=f"{y_col} over {x_col}")
            st.plotly_chart(fig, use_container_width=True)

        elif chart_type == 'pie':
            names_col = categorical_cols[0] if categorical_cols else df.columns[0]
            values_col = numeric_cols[0] if numeric_cols else df.columns[1]
            fig = px.pie(df, names=names_col, values=values_col, title=f"{values_col} by {names_col}")
            st.plotly_chart(fig, use_container_width=True)

        elif chart_type == 'scatter':
            x_col = numeric_cols[0] if len(numeric_cols) >= 2 else df.columns[0]
            y_col = numeric_cols[1] if len(numeric_cols) >= 2 else numeric_cols[0]
            color_col = categorical_cols[0] if categorical_cols else None
            fig = px.scatter(df, x=x_col, y=y_col, color=color_col, title=f"{y_col} vs {x_col}")
            st.plotly_chart(fig, use_container_width=True)

        elif chart_type == 'histogram':
            col = numeric_cols[0] if numeric_cols else df.columns[0]
            fig = px.histogram(df, x=col, title=f"Distribution of {col}")
            st.plotly_chart(fig, use_container_width=True)

        elif chart_type == 'sunburst':
            # Hierarchical visualization
            if len(categorical_cols) >= 2 and numeric_cols:
                path_cols = categorical_cols[:2]
                values_col = numeric_cols[0]
                fig = px.sunburst(df, path=path_cols, values=values_col, title=f"{values_col} by {' > '.join(path_cols)}")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("Sunburst chart requires at least 2 categorical columns and 1 numeric column")

    except Exception as e:
        st.error(f"Error rendering {chart_type} chart: {e}")
        st.info("Try a different chart type or view the data as a table")


def call_cortex_analyst_mock(conn, question: str) -> dict:
    """
    Mock Cortex Analyst for testing when Cortex Analyst not available.
    Maps common questions to pre-written SQL.

    Args:
        conn: Snowflake connection
        question: Natural language question

    Returns:
        dict with keys: sql, results, error
    """
    question_lower = question.lower()

    # Map questions to SQL
    if 'highest risk' in question_lower and 'churn' in question_lower:
        sql = """
            SELECT customer_id, full_name, email, customer_segment,
                   churn_risk_score, churn_risk_category
            FROM GOLD.CUSTOMER_360_PROFILE
            WHERE churn_risk_category = 'High Risk'
            ORDER BY churn_risk_score DESC
            LIMIT 100
        """

    elif 'customers in each segment' in question_lower:
        sql = """
            SELECT customer_segment, COUNT(*) AS customer_count
            FROM GOLD.CUSTOMER_360_PROFILE
            GROUP BY customer_segment
            ORDER BY customer_count DESC
        """

    elif 'lifetime value' in question_lower and 'segment' in question_lower:
        sql = """
            SELECT customer_segment,
                   AVG(lifetime_value) AS avg_ltv,
                   COUNT(*) AS customer_count
            FROM GOLD.CUSTOMER_360_PROFILE
            GROUP BY customer_segment
            ORDER BY avg_ltv DESC
        """

    elif 'total spending' in question_lower and '90 days' in question_lower:
        sql = """
            SELECT SUM(spend_last_90_days) AS total_spend_90d,
                   COUNT(*) AS customer_count
            FROM GOLD.CUSTOMER_360_PROFILE
        """

    elif 'premium' in question_lower and ('medium' in question_lower or 'high' in question_lower) and 'risk' in question_lower:
        sql = """
            SELECT customer_id, full_name, customer_segment,
                   card_type, churn_risk_category, lifetime_value
            FROM GOLD.CUSTOMER_360_PROFILE
            WHERE card_type = 'Premium'
              AND churn_risk_category IN ('Medium Risk', 'High Risk')
            ORDER BY churn_risk_score DESC
            LIMIT 100
        """

    else:
        return {
            'sql': None,
            'results': None,
            'suggestions': [],
            'error': 'Question not recognized by mock. Try a suggested question or wait for Cortex Analyst integration.'
        }

    # Execute SQL
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results, columns=columns)
        cursor.close()

        return {
            'sql': sql,
            'results': df,
            'suggestions': [],
            'error': None
        }
    except Exception as e:
        return {
            'sql': sql,
            'results': None,
            'suggestions': [],
            'error': str(e)
        }


def call_cortex_analyst(conn, question: str, conversation_history: list = None) -> dict:
    """
    Call Snowflake Cortex Analyst REST API to answer natural language question.
    Uses the native Streamlit in Snowflake API for authentication.

    Args:
        conn: Snowflake connection
        question: Natural language question
        conversation_history: Optional list of previous Q&A pairs for context

    Returns:
        dict with keys: sql, results, interpretation, error
    """
    try:
        # Build conversation history for context (multi-turn conversations)
        # Cortex Analyst requires strict alternating roles (user, analyst, user, analyst, etc.)
        # Only include complete pairs (question + successful response) to maintain alternation
        messages = []
        if conversation_history:
            for item in conversation_history[-3:]:  # Last 3 exchanges for context
                response = item.get('response', {})
                # Only add to history if the response was successful (to maintain role alternation)
                if response and not response.get('error'):
                    # Add user question
                    messages.append({
                        "role": "user",
                        "content": [{"type": "text", "text": item.get('question', '')}]
                    })
                    # Add analyst response
                    interpretation = response.get('interpretation') or "Generated SQL query"
                    messages.append({
                        "role": "analyst",
                        "content": [{"type": "text", "text": interpretation}]
                    })

        # Add current question
        messages.append({
            "role": "user",
            "content": [{"type": "text", "text": question}]
        })

        # Request payload
        request_body = {
            "messages": messages,
            "semantic_model_file": "@SEMANTIC_MODELS.DEFINITIONS.SEMANTIC_STAGE/customer_analytics.yaml"
        }

        # Use Snowflake's native API request function for Streamlit in Snowflake
        # This handles authentication automatically
        resp = _snowflake.send_snow_api_request(
            "POST",  # method
            "/api/v2/cortex/analyst/message",  # path
            {},  # headers
            {},  # params
            request_body,  # body
            None,  # request_guid
            50000,  # timeout in milliseconds
        )

        # Parse response content (it's a JSON string)
        parsed_content = json.loads(resp["content"])

        # Extract request ID for debugging
        request_id = parsed_content.get('request_id', 'unknown')

        # Check if the response is successful
        if resp["status"] >= 400:
            error_msg = f"Cortex Analyst API error (status {resp['status']}): {parsed_content.get('message', 'Unknown error')}"
            st.warning(f"⚠️ {error_msg}. Using mock implementation.")
            st.info(f"🔍 **Request ID:** `{request_id}` - Use this to look up query history")

            # Log full response for debugging
            with st.expander("🔧 Debug Info"):
                st.json({
                    "status": resp["status"],
                    "request_id": request_id,
                    "response": parsed_content
                })

            return call_cortex_analyst_mock(conn, question)

        # Extract message content
        message = parsed_content.get('message', {})

        # Extract SQL, interpretation, and suggestions
        generated_sql = None
        interpretation = None
        suggestions = []

        # Try to find SQL, text, and suggestions in content blocks
        content = message.get('content', [])
        for item in content:
            if item.get('type') == 'sql':
                generated_sql = item.get('statement')
            elif item.get('type') == 'text':
                interpretation = item.get('text')
            elif item.get('type') == 'suggestions':
                suggestions = item.get('suggestions', [])

        if not generated_sql:
            return {
                'sql': None,
                'results': None,
                'interpretation': interpretation,
                'suggestions': suggestions,
                'error': 'Cortex Analyst did not generate SQL for this question'
            }

        # Execute the generated SQL
        cursor = conn.cursor()
        cursor.execute(generated_sql)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results, columns=columns)
        cursor.close()

        return {
            'sql': generated_sql,
            'results': df,
            'interpretation': interpretation,
            'suggestions': suggestions,
            'error': None
        }

    except Exception as e:
        error_msg = str(e)
        st.warning(f"⚠️ Cortex Analyst error: {error_msg}. Using mock implementation.")
        return call_cortex_analyst_mock(conn, question)


def render(execute_query, conn):
    """
    Render AI Assistant tab with Cortex Analyst integration.

    Features:
    - Natural language question input
    - Suggested questions by category
    - Generated SQL display
    - Results table
    - Query history
    """
    st.title("🤖 AI Assistant")
    st.markdown("Ask questions about your customers in plain English")

    st.success("✨ **Powered by Snowflake Cortex Analyst** - Natural language to SQL with AI")

    # ========== SUGGESTED QUESTIONS ==========

    st.subheader("💡 Suggested Questions")

    # Category selector
    selected_category = st.selectbox(
        "Browse by category:",
        list(SUGGESTED_QUESTIONS.keys())
    )

    # Display suggested questions as clickable buttons
    st.markdown(f"**{selected_category}:**")

    cols = st.columns(2)
    for idx, question in enumerate(SUGGESTED_QUESTIONS[selected_category]):
        with cols[idx % 2]:
            if st.button(question, key=f"suggested_{selected_category}_{idx}"):
                st.session_state['current_question'] = question

    st.markdown("---")

    # ========== QUESTION INPUT ==========

    st.subheader("❓ Ask Your Question")

    # Text input for custom question
    default_question = st.session_state.get('current_question', '')

    question = st.text_area(
        "Enter your question:",
        value=default_question,
        height=100,
        placeholder="e.g., Which customers spent more than $10,000 in the last 90 days?"
    )

    col1, col2, col3 = st.columns([1, 1, 4])

    with col1:
        ask_button = st.button("🚀 Ask", type="primary")

    with col2:
        clear_button = st.button("🔄 Clear")

    if clear_button:
        st.session_state['current_question'] = ''
        st.session_state.pop('last_response', None)
        st.rerun()

    # ========== QUERY EXECUTION ==========

    if ask_button and question:
        with st.spinner("🤔 Thinking..."):
            # Get conversation history for context
            conversation_history = st.session_state.get('query_history', [])

            # Call Cortex Analyst with conversation context
            response = call_cortex_analyst(conn, question, conversation_history)

            st.session_state['last_response'] = response
            st.session_state['last_question'] = question

            # Add to history
            if 'query_history' not in st.session_state:
                st.session_state['query_history'] = []

            st.session_state['query_history'].append({
                'timestamp': datetime.now(),
                'question': question,
                'response': response
            })

    # ========== DISPLAY RESULTS ==========

    if 'last_response' in st.session_state:
        response = st.session_state['last_response']
        question = st.session_state.get('last_question', '')

        st.markdown("---")
        st.subheader("📊 Results")

        if response['error']:
            st.error(f"❌ Error: {response['error']}")

            st.info("""
            **Troubleshooting Tips:**
            - Rephrase your question to be more specific
            - Use terms from the semantic model (segment, state, churn risk, etc.)
            - Try one of the suggested questions above
            - Ensure Cortex Analyst is enabled in your Snowflake account
            """)

        else:
            # Display question
            st.markdown(f"**Question:** {question}")

            # Display AI interpretation if available
            if response.get('interpretation'):
                st.info(f"**AI Interpretation:** {response['interpretation']}")

            # Display suggestions from Cortex Analyst
            suggestions = response.get('suggestions', [])
            if suggestions:
                st.markdown("**💡 Follow-up suggestions:**")
                suggestion_cols = st.columns(min(len(suggestions), 3))
                for idx, suggestion in enumerate(suggestions[:6]):  # Limit to 6 suggestions
                    with suggestion_cols[idx % 3]:
                        if st.button(suggestion, key=f"suggestion_{idx}"):
                            st.session_state['current_question'] = suggestion
                            st.rerun()

            # Display generated SQL
            with st.expander("🔍 View Generated SQL", expanded=False):
                st.code(response['sql'], language='sql')

            # Display results
            df = response['results']

            if df is not None and not df.empty:
                st.success(f"✅ Found {len(df)} results")

                # Summary metrics (if applicable)
                if len(df) < 20 and len(df.columns) <= 5:
                    # Display as cards for small result sets
                    cols = st.columns(min(len(df.columns), 4))

                    for idx, col_name in enumerate(df.columns[:4]):
                        with cols[idx]:
                            if pd.api.types.is_numeric_dtype(df[col_name]):
                                value = df[col_name].iloc[0] if len(df) == 1 else df[col_name].sum()
                                formatted_col_name = format_column_name(col_name)
                                # Check if column represents currency using comprehensive keyword matching
                                col_lower = col_name.lower()
                                is_currency = any(keyword in col_lower for keyword in [
                                    'amount', 'amounts',
                                    'value', 'values', 'valued',
                                    'ltv', 'clv',
                                    'spend', 'spending', 'spent', 'spends',
                                    'revenue', 'revenues',
                                    'cost', 'costs', 'costing',
                                    'price', 'prices', 'priced', 'pricing',
                                    'limit', 'limits',
                                    'credit', 'credits',
                                    'paid', 'payment', 'payments',
                                    'balance', 'balances',
                                    'total', 'totals',
                                    'sum', 'sums'
                                ])

                                if is_currency:
                                    st.metric(formatted_col_name, f"${value:,.0f}")
                                else:
                                    st.metric(formatted_col_name, f"{value:,.0f}")

                # View toggle: Table or Chart
                chart_types = suggest_chart_type(df)

                if chart_types:
                    # Show view selector if charts are available
                    view_col1, view_col2 = st.columns([1, 4])

                    with view_col1:
                        view_mode = st.radio(
                            "View Mode:",
                            ["📊 Table", "📈 Chart"],
                            horizontal=True,
                            key="view_mode"
                        )

                    with view_col2:
                        if view_mode == "📈 Chart":
                            # Chart type selector with friendly labels
                            def format_chart_label(chart_type):
                                labels = {
                                    'choropleth_usa': '🗺️ US Map (Choropleth)',
                                    'bar': '📊 Bar Chart',
                                    'line': '📈 Line Chart',
                                    'area': '📉 Area Chart',
                                    'pie': '🥧 Pie Chart',
                                    'scatter': '⚫ Scatter Plot',
                                    'histogram': '📊 Histogram',
                                    'sunburst': '☀️ Sunburst'
                                }
                                return labels.get(chart_type, chart_type.title())

                            chart_type = st.selectbox(
                                "Chart Type:",
                                chart_types,
                                format_func=format_chart_label,
                                key="chart_type_selector"
                            )

                    if view_mode == "📈 Chart":
                        # Render chart
                        render_chart(df, chart_type)
                    else:
                        # Show table
                        display_df = format_dataframe_columns(df.copy())
                        st.dataframe(display_df, use_container_width=True, height=400)
                else:
                    # No charts available, just show table
                    display_df = format_dataframe_columns(df.copy())
                    st.dataframe(display_df, use_container_width=True, height=400)

                # Export with human-readable column names
                csv_df = format_dataframe_columns(df.copy())
                st.download_button(
                    label="📥 Download Results (CSV)",
                    data=csv_df.to_csv(index=False),
                    file_name=f"cortex_analyst_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )

            else:
                st.warning("No results found")

    # ========== QUERY HISTORY ==========

    if st.session_state.get('query_history'):
        st.markdown("---")
        st.subheader("📜 Query History")

        history = st.session_state['query_history']

        # Display last 5 queries
        for idx, item in enumerate(reversed(history[-5:])):
            with st.expander(f"{item['timestamp'].strftime('%H:%M:%S')} - {item['question'][:50]}..."):
                st.markdown(f"**Question:** {item['question']}")

                if item['response']['error']:
                    st.error(f"Error: {item['response']['error']}")
                else:
                    st.code(item['response']['sql'], language='sql')

                    if item['response']['results'] is not None:
                        # Apply human-readable column names to history results
                        history_df = format_dataframe_columns(item['response']['results'].copy())
                        st.dataframe(history_df, use_container_width=True)

    # ========== HELP SECTION ==========

    st.markdown("---")

    with st.expander("ℹ️ How to Use AI Assistant"):
        st.markdown("""
        **Tips for asking questions:**

        1. **Be specific:** Instead of "Show customers", try "Show customers in California with high churn risk"

        2. **Use domain terms:** The AI understands:
           - Customer segments: High-Value Travelers, Declining, New & Growing, Budget-Conscious, Stable Mid-Spenders
           - Churn risk: High Risk, Medium Risk, Low Risk
           - Card types: Standard, Premium
           - Metrics: lifetime value, churn risk score, spend last 90 days

        3. **Time periods:** Specify timeframes like "last 30 days", "last 90 days", "last 6 months"

        4. **Comparisons:** Ask to "compare" segments, states, or time periods

        5. **Filters:** Combine multiple criteria: "Premium cardholders in Texas with declining spend"

        **Powered by Snowflake Cortex Analyst** - Real-time natural language to SQL using AI

        *Note: If Cortex Analyst is not available in your Snowflake account, the system will automatically fallback to a mock implementation with pre-defined queries.*
        """)
